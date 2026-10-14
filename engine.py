"""
HOSTING INDICATOR ENGINE v1.0
Low-Resource Quant Engine | 1.5vCore - 2GB RAM - 2GB Disk

Assets  : BTC/USDT - XAU/USD - USO (Crude Oil ETF)
Signals : EMA crossover - RSI OB/OS - BB squeeze - ATR breakout
Stack   : Python 3.11+ - asyncio - aiohttp (with aiohttp.web Dashboard) - stdlib only

Memory strategy:
  - CircularBuffer fixed 300 candles/asset  -> ~72 KB total RAM
  - No pandas/numpy (saves ~80MB baseline RAM)
  - All indicator functions are pure-math implementations
  - State persist = atomic JSON write every 15 min (<2MB)
  - Log rotating 5MB x 2 files = max 15MB on disk
  - Embedded aiohttp.web Dashboard: 0 external dependencies, <5MB RAM
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import math
import os
import sys
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import Optional

# Force stdout/stderr unbuffered mode immediately for cloud container environments (Botkeep/Docker)
os.environ["PYTHONUNBUFFERED"] = "1"
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True, encoding="utf-8", errors="replace")
except Exception:
    pass

print("[BOOT] Indicator Engine bootstrapping...", flush=True)

import aiohttp  # pip install aiohttp
from aiohttp import web


def _load_dotenv(filepath: str = ".env") -> None:
    """Zero-dependency .env loader using standard library only."""
    if not os.path.exists(filepath):
        return
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass


_load_dotenv()


def _update_dotenv(updates: dict[str, str], filepath: str = ".env") -> None:
    """Safely update or append key-value pairs to .env file."""
    lines: list[str] = []
    keys_updated: set[str] = set()
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except Exception:
            lines = []

    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in updates:
                new_lines.append(f"{k}={updates[k]}\n")
                keys_updated.add(k)
                continue
        new_lines.append(line)

    for k, v in updates.items():
        if k not in keys_updated and v:
            new_lines.append(f"{k}={v}\n")

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
    except Exception as exc:
        print(f"[WARN] Failed to write to {filepath}: {exc}", flush=True)


def _parse_port(default: int = 8080) -> int:
    """Safely parse web port from environment variables, handling empty/invalid strings."""
    raw = os.environ.get("SERVER_PORT") or os.environ.get("PORT")
    if not raw:
        return default
    raw_clean = str(raw).strip().lstrip(":")
    try:
        port = int(raw_clean)
        return port if 1 <= port <= 65535 else default
    except (ValueError, TypeError):
        return default


# =============================================================================
# SECTION 1: CONFIGURATION
# =============================================================================

CONFIG = {
    # ---- Web Dashboard --------------------------------------------------
    # Botkeep assigns port via SERVER_PORT, or standard PORT env var
    "web_host":           os.environ.get("HOST", "0.0.0.0"),
    "web_port":           _parse_port(8080),

    # ---- Telegram -------------------------------------------------------
    # Optional: If not provided or default placeholder, engine runs in Web/Console mode
    "telegram_bot_token": os.environ.get("TG_BOT_TOKEN", "").strip(),
    "telegram_chat_id":   os.environ.get("TG_CHAT_ID",   "").strip(),

    # ---- API Keys -------------------------------------------------------
    # TwelveData free tier: 800 requests/day (~33/hour). Used for XAU & OIL.
    "twelvedata_api_key": os.environ.get("TWELVE_API_KEY", "").strip(),

    # ---- Asset definitions ----------------------------------------------
    "assets": {
        "BTC": {
            "name":       "Bitcoin",
            "source":     "binance",
            "symbol":     "BTCUSDT",
            "timeframes": ["15m", "1h"],
        },
        "XAU": {
            "name":       "Gold",
            "source":     "twelvedata",
            "symbol":     "XAU/USD",
            "timeframes": ["15min", "1h"],
        },
        "OIL": {
            "name":       "Crude Oil (USO)",
            "source":     "twelvedata",
            "symbol":     "USO",
            "timeframes": ["15min", "1h"],
        },
    },

    # ---- Circular buffer ------------------------------------------------
    "buffer_size": 300,          # candles kept in RAM per (asset, timeframe)

    # ---- Indicator parameters -------------------------------------------
    "ema_fast":    20,
    "ema_slow":    50,
    "rsi_period":  14,
    "bb_period":   20,
    "bb_std":      2.0,
    "atr_period":  14,

    # ---- Signal thresholds ----------------------------------------------
    "rsi_overbought":          70.0,
    "rsi_oversold":            30.0,
    "atr_breakout_multiplier": 1.5,   # ATR x1.5 spike = breakout signal

    # ---- Confluence Decision Engine thresholds --------------------------
    # Ngưỡng phân loại EMA Trend (đơn vị: số lần ATR giữa EMA20-EMA50)
    "EMA_TREND_STRONG_ATR_MULT":    0.5,   # > 0.5*ATR => "rõ rệt"
    "EMA_TREND_WEAK_ATR_MULT":      0.15,  # > 0.15*ATR => "nghiêng"

    # Ngưỡng phân loại RSI Zone
    "RSI_EXTREME_OVERSOLD":         25,
    "RSI_LEAN_OVERSOLD":            40,
    "RSI_LEAN_OVERBOUGHT":          60,
    "RSI_EXTREME_OVERBOUGHT":       75,

    # Ngưỡng phân loại vị trí Bollinger Band (% độ rộng dải)
    "BB_EDGE_ZONE_PCT":             0.15,  # 15% sát biên => "chạm biên"

    # Số nến gần nhất để kiểm tra tín hiệu mâu thuẫn
    "CONFLICT_LOOKBACK_CANDLES":    2,

    # Hệ số ATR cho Stop Loss / Take Profit
    "SL_ATR_MULTIPLIER":            1.5,
    "TP1_RR_RATIO":                 1.0,   # R:R 1:1 cho TP1
    "TP2_ATR_MULTIPLIER":           3.0,   # TP2 tham khảo (song song rule trailing theo EMA)

    # Ngưỡng coi là "giá đã gần EMA20, có thể entry ngay" thay vì "chờ pullback"
    "ENTRY_PULLBACK_ATR_THRESHOLD": 0.3,

    # Số nến gần nhất dùng để phát hiện biến động ngược dòng mạnh
    "COUNTER_MOVE_LOOKBACK_CANDLES": 3,
    # Ngưỡng: biến động giá (đơn vị lần ATR) trong N nến gần nhất được coi là "counter-move" đủ mạnh để nghi ngờ xu hướng EMA
    "COUNTER_MOVE_ATR_THRESHOLD":    2.0,

    # ---- Polling timing -------------------------------------------------
    # Poll 5 seconds after candle close for exchange data to settle
    "poll_offset_seconds": 5,

    # ---- State persistence (relative path for container portability) ----
    "state_file":           os.environ.get("STATE_FILE", "./data/state.json"),
    "state_max_bytes":      1_500_000,   # 1.5 MB ceiling before trim
    "state_flush_interval": 900,         # flush every 15 minutes

    # ---- Logging (relative path for container portability) --------------
    "log_file":        os.environ.get("LOG_FILE", "./logs/app.log"),
    "log_max_bytes":    5_242_880,       # 5 MB per file
    "log_backup_count": 2,               # 2 backup files => max 15 MB logs

    # ---- Disk watchdog --------------------------------------------------
    "disk_alert_threshold_pct": 70,      # alert when disk > 70 %
    "disk_check_interval":      21600,   # check every 6 hours
}

# Timeframe identifier -> seconds
TF_SECONDS: dict[str, int] = {
    "15m":   900,
    "15min": 900,
    "1h":    3600,
    "4h":    14400,
}


# =============================================================================
# SECTION 2: CIRCULAR BUFFER
# =============================================================================

@dataclass
class Candle:
    """
    Single OHLCV bar.
    Python object overhead ~80 bytes.
    300 candles x 80 B = 24 KB per buffer slot.
    """
    ts:     int    # Unix timestamp (candle close, seconds)
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float = 0.0


class CircularBuffer:
    """
    Fixed-capacity FIFO buffer backed by collections.deque(maxlen=N).
    push() is O(1). Oldest elements are dropped automatically.
    RAM usage is strictly bounded.
    """

    def __init__(self, maxlen: int, asset: str, timeframe: str) -> None:
        self._buf: deque[Candle] = deque(maxlen=maxlen)
        self.asset     = asset
        self.timeframe = timeframe

    def push(self, candle: Candle) -> None:
        self._buf.append(candle)

    def is_full(self) -> bool:
        return len(self._buf) == self._buf.maxlen

    def __len__(self) -> int:
        return len(self._buf)

    @property
    def closes(self) -> list[float]:
        return [c.close for c in self._buf]

    @property
    def highs(self) -> list[float]:
        return [c.high for c in self._buf]

    @property
    def lows(self) -> list[float]:
        return [c.low for c in self._buf]

    @property
    def latest(self) -> Optional[Candle]:
        return self._buf[-1] if self._buf else None

    def last_n_candles(self, n: int) -> list[Candle]:
        return list(self._buf)[-n:]


# =============================================================================
# SECTION 3: INDICATOR ENGINE  (pure-math, zero external dependencies)
# =============================================================================

class IndicatorEngine:
    """
    All technical indicators implemented with Python built-ins.
    Zero NumPy / Pandas dependencies saves ~80 MB baseline RAM.
    """

    # --------------------------------------------------------------------- EMA
    @staticmethod
    def ema(values: list[float], period: int) -> Optional[float]:
        """
        Exponential Moving Average.
        Seed: SMA of first `period` values.
        Multiplier k = 2 / (period + 1).
        """
        if len(values) < period:
            return None
        k   = 2.0 / (period + 1)
        val = sum(values[:period]) / period
        for p in values[period:]:
            val = p * k + val * (1.0 - k)
        return val

    # --------------------------------------------------------------------- RSI
    @staticmethod
    def rsi(closes: list[float], period: int = 14) -> Optional[float]:
        """
        Relative Strength Index using Wilder's smoothing technique.
        RSI = 100 - (100 / (1 + RS))
        """
        if len(closes) < period + 1:
            return None
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains  = [max(d, 0.0)  for d in deltas]
        losses = [max(-d, 0.0) for d in deltas]
        avg_gain = sum(gains[:period])  / period
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i])  / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0.0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # --------------------------------------------------------- BOLLINGER BANDS
    @staticmethod
    def bollinger_bands(
        closes: list[float],
        period: int = 20,
        num_std: float = 2.0,
    ) -> Optional[tuple[float, float, float]]:
        """
        Bollinger Bands: returns (upper, middle, lower).
        middle = SMA(closes, period)
        upper  = middle + num_std * population_std
        lower  = middle - num_std * population_std
        """
        if len(closes) < period:
            return None
        window = closes[-period:]
        mid    = sum(window) / period
        var    = sum((x - mid) ** 2 for x in window) / period
        sd     = math.sqrt(var)
        return (mid + num_std * sd, mid, mid - num_std * sd)

    # --------------------------------------------------------------------- ATR
    @staticmethod
    def atr(candles: list[Candle], period: int = 14) -> Optional[float]:
        """
        Average True Range using Wilder's smoothing.
        TR = max(H - L, |H - C_prev|, |L - C_prev|)
        """
        if len(candles) < period + 1:
            return None
        trs: list[float] = []
        for i in range(1, len(candles)):
            c      = candles[i]
            prev_c = candles[i - 1]
            tr = max(
                c.high - c.low,
                abs(c.high - prev_c.close),
                abs(c.low  - prev_c.close),
            )
            trs.append(tr)
        if len(trs) < period:
            return None
        atr_val = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr_val = (atr_val * (period - 1) + tr) / period
        return atr_val

    # ------------------------------------------------------------------
    @classmethod
    def compute_all(cls, buf: CircularBuffer) -> dict:
        """Compute every indicator for one CircularBuffer. Returns a result dict."""
        candles = buf.last_n_candles(200)   # last 200 bars is enough
        closes  = [c.close for c in candles]
        cfg     = CONFIG
        result: dict = {
            "ts":        int(time.time()),
            "asset":     buf.asset,
            "timeframe": buf.timeframe,
            "close":     closes[-1] if closes else None,
            "ema_fast":  cls.ema(closes, cfg["ema_fast"]),
            "ema_slow":  cls.ema(closes, cfg["ema_slow"]),
            "rsi":       cls.rsi(closes, cfg["rsi_period"]),
            "atr":       cls.atr(candles, cfg["atr_period"]),
        }
        bb = cls.bollinger_bands(closes, cfg["bb_period"], cfg["bb_std"])
        if bb:
            result["bb_upper"], result["bb_mid"], result["bb_lower"] = bb
        else:
            result["bb_upper"] = result["bb_mid"] = result["bb_lower"] = None
        return result


# =============================================================================
# SECTION 4: SIGNAL RULE ENGINE
# =============================================================================

@dataclass
class Signal:
    asset:       str
    timeframe:   str
    signal_type: str   # e.g. "EMA_CROSS_UP", "RSI_OVERBOUGHT", ...
    message:     str   # human-readable text
    price:       float
    ts:          int = field(default_factory=lambda: int(time.time()))

    def to_dict(self) -> dict:
        return {
            "asset": self.asset,
            "timeframe": self.timeframe,
            "signal_type": self.signal_type,
            "message": self.message,
            "price": self.price,
            "ts": self.ts,
            "time_str": datetime.datetime.fromtimestamp(self.ts, datetime.timezone.utc).strftime("%H:%M:%S UTC"),
        }


class RuleEngine:
    """
    Compares current indicator snapshot to previous to detect state transitions.
    Each call to evaluate() may produce zero or more Signal objects.
    """

    def __init__(self) -> None:
        # prev_state[asset][timeframe] = last indicator dict
        self._prev: dict[str, dict[str, dict]] = {}

    def evaluate(self, curr: dict) -> list[Signal]:
        signals: list[Signal] = []
        asset = curr["asset"]
        tf    = curr["timeframe"]
        prev  = self._prev.get(asset, {}).get(tf, {})
        cfg   = CONFIG

        # ------ 1. EMA Golden / Death Cross ----------------------------------
        ef_now, es_now   = curr.get("ema_fast"), curr.get("ema_slow")
        ef_prev, es_prev = prev.get("ema_fast"), prev.get("ema_slow")

        if all(v is not None for v in (ef_now, es_now, ef_prev, es_prev)):
            if ef_prev <= es_prev and ef_now > es_now:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="EMA_CROSS_UP",
                    message=(
                        f"*[GOLDEN CROSS] {asset} {tf}*\n"
                        f"EMA{cfg['ema_fast']}={ef_now:.2f} crossed above "
                        f"EMA{cfg['ema_slow']}={es_now:.2f}\n"
                        f"Price: `{curr['close']:.4f}`"
                    ),
                    price=curr["close"],
                ))
            elif ef_prev >= es_prev and ef_now < es_now:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="EMA_CROSS_DOWN",
                    message=(
                        f"*[DEATH CROSS] {asset} {tf}*\n"
                        f"EMA{cfg['ema_fast']}={ef_now:.2f} crossed below "
                        f"EMA{cfg['ema_slow']}={es_now:.2f}\n"
                        f"Price: `{curr['close']:.4f}`"
                    ),
                    price=curr["close"],
                ))

        # ------ 2. RSI Overbought / Oversold ---------------------------------
        rsi_now, rsi_prev = curr.get("rsi"), prev.get("rsi")
        if rsi_now is not None and rsi_prev is not None:
            ob, oss = cfg["rsi_overbought"], cfg["rsi_oversold"]
            if rsi_prev < ob <= rsi_now:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="RSI_OVERBOUGHT",
                    message=(
                        f"*[RSI OVERBOUGHT] {asset} {tf}*\n"
                        f"RSI=`{rsi_now:.1f}` > {ob}\n"
                        f"Price: `{curr['close']:.4f}` -- Consider closing long"
                    ),
                    price=curr["close"],
                ))
            elif rsi_prev > oss >= rsi_now:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="RSI_OVERSOLD",
                    message=(
                        f"*[RSI OVERSOLD] {asset} {tf}*\n"
                        f"RSI=`{rsi_now:.1f}` < {oss}\n"
                        f"Price: `{curr['close']:.4f}` -- Consider buying"
                    ),
                    price=curr["close"],
                ))

        # ------ 3. Bollinger Band Breakout -----------------------------------
        close    = curr.get("close")
        bb_upper = curr.get("bb_upper")
        bb_lower = curr.get("bb_lower")
        if all(v is not None for v in (close, bb_upper, bb_lower)):
            if close > bb_upper:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="BB_BREAKOUT_UP",
                    message=(
                        f"*[BB BREAKOUT UP] {asset} {tf}*\n"
                        f"Price `{close:.4f}` > BB Upper `{bb_upper:.4f}`"
                    ),
                    price=close,
                ))
            elif close < bb_lower:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="BB_BREAKOUT_DOWN",
                    message=(
                        f"*[BB BREAKOUT DOWN] {asset} {tf}*\n"
                        f"Price `{close:.4f}` < BB Lower `{bb_lower:.4f}`"
                    ),
                    price=close,
                ))

        # ------ 4. ATR Volatility Spike --------------------------------------
        atr_now, atr_prev = curr.get("atr"), prev.get("atr")
        if atr_now is not None and atr_prev and atr_prev > 0:
            mult = cfg["atr_breakout_multiplier"]
            if atr_now > atr_prev * mult:
                signals.append(Signal(
                    asset=asset, timeframe=tf,
                    signal_type="ATR_SPIKE",
                    message=(
                        f"*[ATR SPIKE] {asset} {tf}*\n"
                        f"ATR=`{atr_now:.4f}` > {mult}x prev ATR=`{atr_prev:.4f}`\n"
                        f"Extreme volatility -- exercise caution!"
                    ),
                    price=curr["close"],
                ))

        # Persist current as previous for next evaluation
        self._prev.setdefault(asset, {})[tf] = curr
        return signals


# =============================================================================
# SECTION 4.5: CONFLUENCE DECISION ENGINE
# =============================================================================

from enum import Enum


class TrendState(str, Enum):
    STRONG_UP = "Tăng rõ rệt"
    LEAN_UP = "Nghiêng tăng"
    NEUTRAL = "Cân bằng"
    LEAN_DOWN = "Nghiêng giảm"
    STRONG_DOWN = "Giảm rõ rệt"


class RsiZone(str, Enum):
    EXTREME_OVERSOLD = "Quá bán sâu"
    LEAN_OVERSOLD = "Nghiêng bán"
    NEUTRAL = "Cân bằng"
    LEAN_OVERBOUGHT = "Nghiêng mua"
    EXTREME_OVERBOUGHT = "Quá mua sâu"


class BbPosition(str, Enum):
    BREAK_UPPER = "Vượt đỉnh"
    NEAR_UPPER = "Chạm biên trên"
    MIDDLE = "Trong dải"
    NEAR_LOWER = "Chạm biên dưới"
    BREAK_LOWER = "Thủng đáy"


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"


class Confidence(str, Enum):
    HIGH = "Cao"
    MEDIUM = "Trung bình"
    LOW = "Thấp"


@dataclass
class TradeAdvice:
    action: str                    # BUY / SELL / WAIT
    confidence: str                # Cao / Trung bình / Thấp
    confidence_score: int          # 0-5, số yếu tố đồng thuận
    entry_type: str                # "now" / "wait_pullback" / "none"
    entry_price: Optional[float]
    entry_zone: Optional[float]    # giá EMA20 (vùng chờ pullback)
    stop_loss: Optional[float]
    take_profit_1: Optional[float]
    take_profit_2: Optional[float]
    tp2_rule: str                  # mô tả quy tắc trailing bằng text
    reasoning: list[str]           # checklist lý do (để hiển thị UI)
    conflict_detected: bool
    mtf_agreement: Optional[bool]  # None nếu không có dữ liệu khung kia
    counter_move_detected: bool = False
    counter_move_magnitude_atr: float = 0.0
    disclaimer: str = (
        "Đây là khuyến nghị dựa trên thuật toán kỹ thuật, "
        "KHÔNG PHẢI lời khuyên đầu tư. Người dùng tự chịu "
        "trách nhiệm với quyết định giao dịch của mình."
    )

    def to_dict(self) -> dict:
        return {
            "action": self.action,
            "confidence": self.confidence,
            "confidence_score": self.confidence_score,
            "entry_type": self.entry_type,
            "entry_price": self.entry_price,
            "entry_zone": self.entry_zone,
            "stop_loss": self.stop_loss,
            "take_profit_1": self.take_profit_1,
            "take_profit_2": self.take_profit_2,
            "tp2_rule": self.tp2_rule,
            "reasoning": self.reasoning,
            "conflict_detected": self.conflict_detected,
            "mtf_agreement": self.mtf_agreement,
            "counter_move_detected": self.counter_move_detected,
            "counter_move_magnitude_atr": round(self.counter_move_magnitude_atr, 4),
            "disclaimer": self.disclaimer,
        }


TIMEFRAME_ORDER = {"15m": 1, "15min": 1, "1h": 2}


class ConfluenceDecisionEngine:
    """
    Đọc snapshot hiện tại từ IndicatorEngine + lịch sử tín hiệu từ 
    RuleEngine để sinh ra khuyến nghị BUY/SELL/WAIT kèm Entry/SL/TP.
    
    Phạm vi: CHỈ phục vụ hiển thị UI qua /api/snapshot. 
    KHÔNG dispatch qua bất kỳ kênh thông báo nào (Telegram, email...).
    
    Nguyên tắc: Pure function, KHÔNG giữ state nội bộ ngoài việc 
    đọc input truyền vào. Được gọi lại mỗi khi có nến mới đóng, 
    KHÔNG gọi lại mỗi request HTTP.
    """

    def __init__(self, config: dict) -> None:
        self.cfg = config

    def _get_relative_tf_label(self, current_tf: str, other_tf: Optional[str]) -> str:
        """Trả về mô tả tương đối chính xác về mặt ngữ nghĩa, không giả định cố định 'khung lớn' hay 'khung nhỏ'."""
        if not other_tf:
            return "khung thời gian khác"
        current_rank = TIMEFRAME_ORDER.get(current_tf, 0)
        other_rank = TIMEFRAME_ORDER.get(other_tf, 0)
        other_tf_display = {"15m": "15 phút", "15min": "15 phút", "1h": "1 giờ"}.get(other_tf, other_tf)
        if other_rank > current_rank:
            return f"khung {other_tf_display} (dài hạn hơn)"
        elif other_rank < current_rank:
            return f"khung {other_tf_display} (ngắn hạn hơn)"
        else:
            return f"khung {other_tf_display}"

    def detect_counter_move(
        self, recent_candles: Optional[list], atr: float, trend: TrendState
    ) -> tuple[bool, float]:
        """
        Phát hiện biến động giá ngược chiều với xu hướng EMA hiện tại trong N nến gần nhất,
        kể cả khi không có Signal rời rạc nào được RuleEngine kích hoạt.
        Trả về: (có_phát_hiện: bool, độ_lớn_theo_ATR: float)
        """
        n = self.cfg.get("COUNTER_MOVE_LOOKBACK_CANDLES", 3)
        if not recent_candles or len(recent_candles) < n or atr <= 0:
            return False, 0.0

        window = recent_candles[-n:]
        c_first = getattr(window[0], "close", None) if not isinstance(window[0], dict) else window[0].get("close")
        c_last = getattr(window[-1], "close", None) if not isinstance(window[-1], dict) else window[-1].get("close")
        if c_first is None or c_last is None:
            return False, 0.0

        price_change = float(c_last) - float(c_first)
        change_in_atr = price_change / atr
        threshold = self.cfg.get("COUNTER_MOVE_ATR_THRESHOLD", 2.0)

        trend_is_down = trend in (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN)
        trend_is_up = trend in (TrendState.STRONG_UP, TrendState.LEAN_UP)

        # Counter-move = giá tăng mạnh NGƯỢC xu hướng giảm, hoặc ngược lại
        if trend_is_down and change_in_atr > threshold:
            return True, change_in_atr
        if trend_is_up and change_in_atr < -threshold:
            return True, change_in_atr
        return False, change_in_atr

    def classify_ema_trend(self, ema_fast: float, ema_slow: float, atr: float) -> TrendState:
        if atr <= 0:
            return TrendState.NEUTRAL
        diff_in_atr = (ema_fast - ema_slow) / atr
        s = self.cfg["EMA_TREND_STRONG_ATR_MULT"]
        w = self.cfg["EMA_TREND_WEAK_ATR_MULT"]
        if diff_in_atr > s:
            return TrendState.STRONG_UP
        if diff_in_atr > w:
            return TrendState.LEAN_UP
        if diff_in_atr < -s:
            return TrendState.STRONG_DOWN
        if diff_in_atr < -w:
            return TrendState.LEAN_DOWN
        return TrendState.NEUTRAL

    def classify_rsi(self, rsi: float) -> RsiZone:
        c = self.cfg
        if rsi <= c["RSI_EXTREME_OVERSOLD"]:
            return RsiZone.EXTREME_OVERSOLD
        if rsi <= c["RSI_LEAN_OVERSOLD"]:
            return RsiZone.LEAN_OVERSOLD
        if rsi >= c["RSI_EXTREME_OVERBOUGHT"]:
            return RsiZone.EXTREME_OVERBOUGHT
        if rsi >= c["RSI_LEAN_OVERBOUGHT"]:
            return RsiZone.LEAN_OVERBOUGHT
        return RsiZone.NEUTRAL

    def classify_bb_position(self, close: float, bb_upper: float, bb_lower: float) -> BbPosition:
        width = bb_upper - bb_lower
        if width <= 0:
            return BbPosition.MIDDLE
        edge = width * self.cfg["BB_EDGE_ZONE_PCT"]
        if close > bb_upper:
            return BbPosition.BREAK_UPPER
        if close < bb_lower:
            return BbPosition.BREAK_LOWER
        if close > bb_upper - edge:
            return BbPosition.NEAR_UPPER
        if close < bb_lower + edge:
            return BbPosition.NEAR_LOWER
        return BbPosition.MIDDLE

    def detect_conflict(self, recent_signals: list, asset: str, timeframe: str, trend: TrendState) -> bool:
        """Kiểm tra N tín hiệu gần nhất của asset/timeframe này có loại NGƯỢC CHIỀU với xu hướng EMA hiện tại không."""
        lookback = self.cfg["CONFLICT_LOOKBACK_CANDLES"]
        bullish_types = {"EMA_CROSS_UP", "BB_BREAKOUT_UP", "RSI_OVERSOLD"}
        bearish_types = {"EMA_CROSS_DOWN", "BB_BREAKOUT_DOWN", "RSI_OVERBOUGHT"}

        relevant = [
            s for s in recent_signals
            if (s.get("asset") if isinstance(s, dict) else getattr(s, "asset", None)) == asset
            and (s.get("timeframe") if isinstance(s, dict) else getattr(s, "timeframe", None)) == timeframe
        ]
        relevant.sort(key=lambda x: x.get("ts", 0) if isinstance(x, dict) else getattr(x, "ts", 0))
        recent_window = relevant[-lookback:]

        trend_is_up = trend in (TrendState.STRONG_UP, TrendState.LEAN_UP)
        trend_is_down = trend in (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN)

        for s in recent_window:
            sig_type = s.get("signal_type") if isinstance(s, dict) else getattr(s, "signal_type", None)
            if trend_is_up and sig_type in bearish_types:
                return True
            if trend_is_down and sig_type in bullish_types:
                return True
        return False

    def check_mtf_agreement(self, trend_current_tf: TrendState, trend_other_tf: Optional[TrendState]) -> Optional[bool]:
        if trend_other_tf is None:
            return None
        up_states = (TrendState.STRONG_UP, TrendState.LEAN_UP)
        down_states = (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN)
        if trend_current_tf in up_states and trend_other_tf in up_states:
            return True
        if trend_current_tf in down_states and trend_other_tf in down_states:
            return True
        return False

    def decide(
        self,
        asset: str,
        timeframe: str,
        snapshot: dict,
        recent_signals: list,
        recent_candles: Optional[list] = None,
        other_tf_snapshot: Optional[dict] = None,
        other_timeframe: Optional[str] = None,
    ) -> TradeAdvice:
        """
        snapshot: dict với các key close, ema_fast, ema_slow, rsi, bb_upper, bb_mid, bb_lower, atr.
        other_tf_snapshot: snapshot của timeframe còn lại CÙNG asset.
        """
        if (
            not snapshot
            or snapshot.get("close") is None
            or snapshot.get("atr") is None
            or snapshot.get("ema_fast") is None
            or snapshot.get("ema_slow") is None
            or snapshot.get("rsi") is None
            or snapshot.get("bb_upper") is None
            or snapshot.get("bb_lower") is None
        ):
            return TradeAdvice(
                action=Action.WAIT.value,
                confidence=Confidence.LOW.value,
                confidence_score=0,
                entry_type="none",
                entry_price=None,
                entry_zone=None,
                stop_loss=None,
                take_profit_1=None,
                take_profit_2=None,
                tp2_rule="",
                reasoning=["Đang thu thập đủ dữ liệu nến để tính toán chỉ báo..."],
                conflict_detected=False,
                mtf_agreement=None,
                counter_move_detected=False,
                counter_move_magnitude_atr=0.0,
            )

        close = snapshot["close"]
        atr = snapshot["atr"]
        trend = self.classify_ema_trend(snapshot["ema_fast"], snapshot["ema_slow"], atr)
        rsi_zone = self.classify_rsi(snapshot["rsi"])
        bb_pos = self.classify_bb_position(close, snapshot["bb_upper"], snapshot["bb_lower"])
        
        conflict_from_signals = self.detect_conflict(recent_signals, asset, timeframe, trend)
        counter_move_detected, counter_move_magnitude = self.detect_counter_move(recent_candles, atr, trend)
        conflict = conflict_from_signals or counter_move_detected

        trend_other = None
        if other_tf_snapshot and all(
            other_tf_snapshot.get(k) is not None for k in ("ema_fast", "ema_slow", "atr")
        ):
            trend_other = self.classify_ema_trend(
                other_tf_snapshot["ema_fast"],
                other_tf_snapshot["ema_slow"],
                other_tf_snapshot["atr"],
            )
        mtf_agree = self.check_mtf_agreement(trend, trend_other)

        reasoning: list[str] = []
        score = 0

        # 1. EMA Trend factor
        if trend in (TrendState.STRONG_UP, TrendState.LEAN_UP):
            score += 2 if trend == TrendState.STRONG_UP else 1
            reasoning.append(f"Xu hướng EMA: {trend.value} (ủng hộ MUA)")
        elif trend in (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN):
            score -= 2 if trend == TrendState.STRONG_DOWN else 1
            reasoning.append(f"Xu hướng EMA: {trend.value} (ủng hộ BÁN)")
        else:
            reasoning.append(f"Xu hướng EMA: {trend.value} (trung lập)")

        # 2. RSI Zone factor
        if rsi_zone == RsiZone.EXTREME_OVERBOUGHT:
            score -= 1
            reasoning.append("RSI quá mua sâu (cảnh báo rủi ro đảo chiều)")
        elif rsi_zone == RsiZone.EXTREME_OVERSOLD:
            score += 1
            reasoning.append("RSI quá bán sâu (cảnh báo rủi ro đảo chiều)")
        elif rsi_zone == RsiZone.LEAN_OVERBOUGHT:
            score += 1
            reasoning.append("RSI nghiêng mua, còn dư địa tăng")
        elif rsi_zone == RsiZone.LEAN_OVERSOLD:
            score -= 1
            reasoning.append("RSI nghiêng bán, còn dư địa giảm")
        else:
            reasoning.append("RSI cân bằng")

        # 3. Bollinger Band factor
        if bb_pos == BbPosition.BREAK_UPPER:
            reasoning.append("Giá vượt đỉnh dải Bollinger (biến động mạnh, cẩn trọng mua đuổi)")
        elif bb_pos == BbPosition.BREAK_LOWER:
            reasoning.append("Giá thủng đáy dải Bollinger (biến động mạnh, cẩn trọng bán đuổi)")
        elif bb_pos == BbPosition.NEAR_UPPER:
            score += 1
            reasoning.append("Giá áp sát biên trên (ủng hộ xu hướng tăng)")
        elif bb_pos == BbPosition.NEAR_LOWER:
            score -= 1
            reasoning.append("Giá áp sát biên dưới (ủng hộ xu hướng giảm)")
        else:
            reasoning.append("Giá trong vùng dao động bình thường")

        # 4. Conflict detection factor
        if counter_move_detected:
            direction = "tăng" if counter_move_magnitude > 0 else "giảm"
            reasoning.append(
                f"⚠️ Phát hiện biến động giá {direction} mạnh bất thường "
                f"trong {self.cfg.get('COUNTER_MOVE_LOOKBACK_CANDLES', 3)} nến gần nhất "
                f"(~{abs(counter_move_magnitude):.1f}x ATR) — ngược chiều với "
                f"xu hướng EMA hiện tại, cần thận trọng"
            )
        elif conflict_from_signals:
            reasoning.append("⚠️ Phát hiện tín hiệu MÂU THUẪN gần đây trên chart — cần thận trọng")
        else:
            reasoning.append("Không có tín hiệu mâu thuẫn gần đây")

        # 5. Multi-timeframe agreement factor
        if mtf_agree is not None:
            tf_label = self._get_relative_tf_label(timeframe, other_timeframe)
            if mtf_agree:
                score += 1 if score > 0 else (-1 if score < 0 else 0)
                reasoning.append(f"{tf_label} cũng ĐỒNG THUẬN xu hướng")
            else:
                reasoning.append(f"⚠️ {tf_label} KHÔNG đồng thuận xu hướng")

        # Decision Matrix evaluation
        is_sideway = (trend == TrendState.NEUTRAL and bb_pos == BbPosition.MIDDLE)
        is_extreme_overextended = (
            (bb_pos == BbPosition.BREAK_LOWER and rsi_zone != RsiZone.EXTREME_OVERSOLD and score <= -3)
            or (bb_pos == BbPosition.BREAK_UPPER and rsi_zone != RsiZone.EXTREME_OVERBOUGHT and score >= 3)
        )

        if conflict or is_sideway:
            action = Action.WAIT
        elif is_extreme_overextended:
            action = Action.WAIT
        elif score >= 2:
            action = Action.BUY
        elif score <= -2:
            action = Action.SELL
        else:
            action = Action.WAIT

        if mtf_agree is False:
            confidence_score = max(0, abs(score) - 1)
        else:
            confidence_score = abs(score)

        if confidence_score >= 4:
            confidence = Confidence.HIGH
        elif confidence_score >= 2:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        entry_type, entry_price, entry_zone = "none", None, None
        sl = tp1 = tp2 = None
        tp2_rule = ""

        if action in (Action.BUY, Action.SELL):
            ema20 = snapshot["ema_fast"]
            distance_atr = abs(close - ema20) / atr if atr > 0 else 0
            pullback_threshold = self.cfg["ENTRY_PULLBACK_ATR_THRESHOLD"]

            if distance_atr <= pullback_threshold:
                entry_type = "now"
                entry_price = close
            else:
                entry_type = "wait_pullback"
                entry_zone = ema20
                entry_price = None

            ref_price = entry_price if entry_price is not None else ema20
            sl_mult = self.cfg["SL_ATR_MULTIPLIER"]
            rr = self.cfg["TP1_RR_RATIO"]
            tp2_mult = self.cfg["TP2_ATR_MULTIPLIER"]

            if action == Action.BUY:
                sl = ref_price - sl_mult * atr
                tp1 = ref_price + sl_mult * atr * rr
                tp2 = ref_price + tp2_mult * atr
                tp2_rule = "Giữ lệnh đến khi Xu hướng EMA chuyển từ Tăng sang Cân bằng/Giảm"
            else:
                sl = ref_price + sl_mult * atr
                tp1 = ref_price - sl_mult * atr * rr
                tp2 = ref_price - tp2_mult * atr
                tp2_rule = "Giữ lệnh đến khi Xu hướng EMA chuyển từ Giảm sang Cân bằng/Tăng"

        return TradeAdvice(
            action=action.value,
            confidence=confidence.value,
            confidence_score=confidence_score,
            entry_type=entry_type,
            entry_price=round(entry_price, 4) if entry_price is not None else None,
            entry_zone=round(entry_zone, 4) if entry_zone is not None else None,
            stop_loss=round(sl, 4) if sl is not None else None,
            take_profit_1=round(tp1, 4) if tp1 is not None else None,
            take_profit_2=round(tp2, 4) if tp2 is not None else None,
            tp2_rule=tp2_rule,
            reasoning=reasoning,
            conflict_detected=conflict,
            mtf_agreement=mtf_agree,
            counter_move_detected=counter_move_detected,
            counter_move_magnitude_atr=round(counter_move_magnitude, 4),
        )


# =============================================================================
# SECTION 5: TELEGRAM DISPATCHER (with graceful offline / web fallback)
# =============================================================================

class TelegramDispatcher:
    """
    Async Telegram message sender.
    Reuses one aiohttp session (persistent keep-alive connection).
    Retries up to 3 times with exponential backoff on failure.
    If no valid token is provided, gracefully switches to Web & Console mode.
    """

    _API_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, token: str, chat_id: str) -> None:
        token_str = (token or "").strip()
        chat_id_str = (chat_id or "").strip()
        self._token   = token_str
        self._chat_id = chat_id_str
        self._session: Optional[aiohttp.ClientSession] = None
        self._is_enabled = bool(
            token_str
            and not token_str.startswith("YOUR_")
            and not token_str.startswith("your_")
            and chat_id_str
            and not chat_id_str.startswith("YOUR_")
            and not chat_id_str.startswith("your_")
        )
        if self._is_enabled:
            logger.info("Telegram notification is ENABLED.")
        else:
            logger.info("Telegram bot not configured -> Running in Web UI & Console mode.")

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=10),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def send(self, text: str, retries: int = 3) -> bool:
        """Send a Markdown message. Returns True on success."""
        if not self._is_enabled:
            return True
        url     = self._API_URL.format(token=self._token)
        payload = {"chat_id": self._chat_id, "text": text, "parse_mode": "Markdown"}
        for attempt in range(retries):
            try:
                session = await self._session_get()
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return True
                    logger.warning(f"Telegram HTTP {resp.status}: {(await resp.text())[:200]}")
            except Exception as exc:
                logger.warning(f"Telegram attempt {attempt + 1} failed: {exc}")
            await asyncio.sleep(2 ** attempt)   # backoff: 1s, 2s, 4s
        return False

    async def dispatch_signal(self, sig: Signal) -> None:
        if not self._is_enabled:
            logger.info(f">> [SIGNAL DETECTED] [{sig.asset} {sig.timeframe}] {sig.signal_type} @ {sig.price:.4f}")
            return
        ok = await self.send(sig.message)
        logger.info(f"[SIGNAL {'SENT' if ok else 'FAIL'}] {sig.asset} {sig.timeframe} {sig.signal_type}")

    def update_config(self, token: str, chat_id: str) -> None:
        token_str = (token or "").strip()
        chat_id_str = (chat_id or "").strip()
        self._token   = token_str
        self._chat_id = chat_id_str
        self._is_enabled = bool(
            token_str
            and not token_str.startswith("YOUR_")
            and not token_str.startswith("your_")
            and chat_id_str
            and not chat_id_str.startswith("YOUR_")
            and not chat_id_str.startswith("your_")
        )
        if self._is_enabled:
            logger.info("Telegram notification ENABLED dynamically via UI!")
        else:
            logger.info("Telegram bot disabled or incomplete token.")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# =============================================================================
# SECTION 6: DATA FETCHERS
# =============================================================================

class BinanceFetcher:
    """
    Public Binance REST API -- no API key required.
    GET /api/v3/klines returns the current (open) candle as the last element;
    we always discard it to ensure we only process fully closed bars.
    """

    _BASE = "https://api.binance.com"

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def fetch_candles(
        self, symbol: str, interval: str, limit: int = 300
    ) -> list[Candle]:
        url    = f"{self._BASE}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit + 1}
        try:
            async with self._session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Binance {symbol}/{interval}: HTTP {resp.status}")
                    return []
                rows = await resp.json(content_type=None)
                return [
                    Candle(
                        ts=int(r[6]) // 1000,   # close-time ms -> s
                        open=float(r[1]), high=float(r[2]),
                        low=float(r[3]),  close=float(r[4]),
                        volume=float(r[5]),
                    )
                    for r in rows[:-1]   # drop last unclosed bar
                ]
        except Exception as exc:
            logger.error(f"BinanceFetcher ({symbol}): {exc}")
            return []


class TwelveDataFetcher:
    """
    TwelveData REST API -- free tier: 800 req/day, 8 req/min.
    Used for XAU/USD (Gold) and USO (Crude Oil ETF).
    Built-in throttle enforces >= 8 seconds between requests.
    """

    _BASE = "https://api.twelvedata.com"

    def __init__(self, session: aiohttp.ClientSession, api_key: str) -> None:
        self._session      = session
        self._api_key      = (api_key or "").strip()
        self._last_call    = 0.0
        self._min_gap      = 8.0   # seconds -- stays within 8 req/min
        self.is_configured = bool(
            self._api_key
            and not self._api_key.startswith("YOUR_")
            and not self._api_key.startswith("your_")
        )
        if not self.is_configured:
            logger.info("TwelveData API key not set -> Skipping Gold & Oil requests (Zero-ENV standalone mode).")

    def update_key(self, new_key: str) -> None:
        clean = (new_key or "").strip()
        self._api_key = clean
        self.is_configured = bool(
            clean
            and not clean.startswith("YOUR_")
            and not clean.startswith("your_")
        )
        if self.is_configured:
            logger.info("TwelveData API key configured dynamically via Web UI!")

    async def fetch_candles(
        self, symbol: str, interval: str, limit: int = 300
    ) -> list[Candle]:
        if not self.is_configured:
            return []
        # Throttle: wait until min_gap has elapsed since last request
        gap = time.time() - self._last_call
        if gap < self._min_gap:
            await asyncio.sleep(self._min_gap - gap)
        url    = f"{self._BASE}/time_series"
        params = {
            "symbol":     symbol,
            "interval":   interval,
            "outputsize": limit,
            "apikey":     self._api_key,
            "format":     "JSON",
            "order":      "ASC",   # oldest -> newest
        }
        try:
            async with self._session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                self._last_call = time.time()
                if resp.status != 200:
                    logger.warning(f"TwelveData {symbol}/{interval}: HTTP {resp.status}")
                    return []
                data = await resp.json(content_type=None)
                if data.get("status") == "error":
                    logger.error(f"TwelveData API error: {data.get('message')}")
                    return []
                candles: list[Candle] = []
                for row in data.get("values", []):
                    try:
                        dt = datetime.datetime.fromisoformat(row["datetime"])
                        candles.append(Candle(
                            ts=int(dt.timestamp()),
                            open=float(row["open"]),   high=float(row["high"]),
                            low=float(row["low"]),     close=float(row["close"]),
                            volume=float(row.get("volume", 0)),
                        ))
                    except (KeyError, ValueError) as exc:
                        logger.debug(f"TwelveData row parse skip: {exc}")
                    except Exception as exc:
                        logger.debug(f"TwelveData row error: {exc}")
                return candles
        except Exception as exc:
            logger.error(f"TwelveDataFetcher ({symbol}): {exc}")
            return []


# =============================================================================
# SECTION 7: STATE PERSISTENCE  (atomic JSON write)
# =============================================================================

class StatePersistence:
    """
    Persists the last-known indicator snapshot as a JSON file.
    Uses write-to-temp + os.replace for atomicity on POSIX.
    Enforces a byte-size ceiling to prevent disk bloat.
    """

    def __init__(self, filepath: str, max_bytes: int) -> None:
        self._path      = filepath
        self._max_bytes = max_bytes
        dir_name = os.path.dirname(filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

    def save(self, state: dict) -> None:
        try:
            payload   = json.dumps(state, indent=2)
            byte_size = len(payload.encode())
            if byte_size > self._max_bytes:
                logger.warning(f"State {byte_size}B > {self._max_bytes}B ceiling -- trimming")
                payload = json.dumps(self._trim(state), indent=2)
            dir_path = os.path.dirname(self._path) or "."
            fd, tmp  = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(payload)
                os.replace(tmp, self._path)   # atomic on POSIX
            except Exception:
                if os.path.exists(tmp):
                    os.unlink(tmp)
                raise
            logger.debug(f"State saved {len(payload)}B -> {self._path}")
        except Exception as exc:
            logger.error(f"StatePersistence.save: {exc}")

    def load(self) -> dict:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            logger.warning(f"StatePersistence.load: {exc}")
            return {}

    @staticmethod
    def _trim(state: dict) -> dict:
        """Keep only scalar fields per asset to reduce size."""
        out: dict = {"_trimmed_at": int(time.time()), "assets": {}}
        for asset, tfs in state.get("assets", {}).items():
            out["assets"][asset] = {
                tf: {k: v for k, v in data.items() if not isinstance(v, (list, dict))}
                for tf, data in tfs.items()
            }
        return out


# =============================================================================
# SECTION 8: DISK WATCHDOG
# =============================================================================

class DiskWatchdog:
    """
    Periodically checks disk usage.
    Sends a Telegram alert (once) when usage exceeds threshold.
    Resets the alert flag when disk usage falls back below threshold.
    """

    def __init__(
        self,
        dispatcher: TelegramDispatcher,
        threshold_pct: int,
        interval_seconds: int,
    ) -> None:
        self._tg        = dispatcher
        self._threshold = threshold_pct
        self._interval  = interval_seconds
        self._alerted   = False

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                path_to_check = "." if os.path.exists(".") else "/"
                st       = os.statvfs(path_to_check)
                total    = st.f_blocks * st.f_frsize
                free     = st.f_bfree * st.f_frsize
                used_pct = (total - free) / total * 100 if total else 0
                if used_pct >= self._threshold and not self._alerted:
                    msg = (
                        f"*[DISK ALERT]*\n"
                        f"Usage: `{used_pct:.1f}%` (>{self._threshold}%)\n"
                        f"Free: `{free // 1024**3:.2f} GB`\n"
                        f"Clean logs immediately!"
                    )
                    await self._tg.send(msg)
                    self._alerted = True
                    logger.critical(f"DISK: {used_pct:.1f}% used")
                elif used_pct < self._threshold:
                    self._alerted = False
            except AttributeError:
                pass   # os.statvfs unavailable on Windows -- skip silently
            except Exception as exc:
                logger.error(f"DiskWatchdog: {exc}")


# =============================================================================
# SECTION 9: CANDLE-CLOSE SCHEDULER  (timer arithmetic)
# =============================================================================

def seconds_until_next_close(tf_seconds: int, offset: int = 5) -> float:
    """
    Returns seconds to sleep until the next candle boundary + offset.

    Example: tf_seconds=900 (M15), wall-clock is 300s into current bar
        remaining = 900 - 300 = 600s
        sleep     = 600 + 5   = 605s
    """
    elapsed   = time.time() % tf_seconds
    remaining = tf_seconds - elapsed
    return remaining + offset


# =============================================================================
# SECTION 10: WEB DASHBOARD & EMBEDDED SERVER (aiohttp.web)
# =============================================================================

HTML_DASHBOARD = r"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bảng Chỉ Báo Tài Chính Tự Động - Quant Dashboard</title>
  <!-- TradingView Lightweight Charts v4 (Canvas 60fps, ~45KB gzipped, Apache 2.0) -->
  <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: #111827;
      --card-border: #1f293d;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #3b82f6;
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.2);
      --red: #ef4444;
      --red-glow: rgba(239, 68, 68, 0.2);
      --gold: #f59e0b;
      --purple: #8b5cf6;
      --cyan: #06b6d4;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      padding: 16px 20px;
      line-height: 1.5;
    }
    .container { max-width: 1320px; margin: 0 auto; }

    /* Header */
    header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 18px;
      gap: 12px;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand-icon {
      width: 40px; height: 40px;
      background: linear-gradient(135deg, #2563eb, #7c3aed);
      border-radius: 10px;
      display: flex; align-items: center; justify-content: center;
      font-size: 1.25rem;
    }
    .brand-title h1 { font-size: 1.25rem; font-weight: 700; letter-spacing: -0.01em; }
    .brand-title p { font-size: 0.8rem; color: var(--text-muted); }
    .header-actions { display: flex; align-items: center; gap: 12px; }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 12px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 600;
      background: rgba(16, 185, 129, 0.12);
      color: var(--green);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .pulse-dot {
      width: 8px; height: 8px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 8px var(--green);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.3; transform: scale(0.9); }
      100% { opacity: 1; transform: scale(1); }
    }
    .btn-config {
      background: #1e293b;
      color: var(--text);
      border: 1px solid var(--card-border);
      padding: 6px 14px;
      border-radius: 8px;
      font-size: 0.8rem;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: background 0.2s;
    }
    .btn-config:hover { background: #334155; }

    /* Control Tabs */
    .controls-bar {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      padding: 10px 16px;
      border-radius: 12px;
      margin-bottom: 18px;
      gap: 12px;
    }
    .tabs-group { display: flex; align-items: center; gap: 8px; }
    .group-label { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); font-weight: 600; margin-right: 4px; }
    .tab-btn {
      background: rgba(255, 255, 255, 0.04);
      color: var(--text-muted);
      border: 1px solid transparent;
      padding: 6px 14px;
      border-radius: 8px;
      font-size: 0.85rem;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s;
    }
    .tab-btn:hover { color: var(--text); background: rgba(255, 255, 255, 0.08); }
    .tab-btn.active {
      background: #2563eb;
      color: #fff;
      border-color: #3b82f6;
      box-shadow: 0 0 12px rgba(37, 99, 235, 0.4);
    }

    /* Main Grid: Chart & Composite Gauge */
    .main-grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 18px;
      margin-bottom: 20px;
    }
    @media (max-width: 1024px) {
      .main-grid { grid-template-columns: 1fr; }
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .card-header {
      padding: 14px 18px;
      border-bottom: 1px solid var(--card-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .card-title {
      font-size: 0.95rem;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-body { padding: 16px 18px; flex: 1; position: relative; }

    /* Chart Container */
    #chartContainer {
      width: 100%;
      height: 380px;
      position: relative;
    }
    .chart-legend {
      position: absolute;
      top: 10px;
      left: 14px;
      z-index: 10;
      font-size: 0.78rem;
      background: rgba(15, 23, 42, 0.85);
      backdrop-filter: blur(4px);
      padding: 6px 12px;
      border-radius: 6px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }
    .legend-val { font-weight: 700; color: #fff; }

    /* Composite Gauge */
    .gauge-card-body {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
      padding: 20px 16px;
    }
    .gauge-score-display {
      font-size: 3.2rem;
      font-weight: 800;
      line-height: 1;
      margin-bottom: 6px;
      letter-spacing: -0.02em;
    }
    .gauge-badge {
      display: inline-block;
      padding: 6px 16px;
      border-radius: 9999px;
      font-size: 0.95rem;
      font-weight: 700;
      margin-bottom: 14px;
      letter-spacing: 0.02em;
    }
    .gauge-bar-track {
      width: 100%;
      height: 14px;
      background: #1e293b;
      border-radius: 9999px;
      position: relative;
      margin: 12px 0 18px 0;
      overflow: hidden;
    }
    .gauge-bar-gradient {
      width: 100%;
      height: 100%;
      background: linear-gradient(to right, #ef4444 0%, #f97316 25%, #eab308 50%, #10b981 75%, #06b6d4 100%);
    }
    .gauge-needle {
      position: absolute;
      top: -3px;
      bottom: -3px;
      width: 4px;
      background: #ffffff;
      box-shadow: 0 0 8px #ffffff;
      border-radius: 2px;
      transition: left 0.4s ease;
    }
    .gauge-explanation {
      font-size: 0.88rem;
      color: var(--text-muted);
      line-height: 1.5;
      margin-bottom: 18px;
      padding: 10px 14px;
      background: rgba(255, 255, 255, 0.02);
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.04);
      width: 100%;
    }
    .gauge-components {
      width: 100%;
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 8px;
      text-align: center;
      border-top: 1px solid var(--card-border);
      padding-top: 14px;
    }
    .component-item small { font-size: 0.7rem; color: var(--text-muted); display: block; }
    .component-item span { font-size: 0.82rem; font-weight: 700; }

    /* Trade Advice Card Styles */
    .advice-card-body {
      padding: 16px 18px;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .advice-header-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 10px;
    }
    .advice-action-badge {
      font-size: 1.35rem;
      font-weight: 800;
      letter-spacing: 0.05em;
      padding: 6px 20px;
      border-radius: 8px;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .action-buy {
      background: #10b981;
      color: #ffffff;
      box-shadow: 0 0 16px rgba(16, 185, 129, 0.4);
    }
    .action-sell {
      background: #ef4444;
      color: #ffffff;
      box-shadow: 0 0 16px rgba(239, 68, 68, 0.4);
    }
    .action-wait {
      background: #f59e0b;
      color: #0f172a;
      box-shadow: 0 0 16px rgba(245, 158, 11, 0.4);
    }
    .advice-conf-box {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 4px;
    }
    .advice-conf-label {
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--text-muted);
    }
    .advice-conf-bar-track {
      width: 100px;
      height: 6px;
      background: #1e293b;
      border-radius: 9999px;
      overflow: hidden;
    }
    .advice-conf-bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #3b82f6, #10b981);
      border-radius: 9999px;
      transition: width 0.3s ease;
    }
    .trade-levels-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 10px;
    }
    @media (max-width: 640px) {
      .trade-levels-grid { grid-template-columns: 1fr; }
    }
    .level-card {
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .level-title {
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      font-weight: 700;
    }
    .level-value-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 6px;
    }
    .level-value {
      font-size: 0.95rem;
      font-weight: 700;
      color: #fff;
    }
    .btn-copy-mini {
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid rgba(255, 255, 255, 0.12);
      color: var(--text-muted);
      border-radius: 4px;
      padding: 2px 6px;
      font-size: 0.7rem;
      cursor: pointer;
      transition: all 0.2s;
    }
    .btn-copy-mini:hover {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }
    .level-sub {
      font-size: 0.72rem;
      color: var(--text-muted);
      line-height: 1.3;
    }
    .wait-box {
      background: rgba(245, 158, 11, 0.08);
      border: 1px dashed rgba(245, 158, 11, 0.3);
      border-radius: 8px;
      padding: 14px 16px;
      color: #f59e0b;
      font-size: 0.85rem;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .reasoning-box {
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 12px 14px;
    }
    .reasoning-title {
      font-size: 0.78rem;
      font-weight: 700;
      color: var(--text-muted);
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }
    .reasoning-list {
      list-style: none;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .reasoning-item {
      font-size: 0.82rem;
      line-height: 1.4;
      display: flex;
      align-items: flex-start;
      gap: 6px;
    }
    .reasoning-item.warn {
      color: #f59e0b;
    }
    .reasoning-item.ok {
      color: #10b981;
    }
    .reasoning-item.info {
      color: var(--text);
    }
    .advice-disclaimer {
      font-size: 0.72rem;
      color: #94a3b8;
      border-top: 1px solid rgba(255, 255, 255, 0.06);
      padding-top: 8px;
      line-height: 1.4;
    }
    .copy-toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: #10b981;
      color: #fff;
      padding: 8px 16px;
      border-radius: 6px;
      font-size: 0.82rem;
      font-weight: 600;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
      z-index: 999;
      opacity: 0;
      transform: translateY(10px);
      transition: all 0.2s ease;
      pointer-events: none;
    }
    .copy-toast.show {
      opacity: 1;
      transform: translateY(0);
    }

    /* Tables & Plain Language Badges */
    .table-responsive { width: 100%; overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; text-align: left; }
    th {
      background: rgba(255, 255, 255, 0.02);
      color: var(--text-muted);
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 12px 16px;
      border-bottom: 1px solid var(--card-border);
    }
    td {
      padding: 12px 16px;
      font-size: 0.88rem;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(255, 255, 255, 0.02); }

    .tag-status {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 6px;
      font-size: 0.8rem;
      font-weight: 600;
    }
    .status-safe { background: rgba(16, 185, 129, 0.15); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); }
    .status-warning { background: rgba(239, 68, 68, 0.15); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.3); }
    .status-neutral { background: rgba(245, 158, 11, 0.15); color: var(--gold); border: 1px solid rgba(245, 158, 11, 0.3); }
    .status-calm { background: rgba(148, 163, 184, 0.15); color: var(--text-muted); border: 1px solid rgba(148, 163, 184, 0.2); }

    /* Tooltip Glossary */
    .has-tip {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      cursor: help;
      position: relative;
    }
    .tip-icon {
      font-size: 0.75rem;
      color: var(--text-muted);
      border-radius: 50%;
      border: 1px solid var(--text-muted);
      width: 13px; height: 13px;
      display: inline-flex; align-items: center; justify-content: center;
    }
    .tip-box {
      display: none;
      position: absolute;
      bottom: calc(100% + 8px);
      left: 50%;
      transform: translateX(-50%);
      width: 240px;
      background: #1e293b;
      color: #f1f5f9;
      font-size: 0.75rem;
      font-weight: normal;
      line-height: 1.4;
      padding: 10px 12px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      box-shadow: 0 10px 25px rgba(0,0,0,0.5);
      z-index: 100;
      pointer-events: none;
      text-transform: none;
    }
    .has-tip:hover .tip-box { display: block; }

    /* Signals Story Feed */
    .signals-box {
      max-height: 380px;
      overflow-y: auto;
      padding: 12px 16px;
    }
    .story-item {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      padding: 12px 14px;
      border-radius: 8px;
      margin-bottom: 8px;
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid rgba(255, 255, 255, 0.05);
      gap: 10px;
      transition: background 0.15s;
    }
    .story-item:hover { background: rgba(255, 255, 255, 0.04); }
    .story-left { display: flex; align-items: center; gap: 10px; flex: 1; min-width: 260px; }
    .story-icon { font-size: 1.25rem; }
    .story-text { font-size: 0.88rem; line-height: 1.4; }
    .story-meta { display: flex; align-items: center; gap: 12px; font-size: 0.8rem; color: var(--text-muted); }
    .conf-tag {
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .conf-high { background: rgba(16, 185, 129, 0.15); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); }
    .conf-mid { background: rgba(245, 158, 11, 0.15); color: var(--gold); border: 1px solid rgba(245, 158, 11, 0.3); }
    .conf-alert { background: rgba(239, 68, 68, 0.15); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.3); }

    /* Modal */
    .modal-backdrop {
      display: none;
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0, 0, 0, 0.75);
      z-index: 200;
      align-items: center;
      justify-content: center;
    }
    .modal-card {
      background: #111827;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      width: 90%;
      max-width: 480px;
      padding: 24px;
      box-shadow: 0 20px 40px rgba(0,0,0,0.6);
    }
    .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; }
    .modal-title { font-size: 1.1rem; font-weight: 700; }
    .modal-close { background: none; border: none; color: var(--text-muted); font-size: 1.25rem; cursor: pointer; }
    .form-group { margin-bottom: 14px; }
    .form-group label { display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 6px; }
    .form-input {
      width: 100%;
      background: #1e293b;
      border: 1px solid #334155;
      padding: 8px 12px;
      border-radius: 6px;
      color: #fff;
      font-size: 0.85rem;
    }
    .modal-footer { display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px; }
    .btn-primary { background: #2563eb; color: #fff; border: none; padding: 8px 18px; border-radius: 6px; cursor: pointer; font-weight: 600; }
    .btn-secondary { background: #334155; color: #fff; border: none; padding: 8px 18px; border-radius: 6px; cursor: pointer; }
  </style>
</head>
<body>
  <div class="container">
    <!-- Header -->
    <header>
      <div class="brand">
        <div class="brand-icon">📈</div>
        <div class="brand-title">
          <h1>Chỉ Báo Thị Trường Tự Động</h1>
          <p>Hệ thống giám sát định lượng thời gian thực — Dành cho mọi nhà đầu tư</p>
        </div>
      </div>
      <div class="header-actions">
        <div class="status-badge">
          <div class="pulse-dot"></div>
          <span id="systemStatusLabel">HỆ THỐNG ONLINE</span>
        </div>
        <button class="btn-config" onclick="openConfigModal()">⚙️ Cấu hình</button>
      </div>
    </header>

    <!-- Controls Bar -->
    <div class="controls-bar">
      <div class="tabs-group">
        <span class="group-label">Tài sản:</span>
        <button class="tab-btn active" id="btnAssetBTC" onclick="selectAsset('BTC')">₿ Bitcoin (BTC)</button>
        <button class="tab-btn" id="btnAssetXAU" onclick="selectAsset('XAU')">🟡 Vàng (XAU)</button>
        <button class="tab-btn" id="btnAssetOIL" onclick="selectAsset('OIL')">🛢️ Dầu thô (USO)</button>
      </div>
      <div class="tabs-group">
        <span class="group-label">Khung giờ:</span>
        <button class="tab-btn active" id="btnTf15m" onclick="selectTimeframe('15m')">15 Phút (Ngắn hạn)</button>
        <button class="tab-btn" id="btnTf1h" onclick="selectTimeframe('1h')">1 Giờ (Trung hạn)</button>
      </div>
    </div>

    <!-- Main Grid: Chart & Composite Signal Gauge -->
    <div class="main-grid">
      <!-- Candlestick Chart -->
      <div class="card">
        <div class="card-header">
          <div class="card-title">
            <span>📊 Biểu Đồ Nến Trực Quan</span>
            <span id="activeChartTitle" style="color:var(--accent);font-size:0.85rem;">[BTC · 15m]</span>
          </div>
          <div style="font-size:0.75rem;color:var(--text-muted);">
            🟡 EMA 20 &nbsp;|&nbsp; 🔵 EMA 50 &nbsp;|&nbsp; 🟣 Bollinger
          </div>
        </div>
        <div class="card-body">
          <div class="chart-legend" id="chartLegend">
            <span>Giá: <span class="legend-val" id="legPrice">--</span></span>
            <span>Mở: <span class="legend-val" id="legOpen">--</span></span>
            <span>Cao: <span class="legend-val" id="legHigh">--</span></span>
            <span>Thấp: <span class="legend-val" id="legLow">--</span></span>
          </div>
          <div id="chartContainer"></div>
        </div>
      </div>

      <!-- Right Column: Gauge & Trade Advice -->
      <div style="display:flex; flex-direction:column; gap:18px;">
        <!-- Composite Signal Gauge -->
        <div class="card">
          <div class="card-header">
            <div class="card-title">
              <span>🧭 Thước Đo Tâm Lý Thị Trường</span>
            </div>
            <div class="has-tip">
              <span class="tip-icon">ℹ</span>
              <div class="tip-box">
                Chỉ số tổng hợp từ 3 tín hiệu kỹ thuật (Xu hướng 40%, Sức mua 35%, Biên độ 25%). Điểm càng cao phe mua càng áp đảo, điểm càng thấp phe bán càng chiếm ưu thế.
              </div>
            </div>
          </div>
          <div class="gauge-card-body">
            <div class="gauge-score-display" id="gaugeScore">50</div>
            <div class="gauge-badge" id="gaugeBadge" style="background:rgba(234,179,8,0.15);color:#eab308;border:1px solid rgba(234,179,8,0.3);">
              ⚖️ TRUNG LẬP
            </div>
            <p style="font-size:0.75rem; color:var(--text-muted); margin: 6px 0 10px 0; text-align:center; line-height:1.4;">
              ℹ️ Đây là chỉ số tâm lý thị trường tổng quát. Xem khuyến nghị hành động cụ thể kèm độ tin cậy riêng ở khung <strong>"Khuyến Nghị Giao Dịch"</strong> bên dưới.
            </p>
            <div class="gauge-bar-track">
              <div class="gauge-bar-gradient"></div>
              <div class="gauge-needle" id="gaugeNeedle" style="left:50%;"></div>
            </div>
            <div class="gauge-explanation" id="gaugeExplanation">
              Thị trường đang ở trạng thái cân bằng. Lực mua và lực bán giằng co, chưa rõ xu hướng bứt phá.
            </div>
            <div class="gauge-components">
              <div class="component-item">
                <small>Xu Hướng (EMA)</small>
                <span id="compEma">--</span>
              </div>
              <div class="component-item">
                <small>Sức Mua (RSI)</small>
                <span id="compRsi">--</span>
              </div>
              <div class="component-item">
                <small>Vị Trí Dải (BB)</small>
                <span id="compBb">--</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 🎯 Khuyến Nghị Giao Dịch (Card Mới) -->
        <div class="card" id="cardTradeAdvice">
          <div class="card-header">
            <div class="card-title">
              <span>🎯 Khuyến Nghị Giao Dịch</span>
              <span id="adviceAssetBadge" style="color:var(--accent);font-size:0.8rem;">[BTC · 15m]</span>
            </div>
            <div class="has-tip">
              <span class="tip-icon">ℹ</span>
              <div class="tip-box">
                Hệ thống tự động đối chiếu đa khung thời gian (Confluence Engine) giữa EMA Trend, RSI, Bollinger Bands và tín hiệu cảnh báo để gợi ý hành động cụ thể.
              </div>
            </div>
          </div>
          <div class="advice-card-body">
            <!-- Action & Confidence -->
            <div class="advice-header-row">
              <div id="adviceActionBadge" class="advice-action-badge action-wait">
                <span id="adviceActionText">WAIT</span>
              </div>
              <div class="advice-conf-box">
                <div class="advice-conf-label" style="display:flex; align-items:center; justify-content:flex-end; gap:4px;">
                  <span id="adviceConfLabel">Độ tin cậy: Thấp (0/5)</span>
                  <span class="has-tip" style="cursor:help;">
                    <span class="tip-icon" style="width:13px; height:13px; font-size:0.7rem;">ℹ</span>
                    <div class="tip-box" style="right:0; left:auto; transform:none; width:250px;">
                      Độ tin cậy được tính từ số lượng yếu tố kỹ thuật (Xu hướng, RSI, Vị trí dải giá, Tín hiệu mâu thuẫn, Đa khung thời gian) đang đồng thuận với nhau. Đây là thang đo RIÊNG, khác với Thước Đo Tâm Lý Thị Trường ở trên, nhưng cả hai đều phản ánh cùng một xu hướng thị trường.
                    </div>
                  </span>
                </div>
                <div class="advice-conf-bar-track">
                  <div class="advice-conf-bar-fill" id="adviceConfBar" style="width: 20%;"></div>
                </div>
              </div>
            </div>

            <!-- Entry / Stop Loss / Take Profit (Ẩn nếu WAIT) -->
            <div id="adviceLevelsGrid" class="trade-levels-grid" style="display:none;">
              <div class="level-card">
                <span class="level-title">📍 Điểm Vào (Entry)</span>
                <div class="level-value-row">
                  <span class="level-value" id="adviceEntryVal">--</span>
                  <button class="btn-copy-mini" onclick="copyValue('adviceEntryVal')">📋 Copy</button>
                </div>
                <span class="level-sub" id="adviceEntrySub">Vào ngay</span>
              </div>
              <div class="level-card">
                <span class="level-title" style="color:#f87171;">🛑 Cắt Lỗ (SL)</span>
                <div class="level-value-row">
                  <span class="level-value" id="adviceSlVal">--</span>
                  <button class="btn-copy-mini" onclick="copyValue('adviceSlVal')">📋 Copy</button>
                </div>
                <span class="level-sub">R:R 1:1 bảo toàn vốn</span>
              </div>
              <div class="level-card">
                <span class="level-title" style="color:#34d399;">🎯 Chốt Lời (TP)</span>
                <div class="level-value-row">
                  <span class="level-value" id="adviceTp1Val">--</span>
                  <button class="btn-copy-mini" onclick="copyValue('adviceTp1Val')">📋 Copy</button>
                </div>
                <span class="level-sub" id="adviceTp2Sub">TP2: --</span>
              </div>
            </div>

            <!-- Thông điệp khi WAIT -->
            <div id="adviceWaitMessage" class="wait-box">
              <span>⏸</span>
              <span>Chưa có điểm vào lệnh phù hợp lúc này — hệ thống đang chờ tín hiệu rõ ràng hơn.</span>
            </div>

            <!-- Reasoning Checklist -->
            <div class="reasoning-box">
              <div class="reasoning-title">📋 Vì sao có khuyến nghị này?</div>
              <ul class="reasoning-list" id="adviceReasoningList">
                <li class="reasoning-item info"><span>ℹ️</span><span>Đang tải phân tích định lượng...</span></li>
              </ul>
            </div>

            <!-- Disclaimer -->
            <div class="advice-disclaimer">
              ⚠️ Đây là khuyến nghị dựa trên thuật toán kỹ thuật, không phải lời khuyên đầu tư. Bạn tự chịu trách nhiệm với quyết định của mình.
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Table: Live Indicators Overview -->
    <div class="card" style="margin-bottom:20px;">
      <div class="card-header">
        <div class="card-title">
          <span>📋 Bảng Theo Dõi Sức Khỏe Thị Trường (Diễn giải ngôn ngữ đời thường)</span>
        </div>
        <div style="font-size:0.75rem;color:var(--text-muted);">
          Cập nhật tự động mỗi 3 giây
        </div>
      </div>
      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Tài sản</th>
              <th>Khung giờ</th>
              <th>Giá hiện tại</th>
              <th>
                <span class="has-tip">
                  Sức Mua / Bán <span class="tip-icon">ℹ</span>
                  <div class="tip-box">Đo lường thị trường có đang bị mua quá đà (dễ đảo chiều giảm) hay bán tháo quá mức (dễ hồi phục).</div>
                </span>
              </th>
              <th>
                <span class="has-tip">
                  Xu Hướng Chính <span class="tip-icon">ℹ</span>
                  <div class="tip-box">Đánh giá phe nào đang làm chủ cuộc chơi: Phe mua (Tăng) hay Phe bán (Giảm).</div>
                </span>
              </th>
              <th>
                <span class="has-tip">
                  Dải Biến Động <span class="tip-icon">ℹ</span>
                  <div class="tip-box">Biên độ giá chạy thông thường. Vượt ra khỏi biên cảnh báo giá đang biến động dị thường.</div>
                </span>
              </th>
              <th>
                <span class="has-tip">
                  Mức Độ Rung Lắc <span class="tip-icon">ℹ</span>
                  <div class="tip-box">Biên độ giật sóng của giá. Cao là thị trường bão tố giật mạnh, Thấp là sóng êm đềm.</div>
                </span>
              </th>
            </tr>
          </thead>
          <tbody id="indicatorsTbody">
            <tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:24px;">Đang nạp dữ liệu thị trường...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Live Signals Story Feed -->
    <div class="card">
      <div class="card-header">
        <div class="card-title">
          <span>🔔 Dòng Tín Hiệu & Câu Chuyện Thị Trường</span>
        </div>
        <span style="font-size:0.8rem;color:var(--text-muted);" id="signalsCountBadge">0 tín hiệu</span>
      </div>
      <div class="signals-box" id="signalsFeedBox">
        <div style="text-align:center;color:var(--text-muted);padding:30px;">
          Chưa có tín hiệu bất thường. Hệ thống đang theo dõi 24/7...
        </div>
      </div>
    </div>
  </div>

  <!-- Modal Cấu hình -->
  <div class="modal-backdrop" id="configModal">
    <div class="modal-card">
      <div class="modal-header">
        <div class="modal-title">⚙️ Cấu Hình Kết Nối API</div>
        <button class="modal-close" onclick="closeConfigModal()">✕</button>
      </div>
      <div class="form-group">
        <label>TwelveData API Key (Cho Vàng & Dầu)</label>
        <input type="password" id="cfgTwelveKey" class="form-input" placeholder="Nhập API key TwelveData">
      </div>
      <div class="form-group">
        <label>Telegram Bot Token (Tùy chọn)</label>
        <input type="password" id="cfgTgToken" class="form-input" placeholder="123456789:ABCdef...">
      </div>
      <div class="form-group">
        <label>Telegram Chat ID (Tùy chọn)</label>
        <input type="text" id="cfgTgChat" class="form-input" placeholder="-100xxxxxxxxxx">
      </div>
      <div class="modal-footer">
        <button class="btn-secondary" onclick="closeConfigModal()">Đóng</button>
        <button class="btn-primary" onclick="saveConfig()">Lưu cấu hình</button>
      </div>
    </div>
  </div>

  <script>
    // --- BIẾN TRẠNG THÁI CLIENT ---
    let currentAsset = 'BTC';
    let currentTimeframe = '15m';
    let chartInstance = null;
    let candleSeries = null;
    let ema20Series = null;
    let ema50Series = null;
    let bbUpperSeries = null;
    let bbLowerSeries = null;
    let cachedSnapshot = null;

    // --- KHỞI TẠO LIGHTWEIGHT CHARTS (CANVAS 60FPS) ---
    function initChart() {
      const container = document.getElementById('chartContainer');
      if (!container) return;

      chartInstance = LightweightCharts.createChart(container, {
        layout: {
          background: { color: '#111827' },
          textColor: '#94a3b8',
        },
        grid: {
          vertLines: { color: 'rgba(31, 41, 55, 0.6)' },
          horzLines: { color: 'rgba(31, 41, 55, 0.6)' },
        },
        crosshair: {
          mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
          borderColor: '#1f2937',
        },
        timeScale: {
          borderColor: '#1f2937',
          timeVisible: true,
          secondsVisible: false,
        },
      });

      // Series nến
      candleSeries = chartInstance.addCandlestickSeries({
        upColor: '#10b981',
        downColor: '#ef4444',
        borderVisible: false,
        wickUpColor: '#10b981',
        wickDownColor: '#ef4444',
      });

      // Overlay EMA 20
      ema20Series = chartInstance.addLineSeries({
        color: '#eab308',
        lineWidth: 2,
        title: 'EMA 20',
      });

      // Overlay EMA 50
      ema50Series = chartInstance.addLineSeries({
        color: '#38bdf8',
        lineWidth: 2,
        title: 'EMA 50',
      });

      // Overlay Bollinger Upper & Lower
      bbUpperSeries = chartInstance.addLineSeries({
        color: 'rgba(168, 85, 247, 0.6)',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        title: 'BB Upper',
      });
      bbLowerSeries = chartInstance.addLineSeries({
        color: 'rgba(168, 85, 247, 0.6)',
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        title: 'BB Lower',
      });

      // Lắng nghe di chuột để hiển thị legend O, H, L, C
      chartInstance.subscribeCrosshairMove(param => {
        if (!param || !param.time || !param.seriesData.get(candleSeries)) {
          return;
        }
        const data = param.seriesData.get(candleSeries);
        document.getElementById('legPrice').textContent = '$' + Number(data.close).toFixed(2);
        document.getElementById('legOpen').textContent  = '$' + Number(data.open).toFixed(2);
        document.getElementById('legHigh').textContent  = '$' + Number(data.high).toFixed(2);
        document.getElementById('legLow').textContent   = '$' + Number(data.low).toFixed(2);
      });

      window.addEventListener('resize', () => {
        if (chartInstance && container) {
          chartInstance.applyOptions({
            width: container.clientWidth,
            height: container.clientHeight,
          });
        }
      });
    }

    // --- TÍNH TOÁN OVERLAYS TẠI CLIENT (<1ms) ---
    function computeClientOverlays(candles) {
      const ema20 = [];
      const ema50 = [];
      const bbUp = [];
      const bbLow = [];

      const calcEma = (period) => {
        const k = 2 / (period + 1);
        let prev = null;
        const out = [];
        for (let i = 0; i < candles.length; i++) {
          const c = candles[i].close;
          if (prev === null) {
            if (i === period - 1) {
              let sum = 0;
              for (let j = 0; j < period; j++) sum += candles[j].close;
              prev = sum / period;
              out.push({ time: candles[i].time, value: prev });
            }
          } else {
            prev = c * k + prev * (1 - k);
            out.push({ time: candles[i].time, value: prev });
          }
        }
        return out;
      };

      // Bollinger Bands (20, 2)
      for (let i = 19; i < candles.length; i++) {
        let sum = 0;
        for (let j = i - 19; j <= i; j++) sum += candles[j].close;
        const mean = sum / 20;
        let sumSq = 0;
        for (let j = i - 19; j <= i; j++) sumSq += Math.pow(candles[j].close - mean, 2);
        const std = Math.sqrt(sumSq / 20);
        bbUp.push({ time: candles[i].time, value: mean + 2 * std });
        bbLow.push({ time: candles[i].time, value: mean - 2 * std });
      }

      return {
        ema20: calcEma(20),
        ema50: calcEma(50),
        bbUp,
        bbLow,
      };
    }

    // --- TẢI DỮ LIỆU NẾN CHO CHART ---
    async function fetchCandleData() {
      try {
        const tfParam = (currentAsset === 'BTC') ? currentTimeframe : (currentTimeframe === '15m' ? '15min' : '1h');
        const res = await fetch(`/api/candles?asset=${currentAsset}&tf=${tfParam}&limit=100`);
        if (!res.ok) return;
        const candles = await res.json();
        if (!candles || candles.length === 0) return;

        candleSeries.setData(candles);

        const overlays = computeClientOverlays(candles);
        ema20Series.setData(overlays.ema20);
        ema50Series.setData(overlays.ema50);
        bbUpperSeries.setData(overlays.bbUp);
        bbLowerSeries.setData(overlays.bbLow);

        // Cập nhật legend giá đóng cửa nến cuối
        const lastBar = candles[candles.length - 1];
        document.getElementById('legPrice').textContent = '$' + Number(lastBar.close).toFixed(2);
        document.getElementById('legOpen').textContent  = '$' + Number(lastBar.open).toFixed(2);
        document.getElementById('legHigh').textContent  = '$' + Number(lastBar.high).toFixed(2);
        document.getElementById('legLow').textContent   = '$' + Number(lastBar.low).toFixed(2);

        // Marker tín hiệu gần nhất nếu có trong feed
        if (cachedSnapshot && cachedSnapshot.signals) {
          const sigs = cachedSnapshot.signals.filter(s => s.asset === currentAsset);
          if (sigs.length > 0) {
            const markers = [];
            sigs.slice(0, 3).forEach(s => {
              const isBull = s.signal_type.includes('UP') || s.signal_type.includes('OVERSOLD');
              markers.push({
                time: lastBar.time,
                position: isBull ? 'belowBar' : 'aboveBar',
                color: isBull ? '#10b981' : '#ef4444',
                shape: isBull ? 'arrowUp' : 'arrowDown',
                text: isBull ? 'Tín hiệu Tăng' : 'Tín hiệu Giảm',
              });
            });
            candleSeries.setMarkers(markers);
          }
        }

        chartInstance.timeScale().fitContent();
      } catch (err) {
        console.warn('Lỗi tải dữ liệu nến:', err);
      }
    }

    // --- TÍNH TOÁN VÀ CẬP NHẬT COMPOSITE SIGNAL GAUGE ---
    function updateCompositeGauge(assetData) {
      if (!assetData) return;

      const rsi = assetData.rsi != null ? Number(assetData.rsi) : 50;
      const ef  = assetData.ema_fast != null ? Number(assetData.ema_fast) : 0;
      const es  = assetData.ema_slow != null ? Number(assetData.ema_slow) : 0;
      const close = assetData.close != null ? Number(assetData.close) : 0;
      const bLow  = assetData.bb_lower != null ? Number(assetData.bb_lower) : 0;
      const bUp   = assetData.bb_upper != null ? Number(assetData.bb_upper) : 0;

      // 1. Điểm Xu hướng EMA (40%)
      let scoreEma = 50;
      let textEma = '↔️ Đi ngang';
      if (ef > 0 && es > 0) {
        if (close > ef && ef > es) { scoreEma = 100; textEma = '📈 Tăng mạnh'; }
        else if (close < ef && ef < es) { scoreEma = 0; textEma = '📉 Giảm mạnh'; }
        else if (ef > es) { scoreEma = 75; textEma = '↗️ Nghiêng tăng'; }
        else { scoreEma = 25; textEma = '↘️ Nghiêng giảm'; }
      }

      // 2. Điểm Xung lực RSI (35%)
      const scoreRsi = Math.max(0, Math.min(100, rsi));
      let textRsi = '⚖️ Cân bằng';
      if (rsi >= 70) textRsi = '🔥 Quá mua';
      else if (rsi <= 30) textRsi = '❄️ Quá bán';

      // 3. Điểm Vị trí Bollinger Bands (25%)
      let scoreBb = 50;
      let textBb = '🛡️ An toàn';
      if (bUp > bLow && bUp !== 0) {
        const rawBb = ((close - bLow) / (bUp - bLow)) * 100;
        scoreBb = Math.max(0, Math.min(100, rawBb));
        if (close > bUp) textBb = '⚠️ Vượt đỉnh';
        else if (close < bLow) textBb = '🔻 Thủng đáy';
      }

      // Điểm tổng hợp (0 - 100)
      const totalScore = Math.round((0.40 * scoreEma) + (0.35 * scoreRsi) + (0.25 * scoreBb));

      // Cập nhật giao diện Gauge
      document.getElementById('gaugeScore').textContent = totalScore;
      document.getElementById('gaugeNeedle').style.left = totalScore + '%';
      document.getElementById('compEma').textContent = textEma;
      document.getElementById('compRsi').textContent = `${textRsi} (${rsi.toFixed(1)})`;
      document.getElementById('compBb').textContent = textBb;

      const badge = document.getElementById('gaugeBadge');
      const desc = document.getElementById('gaugeExplanation');

      if (totalScore >= 76) {
        badge.textContent = '💎 MUA MẠNH';
        badge.style = 'background:rgba(6,182,212,0.15);color:#06b6d4;border:1px solid rgba(6,182,212,0.3);';
        desc.textContent = 'Thị trường hưng phấn tột độ! Phe mua kiểm soát hoàn toàn, đà tăng giá cực mạnh.';
      } else if (totalScore >= 56) {
        badge.textContent = '🟢 NGHIÊNG TĂNG';
        badge.style = 'background:rgba(16,185,129,0.15);color:#10b981;border:1px solid rgba(16,185,129,0.3);';
        desc.textContent = 'Phe mua đang thắng thế rõ rệt. Động lực tăng giá đang được duy trì tốt.';
      } else if (totalScore >= 45) {
        badge.textContent = '🟡 TRUNG LẬP';
        badge.style = 'background:rgba(234,179,8,0.15);color:#eab308;border:1px solid rgba(234,179,8,0.3);';
        desc.textContent = 'Thị trường đi ngang và giằng co. Hai bên mua bán cân bằng, chưa rõ hướng bứt phá.';
      } else if (totalScore >= 25) {
        badge.textContent = '🟠 NGHIÊNG GIẢM';
        badge.style = 'background:rgba(249,115,22,0.15);color:#f97316;border:1px solid rgba(249,115,22,0.3);';
        desc.textContent = 'Áp lực bán đang đè nặng. Thị trường trượt giá dần, phe bán chiếm ưu thế.';
      } else {
        badge.textContent = '🔴 BÁN MẠNH';
        badge.style = 'background:rgba(239,68,68,0.15);color:#ef4444;border:1px solid rgba(239,68,68,0.3);';
        desc.textContent = 'Tâm lý bi quan bao trùm! Áp lực bán tháo dữ dội, rủi ro cao, không nên mua bắt đáy vội.';
      }
    }

    // --- CẬP NHẬT TOÀN BỘ DASHBOARD TỪ SNAPSHOT ---
    async function updateDashboard() {
      try {
        const res = await fetch('/api/snapshot');
        if (!res.ok) return;
        const data = await res.json();
        cachedSnapshot = data;

        // 1. Cập nhật bảng chỉ báo với ngôn ngữ đời thường
        const tbody = document.getElementById('indicatorsTbody');
        const indMap = data.indicators || {};
        let rowsHtml = '';
        const assets = ['BTC', 'XAU', 'OIL'];

        assets.forEach(asset => {
          const tfs = indMap[asset] || {};
          Object.keys(tfs).forEach(tf => {
            const item = tfs[tf];
            if (!item) return;

            const price = item.close != null ? Number(item.close).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4}) : '--';

            // Dịch RSI
            let rsiTag = '<span class="tag-status status-calm">--</span>';
            if (item.rsi != null) {
              const r = Number(item.rsi);
              if (r >= 70) {
                rsiTag = `<span class="tag-status status-warning">🔥 Tăng quá nóng (${r.toFixed(1)})</span>`;
              } else if (r <= 30) {
                rsiTag = `<span class="tag-status status-neutral">❄️ Bán tháo quá đà (${r.toFixed(1)})</span>`;
              } else {
                rsiTag = `<span class="tag-status status-safe">⚖️ Cân bằng (${r.toFixed(1)})</span>`;
              }
            }

            // Dịch Xu hướng EMA
            let trendTag = '<span class="tag-status status-calm">--</span>';
            if (item.ema_fast != null && item.ema_slow != null) {
              const ef = Number(item.ema_fast);
              const es = Number(item.ema_slow);
              const cl = Number(item.close || 0);
              if (cl > ef && ef > es) {
                trendTag = `<span class="tag-status status-safe">📈 TĂNG RÕ RỆT</span>`;
              } else if (cl < ef && ef < es) {
                trendTag = `<span class="tag-status status-warning">📉 GIẢM RÕ RỆT</span>`;
              } else if (ef > es) {
                trendTag = `<span class="tag-status status-safe">↗️ Nghiêng Tăng</span>`;
              } else {
                trendTag = `<span class="tag-status status-warning">↘️ Nghiêng Giảm</span>`;
              }
            }

            // Dịch Bollinger Bands
            let bbTag = '<span class="tag-status status-calm">--</span>';
            if (item.bb_lower != null && item.bb_upper != null && item.close != null) {
              const cl = Number(item.close);
              const up = Number(item.bb_upper);
              const low = Number(item.bb_lower);
              if (cl > up) {
                bbTag = `<span class="tag-status status-warning">⚠️ Vượt đỉnh biên</span>`;
              } else if (cl < low) {
                bbTag = `<span class="tag-status status-neutral">🔻 Thủng đáy biên</span>`;
              } else {
                bbTag = `<span class="tag-status status-safe">🛡️ Vùng bình thường</span>`;
              }
            }

            // Dịch ATR (Biến động)
            let atrTag = '<span class="tag-status status-calm">--</span>';
            if (item.atr != null) {
              const a = Number(item.atr);
              atrTag = `<span class="tag-status status-safe">🍃 Sóng êm (${a.toFixed(2)})</span>`;
            }

            let assetName = '₿ Bitcoin (BTC)';
            if (asset === 'XAU') assetName = '🟡 Vàng (XAU)';
            if (asset === 'OIL') assetName = '🛢️ Dầu thô (USO)';

            let tfName = tf.includes('15') ? '15 Phút' : '1 Giờ';

            rowsHtml += `
              <tr>
                <td style="font-weight:700;">${assetName}</td>
                <td><span class="tag-status status-calm">${tfName}</span></td>
                <td style="font-weight:700;color:#fff;">$${price}</td>
                <td>${rsiTag}</td>
                <td>${trendTag}</td>
                <td>${bbTag}</td>
                <td>${atrTag}</td>
              </tr>
            `;
          });
        });

        if (rowsHtml) tbody.innerHTML = rowsHtml;

        // 2. Cập nhật Gauge cho tài sản đang được chọn
        const targetTf = (currentAsset === 'BTC') ? currentTimeframe : (currentTimeframe === '15m' ? '15min' : '1h');
        const activeAssetData = indMap[currentAsset]?.[targetTf];
        if (activeAssetData) {
          updateCompositeGauge(activeAssetData);
        }

        // Cập nhật Khuyến nghị Giao dịch (Trade Advice)
        renderAdviceCard(data, currentAsset, currentTimeframe);

        // 3. Dịch Signals Feed sang câu chuyện có ngữ cảnh
        const signals = data.signals || [];
        document.getElementById('signalsCountBadge').textContent = `${signals.length} tín hiệu ghi nhận`;
        const feedBox = document.getElementById('signalsFeedBox');

        if (signals.length === 0) {
          feedBox.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:30px;">Chưa có tín hiệu bất thường. Hệ thống đang theo dõi 24/7...</div>';
        } else {
          feedBox.innerHTML = signals.map(s => {
            let story = '';
            let icon = '🔔';
            let confHtml = '<span class="conf-tag conf-mid">🟡 ĐỘ TIN CẬY: VỪA</span>';

            const assetLabel = s.asset === 'BTC' ? 'Bitcoin' : (s.asset === 'XAU' ? 'Vàng' : 'Dầu thô');
            const tfLabel = s.timeframe.includes('15') ? '15 phút' : '1 giờ';
            const priceStr = '$' + Number(s.price).toFixed(2);

            if (s.signal_type.includes('BB_BREAKOUT_DOWN')) {
              icon = '🔻';
              story = `<strong>${assetLabel} (${tfLabel})</strong> vừa giảm mạnh xuyên qua vùng đáy biên quen thuộc, rớt về <strong>${priceStr}</strong>. Cần chú ý nhịp hồi phục kỹ thuật hoặc nguy cơ giảm sâu tiếp.`;
              confHtml = '<span class="conf-tag conf-mid">🟡 ĐỘ TIN CẬY: VỪA</span>';
            } else if (s.signal_type.includes('BB_BREAKOUT_UP')) {
              icon = '⚠️';
              story = `<strong>${assetLabel} (${tfLabel})</strong> vừa bứt phá vượt qua đỉnh dải biên dao động tại mức <strong>${priceStr}</strong>. Động lực tăng mạnh nhưng rủi ro chốt lời đang tăng dần.`;
              confHtml = '<span class="conf-tag conf-mid">🟡 ĐỘ TIN CẬY: VỪA</span>';
            } else if (s.signal_type.includes('EMA_CROSS') && s.signal_type.includes('UP')) {
              icon = '🚀';
              story = `<strong>${assetLabel} (${tfLabel})</strong> vừa xác nhận mở ra chu kỳ TĂNG mới khi đường giá ngắn hạn vượt qua đường dài hạn tại mức <strong>${priceStr}</strong>.`;
              confHtml = '<span class="conf-tag conf-high">🟢 ĐỘ TIN CẬY: CAO</span>';
            } else if (s.signal_type.includes('EMA_CROSS')) {
              icon = '📉';
              story = `<strong>${assetLabel} (${tfLabel})</strong> vừa xác nhận tín hiệu trượt dốc GIẢM khi xu hướng ngắn hạn cắt xuống đường dài hạn tại mức <strong>${priceStr}</strong>.`;
              confHtml = '<span class="conf-tag conf-high">🟢 ĐỘ TIN CẬY: CAO</span>';
            } else if (s.signal_type.includes('OVERBOUGHT')) {
              icon = '🔥';
              story = `<strong>${assetLabel} (${tfLabel})</strong> đang tăng quá nóng ở mức <strong>${priceStr}</strong>. Lực mua bắt đầu đuối sức, cẩn trọng nhịp đảo chiều giảm.`;
              confHtml = '<span class="conf-tag conf-high">🟢 ĐỘ TIN CẬY: CAO</span>';
            } else if (s.signal_type.includes('OVERSOLD')) {
              icon = '❄️';
              story = `<strong>${assetLabel} (${tfLabel})</strong> rơi vào vùng bán tháo quá mức tại giá <strong>${priceStr}</strong>. Áp lực xả hàng đang suy yếu, mở ra cơ hội phục hồi.`;
              confHtml = '<span class="conf-tag conf-high">🟢 ĐỘ TIN CẬY: CAO</span>';
            } else if (s.signal_type.includes('SPIKE')) {
              icon = '⚡';
              story = `<strong>${assetLabel} (${tfLabel})</strong> ghi nhận biên độ biến động giật mạnh bất thường tại giá <strong>${priceStr}</strong>. Thị trường đang nhiều bão tố, nên thận trọng.`;
              confHtml = '<span class="conf-tag conf-alert">⚡ CẢNH BÁO RỦI RO</span>';
            } else {
              story = `<strong>${assetLabel} (${tfLabel})</strong>: ${s.message || s.signal_type} tại mức ${priceStr}.`;
            }

            return `
              <div class="story-item">
                <div class="story-left">
                  <div class="story-icon">${icon}</div>
                  <div class="story-text">${story}</div>
                </div>
                <div class="story-meta">
                  ${confHtml}
                  <span>${s.time_str || ''}</span>
                </div>
              </div>
            `;
          }).join('');
        }

      } catch (err) {
        console.warn('Lỗi fetch snapshot:', err);
      }
    }

    // --- COPY TO CLIPBOARD HELPER ---
    function copyValue(elementId) {
      const el = document.getElementById(elementId);
      if (!el) return;
      const raw = el.getAttribute('data-raw');
      const val = raw || el.textContent.replace(/[^0-9.]/g, '');
      if (!val) return;
      copyToClipboard(val);
    }

    function copyToClipboard(text) {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
      } else {
        fallbackCopy(text);
      }
      showToast('📋 Đã copy: ' + text);
    }

    function fallbackCopy(text) {
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); } catch (e) {}
      document.body.removeChild(ta);
    }

    function showToast(msg) {
      let toast = document.getElementById('copyToast');
      if (!toast) {
        toast = document.createElement('div');
        toast.id = 'copyToast';
        toast.className = 'copy-toast';
        document.body.appendChild(toast);
      }
      toast.textContent = msg;
      toast.classList.add('show');
      setTimeout(() => {
        toast.classList.remove('show');
      }, 1500);
    }

    // --- RENDER TRADE ADVICE CARD ---
    function renderAdviceCard(snapshot, asset, tf) {
      if (!snapshot || !snapshot.advice) return;
      const targetTf = (asset === 'BTC') ? tf : (tf === '15m' ? '15min' : '1h');
      const advice = snapshot.advice?.[asset]?.[targetTf];

      const badgeAsset = document.getElementById('adviceAssetBadge');
      if (badgeAsset) badgeAsset.textContent = `[${asset} · ${tf}]`;

      if (!advice) {
        const waitBox = document.getElementById('adviceWaitMessage');
        if (waitBox) {
          waitBox.style.display = 'flex';
          waitBox.innerHTML = '<span>⏳</span><span>Đang thu thập dữ liệu nến cho tài sản này...</span>';
        }
        const levelsGrid = document.getElementById('adviceLevelsGrid');
        if (levelsGrid) levelsGrid.style.display = 'none';
        return;
      }

      // Action Badge
      const actionBadge = document.getElementById('adviceActionBadge');
      const actionText = document.getElementById('adviceActionText');
      if (actionBadge && actionText) {
        actionText.textContent = advice.action;
        actionBadge.className = 'advice-action-badge ' + (
          advice.action === 'BUY' ? 'action-buy' :
          advice.action === 'SELL' ? 'action-sell' : 'action-wait'
        );
      }

      // Confidence
      const confLabel = document.getElementById('adviceConfLabel');
      const confBar = document.getElementById('adviceConfBar');
      if (confLabel) {
        confLabel.textContent = `Độ tin cậy: ${advice.confidence} (${advice.confidence_score}/5)`;
      }
      if (confBar) {
        const pct = Math.min(Math.max((advice.confidence_score / 5) * 100, 15), 100);
        confBar.style.width = pct + '%';
        confBar.style.background = advice.confidence === 'Cao'
          ? '#10b981'
          : (advice.confidence === 'Trung bình' ? '#3b82f6' : '#f59e0b');
      }

      // Entry / SL / TP
      const levelsGrid = document.getElementById('adviceLevelsGrid');
      const waitMessage = document.getElementById('adviceWaitMessage');
      if (advice.action === 'WAIT') {
        if (levelsGrid) levelsGrid.style.display = 'none';
        if (waitMessage) {
          waitMessage.style.display = 'flex';
          waitMessage.innerHTML = '<span>⏸</span><span>Chưa có điểm vào lệnh phù hợp lúc này — hệ thống đang chờ tín hiệu rõ ràng hơn.</span>';
        }
      } else {
        if (levelsGrid) levelsGrid.style.display = 'grid';
        if (waitMessage) waitMessage.style.display = 'none';

        const entryValEl = document.getElementById('adviceEntryVal');
        const entrySubEl = document.getElementById('adviceEntrySub');
        if (advice.entry_type === 'now') {
          entryValEl.textContent = '$' + Number(advice.entry_price).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          entryValEl.setAttribute('data-raw', advice.entry_price);
          entrySubEl.textContent = '⚡ Khớp lệnh ngay giá hiện tại';
        } else {
          entryValEl.textContent = '$' + Number(advice.entry_zone).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          entryValEl.setAttribute('data-raw', advice.entry_zone);
          entrySubEl.textContent = '⏳ Chờ giá hồi về vùng EMA20';
        }

        const slValEl = document.getElementById('adviceSlVal');
        if (slValEl) {
          slValEl.textContent = '$' + Number(advice.stop_loss).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          slValEl.setAttribute('data-raw', advice.stop_loss);
        }

        const tp1ValEl = document.getElementById('adviceTp1Val');
        const tp2SubEl = document.getElementById('adviceTp2Sub');
        if (tp1ValEl) {
          tp1ValEl.textContent = '$' + Number(advice.take_profit_1).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          tp1ValEl.setAttribute('data-raw', advice.take_profit_1);
        }
        if (tp2SubEl) {
          tp2SubEl.textContent = `TP2: $${Number(advice.take_profit_2).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4})} (${advice.tp2_rule || 'Trailing EMA'})`;
        }
      }

      // Reasoning Checklist
      const reasoningList = document.getElementById('adviceReasoningList');
      if (reasoningList && Array.isArray(advice.reasoning)) {
        reasoningList.innerHTML = advice.reasoning.map(r => {
          const isWarn = r.includes('⚠️');
          const isOk = r.includes('ủng hộ') || r.includes('ĐỒNG THUẬN') || r.includes('dư địa') || r.includes('Không có');
          const itemClass = isWarn ? 'warn' : (isOk ? 'ok' : 'info');
          const icon = isWarn ? '⚠️' : (isOk ? '✅' : 'ℹ️');
          const cleanText = r.replace(/^[⚠️✅ℹ️\s]+/, '');
          return `<li class="reasoning-item ${itemClass}"><span>${icon}</span><span>${cleanText}</span></li>`;
        }).join('');
      }
    }

    // --- CHUYỂN ĐỔI TÀI SẢN & KHUNG GIỜ ---
    function selectAsset(asset) {
      currentAsset = asset;
      ['BTC', 'XAU', 'OIL'].forEach(a => {
        const btn = document.getElementById('btnAsset' + a);
        if (btn) btn.classList.toggle('active', a === asset);
      });
      document.getElementById('activeChartTitle').textContent = `[${asset} · ${currentTimeframe}]`;
      fetchCandleData();
      if (cachedSnapshot) {
        const tf = (asset === 'BTC') ? currentTimeframe : (currentTimeframe === '15m' ? '15min' : '1h');
        if (cachedSnapshot.indicators) {
          updateCompositeGauge(cachedSnapshot.indicators[asset]?.[tf]);
        }
        renderAdviceCard(cachedSnapshot, asset, currentTimeframe);
      }
    }

    function selectTimeframe(tf) {
      currentTimeframe = tf;
      document.getElementById('btnTf15m').classList.toggle('active', tf === '15m');
      document.getElementById('btnTf1h').classList.toggle('active', tf === '1h');
      document.getElementById('activeChartTitle').textContent = `[${currentAsset} · ${tf}]`;
      fetchCandleData();
      if (cachedSnapshot) {
        const targetTf = (currentAsset === 'BTC') ? tf : (tf === '15m' ? '15min' : '1h');
        if (cachedSnapshot.indicators) {
          updateCompositeGauge(cachedSnapshot.indicators[currentAsset]?.[targetTf]);
        }
        renderAdviceCard(cachedSnapshot, currentAsset, tf);
      }
    }

    // --- MODAL CẤU HÌNH ---
    function openConfigModal() {
      document.getElementById('configModal').style.display = 'flex';
    }
    function closeConfigModal() {
      document.getElementById('configModal').style.display = 'none';
    }
    async function saveConfig() {
      const twelveKey = document.getElementById('cfgTwelveKey').value.trim();
      const tgToken   = document.getElementById('cfgTgToken').value.trim();
      const tgChat    = document.getElementById('cfgTgChat').value.trim();
      try {
        const res = await fetch('/api/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            twelvedata_api_key: twelveKey,
            telegram_bot_token: tgToken,
            telegram_chat_id: tgChat
          })
        });
        const data = await res.json();
        alert(data.message || 'Đã cập nhật cấu hình thành công!');
        closeConfigModal();
        updateDashboard();
      } catch (err) {
        alert('Lỗi lưu cấu hình: ' + err);
      }
    }

    // --- VÒNG LẶP CẬP NHẬT (PERFORMANCE GUARDRAILS) ---
    window.addEventListener('DOMContentLoaded', () => {
      initChart();
      updateDashboard();
      fetchCandleData();

      // Snapshot số liệu & tín hiệu: mỗi 3 giây
      setInterval(updateDashboard, 3000);

      // Biểu đồ nến: mỗi 15 giây (tiết kiệm băng thông & tải render canvas)
      setInterval(fetchCandleData, 15000);
    });
  </script>
</body>
</html>
"""


class DashboardServer:
    """
    Lightweight web server embedded inside engine.py via aiohttp.web.
    Provides:
      GET /             -> Interactive Dark Mode Web Dashboard
      GET /api/snapshot -> Realtime JSON snapshot of indicators & signals
      GET /api/health   -> Lightweight health check
    Consumes < 5MB RAM and reuses the existing aiohttp library.
    """

    def __init__(self, orchestrator: IndicatorOrchestrator, host: str, port: int) -> None:
        self._orchestrator = orchestrator
        self._host         = host
        self._port         = port
        self._app          = web.Application()
        self._app.router.add_get("/",             self.handle_index)
        self._app.router.add_get("/health",       self.handle_health)
        self._app.router.add_get("/api/snapshot", self.handle_snapshot)
        self._app.router.add_get("/api/candles",  self.handle_candles)
        self._app.router.add_get("/api/health",   self.handle_health)
        self._app.router.add_post("/api/config",  self.handle_save_config)
        self._runner: Optional[web.AppRunner] = None
        self._site:   Optional[web.TCPSite]   = None

    async def handle_index(self, request: web.Request) -> web.Response:
        content = HTML_DASHBOARD
        dash_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        if os.path.exists(dash_path):
            try:
                with open(dash_path, "r", encoding="utf-8") as f:
                    content = f.read()
            except Exception:
                pass
        return web.Response(text=content, content_type="text/html", charset="utf-8")

    async def handle_candles(self, request: web.Request) -> web.Response:
        """
        Lightweight read-only endpoint returning OHLC candle history for charting.
        Pulls directly from circular buffer in RAM without heavy computation.
        """
        asset = request.query.get("asset", "BTC").upper()
        tf = request.query.get("tf", "15m").lower()
        limit_str = request.query.get("limit", "100")
        try:
            limit = min(max(int(limit_str), 10), 300)
        except ValueError:
            limit = 100

        # Support timeframe alias between 15m and 15min
        buf = None
        asset_bufs = self._orchestrator._bufs.get(asset, {})
        if tf in asset_bufs:
            buf = asset_bufs[tf]
        elif tf == "15m" and "15min" in asset_bufs:
            buf = asset_bufs["15min"]
        elif tf == "15min" and "15m" in asset_bufs:
            buf = asset_bufs["15m"]

        if not buf:
            return web.json_response([], status=200)

        candles = buf.last_n_candles(limit)
        data = [
            {
                "time": int(c.ts),
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
            }
            for c in candles
        ]
        return web.json_response(data)

    async def handle_snapshot(self, request: web.Request) -> web.Response:
        now = time.time()
        uptime_sec = int(now - self._orchestrator._start_time)

        advice_payload: dict = {}
        for a, tfs in self._orchestrator._latest_advice.items():
            advice_payload[a] = {}
            for tf, adv in tfs.items():
                advice_payload[a][tf] = adv.to_dict() if hasattr(adv, "to_dict") else adv

        data = {
            "status": "online",
            "uptime_seconds": uptime_sec,
            "uptime_formatted": str(datetime.timedelta(seconds=uptime_sec)),
            "server_time": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "telegram_enabled": self._orchestrator._tg.is_enabled if self._orchestrator._tg else False,
            "twelvedata_configured": bool(self._orchestrator._twelve and self._orchestrator._twelve.is_configured),
            "indicators": self._orchestrator._latest_indicators,
            "signals": list(self._orchestrator._recent_signals),
            "advice": advice_payload,
        }
        return web.json_response(data)

    async def handle_save_config(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"status": "error", "message": "Invalid JSON"}, status=400)

        twelve_key = str(data.get("twelvedata_api_key", "")).strip()
        tg_token   = str(data.get("telegram_bot_token", "")).strip()
        tg_chat    = str(data.get("telegram_chat_id", "")).strip()

        updates_to_save: dict[str, str] = {}

        if twelve_key:
            CONFIG["twelvedata_api_key"] = twelve_key
            updates_to_save["TWELVE_API_KEY"] = twelve_key
            if self._orchestrator._twelve:
                self._orchestrator._twelve.update_key(twelve_key)
                asyncio.create_task(self._orchestrator.reload_asset_buffers(["XAU", "OIL"]))

        if tg_token or tg_chat:
            if tg_token:
                CONFIG["telegram_bot_token"] = tg_token
                updates_to_save["TG_BOT_TOKEN"] = tg_token
            if tg_chat:
                CONFIG["telegram_chat_id"] = tg_chat
                updates_to_save["TG_CHAT_ID"] = tg_chat
            if self._orchestrator._tg:
                self._orchestrator._tg.update_config(CONFIG["telegram_bot_token"], CONFIG["telegram_chat_id"])

        if updates_to_save:
            _update_dotenv(updates_to_save)

        return web.json_response({
            "status": "ok",
            "message": "Configuration saved & applied in realtime!",
            "twelvedata_configured": bool(self._orchestrator._twelve and self._orchestrator._twelve.is_configured),
            "telegram_enabled": bool(self._orchestrator._tg and self._orchestrator._tg.is_enabled),
        })

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({
            "status": "ok",
            "time": int(time.time()),
        })

    async def start(self) -> None:
        try:
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            logger.info(f"Web Dashboard listening on http://{self._host}:{self._port}")
        except Exception as exc:
            logger.error(f"Failed to start Web Dashboard on {self._host}:{self._port}: {exc}")

    async def stop(self) -> None:
        if self._runner:
            try:
                await self._runner.cleanup()
                logger.info("Web Dashboard stopped cleanly")
            except Exception as exc:
                logger.error(f"Error stopping Web Dashboard: {exc}")


# =============================================================================
# SECTION 11: MAIN ORCHESTRATOR
# =============================================================================

class IndicatorOrchestrator:
    """
    Top-level coordinator for the entire pipeline:
      _init_session  -> create shared aiohttp session + fetchers
      _init_buffers  -> create CircularBuffers and pre-fill with history
      _asset_loop    -> per-(asset, tf) infinite poll-compute-signal loop
      _flush_loop    -> periodic state snapshot to disk
      run            -> starts all asyncio tasks & web dashboard
    """

    def __init__(self) -> None:
        self._bufs:              dict[str, dict[str, CircularBuffer]] = {}
        self._rules              = RuleEngine()
        self._confluence_engine  = ConfluenceDecisionEngine(CONFIG)
        self._persist:           Optional[StatePersistence]   = None
        self._tg:                Optional[TelegramDispatcher] = None
        self._session:           Optional[aiohttp.ClientSession] = None
        self._binance:           Optional[BinanceFetcher]     = None
        self._twelve:            Optional[TwelveDataFetcher]  = None
        self._dashboard:         Optional[DashboardServer]    = None
        self._start_time:        float                        = time.time()
        self._latest_indicators: dict[str, dict[str, dict]]   = {}
        self._recent_signals:    deque[dict]                  = deque(maxlen=50)
        self._latest_advice:     dict[str, dict[str, TradeAdvice]] = {}

    def _compute_advice_for_asset(self, asset: str, tf: str, ind: dict) -> None:
        """Compute trade advice using multi-timeframe indicators without triggering notifications."""
        cfg = CONFIG["assets"].get(asset, {})
        cfg_tfs = cfg.get("timeframes", [])
        other_tf = next((t for t in cfg_tfs if t != tf), None)
        other_tf_snapshot = self._latest_indicators.get(asset, {}).get(other_tf) if other_tf else None

        buf = self._bufs.get(asset, {}).get(tf)
        n_candles = CONFIG.get("COUNTER_MOVE_LOOKBACK_CANDLES", 3)
        recent_candles = buf.last_n_candles(n_candles) if buf else []

        advice = self._confluence_engine.decide(
            asset=asset,
            timeframe=tf,
            snapshot=ind,
            recent_signals=list(self._recent_signals),
            recent_candles=recent_candles,
            other_tf_snapshot=other_tf_snapshot,
            other_timeframe=other_tf,
        )
        self._latest_advice.setdefault(asset, {})[tf] = advice

    # ------------------------------------------------------------------ setup
    async def _init_session(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300),
            timeout=aiohttp.ClientTimeout(total=15),
        )
        self._binance = BinanceFetcher(self._session)
        self._twelve  = TwelveDataFetcher(self._session, CONFIG["twelvedata_api_key"])

    async def _init_buffers(self) -> None:
        logger.info("Pre-filling circular buffers with historical data...")
        for asset, cfg in CONFIG["assets"].items():
            self._bufs[asset] = {}
            for tf in cfg["timeframes"]:
                buf = CircularBuffer(CONFIG["buffer_size"], asset, tf)
                self._bufs[asset][tf] = buf
                candles = await self._fetch(asset, cfg, tf, limit=300)
                for c in candles:
                    buf.push(c)
                logger.info(f"  [{asset} {tf}] loaded {len(buf)} candles")
                # Immediately compute indicators so Web Dashboard has data on launch
                if len(buf) > 0:
                    ind = IndicatorEngine.compute_all(buf)
                    self._latest_indicators.setdefault(asset, {})[tf] = ind
                await asyncio.sleep(1)   # respect rate limits during init

        # Compute initial advice for all buffers
        for a, cfg_a in CONFIG["assets"].items():
            for t in cfg_a.get("timeframes", []):
                ind_snap = self._latest_indicators.get(a, {}).get(t)
                if ind_snap:
                    self._compute_advice_for_asset(a, t, ind_snap)

    async def reload_asset_buffers(self, asset_names: list[str]) -> None:
        """Fetch historical candles and compute indicators for specific assets in background."""
        logger.info(f"Reloading historical buffers for assets: {asset_names}...")
        for asset in asset_names:
            if asset not in CONFIG["assets"]:
                continue
            cfg = CONFIG["assets"][asset]
            for tf in cfg["timeframes"]:
                buf = self._bufs.get(asset, {}).get(tf)
                if not buf:
                    buf = CircularBuffer(CONFIG["buffer_size"], asset, tf)
                    self._bufs.setdefault(asset, {})[tf] = buf
                candles = await self._fetch(asset, cfg, tf, limit=300)
                for c in candles:
                    buf.push(c)
                logger.info(f"  [{asset} {tf}] loaded {len(buf)} candles (hot-reload)")
                if len(buf) > 0:
                    ind = IndicatorEngine.compute_all(buf)
                    self._latest_indicators.setdefault(asset, {})[tf] = ind
                await asyncio.sleep(1)

            # Recompute advice for reloaded assets
            for tf in cfg["timeframes"]:
                ind_snap = self._latest_indicators.get(asset, {}).get(tf)
                if ind_snap:
                    self._compute_advice_for_asset(asset, tf, ind_snap)

    # --------------------------------------------------------------- fetching
    async def _fetch(
        self, asset: str, cfg: dict, tf: str, limit: int = 2
    ) -> list[Candle]:
        src = cfg["source"]
        sym = cfg["symbol"]
        if src == "binance":
            return await self._binance.fetch_candles(sym, tf, limit)
        if src == "twelvedata":
            return await self._twelve.fetch_candles(sym, tf, limit)
        logger.warning(f"Unknown source '{src}' for {asset}")
        return []

    # ------------------------------------------------------------- per-candle
    async def _process(self, asset: str, tf: str) -> None:
        """One candle-close cycle: fetch -> push -> compute -> evaluate -> signal."""
        cfg     = CONFIG["assets"][asset]
        candles = await self._fetch(asset, cfg, tf, limit=2)
        if not candles:
            logger.warning(f"No candles: {asset} {tf}")
            return
        latest = candles[-1]
        buf    = self._bufs[asset][tf]
        # Deduplication guard
        if buf.latest and buf.latest.ts == latest.ts:
            logger.debug(f"[{asset} {tf}] duplicate ts={latest.ts}, skipped")
            return
        buf.push(latest)
        ind = IndicatorEngine.compute_all(buf)
        # Store in latest indicators cache for Web UI & snapshot API
        self._latest_indicators.setdefault(asset, {})[tf] = ind

        # Compact log line
        rsi = f"{ind['rsi']:.1f}" if ind.get("rsi") is not None else "N/A"
        atr = f"{ind['atr']:.4f}" if ind.get("atr") is not None else "N/A"
        logger.info(f"[{asset} {tf}] close={latest.close:.4f} RSI={rsi} ATR={atr}")
        for sig in self._rules.evaluate(ind):
            logger.info(f"  >> {sig.signal_type}")
            self._recent_signals.appendleft(sig.to_dict())
            asyncio.create_task(self._tg.dispatch_signal(sig))

        # Compute trade advice (Web UI only -- NO Telegram dispatch)
        self._compute_advice_for_asset(asset, tf, ind)

    # ------------------------------------------------------------------ loops
    async def _asset_loop(self, asset: str, tf: str) -> None:
        tf_sec = TF_SECONDS.get(tf, 900)
        logger.info(f"Loop started: {asset} {tf} (period={tf_sec}s)")
        while True:
            wait = seconds_until_next_close(tf_sec, CONFIG["poll_offset_seconds"])
            logger.debug(f"[{asset} {tf}] sleep {wait:.1f}s")
            await asyncio.sleep(wait)
            try:
                await self._process(asset, tf)
            except Exception as exc:
                logger.error(f"[{asset} {tf}] loop error: {exc}", exc_info=True)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(CONFIG["state_flush_interval"])
            try:
                state = {
                    "flushed_at": int(time.time()),
                    "assets": {
                        a: {
                            tf: {
                                "buf_len":      len(b),
                                "latest_ts":    b.latest.ts    if b.latest else None,
                                "latest_close": b.latest.close if b.latest else None,
                            }
                            for tf, b in tfs.items()
                        }
                        for a, tfs in self._bufs.items()
                    },
                }
                self._persist.save(state)
            except Exception as exc:
                logger.error(f"Flush loop error: {exc}")

    # ------------------------------------------------------------------- main
    async def run(self) -> None:
        await self._init_session()
        self._persist = StatePersistence(CONFIG["state_file"], CONFIG["state_max_bytes"])
        self._tg      = TelegramDispatcher(
            CONFIG["telegram_bot_token"], CONFIG["telegram_chat_id"]
        )

        # Launch Web Dashboard
        self._dashboard = DashboardServer(self, CONFIG["web_host"], CONFIG["web_port"])
        await self._dashboard.start()

        await self._init_buffers()
        if self._tg.is_enabled:
            await self._tg.send(
                f"*[ENGINE STARTED]*\n"
                f"Assets: `{', '.join(CONFIG['assets'])}`\n"
                f"EMA {CONFIG['ema_fast']}/{CONFIG['ema_slow']} | "
                f"RSI {CONFIG['rsi_period']} | BB | ATR\n"
                f"`{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}`"
            )

        tasks = [
            asyncio.create_task(self._asset_loop(a, tf), name=f"{a}_{tf}")
            for a, cfg in CONFIG["assets"].items()
            for tf in cfg["timeframes"]
        ]
        tasks.append(asyncio.create_task(self._flush_loop(),       name="flush"))
        tasks.append(asyncio.create_task(
            DiskWatchdog(
                self._tg,
                CONFIG["disk_alert_threshold_pct"],
                CONFIG["disk_check_interval"],
            ).run(),
            name="watchdog",
        ))
        logger.info(f"All {len(tasks)} tasks started -- engine running 24/7")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Shutdown requested")
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self._dashboard:
                await self._dashboard.stop()
            if self._session:
                await self._session.close()
            if self._tg:
                await self._tg.close()
            logger.info("Engine stopped cleanly")


# =============================================================================
# SECTION 12: LOGGING SETUP & ENTRY POINT
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Rotating file logger + unbuffered stdout handler.
    5 MB per file, 2 backup files => max 15 MB logs on disk.
    Gracefully falls back to stdout if file write permission is denied in container.
    """
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handlers: list[logging.Handler] = []

    try:
        log_dir = os.path.dirname(CONFIG["log_file"])
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        fh = RotatingFileHandler(
            CONFIG["log_file"],
            maxBytes=CONFIG["log_max_bytes"],
            backupCount=CONFIG["log_backup_count"],
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        handlers.append(fh)
    except Exception as exc:
        print(f"[WARN] Cannot create file logger ({exc}) -> Fallback to Console-only", flush=True)

    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    handlers.append(ch)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    # Clear existing handlers to prevent duplicate lines
    root.handlers.clear()
    for h in handlers:
        root.addHandler(h)

    return logging.getLogger("indicator")


logger = setup_logging()


def main() -> None:
    try:
        logger.info("=" * 60)
        logger.info("  HOSTING INDICATOR ENGINE v1.0  (low-resource edition)")
        logger.info(f"  Python {sys.version.split()[0]}")
        logger.info(f"  Dashboard available at http://{CONFIG['web_host']}:{CONFIG['web_port']}")
        logger.info("=" * 60)
        asyncio.run(IndicatorOrchestrator().run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt -- shutting down cleanly.")
    except BaseException as exc:
        import traceback
        print("\n" + "!" * 60, file=sys.stderr, flush=True)
        print(f"  [FATAL BOOT CRASH] Unhandled Exception: {exc}", file=sys.stderr, flush=True)
        print("  Traceback:", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        print("!" * 60 + "\n", file=sys.stderr, flush=True)
        try:
            sys.stderr.flush()
            sys.stdout.flush()
        except Exception:
            pass
        # Pause 15 seconds so cloud dashboard has time to ingest and display full error logs
        time.sleep(15)
        sys.exit(1)


if __name__ == "__main__":
    main()
