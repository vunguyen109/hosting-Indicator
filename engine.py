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
import contextlib
import datetime
import json
import logging
import math
import os
import sqlite3
import sys
import tempfile
import time
import uuid
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


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
            "timeframes": ["1h", "4h"],
        },
        "XAU": {
            "name":       "Gold",
            "source":     "twelvedata",
            "symbol":     "XAU/USD",
            "timeframes": ["1h", "4h"],
        },
        "OIL": {
            "name":       "Crude Oil (USO)",
            "source":     "twelvedata",
            "symbol":     "USO",
            "timeframes": ["1h", "4h"],
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

    # Ngưỡng cảnh báo advice có khả năng lỗi thời khi giá trôi quá X lần ATR kể từ lúc tính
    "STALE_ADVICE_ATR_THRESHOLD":    1.0,

    # Ngưỡng cập nhật/làm tươi lại vùng entry_zone (bội số ATR lệch khỏi EMA20)
    "ENTRY_ZONE_REFRESH_ATR_THRESHOLD": 0.3,

    # Số nến gần nhất dùng để phát hiện biến động ngược dòng mạnh
    "COUNTER_MOVE_LOOKBACK_CANDLES": 3,
    # Ngưỡng: biến động giá (đơn vị lần ATR) trong N nến gần nhất được coi là "counter-move" đủ mạnh để nghi ngờ xu hướng EMA
    "COUNTER_MOVE_ATR_THRESHOLD":    2.0,

    # Đòn bẩy và % vốn mặc định — dùng để quy đổi và hiển thị minh họa
    "DEFAULT_LEVERAGE":             5,
    "DEFAULT_ACCOUNT_RISK_PCT":     0.10,

    # SL cho swing trading khung 1h/4h (bội số ATR)
    "SWING_SL_ATR_MULTIPLIER":      1.5,

    # Multi-tier TP định nghĩa trực tiếp bằng % LỢI NHUẬN TRÊN VỐN KÝ QUỸ mong muốn
    # price_move_pct = margin_profit_pct / leverage (với leverage=5, margin 5% -> giá 1%)
    "SWING_TP_LEVELS_MARGIN_PCT": [
        {"margin_profit_pct": 5.0,  "close_pct": 0.40, "label": "TP1 - Chốt lời sớm"},
        {"margin_profit_pct": 7.5,  "close_pct": 0.35, "label": "TP2 - Mục tiêu chính"},
        {"margin_profit_pct": 10.0, "close_pct": 0.25, "label": "TP3 - Mục tiêu tối đa"},
    ],

    # Ngưỡng đánh giá tính khả thi — tính theo % BIẾN ĐỘNG GIÁ thực tế (đã quy đổi từ margin %)
    "PRICE_MOVE_FEASIBLE_MAX_PCT":      0.03,  # price move > 3% mới coi là "tham vọng"
    "PRICE_MOVE_FEASIBLE_MIN_ATR_MULT": 1.0,   # price move < 1x ATR thì "quá gần, dễ đạt nhưng R:R kém"

    # ---- Polling timing -------------------------------------------------
    # Poll 5 seconds after candle close for exchange data to settle
    "poll_offset_seconds": 5,

    # ---- State persistence (absolute path based on BASE_DIR) ------------
    "state_file":           os.path.abspath(os.environ.get("STATE_FILE", os.path.join(BASE_DIR, "data", "state.json"))),
    "state_max_bytes":      1_500_000,   # 1.5 MB ceiling before trim
    "state_flush_interval": 900,         # flush every 15 minutes

    # ---- Logging (absolute path based on BASE_DIR) ----------------------
    "log_file":        os.path.abspath(os.environ.get("LOG_FILE", os.path.join(BASE_DIR, "logs", "app.log"))),
    "log_max_bytes":    5_242_880,       # 5 MB per file
    "log_backup_count": 2,               # 2 backup files => max 15 MB logs

    # ---- Disk watchdog --------------------------------------------------
    "disk_alert_threshold_pct": 70,      # alert when disk > 70 %
    "disk_check_interval":      21600,   # check every 6 hours

    # ---- Signal Outcome Tracker (Paper-tracking / Forward-testing) ------
    "TRACKER_ENABLED_TIMEFRAMES":   ["1h", "4h"],
    "TRACK_PENDING_EXPIRY_CANDLES": {"1h": 12, "4h": 8},
    "TRACK_OPEN_EXPIRY_CANDLES":    {"1h": 72, "4h": 30},
    "TRACKER_DEFAULT_LEVERAGE":     5,
    "TRACKER_MAX_HISTORY_RECORDS":  500,
    "TRACKER_MAX_DB_SIZE_MB":       20,
    "TRACKER_DB_PATH":              os.path.abspath(os.environ.get("TRACKER_DB_PATH", os.path.join(BASE_DIR, "data", "signal_tracker.db"))),

    # ---- Supported & Deprecated Timeframes ------------------------------
    "SUPPORTED_TIMEFRAMES":         ["1h", "4h"],
    "DEPRECATED_TIMEFRAMES":        ["15m", "15min"],
    "TIMEFRAME_ORDER":              ["1h", "4h"],
    "TRACKABLE_TIMEFRAMES":         ["1h", "4h"],
    "CONFIDENCE_HIDE_ACTION_LEVELS":["LOW", "Thấp"],
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


RSI_ZONE_LABEL_MAP: dict[str, str] = {
    "EXTREME_OVERSOLD": "Quá bán sâu",
    "LEAN_OVERSOLD": "Nghiêng bán",
    "NEUTRAL": "Cân bằng",
    "LEAN_OVERBOUGHT": "Nghiêng mua",
    "EXTREME_OVERBOUGHT": "Quá mua sâu",
}


def classify_rsi_zone(rsi: float, cfg: Optional[dict] = None) -> str:
    """Classify RSI value into one of 5 standard zones using unified thresholds (SSOT)."""
    c = cfg or CONFIG
    extreme_os = c.get("RSI_EXTREME_OVERSOLD", 25)
    lean_os = c.get("RSI_LEAN_OVERSOLD", 40)
    lean_ob = c.get("RSI_LEAN_OVERBOUGHT", 60)
    extreme_ob = c.get("RSI_EXTREME_OVERBOUGHT", 75)

    if rsi <= extreme_os:
        return "EXTREME_OVERSOLD"
    if rsi <= lean_os:
        return "LEAN_OVERSOLD"
    if rsi >= extreme_ob:
        return "EXTREME_OVERBOUGHT"
    if rsi >= lean_ob:
        return "LEAN_OVERBOUGHT"
    return "NEUTRAL"


def build_market_mood_gauge(
    rsi: float = 50.0,
    ema_trend: str = "NEUTRAL",
    bb_pos: str = "MIDDLE",
    cfg: Optional[dict] = None,
) -> dict:
    """
    Build Market Mood Gauge object with unified RSI zone classification.
    Guarantees exact label consistency across Gauge and Advice Cards.
    """
    zone = classify_rsi_zone(rsi, cfg=cfg)
    rsi_label = RSI_ZONE_LABEL_MAP.get(zone, "Cân bằng")
    score_rsi = max(0.0, min(100.0, float(rsi)))
    score_ema = 50.0
    score_bb = 50.0
    total_score = round(0.40 * score_ema + 0.35 * score_rsi + 0.25 * score_bb)
    return {
        "total_score": total_score,
        "rsi": float(rsi),
        "rsi_zone": zone,
        "rsi_label": rsi_label,
    }


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


class AdviceValidityStatus(str, Enum):
    FRESH = "fresh"              # advice còn mới, giá chưa đi xa
    STALE = "stale"              # giá đã đi khá xa, cần thận trọng
    INVALIDATED = "invalidated"  # giá đã vượt qua SL/Entry, advice vô nghĩa
    ZONE_UPDATED = "zone_updated"  # vùng chờ EMA20 đã dịch chuyển và giá hiện tại đã chạm vùng mới
    ZONE_DRIFTED = "zone_drifted"  # vùng chờ EMA20 đã dịch chuyển đáng kể so với lúc tính toán


@dataclass
class TradeAdvice:
    action: str = "WAIT"                    # BUY / SELL / WAIT
    confidence: str = "Thấp"                # Cao / Trung bình / Thấp
    confidence_score: int = 0               # 0-5, số yếu tố đồng thuận
    entry_type: str = "none"                # "now" / "wait_pullback" / "none"
    entry_price: Optional[float] = None
    entry_zone: Optional[float] = None      # giá EMA20 (vùng chờ pullback)
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None
    tp2_rule: str = ""                      # mô tả quy tắc trailing bằng text
    reasoning: list[str] = field(default_factory=list) # checklist lý do (để hiển thị UI)
    conflict_detected: bool = False
    mtf_agreement: Optional[bool] = None    # None nếu không có dữ liệu khung kia
    counter_move_detected: bool = False
    counter_move_magnitude_atr: float = 0.0
    swing_mode: bool = False
    tp_levels: list = field(default_factory=list)
    sl_margin_loss_pct: float = 0.0
    target_feasibility: str = ""
    estimated_holding_time: str = ""
    validity: dict = field(default_factory=lambda: {"status": "fresh", "reason": None})
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
            "swing_mode": self.swing_mode,
            "tp_levels": self.tp_levels,
            "sl_margin_loss_pct": self.sl_margin_loss_pct,
            "target_feasibility": self.target_feasibility,
            "estimated_holding_time": self.estimated_holding_time,
            "disclaimer": self.disclaimer,
        }


TIMEFRAME_ORDER = {"1h": 1, "4h": 2}


@dataclass
class QualityResult:
    is_official_recommendation: bool
    reject_reasons: list[str]


def evaluate_quality(advice: TradeAdvice) -> QualityResult:
    """Evaluate whether advice qualifies as an official high-conviction recommendation."""
    reasons = []
    tf = getattr(advice, "timeframe", "1h")
    trackable_tfs = CONFIG.get("TRACKABLE_TIMEFRAMES", ["1h", "4h"])
    if tf not in trackable_tfs:
        reasons.append("timeframe_not_eligible")
    if advice.action not in ("BUY", "SELL"):
        reasons.append("action_not_decisive")

    conf = getattr(advice, "confidence_level", None) or getattr(advice, "confidence", "")
    if conf not in ("HIGH", "MEDIUM", "Cao", "Trung bình"):
        reasons.append("confidence_too_low")

    val = getattr(advice, "validity", {})
    val_status = getattr(advice, "validity_status", None) or (val.get("status") if isinstance(val, dict) else str(val))
    if val_status in ("INVALIDATED", AdviceValidityStatus.INVALIDATED.value):
        reasons.append("advice_invalidated")

    entry = getattr(advice, "entry_price_or_zone", None) or getattr(advice, "entry_price", None) or getattr(advice, "entry_zone", None)
    sl = getattr(advice, "stop_loss", None)
    tp1 = getattr(advice, "tp1", None) or getattr(advice, "take_profit_1", None) or (advice.tp_levels[0]["price"] if getattr(advice, "tp_levels", None) else None)
    if not (entry and sl and tp1):
        reasons.append("incomplete_risk_geometry")

    return QualityResult(is_official_recommendation=(len(reasons) == 0), reject_reasons=reasons)


def build_advice_payload(advice: TradeAdvice) -> dict:
    """Build enriched advice payload with quality gate and action hiding for low confidence."""
    if hasattr(advice, "to_dict"):
        payload = advice.to_dict()
    elif isinstance(advice, dict):
        payload = dict(advice)
    else:
        payload = {}

    quality = evaluate_quality(advice)
    payload["is_official_recommendation"] = quality.is_official_recommendation
    payload["reject_reasons"] = quality.reject_reasons

    conf = getattr(advice, "confidence_level", None) or getattr(advice, "confidence", "")
    hide_levels = CONFIG.get("CONFIDENCE_HIDE_ACTION_LEVELS", ["LOW", "Thấp"])
    if advice.action in ("BUY", "SELL") and conf in hide_levels:
        payload["display_action"] = "HIDDEN"
        payload["display_label"] = "Tín hiệu yếu – chưa đủ điều kiện khuyến nghị"
        payload["entry_price"] = None
        payload["tp1_price"] = None
        payload["tp2_price"] = None
        payload["tp3_price"] = None
        payload["sl_price"] = None
    else:
        payload["display_action"] = advice.action
        payload["display_label"] = None

        entry = getattr(advice, "entry_price", None) or getattr(advice, "entry_zone", None)
        if entry is not None:
            payload["entry_price"] = float(entry)
        if getattr(advice, "take_profit_1", None) is not None:
            payload["tp1_price"] = float(advice.take_profit_1)
        if getattr(advice, "take_profit_2", None) is not None:
            payload["tp2_price"] = float(advice.take_profit_2)
        if getattr(advice, "tp_levels", None) and len(advice.tp_levels) > 2:
            payload["tp3_price"] = float(advice.tp_levels[2]["price"])
        elif "tp3_price" not in payload and getattr(advice, "tp_levels", None) and len(advice.tp_levels) > 0:
            payload["tp3_price"] = float(advice.tp_levels[-1]["price"])
        if getattr(advice, "stop_loss", None) is not None:
            payload["sl_price"] = float(advice.stop_loss)

    return payload


def build_timeframe_payload(asset: str, tf: str) -> dict:
    dep_tfs = CONFIG.get("DEPRECATED_TIMEFRAMES", ["15m", "15min"])
    if tf in dep_tfs:
        return {
            "timeframe": tf,
            "status": "deprecated",
            "available": False,
            "enabled": False,
            "deprecated": True,
            "deprecated_reason": "Hệ thống ngừng hỗ trợ khung 15m để tối ưu tải hosting",
            "reason": "Hệ thống ngừng hỗ trợ khung 15m để tối ưu tải hosting",
            "replacement_timeframes": ["1h", "4h"],
            "candles": [],
            "indicators": None,
            "advice": None,
        }
    return {
        "timeframe": tf,
        "status": "active",
        "available": True,
        "enabled": True,
        "deprecated": False,
        "deprecated_reason": None,
        "replacement_timeframes": [],
    }


def build_snapshot(orchestrator: Optional[Any] = None) -> dict:
    snapshot = {"timeframes": {}}
    assets = CONFIG.get("assets", {"BTC": {}, "XAU": {}, "OIL": {}})
    for a in assets:
        snapshot[a] = {}
        snapshot["timeframes"][a] = {}
        for tf in ["15m", "1h", "4h"]:
            tf_data = build_timeframe_payload(a, tf)
            snapshot[a][tf] = tf_data
            snapshot["timeframes"][a][tf] = tf_data
    if orchestrator:
        for a, tfs in getattr(orchestrator, "_latest_indicators", {}).items():
            for tf, ind in tfs.items():
                if a in snapshot and tf in snapshot[a]:
                    snapshot[a][tf]["indicators"] = ind
                    snapshot["timeframes"][a][tf]["indicators"] = ind
        for a, tfs in getattr(orchestrator, "_latest_advice", {}).items():
            for tf, adv in tfs.items():
                if a in snapshot and tf in snapshot[a]:
                    adv_payload = build_advice_payload(adv) if adv else None
                    snapshot[a][tf]["advice"] = adv_payload
                    snapshot["timeframes"][a][tf]["advice"] = adv_payload
    return snapshot


def make_advice(action="BUY", confidence="HIGH", **kwargs) -> TradeAdvice:
    """Helper for testing & simulation."""
    conf = kwargs.get("confidence", kwargs.get("confidence_level", confidence))
    entry = kwargs.get("entry_price", kwargs.get("entry", 100.0))
    sl = kwargs.get("stop_loss", kwargs.get("sl", 95.0))
    tp1 = kwargs.get("take_profit_1", kwargs.get("tp1", 105.0))
    tp2 = kwargs.get("take_profit_2", kwargs.get("tp2", 110.0))
    tp3 = kwargs.get("take_profit_3", kwargs.get("tp3", 115.0))
    tp_levels = kwargs.get("tp_levels", [
        {"label": "TP1", "price": tp1, "close_pct": 0.4, "margin_profit_pct": 5.0},
        {"label": "TP2", "price": tp2, "close_pct": 0.35, "margin_profit_pct": 7.5},
        {"label": "TP3", "price": tp3, "close_pct": 0.25, "margin_profit_pct": 10.0},
    ])
    reasoning = kwargs.get("reasoning", ["Test reasoning"])

    return TradeAdvice(
        action=action,
        confidence=conf,
        confidence_score=kwargs.get("confidence_score", 4),
        entry_type=kwargs.get("entry_type", "now"),
        entry_price=entry,
        stop_loss=sl,
        take_profit_1=tp1,
        take_profit_2=tp2,
        tp_levels=tp_levels,
        reasoning=reasoning,
    )


def replay_case_btc_1h() -> TradeAdvice:
    """Replay exact verified BTC 1h test case."""
    return TradeAdvice(
        action="SELL",
        confidence="Cao",
        confidence_score=4,
        entry_type="now",
        entry_price=81698.1219,
        stop_loss=82594.2353,
        take_profit_1=80881.1407,
        take_profit_2=80472.6501,
        tp_levels=[
            {"label": "TP1", "price": 80881.1407, "close_pct": 0.4, "margin_profit_pct": 5.0},
            {"label": "TP2", "price": 80472.6501, "close_pct": 0.35, "margin_profit_pct": 7.5},
            {"label": "TP3", "price": 80064.1594, "close_pct": 0.25, "margin_profit_pct": 10.0},
        ],
        reasoning=["Trend giảm", "RSI nghiêng bán"],
    )


def migrate_deprecated_timeframe_trades(db_conn_or_path: Any = None) -> int:
    """Standalone migration helper closing stale tracks on deprecated timeframes (15m)."""
    close_conn = False
    if db_conn_or_path is None:
        db_path = CONFIG.get("TRACKER_DB_PATH", os.path.join(BASE_DIR, "data", "signal_tracker.db"))
        conn = sqlite3.connect(db_path)
        close_conn = True
    elif isinstance(db_conn_or_path, str):
        conn = sqlite3.connect(db_conn_or_path)
        close_conn = True
    else:
        conn = db_conn_or_path

    try:
        cursor = conn.cursor()
        total_closed = 0
        for tbl in ["signal_tracks", "trade_outcomes"]:
            try:
                cursor.execute(f"""
                    UPDATE {tbl}
                    SET status = 'CLOSED_DEPRECATED',
                        close_reason = 'timeframe_deprecated'
                    WHERE timeframe IN ('15m', '15min')
                      AND UPPER(status) IN ('PENDING_ENTRY', 'OPEN', 'PARTIAL_TP1', 'PARTIAL_TP2')
                """)
                if cursor.rowcount > 0:
                    total_closed += cursor.rowcount
            except Exception:
                pass
        conn.commit()
        logger.info(f"[MIGRATION] Closed {total_closed} stale 15m tracks")
        return total_closed
    except Exception as e:
        logger.error(f"[MIGRATION] Migration error: {e}", exc_info=True)
        return 0
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass



def check_advice_validity(
    advice: TradeAdvice,
    current_price: float,
    *args,
    atr: float | None = None,
    cfg: dict | None = None,
    current_ema_fast: float | None = None,
    **kwargs,
) -> dict:
    """
    Phép so sánh NHẸ (O(1)), gọi mỗi request API — KHÔNG tính lại 
    indicator hay re-run toàn bộ decide(). Chỉ kiểm tra xem Entry/SL 
    đã tính trước đó còn ý nghĩa với giá HIỆN TẠI hay không.
    Hỗ trợ kiểm tra trôi vùng EMA20 cho entry_type="wait_pullback" (v1.3).
    """
    if len(args) == 1:
        if atr is None:
            atr = args[0]
    elif len(args) == 2:
        if atr is None:
            atr = args[0]
        if cfg is None:
            cfg = args[1]
    elif len(args) >= 3:
        if current_ema_fast is None:
            current_ema_fast = args[0]
        if atr is None:
            atr = args[1]
        if cfg is None:
            cfg = args[2]

    if cfg is None:
        cfg = CONFIG
    if atr is None:
        atr = 0.0

    if advice.action not in ("BUY", "SELL") or atr <= 0:
        return {
            "status": AdviceValidityStatus.FRESH.value,
            "reason": None,
            "price_drift_atr": 0.0,
        }

    reference_price = advice.entry_price or advice.entry_zone
    if reference_price is None:
        return {
            "status": AdviceValidityStatus.FRESH.value,
            "reason": None,
            "price_drift_atr": 0.0,
        }

    drift = current_price - reference_price
    drift_in_atr = abs(drift) / atr

    is_sell = advice.action == "SELL"

    # 1. Kiểm tra INVALIDATED trước — điều kiện nghiêm trọng nhất:
    # giá đã di chuyển theo hướng NGƯỢC LẠI và vượt qua chính mức SL
    if is_sell and advice.stop_loss is not None and current_price >= advice.stop_loss:
        return {
            "status": AdviceValidityStatus.INVALIDATED.value,
            "reason": (
                f"Giá hiện tại (${current_price:.2f}) đã vượt "
                f"qua mức Cắt Lỗ dự kiến (${advice.stop_loss:.2f}). "
                f"Khuyến nghị này không còn hợp lệ."
            ),
            "price_drift_atr": round(drift_in_atr, 2),
        }
    if not is_sell and advice.stop_loss is not None and current_price <= advice.stop_loss:
        return {
            "status": AdviceValidityStatus.INVALIDATED.value,
            "reason": (
                f"Giá hiện tại (${current_price:.2f}) đã vượt "
                f"qua mức Cắt Lỗ dự kiến (${advice.stop_loss:.2f}). "
                f"Khuyến nghị này không còn hợp lệ."
            ),
            "price_drift_atr": round(drift_in_atr, 2),
        }

    # 2. Xử lý riêng cho entry_type == "wait_pullback" (v1.3)
    if advice.entry_type == "wait_pullback" and advice.entry_zone is not None and current_ema_fast is not None:
        zone_drift = abs(current_ema_fast - advice.entry_zone)
        zone_drift_atr = zone_drift / atr if atr > 0 else 0.0
        refresh_threshold = cfg.get("ENTRY_ZONE_REFRESH_ATR_THRESHOLD", 0.3)

        if zone_drift_atr >= refresh_threshold:
            # Kiểm tra xem giá hiện tại đã "chạm" vùng EMA20 MỚI hay chưa
            distance_to_new_zone = abs(current_price - current_ema_fast) / atr if atr > 0 else 0.0
            pullback_threshold = cfg.get("ENTRY_PULLBACK_ATR_THRESHOLD", 0.3)

            if distance_to_new_zone <= pullback_threshold:
                return {
                    "status": AdviceValidityStatus.ZONE_UPDATED.value,
                    "reason": (
                        f"Vùng chờ Entry đã thay đổi theo chỉ báo "
                        f"mới nhất (EMA20 hiện tại: ${current_ema_fast:.2f}). "
                        f"Giá hiện tại đã nằm trong vùng này — "
                        f"có thể cân nhắc vào lệnh ngay thay vì chờ "
                        f"mức ${advice.entry_zone:.2f} cũ."
                    ),
                    "updated_entry_zone": round(current_ema_fast, 4),
                    "price_drift_atr": round(zone_drift_atr, 2),
                }
            else:
                return {
                    "status": AdviceValidityStatus.ZONE_DRIFTED.value,
                    "reason": (
                        f"Vùng chờ Entry đã dịch chuyển: "
                        f"EMA20 hiện tại là ${current_ema_fast:.2f}, "
                        f"không còn là ${advice.entry_zone:.2f} như "
                        f"lúc tính toán ban đầu."
                    ),
                    "updated_entry_zone": round(current_ema_fast, 4),
                    "price_drift_atr": round(zone_drift_atr, 2),
                }

    # 3. Kiểm tra STALE — áp dụng khi entry_type == "now" (hoặc không phải wait_pullback)
    threshold = cfg.get("STALE_ADVICE_ATR_THRESHOLD", 1.0)
    if drift_in_atr >= threshold:
        direction = "tăng" if drift > 0 else "giảm"
        return {
            "status": AdviceValidityStatus.STALE.value,
            "reason": (
                f"Giá đã {direction} đáng kể "
                f"(~{drift_in_atr:.1f}x ATR) kể từ lúc tính "
                f"khuyến nghị. Cân nhắc chờ cập nhật mới."
            ),
            "price_drift_atr": round(drift_in_atr, 2),
        }

    return {
        "status": AdviceValidityStatus.FRESH.value,
        "reason": None,
        "price_drift_atr": round(drift_in_atr, 2),
    }


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
        other_tf_display = {"15m": "15 phút", "15min": "15 phút", "1h": "1 giờ", "4h": "4 giờ"}.get(other_tf, other_tf)
        if other_rank > current_rank:
            return f"khung {other_tf_display} (dài hạn hơn)"
        elif other_rank < current_rank:
            return f"khung {other_tf_display} (ngắn hạn hơn)"
        else:
            return f"khung {other_tf_display}"

    def detect_counter_move(
        self,
        recent_candles: Optional[list],
        atr: float,
        trend: TrendState,
        asset: str = "",
        timeframe: str = "",
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
        detected = False
        if trend_is_down and change_in_atr > threshold:
            detected = True
        elif trend_is_up and change_in_atr < -threshold:
            detected = True

        window_closes = [
            getattr(c, "close", None) if not isinstance(c, dict) else c.get("close")
            for c in window
        ]
        trend_val = trend.value if hasattr(trend, "value") else str(trend)
        logger.debug(
            f"[{asset}/{timeframe}] counter_move check: "
            f"window_closes={window_closes}, "
            f"change_in_atr={change_in_atr:.2f}, "
            f"threshold={threshold}, trend={trend_val}, "
            f"detected={detected}"
        )
        return detected, change_in_atr

    def estimate_holding_period(self, price_distance: float, atr: float, timeframe: str) -> dict:
        """
        Ước tính thời gian cần thiết để giá di chuyển khoảng cách price_distance dựa trên ATR và timeframe.
        Tốc độ dịch chuyển trung bình khi có trend ước tính ~0.46 ATR / nến.
        """
        if atr <= 0 or price_distance <= 0:
            return {"candles": 1, "hours": 1.0, "time_label": "1h"}

        drift_rate = 0.46 * atr
        candles = max(1, round(price_distance / drift_rate))

        tf_normalized = timeframe.lower()
        if "15" in tf_normalized:
            hours = candles * 0.25
        elif "4h" in tf_normalized:
            hours = candles * 4.0
        elif "1d" in tf_normalized or "d" in tf_normalized:
            hours = candles * 24.0
        else:  # default 1h
            hours = candles * 1.0

        if hours < 24:
            time_label = f"{int(round(hours))}h"
        else:
            days = round(hours / 24, 1)
            time_label = f"{days:g}d"

        return {
            "candles": candles,
            "hours": round(hours, 1),
            "time_label": time_label,
        }

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
        zone_str = classify_rsi_zone(rsi, self.cfg)
        return RsiZone[zone_str]

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
                swing_mode=False,
                tp_levels=[],
                sl_margin_loss_pct=0.0,
                target_feasibility="",
                estimated_holding_time="",
            )

        close = snapshot["close"]
        atr = snapshot["atr"]
        trend = self.classify_ema_trend(snapshot["ema_fast"], snapshot["ema_slow"], atr)
        rsi_zone = self.classify_rsi(snapshot["rsi"])
        bb_pos = self.classify_bb_position(close, snapshot["bb_upper"], snapshot["bb_lower"])
        
        conflict_from_signals = self.detect_conflict(recent_signals, asset, timeframe, trend)
        counter_move_detected, counter_move_magnitude = self.detect_counter_move(
            recent_candles, atr, trend, asset, timeframe
        )
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
        swing_mode = False
        tp_levels_result = []
        sl_margin_loss_pct = 0.0
        target_feasibility = ""
        estimated_holding_time = ""

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

            # Swing trading logic for 1h / 4h
            if timeframe in ("1h", "4h"):
                leverage = self.cfg.get("DEFAULT_LEVERAGE", 5)
                sl_mult = self.cfg.get("SWING_SL_ATR_MULTIPLIER", 1.5)

                sl = (ref_price - sl_mult * atr) if action == Action.BUY else (ref_price + sl_mult * atr)
                risk_distance = abs(ref_price - sl)
                sl_price_move_pct = (risk_distance / ref_price * 100) if ref_price > 0 else 0.0
                sl_margin_loss_pct = sl_price_move_pct * leverage  # % vốn mất nếu dính SL

                for level in self.cfg.get("SWING_TP_LEVELS_MARGIN_PCT", []):
                    margin_target = level["margin_profit_pct"]

                    # === CÔNG THỨC CỐT LÕI ĐÃ SỬA ĐÚNG ===
                    price_move_pct_needed = margin_target / leverage   # VD: 5%/5 = 1.0%
                    price_distance = ref_price * (price_move_pct_needed / 100)

                    tp_price = (ref_price + price_distance) if action == Action.BUY else (ref_price - price_distance)
                    rr_ratio = (price_distance / risk_distance) if risk_distance > 0 else 0.0
                    holding = self.estimate_holding_period(price_distance, atr, timeframe)

                    tp_levels_result.append({
                        "label": level["label"],
                        "price": round(tp_price, 4),
                        "price_move_pct": round(price_move_pct_needed, 2),      # % GIÁ
                        "margin_profit_pct": margin_target,                      # % VỐN KÝ QUỸ
                        "rr_ratio": round(rr_ratio, 2),
                        "close_pct": int(level["close_pct"] * 100),
                        "holding_estimate": holding["time_label"],
                    })

                farthest_price_move = (tp_levels_result[-1]["price_move_pct"] / 100) if tp_levels_result else 0.0
                max_feasible = self.cfg.get("PRICE_MOVE_FEASIBLE_MAX_PCT", 0.03)

                if farthest_price_move <= max_feasible:
                    target_feasibility = (
                        f"✅ Hoàn toàn khả thi — biến động giá cần thiết nằm trong biên độ dao động thông thường của khung {timeframe}"
                    )
                else:
                    target_feasibility = (
                        "⚠️ Cần biến động giá lớn hơn bình thường — có thể mất nhiều thời gian hơn ước tính hoặc không đạt nếu thị trường đi ngang"
                    )

                swing_mode = True
                estimated_holding_time = tp_levels_result[-1]["holding_estimate"] if tp_levels_result else ""

                if tp_levels_result:
                    tp1 = tp_levels_result[0]["price"]
                    tp2 = tp_levels_result[-1]["price"]
                    tp2_rule = f"Chốt lời từng phần: {tp_levels_result[0]['label']} (40%), {tp_levels_result[1]['label']} (35%), {tp_levels_result[2]['label']} (25%)"
            else:
                # Scalping logic for intraday timeframes (e.g. 15m)
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
            swing_mode=swing_mode,
            tp_levels=tp_levels_result,
            sl_margin_loss_pct=round(sl_margin_loss_pct, 2),
            target_feasibility=target_feasibility,
            estimated_holding_time=estimated_holding_time,
        )


# =============================================================================
# SECTION 4.6: SIGNAL OUTCOME TRACKER (Paper-tracking / Forward-testing)
# =============================================================================

class TrackStatus(str, Enum):
    PENDING_ENTRY = "pending_entry"
    OPEN = "open"
    PARTIAL_TP1 = "partial_tp1"
    PARTIAL_TP2 = "partial_tp2"
    CLOSED_TP3 = "closed_tp3"
    CLOSED_SL = "closed_sl"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"
    AMBIGUOUS = "ambiguous"


@dataclass
class SignalTrack:
    track_id: str
    created_at_ts: int
    created_at_str: str

    asset: str
    timeframe: str
    direction: str                 # BUY / SELL
    confidence: str
    confidence_score: int

    status: str                    # TrackStatus
    entry_type: str                # now / wait_pullback
    planned_entry: float
    actual_entry: float | None
    entry_filled_at_ts: int | None

    stop_loss: float
    tp_levels: list                # copy immutable từ advice.tp_levels

    initial_advice_validity: str
    advice_reasoning: list[str]

    tp1_hit_at_ts: int | None = None
    tp2_hit_at_ts: int | None = None
    tp3_hit_at_ts: int | None = None
    sl_hit_at_ts: int | None = None
    closed_at_ts: int | None = None

    realized_r: float = 0.0
    realized_margin_pnl_pct: float = 0.0

    max_favorable_excursion_r: float = 0.0
    max_adverse_excursion_r: float = 0.0

    expiry_at_ts: int = 0
    result_note: str = ""

    def to_dict(self) -> dict:
        return {
            "track_id": self.track_id,
            "created_at_ts": self.created_at_ts,
            "created_at_str": self.created_at_str,
            "asset": self.asset,
            "timeframe": self.timeframe,
            "direction": self.direction,
            "confidence": self.confidence,
            "confidence_score": self.confidence_score,
            "status": self.status,
            "entry_type": self.entry_type,
            "planned_entry": self.planned_entry,
            "actual_entry": self.actual_entry,
            "entry_filled_at_ts": self.entry_filled_at_ts,
            "stop_loss": self.stop_loss,
            "tp_levels": self.tp_levels,
            "tp1": self.tp_levels[0]["price"] if len(self.tp_levels) > 0 else None,
            "tp2": self.tp_levels[1]["price"] if len(self.tp_levels) > 1 else None,
            "tp3": self.tp_levels[2]["price"] if len(self.tp_levels) > 2 else None,
            "initial_advice_validity": self.initial_advice_validity,
            "advice_reasoning": self.advice_reasoning,
            "tp1_hit_at_ts": self.tp1_hit_at_ts,
            "tp2_hit_at_ts": self.tp2_hit_at_ts,
            "tp3_hit_at_ts": self.tp3_hit_at_ts,
            "sl_hit_at_ts": self.sl_hit_at_ts,
            "closed_at_ts": self.closed_at_ts,
            "realized_r": round(self.realized_r, 4),
            "realized_margin_pnl_pct": round(self.realized_margin_pnl_pct, 2),
            "expiry_at_ts": self.expiry_at_ts,
            "result_note": self.result_note,
        }


def _normalize_pct(val: float | int) -> float:
    """Helper to convert close_pct (e.g. 40 or 0.40) to fraction 0..1."""
    v = float(val)
    return v / 100.0 if v > 1.0 else v


class SignalOutcomeTracker:
    """
    Paper-tracking & Forward-testing engine for TradeAdvice.
    Evaluates outcomes strictly based on closed OHLC bars (zero look-ahead bias).
    Bounded SQLite persistence with WAL mode & automated cleanup.
    """

    def __init__(self, db_path: str = None, config: dict = None) -> None:
        self.config = config or CONFIG
        raw_db = db_path or self.config.get("TRACKER_DB_PATH", os.path.join(BASE_DIR, "data", "signal_tracker.db"))
        self.db_path = os.path.abspath(raw_db)
        self._active_tracks: dict[str, SignalTrack] = {}  # key: f"{asset}_{timeframe}"
        self._last_vacuum_ts: float = 0.0
        self._cached_metrics: Optional[dict] = None
        self._cached_metrics_ts: float = 0.0
        self._init_db()
        self._load_active_tracks()

    @contextlib.contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
            except Exception as e:
                logger.error(f"[Tracker] Could not create directory for SQLite: {e}")

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS signal_tracks (
                        track_id TEXT PRIMARY KEY,
                        created_at_ts INTEGER NOT NULL,
                        asset TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        confidence TEXT NOT NULL,
                        confidence_score INTEGER NOT NULL,
                        status TEXT NOT NULL,

                        entry_type TEXT NOT NULL,
                        planned_entry REAL NOT NULL,
                        actual_entry REAL,

                        stop_loss REAL NOT NULL,
                        tp1 REAL,
                        tp2 REAL,
                        tp3 REAL,

                        realized_r REAL DEFAULT 0,
                        realized_margin_pnl_pct REAL DEFAULT 0,

                        entry_filled_at_ts INTEGER,
                        closed_at_ts INTEGER,
                        expiry_at_ts INTEGER NOT NULL,

                        result_note TEXT,
                        payload_json TEXT NOT NULL
                    );
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_signal_tracks_asset_tf
                    ON signal_tracks(asset, timeframe);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_signal_tracks_status
                    ON signal_tracks(status);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_signal_tracks_created
                    ON signal_tracks(created_at_ts);
                """)
                conn.commit()
            logger.info(f"[Tracker] SQLite initialized successfully at {self.db_path}")
        except Exception as exc:
            logger.error(f"[Tracker] Database initialization failed: {exc}")

    def _load_active_tracks(self) -> None:
        """Load pending and open tracks from database into memory upon startup."""
        active_statuses = (
            TrackStatus.PENDING_ENTRY.value,
            TrackStatus.OPEN.value,
            TrackStatus.PARTIAL_TP1.value,
            TrackStatus.PARTIAL_TP2.value,
        )
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                q = f"SELECT payload_json FROM signal_tracks WHERE status IN ({','.join(['?']*len(active_statuses))})"
                cursor.execute(q, active_statuses)
                rows = cursor.fetchall()
                for r in rows:
                    try:
                        p = json.loads(r["payload_json"])
                        track = SignalTrack(
                            track_id=p["track_id"],
                            created_at_ts=p["created_at_ts"],
                            created_at_str=p["created_at_str"],
                            asset=p["asset"],
                            timeframe=p["timeframe"],
                            direction=p["direction"],
                            confidence=p["confidence"],
                            confidence_score=p["confidence_score"],
                            status=p["status"],
                            entry_type=p["entry_type"],
                            planned_entry=p["planned_entry"],
                            actual_entry=p.get("actual_entry"),
                            entry_filled_at_ts=p.get("entry_filled_at_ts"),
                            stop_loss=p["stop_loss"],
                            tp_levels=p.get("tp_levels", []),
                            initial_advice_validity=p.get("initial_advice_validity", "fresh"),
                            advice_reasoning=p.get("advice_reasoning", []),
                            tp1_hit_at_ts=p.get("tp1_hit_at_ts"),
                            tp2_hit_at_ts=p.get("tp2_hit_at_ts"),
                            tp3_hit_at_ts=p.get("tp3_hit_at_ts"),
                            sl_hit_at_ts=p.get("sl_hit_at_ts"),
                            closed_at_ts=p.get("closed_at_ts"),
                            realized_r=p.get("realized_r", 0.0),
                            realized_margin_pnl_pct=p.get("realized_margin_pnl_pct", 0.0),
                            expiry_at_ts=p.get("expiry_at_ts", 0),
                            result_note=p.get("result_note", ""),
                        )
                        key = f"{track.asset}_{track.timeframe}"
                        self._active_tracks[key] = track
                    except Exception as parse_err:
                        logger.error(f"[Tracker] Error parsing track JSON: {parse_err}")
            logger.info(f"[Tracker] Loaded {len(self._active_tracks)} active tracks from DB.")
        except Exception as exc:
            logger.error(f"[Tracker] Failed to load active tracks: {exc}")

    def _save_track(self, track: SignalTrack) -> None:
        """Upsert a track record into SQLite."""
        try:
            tp1 = track.tp_levels[0]["price"] if len(track.tp_levels) > 0 else None
            tp2 = track.tp_levels[1]["price"] if len(track.tp_levels) > 1 else None
            tp3 = track.tp_levels[2]["price"] if len(track.tp_levels) > 2 else None
            payload_json = json.dumps(track.to_dict(), ensure_ascii=False)

            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO signal_tracks (
                        track_id, created_at_ts, asset, timeframe, direction,
                        confidence, confidence_score, status, entry_type,
                        planned_entry, actual_entry, stop_loss, tp1, tp2, tp3,
                        realized_r, realized_margin_pnl_pct, entry_filled_at_ts,
                        closed_at_ts, expiry_at_ts, result_note, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(track_id) DO UPDATE SET
                        status = excluded.status,
                        actual_entry = excluded.actual_entry,
                        realized_r = excluded.realized_r,
                        realized_margin_pnl_pct = excluded.realized_margin_pnl_pct,
                        entry_filled_at_ts = excluded.entry_filled_at_ts,
                        closed_at_ts = excluded.closed_at_ts,
                        expiry_at_ts = excluded.expiry_at_ts,
                        result_note = excluded.result_note,
                        payload_json = excluded.payload_json;
                """, (
                    track.track_id, track.created_at_ts, track.asset, track.timeframe, track.direction,
                    track.confidence, track.confidence_score, track.status, track.entry_type,
                    track.planned_entry, track.actual_entry, track.stop_loss, tp1, tp2, tp3,
                    track.realized_r, track.realized_margin_pnl_pct, track.entry_filled_at_ts,
                    track.closed_at_ts, track.expiry_at_ts, track.result_note, payload_json
                ))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"[Tracker] Database write error (SQLite): {e}", exc_info=True)
        except Exception as exc:
            logger.error(f"[Tracker] Database write error for track {track.track_id}: {exc}", exc_info=True)

    def _trim_database_if_needed(self) -> None:
        """Keep database bounded within TRACKER_MAX_HISTORY_RECORDS and TRACKER_MAX_DB_SIZE_MB."""
        max_records = self.config.get("TRACKER_MAX_HISTORY_RECORDS", 500)
        max_mb = self.config.get("TRACKER_MAX_DB_SIZE_MB", 20)

        terminal_statuses = (
            TrackStatus.CLOSED_TP3.value,
            TrackStatus.CLOSED_SL.value,
            TrackStatus.EXPIRED.value,
            TrackStatus.INVALIDATED.value,
            TrackStatus.AMBIGUOUS.value,
        )

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as cnt FROM signal_tracks")
                total_cnt = cursor.fetchone()["cnt"]

                if total_cnt > max_records:
                    delete_count = total_cnt - max_records
                    cursor.execute(f"""
                        DELETE FROM signal_tracks
                        WHERE track_id IN (
                            SELECT track_id FROM signal_tracks
                            WHERE status IN ({','.join(['?']*len(terminal_statuses))})
                            ORDER BY created_at_ts ASC
                            LIMIT ?
                        )
                    """, (*terminal_statuses, delete_count))
                    conn.commit()
                    logger.info(f"[Tracker] Trimmed {delete_count} old terminal tracks from database.")

            # Check DB file size
            if os.path.exists(self.db_path):
                db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
                now = time.time()
                if db_size_mb > max_mb and (now - self._last_vacuum_ts) > 86400:
                    with self._get_connection() as conn:
                        conn.execute("VACUUM;")
                    self._last_vacuum_ts = now
                    logger.info(f"[Tracker] DB size {db_size_mb:.2f}MB exceeded {max_mb}MB. VACUUM executed.")
        except Exception as exc:
            logger.warning(f"[Tracker] Database trim error: {exc}")

    def should_create_track(self, advice: TradeAdvice, asset: str, timeframe: str) -> bool:
        """Deduplication & eligibility rules for creating a new SignalTrack."""
        enabled_tfs = self.config.get("TRACKER_ENABLED_TIMEFRAMES", ["1h", "4h"])
        if timeframe not in enabled_tfs:
            return False

        if advice.action not in ("BUY", "SELL"):
            return False

        if advice.confidence not in ("Cao", "Trung bình"):
            return False

        validity = getattr(advice, "validity", {})
        val_status = validity.get("status") if isinstance(validity, dict) else str(validity)
        if val_status == AdviceValidityStatus.INVALIDATED.value:
            return False

        entry = advice.entry_price or advice.entry_zone
        if not entry or not advice.stop_loss:
            return False

        tp1 = advice.take_profit_1 or (advice.tp_levels[0]["price"] if advice.tp_levels else None)
        if not tp1:
            return False

        # Stream deduplication check
        stream_key = f"{asset}_{timeframe}"
        active = self._active_tracks.get(stream_key)
        if active:
            active_statuses = (
                TrackStatus.PENDING_ENTRY.value,
                TrackStatus.OPEN.value,
                TrackStatus.PARTIAL_TP1.value,
                TrackStatus.PARTIAL_TP2.value,
            )
            if active.status in active_statuses:
                if active.direction != advice.action:
                    logger.warning(
                        f"[Tracker] [{asset} {timeframe}] Opposite advice {advice.action} received while "
                        f"track {active.track_id} ({active.direction}) is active ({active.status}). "
                        f"Preserving existing track until resolution."
                    )
                return False

        return True

    def maybe_create_track(
        self,
        asset: str,
        timeframe: str,
        advice: TradeAdvice,
        candle_ts: int,
    ) -> Optional[SignalTrack]:
        """Conditionally create a new SignalTrack if advice passes criteria."""
        if not self.should_create_track(advice, asset, timeframe):
            return None

        planned_entry = float(advice.entry_price or advice.entry_zone)
        stop_loss = float(advice.stop_loss)

        # Build clean immutable tp_levels (maximum 3 levels)
        tp_levels: list[dict] = []
        if advice.tp_levels:
            for item in advice.tp_levels[:3]:
                tp_levels.append({
                    "label": item.get("label", "TP"),
                    "price": float(item.get("price", 0.0)),
                    "close_pct": _normalize_pct(item.get("close_pct", 0.4)),
                    "margin_profit_pct": float(item.get("margin_profit_pct", 5.0)),
                })
        else:
            tp_levels = [
                {"label": "TP1", "price": float(advice.take_profit_1 or planned_entry), "close_pct": 0.40, "margin_profit_pct": 5.0},
            ]
            if advice.take_profit_2:
                tp_levels.append(
                    {"label": "TP2", "price": float(advice.take_profit_2), "close_pct": 0.35, "margin_profit_pct": 7.5}
                )

        tf_sec = TF_SECONDS.get(timeframe, 3600)
        pending_candles = self.config.get("TRACK_PENDING_EXPIRY_CANDLES", {}).get(timeframe, 12)
        expiry_at_ts = int(candle_ts + pending_candles * tf_sec)

        val = getattr(advice, "validity", {})
        val_status = val.get("status", "fresh") if isinstance(val, dict) else str(val)

        created_str = datetime.datetime.fromtimestamp(candle_ts, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        track = SignalTrack(
            track_id=uuid.uuid4().hex,
            created_at_ts=int(candle_ts),
            created_at_str=created_str,
            asset=asset,
            timeframe=timeframe,
            direction=advice.action,
            confidence=advice.confidence,
            confidence_score=int(advice.confidence_score),
            status=TrackStatus.PENDING_ENTRY.value,
            entry_type=advice.entry_type or "now",
            planned_entry=round(planned_entry, 4),
            actual_entry=None,
            entry_filled_at_ts=None,
            stop_loss=round(stop_loss, 4),
            tp_levels=tp_levels,
            initial_advice_validity=val_status,
            advice_reasoning=list(advice.reasoning) if advice.reasoning else [],
            expiry_at_ts=expiry_at_ts,
            result_note="Đang chờ khớp điểm vào lệnh (Pending Entry).",
        )

        stream_key = f"{asset}_{timeframe}"
        self._active_tracks[stream_key] = track
        self._save_track(track)
        self._cached_metrics = None  # invalidate metrics cache
        logger.info(f"[Tracker] Created track {track.track_id[:8]} [{asset} {timeframe} {track.direction}] @ {track.planned_entry}")
        return track

    def process_closed_candle(
        self,
        asset: str,
        timeframe: str,
        candle: Candle,
        latest_advice: Optional[TradeAdvice] = None,
    ) -> Optional[SignalTrack]:
        """
        Evaluate existing track on candle close.
        Executes strict anti-look-ahead bias policies.
        """
        enabled_tfs = self.config.get("TRACKER_ENABLED_TIMEFRAMES", ["1h", "4h"])
        if timeframe not in enabled_tfs:
            return None

        stream_key = f"{asset}_{timeframe}"
        track = self._active_tracks.get(stream_key)
        if not track:
            return None

        leverage = self.config.get("TRACKER_DEFAULT_LEVERAGE", 5)

        # ---------------------------------------------------------------------
        # 1. Track is in PENDING_ENTRY
        # ---------------------------------------------------------------------
        if track.status == TrackStatus.PENDING_ENTRY.value:
            # Check if advice was invalidated before entry
            if latest_advice:
                val = getattr(latest_advice, "validity", {})
                val_status = val.get("status") if isinstance(val, dict) else str(val)
                if val_status == AdviceValidityStatus.INVALIDATED.value:
                    track.status = TrackStatus.INVALIDATED.value
                    track.closed_at_ts = int(candle.ts)
                    track.result_note = "Khuyến nghị bị vô hiệu hóa trước khi giá khớp Entry."
                    self._finish_track(track)
                    return track

            # Check fill criteria
            filled = False
            actual_entry = track.planned_entry

            if track.entry_type == "now":
                # Fills on the next closed candle after track creation
                if track.direction == "BUY":
                    actual_entry = max(track.planned_entry, candle.open)
                else:
                    actual_entry = min(track.planned_entry, candle.open)
                filled = True
            elif track.entry_type == "wait_pullback":
                if candle.low <= track.planned_entry <= candle.high:
                    filled = True
                    # Check gap over entry:
                    # BUY pullback chờ giá thấp hơn: gap xảy ra nếu nến mở cửa nhảy xuống dưới entry
                    # SELL pullback chờ giá cao hơn: gap xảy ra nếu nến mở cửa nhảy lên trên entry
                    if track.direction == "BUY" and candle.open < track.planned_entry:
                        actual_entry = max(track.planned_entry, candle.open)
                    elif track.direction == "SELL" and candle.open > track.planned_entry:
                        actual_entry = min(track.planned_entry, candle.open)
                    else:
                        actual_entry = track.planned_entry

            if filled:
                track.status = TrackStatus.OPEN.value
                track.actual_entry = round(float(actual_entry), 4)
                track.entry_filled_at_ts = int(candle.ts)
                tf_sec = TF_SECONDS.get(timeframe, 3600)
                open_candles = self.config.get("TRACK_OPEN_EXPIRY_CANDLES", {}).get(timeframe, 72)
                track.expiry_at_ts = int(candle.ts + open_candles * tf_sec)
                track.result_note = f"Đã khớp Entry tại {track.actual_entry:.4f}. Bắt đầu theo dõi TP/SL."
                self._save_track(track)
                logger.info(f"[Tracker] Track {track.track_id[:8]} filled Entry @ {track.actual_entry}")
                # ANTI-LOOK-AHEAD BIAS: Do NOT evaluate TP/SL on the same candle that filled Entry!
                return track

            # If not filled, check expiry
            if candle.ts >= track.expiry_at_ts:
                track.status = TrackStatus.EXPIRED.value
                track.closed_at_ts = int(candle.ts)
                track.result_note = "Lệnh chờ hết hạn theo dõi do không khớp Entry trong số nến quy định."
                self._finish_track(track)
                return track

            return track

        # ---------------------------------------------------------------------
        # 2. Track is OPEN, PARTIAL_TP1, or PARTIAL_TP2
        # ---------------------------------------------------------------------
        if track.status in (TrackStatus.OPEN.value, TrackStatus.PARTIAL_TP1.value, TrackStatus.PARTIAL_TP2.value):
            # Check expiry first
            if candle.ts >= track.expiry_at_ts:
                # Force close remaining position at candle close
                self._close_remaining_at_price(track, candle.close, leverage, candle.ts, "Lệnh hết thời hạn theo dõi, đóng mô phỏng tại giá đóng cửa hiện tại.")
                track.status = TrackStatus.EXPIRED.value
                self._finish_track(track)
                return track

            # Evaluate TP / SL on this closed candle
            risk_unit = abs(track.actual_entry - track.stop_loss)
            if risk_unit < 1e-6:
                risk_unit = 1e-6

            def r_at_exit(p: float) -> float:
                return (p - track.actual_entry) / risk_unit if track.direction == "BUY" else (track.actual_entry - p) / risk_unit

            def margin_pnl_at_exit(p: float) -> float:
                pct = ((p - track.actual_entry) / track.actual_entry) if track.direction == "BUY" else ((track.actual_entry - p) / track.actual_entry)
                return pct * leverage * 100.0

            tp1_p = track.tp_levels[0]["price"] if len(track.tp_levels) > 0 else None
            tp2_p = track.tp_levels[1]["price"] if len(track.tp_levels) > 1 else None
            tp3_p = track.tp_levels[2]["price"] if len(track.tp_levels) > 2 else None

            if track.direction == "BUY":
                sl_hit = candle.low <= track.stop_loss
                tp1_hit = (candle.high >= tp1_p) if tp1_p else False
                tp2_hit = (candle.high >= tp2_p) if tp2_p else False
                tp3_hit = (candle.high >= tp3_p) if tp3_p else False
            else:
                sl_hit = candle.high >= track.stop_loss
                tp1_hit = (candle.low <= tp1_p) if tp1_p else False
                tp2_hit = (candle.low <= tp2_p) if tp2_p else False
                tp3_hit = (candle.low <= tp3_p) if tp3_p else False

            # Check next target TP
            next_tp_hit = False
            if track.status == TrackStatus.OPEN.value:
                next_tp_hit = tp1_hit
            elif track.status == TrackStatus.PARTIAL_TP1.value:
                next_tp_hit = tp2_hit
            elif track.status == TrackStatus.PARTIAL_TP2.value:
                next_tp_hit = tp3_hit

            # Ambiguous Candle check: Both SL and next Target TP breached in the same candle
            if sl_hit and next_tp_hit:
                track.status = TrackStatus.AMBIGUOUS.value
                track.closed_at_ts = int(candle.ts)
                track.result_note = (
                    "Nến cùng lúc chạm Stop Loss và Take Profit. "
                    "Dữ liệu OHLC không xác định được thứ tự khớp, "
                    "do đó không tính thắng/thua vào thống kê chính."
                )
                self._finish_track(track)
                return track

            pct1 = _normalize_pct(track.tp_levels[0].get("close_pct", 0.40)) if len(track.tp_levels) > 0 else 0.40
            pct2 = _normalize_pct(track.tp_levels[1].get("close_pct", 0.35)) if len(track.tp_levels) > 1 else 0.35
            pct3 = _normalize_pct(track.tp_levels[2].get("close_pct", 0.25)) if len(track.tp_levels) > 2 else 0.25

            # ---------------- Case A: Status is OPEN ----------------
            if track.status == TrackStatus.OPEN.value:
                if tp1_hit:
                    track.tp1_hit_at_ts = int(candle.ts)
                    track.realized_r += r_at_exit(tp1_p) * pct1
                    track.realized_margin_pnl_pct += margin_pnl_at_exit(tp1_p) * pct1
                    track.status = TrackStatus.PARTIAL_TP1.value
                    track.result_note = f"Đạt TP1 tại {tp1_p:.4f}, chốt {int(pct1*100)}% vị thế."

                    if tp2_hit and tp2_p:
                        track.tp2_hit_at_ts = int(candle.ts)
                        track.realized_r += r_at_exit(tp2_p) * pct2
                        track.realized_margin_pnl_pct += margin_pnl_at_exit(tp2_p) * pct2
                        track.status = TrackStatus.PARTIAL_TP2.value
                        track.result_note = f"Đạt TP1 và TP2 tại {tp2_p:.4f}, chốt {int((pct1+pct2)*100)}% vị thế."

                        if tp3_hit and tp3_p:
                            track.tp3_hit_at_ts = int(candle.ts)
                            track.realized_r += r_at_exit(tp3_p) * pct3
                            track.realized_margin_pnl_pct += margin_pnl_at_exit(tp3_p) * pct3
                            track.status = TrackStatus.CLOSED_TP3.value
                            track.closed_at_ts = int(candle.ts)
                            track.result_note = f"Đạt toàn bộ TP1-TP3, hoàn tất 100% vị thế thành công."
                            self._finish_track(track)
                            return track

                    self._save_track(track)
                    return track

                elif sl_hit:
                    track.sl_hit_at_ts = int(candle.ts)
                    track.closed_at_ts = int(candle.ts)
                    track.status = TrackStatus.CLOSED_SL.value
                    track.realized_r = -1.0
                    track.realized_margin_pnl_pct = margin_pnl_at_exit(track.stop_loss)
                    track.result_note = f"Chạm Stop Loss tại {track.stop_loss:.4f} (-1.0R)."
                    self._finish_track(track)
                    return track

            # ---------------- Case B: Status is PARTIAL_TP1 ----------------
            elif track.status == TrackStatus.PARTIAL_TP1.value:
                if tp2_hit and tp2_p:
                    track.tp2_hit_at_ts = int(candle.ts)
                    track.realized_r += r_at_exit(tp2_p) * pct2
                    track.realized_margin_pnl_pct += margin_pnl_at_exit(tp2_p) * pct2
                    track.status = TrackStatus.PARTIAL_TP2.value
                    track.result_note = f"Đạt TP2 tại {tp2_p:.4f}, chốt thêm {int(pct2*100)}% vị thế."

                    if tp3_hit and tp3_p:
                        track.tp3_hit_at_ts = int(candle.ts)
                        track.realized_r += r_at_exit(tp3_p) * pct3
                        track.realized_margin_pnl_pct += margin_pnl_at_exit(tp3_p) * pct3
                        track.status = TrackStatus.CLOSED_TP3.value
                        track.closed_at_ts = int(candle.ts)
                        track.result_note = f"Đạt toàn bộ TP3, hoàn tất 100% vị thế thành công."
                        self._finish_track(track)
                        return track

                    self._save_track(track)
                    return track

                elif sl_hit:
                    remaining_fraction = max(0.0, 1.0 - pct1)
                    track.sl_hit_at_ts = int(candle.ts)
                    track.closed_at_ts = int(candle.ts)
                    track.status = TrackStatus.CLOSED_SL.value
                    track.realized_r += (-1.0) * remaining_fraction
                    track.realized_margin_pnl_pct += margin_pnl_at_exit(track.stop_loss) * remaining_fraction
                    track.result_note = f"Chạm Stop Loss phần {int(remaining_fraction*100)}% vị thế còn lại sau khi đã chốt TP1."
                    self._finish_track(track)
                    return track

            # ---------------- Case C: Status is PARTIAL_TP2 ----------------
            elif track.status == TrackStatus.PARTIAL_TP2.value:
                if tp3_hit and tp3_p:
                    track.tp3_hit_at_ts = int(candle.ts)
                    track.realized_r += r_at_exit(tp3_p) * pct3
                    track.realized_margin_pnl_pct += margin_pnl_at_exit(tp3_p) * pct3
                    track.status = TrackStatus.CLOSED_TP3.value
                    track.closed_at_ts = int(candle.ts)
                    track.result_note = f"Đạt mục tiêu tối đa TP3 tại {tp3_p:.4f}. Hoàn tất toàn bộ vị thế."
                    self._finish_track(track)
                    return track

                elif sl_hit:
                    remaining_fraction = max(0.0, 1.0 - pct1 - pct2)
                    track.sl_hit_at_ts = int(candle.ts)
                    track.closed_at_ts = int(candle.ts)
                    track.status = TrackStatus.CLOSED_SL.value
                    track.realized_r += (-1.0) * remaining_fraction
                    track.realized_margin_pnl_pct += margin_pnl_at_exit(track.stop_loss) * remaining_fraction
                    track.result_note = f"Chạm Stop Loss phần {int(remaining_fraction*100)}% vị thế còn lại sau khi đã chốt TP1 & TP2."
                    self._finish_track(track)
                    return track

        return track

    def _close_remaining_at_price(
        self,
        track: SignalTrack,
        price: float,
        leverage: int,
        closed_ts: int,
        note: str,
    ) -> None:
        """Close whatever fraction remains of an open track at a specific price (e.g. on expiry)."""
        pct1 = _normalize_pct(track.tp_levels[0].get("close_pct", 0.40)) if len(track.tp_levels) > 0 else 0.40
        pct2 = _normalize_pct(track.tp_levels[1].get("close_pct", 0.35)) if len(track.tp_levels) > 1 else 0.35

        closed_pct = 0.0
        if track.status == TrackStatus.PARTIAL_TP1.value:
            closed_pct = pct1
        elif track.status == TrackStatus.PARTIAL_TP2.value:
            closed_pct = pct1 + pct2

        rem_pct = max(0.0, 1.0 - closed_pct)
        if rem_pct > 0.0 and track.actual_entry:
            risk_unit = abs(track.actual_entry - track.stop_loss) or 1e-6
            r_exit = (price - track.actual_entry) / risk_unit if track.direction == "BUY" else (track.actual_entry - price) / risk_unit
            pnl_pct = (((price - track.actual_entry) / track.actual_entry) if track.direction == "BUY" else ((track.actual_entry - price) / track.actual_entry)) * leverage * 100.0

            track.realized_r += r_exit * rem_pct
            track.realized_margin_pnl_pct += pnl_pct * rem_pct

        track.closed_at_ts = int(closed_ts)
        track.result_note = note

    def _finish_track(self, track: SignalTrack) -> None:
        """Move track from active state to terminal state in database and refresh caches."""
        stream_key = f"{track.asset}_{track.timeframe}"
        self._active_tracks.pop(stream_key, None)
        track.realized_r = round(track.realized_r, 4)
        track.realized_margin_pnl_pct = round(track.realized_margin_pnl_pct, 2)
        self._save_track(track)
        self._cached_metrics = None
        self._trim_database_if_needed()
        logger.info(f"[Tracker] Finished track {track.track_id[:8]} [{track.asset} {track.timeframe}] Status={track.status} RealizedR={track.realized_r:.2f}R")

    def get_metrics(
        self,
        asset: str = "ALL",
        timeframe: str = "ALL",
        confidence: str = "ALL",
    ) -> dict:
        """
        Compute performance metrics for closed tracks.
        Cached for O(1) polling responses.
        """
        now = time.time()
        is_default = (asset == "ALL" and timeframe == "ALL" and confidence == "ALL")
        if is_default and self._cached_metrics and (now - self._cached_metrics_ts) < 900:
            return self._cached_metrics

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                query = "SELECT * FROM signal_tracks WHERE 1=1"
                params: list = []
                if asset != "ALL":
                    query += " AND asset = ?"
                    params.append(asset)
                if timeframe != "ALL":
                    query += " AND timeframe = ?"
                    params.append(timeframe)
                if confidence != "ALL":
                    query += " AND confidence = ?"
                    params.append(confidence)

                query += " ORDER BY created_at_ts ASC"
                cursor.execute(query, params)
                rows = cursor.fetchall()

                total_tracks = len(rows)
                active_statuses = (
                    TrackStatus.PENDING_ENTRY.value,
                    TrackStatus.OPEN.value,
                    TrackStatus.PARTIAL_TP1.value,
                    TrackStatus.PARTIAL_TP2.value,
                )
                active_count = 0
                pending_count = 0
                open_count = 0

                closed_count = 0
                win_count = 0
                loss_count = 0
                expired_count = 0
                invalidated_count = 0
                ambiguous_count = 0

                sum_win_r = 0.0
                sum_loss_r = 0.0
                total_realized_r = 0.0

                filled_tracks = 0
                tp1_hit_count = 0
                tp2_hit_count = 0
                tp3_hit_count = 0

                equity_curve: list[dict] = []
                cumulative_r = 0.0
                peak_r = 0.0
                max_drawdown = 0.0

                for r in rows:
                    st = r["status"]
                    rr = float(r["realized_r"] or 0.0)

                    if st in active_statuses:
                        active_count += 1
                        if st == TrackStatus.PENDING_ENTRY.value:
                            pending_count += 1
                        else:
                            open_count += 1

                    if r["actual_entry"] is not None:
                        filled_tracks += 1

                    # Parse payload to check tp hits
                    try:
                        p = json.loads(r["payload_json"])
                        if p.get("tp1_hit_at_ts"):
                            tp1_hit_count += 1
                        if p.get("tp2_hit_at_ts"):
                            tp2_hit_count += 1
                        if p.get("tp3_hit_at_ts"):
                            tp3_hit_count += 1
                    except Exception:
                        pass

                    if st in (TrackStatus.CLOSED_TP3.value, TrackStatus.CLOSED_SL.value, TrackStatus.EXPIRED.value):
                        closed_count += 1
                        total_realized_r += rr
                        if rr > 0:
                            win_count += 1
                            sum_win_r += rr
                        elif rr < 0:
                            loss_count += 1
                            sum_loss_r += abs(rr)

                        cumulative_r += rr
                        if cumulative_r > peak_r:
                            peak_r = cumulative_r
                        dd = cumulative_r - peak_r
                        if dd < max_drawdown:
                            max_drawdown = dd

                        equity_curve.append({
                            "track_id": r["track_id"][:8],
                            "closed_at_ts": r["closed_at_ts"] or r["created_at_ts"],
                            "realized_r": round(rr, 2),
                            "cumulative_r": round(cumulative_r, 2),
                        })

                    elif st == TrackStatus.INVALIDATED.value:
                        invalidated_count += 1
                    elif st == TrackStatus.AMBIGUOUS.value:
                        ambiguous_count += 1

                entry_fill_rate = round((filled_tracks / total_tracks * 100.0), 1) if total_tracks > 0 else 0.0
                tp1_rate = round((tp1_hit_count / filled_tracks * 100.0), 1) if filled_tracks > 0 else 0.0
                tp2_rate = round((tp2_hit_count / filled_tracks * 100.0), 1) if filled_tracks > 0 else 0.0
                tp3_rate = round((tp3_hit_count / filled_tracks * 100.0), 1) if filled_tracks > 0 else 0.0

                valid_closed = win_count + loss_count
                win_rate = round((win_count / valid_closed * 100.0), 1) if valid_closed > 0 else 0.0
                profit_factor = round(sum_win_r / sum_loss_r, 2) if sum_loss_r > 0 else None
                expectancy_r = round(total_realized_r / closed_count, 2) if closed_count > 0 else 0.0

                warning = None
                if closed_count < 30:
                    warning = "Chưa đủ mẫu thống kê — cần tối thiểu 30 lệnh đã đóng hợp lệ."

                metrics = {
                    "total_tracks": total_tracks,
                    "active_tracks": active_count,
                    "pending_count": pending_count,
                    "open_count": open_count,
                    "closed_count": closed_count,
                    "win_count": win_count,
                    "loss_count": loss_count,
                    "expired_count": expired_count,
                    "invalidated_count": invalidated_count,
                    "ambiguous_count": ambiguous_count,
                    "entry_fill_rate_pct": entry_fill_rate,
                    "tp1_hit_rate_pct": tp1_rate,
                    "tp2_hit_rate_pct": tp2_rate,
                    "tp3_hit_rate_pct": tp3_rate,
                    "win_rate_pct": win_rate,
                    "profit_factor": profit_factor,
                    "expectancy_r": expectancy_r,
                    "average_r": expectancy_r,
                    "total_r": round(total_realized_r, 2),
                    "max_drawdown_r": round(max_drawdown, 2),
                    "equity_curve_r": equity_curve[-500:],
                    "sample_size_warning": warning,
                }

                if is_default:
                    self._cached_metrics = metrics
                    self._cached_metrics_ts = now

                return metrics
        except Exception as exc:
            logger.error(f"[Tracker] Error calculating metrics: {exc}")
            return {
                "total_tracks": 0, "active_tracks": 0, "closed_count": 0,
                "win_count": 0, "loss_count": 0, "entry_fill_rate_pct": 0.0,
                "win_rate_pct": 0.0, "profit_factor": None, "expectancy_r": 0.0,
                "total_r": 0.0, "max_drawdown_r": 0.0, "equity_curve_r": [],
                "sample_size_warning": "Lỗi truy vấn cơ sở dữ liệu.",
            }

    def get_recent_tracks(
        self,
        asset: str = "ALL",
        timeframe: str = "ALL",
        confidence: str = "ALL",
        limit: int = 20,
    ) -> list[dict]:
        """Fetch latest tracks from database ordered by creation date descending."""
        limit = min(max(int(limit), 5), 100)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                query = "SELECT payload_json FROM signal_tracks WHERE 1=1"
                params: list = []
                if asset != "ALL":
                    query += " AND asset = ?"
                    params.append(asset)
                if timeframe != "ALL":
                    query += " AND timeframe = ?"
                    params.append(timeframe)
                if confidence != "ALL":
                    query += " AND confidence = ?"
                    params.append(confidence)

                query += " ORDER BY created_at_ts DESC LIMIT ?"
                params.append(limit)

                cursor.execute(query, params)
                rows = cursor.fetchall()
                results = []
                for r in rows:
                    try:
                        results.append(json.loads(r["payload_json"]))
                    except Exception:
                        pass
                return results
        except Exception as exc:
            logger.error(f"[Tracker] Error retrieving recent tracks: {exc}")
            return []

    def get_summary(self) -> dict:
        """Lightweight summary to embed inside /api/snapshot without extra query overhead."""
        m = self.get_metrics("ALL", "ALL", "ALL")
        return {
            "active_tracks": m.get("active_tracks", 0),
            "closed_count": m.get("closed_count", 0),
            "profit_factor": m.get("profit_factor"),
            "expectancy_r": m.get("expectancy_r", 0.0),
            "win_rate_pct": m.get("win_rate_pct", 0.0),
            "sample_size_warning": m.get("sample_size_warning"),
        }


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

    INTERVAL_MAP: dict[str, str] = {
        "1h": "1h",
        "4h": "4h",
    }

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

    INTERVAL_MAP: dict[str, str] = {
        "1h": "1h",
        "4h": "4h",
    }

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
    /* Multi-tier TP Container for Swing Mode */
    .swing-tp-container {
      background: rgba(15, 23, 42, 0.7);
      border: 1px solid rgba(59, 130, 246, 0.25);
      border-radius: 8px;
      padding: 12px 14px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .swing-tp-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.8rem;
      font-weight: 700;
      color: #93c5fd;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      padding-bottom: 6px;
    }
    .leverage-badge {
      background: rgba(59, 130, 246, 0.15);
      border: 1px solid rgba(59, 130, 246, 0.3);
      color: #60a5fa;
      font-size: 0.72rem;
      padding: 2px 8px;
      border-radius: 9999px;
    }
    .tp-table {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .tp-row {
      display: grid;
      grid-template-columns: 1.4fr 1.3fr 1.3fr 0.7fr auto;
      align-items: center;
      gap: 6px;
      padding: 8px 10px;
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 6px;
      font-size: 0.8rem;
    }
    @media (max-width: 640px) {
      .tp-row {
        grid-template-columns: 1fr 1fr;
        gap: 8px;
      }
    }
    .tp-row .tp-label { font-weight: 700; color: #f1f5f9; }
    .tp-row .tp-price { font-weight: 700; color: #34d399; font-family: monospace; }
    .tp-row .tp-price small { color: var(--text-muted); font-size: 0.72rem; margin-left: 2px; }
    .tp-row .tp-margin-profit { color: #60a5fa; font-weight: 700; }
    .tp-row .tp-holding { color: var(--text-muted); font-size: 0.72rem; }
    .feasibility-box {
      font-size: 0.78rem;
      line-height: 1.4;
      padding: 8px 10px;
      background: rgba(16, 185, 129, 0.08);
      border-left: 3px solid #10b981;
      border-radius: 4px;
      color: #d1fae5;
    }
    .feasibility-box.warn {
      background: rgba(245, 158, 11, 0.08);
      border-left-color: #f59e0b;
      color: #fef3c7;
    }
    .tp-explanation-note {
      font-size: 0.72rem;
      color: #94a3b8;
      line-height: 1.4;
      background: rgba(255, 255, 255, 0.02);
      border-radius: 4px;
      padding: 6px 8px;
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
    /* Performance Tracker Styles */
    .perf-disclaimer {
      background: rgba(245, 158, 11, 0.08);
      border-left: 4px solid var(--gold);
      padding: 10px 14px;
      border-radius: 6px;
      font-size: 0.8rem;
      color: #fef3c7;
      margin-bottom: 16px;
      line-height: 1.5;
    }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }
    .kpi-card {
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .kpi-label {
      font-size: 0.75rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .kpi-val {
      font-size: 1.45rem;
      font-weight: 700;
      color: var(--text);
    }
    .kpi-sub {
      font-size: 0.72rem;
      color: var(--text-muted);
    }
    .val-green { color: var(--green) !important; }
    .val-yellow { color: var(--gold) !important; }
    .val-red { color: var(--red) !important; }
    .val-muted { color: var(--text-muted) !important; }

    .perf-filter-bar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-bottom: 14px;
      padding: 8px 12px;
      background: rgba(255, 255, 255, 0.02);
      border-radius: 8px;
    }
    .perf-filter-label {
      font-size: 0.75rem;
      color: var(--text-muted);
      margin-right: 4px;
    }
    .perf-filter-btn {
      background: #1e293b;
      color: var(--text-muted);
      border: 1px solid var(--card-border);
      padding: 4px 10px;
      border-radius: 6px;
      font-size: 0.75rem;
      cursor: pointer;
      transition: all 0.2s;
    }
    .perf-filter-btn.active, .perf-filter-btn:hover {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }

    .svg-equity-container {
      width: 100%;
      height: 220px;
      background: rgba(0, 0, 0, 0.2);
      border-radius: 8px;
      border: 1px solid var(--card-border);
      position: relative;
      margin-bottom: 20px;
      overflow: hidden;
    }
    .svg-equity-container svg {
      width: 100%;
      height: 100%;
      display: block;
    }
    .status-badge-track {
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 0.72rem;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }
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
        <button class="tab-btn" id="btnTf15m" style="display:none;" onclick="selectTimeframe('15m')">15 Phút (Ngắn hạn)</button>
        <button class="tab-btn active" id="btnTf1h" onclick="selectTimeframe('1h')">1 Giờ (Trung hạn)</button>
        <button class="tab-btn" id="btnTf4h" onclick="selectTimeframe('4h')">4 Giờ (Swing)</button>
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
              <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                <div id="adviceActionBadge" class="advice-action-badge action-wait">
                  <span id="adviceActionText">WAIT</span>
                </div>
                <span id="adviceMetaBadge" class="leverage-badge" style="display:none;">⚡ Đòn bẩy x5 · Vốn 10%</span>
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
                <span class="level-sub" id="adviceSlSub">R:R 1:1 bảo toàn vốn</span>
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

            <!-- Multi-Tier TP Table cho Swing Mode (1h/4h) -->
            <div id="adviceSwingTpBox" class="swing-tp-container" style="display:none;">
              <div class="swing-tp-header">
                <span>🎯 Mục Tiêu Chốt Lời Đa Tầng (Multi-Tier TP)</span>
                <span class="leverage-badge">⚡ Lãi tính trên Vốn Ký Quỹ</span>
              </div>
              <div class="tp-table" id="adviceTpTable">
                <!-- Rendered dynamically -->
              </div>
              <div class="feasibility-box" id="adviceFeasibilityBox"></div>
              <div class="tp-explanation-note">
                ⚠️ <strong>Lưu ý quan trọng:</strong> Cột "% Lãi trên vốn ký quỹ" đã nhân đòn bẩy x5. Đây KHÔNG phải % biến động giá thực tế của tài sản (chỉ cần di chuyển 1%–2% là đạt mục tiêu 5%–10% lãi).
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

    <!-- SECTION: HIỆU SUẤT ĐỀ XUẤT (PAPER-TRACKING) -->
    <div class="card" id="cardPerformanceTracker">
      <div class="card-header">
        <div class="card-title">
          <span>📊 Hiệu Suất Đề Xuất (Paper-Tracking Forward-Test)</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <span id="sampleWarningBadge" class="status-badge" style="background:rgba(245,158,11,0.12);color:var(--gold);border-color:rgba(245,158,11,0.3);display:none;">⚠️ Chưa đủ dữ liệu</span>
          <span style="font-size:0.8rem;color:var(--text-muted);" id="perfLastUpdated">--</span>
        </div>
      </div>

      <!-- 11.1 Disclaimer -->
      <div class="perf-disclaimer">
        ⚠️ <strong>Lưu ý quan trọng:</strong> Đây là kết quả theo dõi giả lập dựa trên nến OHLC đã đóng. Chưa bao gồm spread, phí giao dịch, funding, swap và trượt giá thực tế. Không phải cam kết lợi nhuận hoặc lời khuyên đầu tư.
      </div>

      <!-- Filters -->
      <div class="perf-filter-bar">
        <span class="perf-filter-label">Tài sản:</span>
        <button class="perf-filter-btn active" data-filter="asset" data-val="ALL" onclick="setPerfFilter('asset', 'ALL')">Tất cả</button>
        <button class="perf-filter-btn" data-filter="asset" data-val="BTC" onclick="setPerfFilter('asset', 'BTC')">BTC</button>
        <button class="perf-filter-btn" data-filter="asset" data-val="XAU" onclick="setPerfFilter('asset', 'XAU')">XAU</button>
        <button class="perf-filter-btn" data-filter="asset" data-val="OIL" onclick="setPerfFilter('asset', 'OIL')">OIL</button>

        <span class="perf-filter-label" style="margin-left:10px;">Khung:</span>
        <button class="perf-filter-btn active" data-filter="tf" data-val="ALL" onclick="setPerfFilter('tf', 'ALL')">Tất cả</button>
        <button class="perf-filter-btn" data-filter="tf" data-val="1h" onclick="setPerfFilter('tf', '1h')">1h</button>
        <button class="perf-filter-btn" data-filter="tf" data-val="4h" onclick="setPerfFilter('tf', '4h')">4h</button>

        <span class="perf-filter-label" style="margin-left:10px;">Độ tin cậy:</span>
        <button class="perf-filter-btn active" data-filter="conf" data-val="ALL" onclick="setPerfFilter('conf', 'ALL')">Tất cả</button>
        <button class="perf-filter-btn" data-filter="conf" data-val="Cao" onclick="setPerfFilter('conf', 'Cao')">Cao</button>
        <button class="perf-filter-btn" data-filter="conf" data-val="Trung bình" onclick="setPerfFilter('conf', 'Trung bình')">Trung bình</button>

        <span class="perf-filter-label" style="margin-left:10px;">Hướng:</span>
        <button class="perf-filter-btn active" data-filter="dir" data-val="ALL" onclick="setPerfFilter('dir', 'ALL')">Tất cả</button>
        <button class="perf-filter-btn" data-filter="dir" data-val="BUY" onclick="setPerfFilter('dir', 'BUY')">BUY</button>
        <button class="perf-filter-btn" data-filter="dir" data-val="SELL" onclick="setPerfFilter('dir', 'SELL')">SELL</button>
      </div>

      <!-- 11.2 KPI Cards -->
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-label">Profit Factor</div>
          <div class="kpi-val" id="kpiProfitFactor">--</div>
          <div class="kpi-sub" id="kpiProfitFactorSub">Tổng lời R / Tổng lỗ R</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Expectancy (Kỳ vọng)</div>
          <div class="kpi-val" id="kpiExpectancy">--</div>
          <div class="kpi-sub">Kỳ vọng lợi nhuận / lệnh</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Max Drawdown</div>
          <div class="kpi-val val-red" id="kpiMaxDrawdown">--</div>
          <div class="kpi-sub">Sụt giảm lớn nhất từ đỉnh</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Win Rate</div>
          <div class="kpi-val" id="kpiWinRate">--</div>
          <div class="kpi-sub" id="kpiWinCountSub">0 thắng / 0 lệnh</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Entry Fill Rate</div>
          <div class="kpi-val" id="kpiFillRate">--</div>
          <div class="kpi-sub">Tỉ lệ tín hiệu khớp điểm vào</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Mẫu Đã Đóng Hợp Lệ</div>
          <div class="kpi-val" id="kpiSampleSize">--</div>
          <div class="kpi-sub" id="kpiSampleSizeSub">Yêu cầu tối thiểu 30 lệnh</div>
        </div>
      </div>

      <!-- 11.3 Equity Curve Chart (Pure SVG) -->
      <div style="margin-bottom:8px;font-size:0.85rem;font-weight:600;display:flex;justify-content:space-between;align-items:center;">
        <span>📈 Biểu Đồ Tăng Trưởng Vốn (Equity Curve theo R-Multiple)</span>
        <span style="font-size:0.75rem;color:var(--text-muted);">Trục X: Số lệnh | Trục Y: Tổng R tích lũy</span>
      </div>
      <div class="svg-equity-container" id="equityChartContainer">
        <svg id="equitySvg" viewBox="0 0 800 220" preserveAspectRatio="none">
          <text x="400" y="110" fill="#94a3b8" font-size="14" text-anchor="middle">Đang tải dữ liệu tăng trưởng...</text>
        </svg>
      </div>

      <!-- 11.4 Recent Tracks Table -->
      <div style="margin-bottom:8px;font-size:0.85rem;font-weight:600;">
        <span>📋 Lịch Sử Khuyến Nghị Gần Đây</span>
      </div>
      <div class="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Thời gian</th>
              <th>Tài sản</th>
              <th>Khung</th>
              <th>Hướng</th>
              <th>Độ tin cậy</th>
              <th style="text-align:right;">Entry</th>
              <th>Kết quả / Ghi chú</th>
              <th style="text-align:right;">Realized R</th>
              <th>Trạng thái</th>
            </tr>
          </thead>
          <tbody id="recentTracksTbody">
            <tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px;">Đang tải danh sách theo dõi...</td></tr>
          </tbody>
        </table>
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
    let currentTimeframe = '1h';
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
        const tfParam = (currentAsset !== 'BTC' && currentTimeframe === '15m') ? '15min' : currentTimeframe;
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
      if (rsi >= 75) textRsi = '🔥 Quá mua';
      else if (rsi >= 60) textRsi = '📈 Nghiêng mua';
      else if (rsi <= 25) textRsi = '❄️ Quá bán';
      else if (rsi <= 40) textRsi = '📉 Nghiêng bán';

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

            let tfName = tf.includes('15') ? '15 Phút' : (tf.includes('4') ? '4 Giờ' : '1 Giờ');

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
        const targetTf = (currentAsset !== 'BTC' && currentTimeframe === '15m') ? '15min' : currentTimeframe;
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
            const tfLabel = s.timeframe.includes('15') ? '15 phút' : (s.timeframe.includes('4') ? '4 giờ' : '1 giờ');
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

    function copyRaw(val) {
      if (!val) return;
      navigator.clipboard.writeText(val.toString()).then(() => {
        showToast('Đã copy: ' + val);
      }).catch(() => {
        const ta = document.createElement('textarea');
        ta.value = val;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        showToast('Đã copy: ' + val);
      });
    }

    // --- RENDER TRADE ADVICE CARD ---
    function renderAdviceCard(snapshot, asset, tf) {
      if (!snapshot || !snapshot.advice) return;
      const targetTf = (asset !== 'BTC' && tf === '15m') ? '15min' : tf;
      const advice = snapshot.advice?.[asset]?.[targetTf];

      const badgeAsset = document.getElementById('adviceAssetBadge');
      if (badgeAsset) badgeAsset.textContent = `[${asset} · ${tf}]`;

      const levelsGrid = document.getElementById('adviceLevelsGrid');
      const swingTpBox = document.getElementById('adviceSwingTpBox');
      const waitMessage = document.getElementById('adviceWaitMessage');
      const metaBadge = document.getElementById('adviceMetaBadge');
      const cardEl = document.getElementById('cardTradeAdvice') || document.getElementById('advice-card');
      const bodyEl = cardEl ? cardEl.querySelector('.advice-card-body') : null;
      const targetContainer = bodyEl || cardEl;

      // Xóa banner cảnh báo cũ nếu có
      const oldBanner = document.getElementById('advice-stale-banner');
      if (oldBanner) oldBanner.remove();

      if (!advice) {
        if (waitMessage) {
          waitMessage.style.display = 'flex';
          waitMessage.innerHTML = '<span>⏳</span><span>Đang thu thập dữ liệu nến cho tài sản này...</span>';
        }
        if (levelsGrid) levelsGrid.style.display = 'none';
        if (swingTpBox) swingTpBox.style.display = 'none';
        if (metaBadge) metaBadge.style.display = 'none';
        return;
      }

      // Xử lý Validity Banner (Fresh / Stale / Invalidated)
      const validity = advice.validity || { status: 'fresh', reason: null, price_drift_atr: 0.0 };
      const setLevelsOpacity = (op) => {
        if (levelsGrid) levelsGrid.style.opacity = op;
        if (swingTpBox) swingTpBox.style.opacity = op;
      };

      if (validity.status === 'invalidated') {
        const banner = document.createElement('div');
        banner.id = 'advice-stale-banner';
        banner.style.cssText = 'background: rgba(153, 27, 27, 0.45); border: 2px solid #ef4444; border-radius: 8px; padding: 12px 14px; margin-bottom: 14px;';
        banner.innerHTML = `
          <div style="display:flex; align-items:center; gap:8px; margin-bottom:4px;">
            <span style="font-size:1.15rem;">🚫</span>
            <strong style="color:#fca5a5; font-size:0.95rem; text-transform:uppercase; letter-spacing:0.5px;">KHUYẾN NGHỊ ĐÃ VÔ HIỆU</strong>
          </div>
          <p style="color:#fee2e2; font-size:0.85rem; margin:0 0 6px 0; line-height:1.4;">${validity.reason || 'Giá hiện tại đã vượt qua mức Cắt Lỗ dự kiến. Khuyến nghị này không còn hợp lệ.'}</p>
          <p style="color:#fca5a5; font-size:0.75rem; margin:0; opacity:0.9;">⏳ Vui lòng chờ nến tiếp theo đóng cửa để hệ thống cập nhật khuyến nghị mới.</p>
        `;
        if (targetContainer) {
          targetContainer.prepend(banner);
        }
        setLevelsOpacity('0.35');
      } else if (validity.status === 'zone_updated') {
        const banner = document.createElement('div');
        banner.id = 'advice-stale-banner';
        banner.style.cssText = 'background: rgba(6, 78, 59, 0.55); border: 2px solid #10b981; border-radius: 8px; padding: 12px 14px; margin-bottom: 14px;';
        const newZoneStr = validity.updated_entry_zone != null ? ('$' + Number(validity.updated_entry_zone).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4})) : '';
        banner.innerHTML = `
          <div style="display:flex; align-items:center; gap:8px; margin-bottom:4px;">
            <span style="font-size:1.15rem;">🎯</span>
            <strong style="color:#6ee7b7; font-size:0.95rem; text-transform:uppercase; letter-spacing:0.5px;">VÙNG ENTRY ĐÃ ĐẠT ${newZoneStr ? `(${newZoneStr})` : ''}</strong>
          </div>
          <p style="color:#d1fae5; font-size:0.85rem; margin:0 0 4px 0; line-height:1.4;">${validity.reason || 'Vùng chờ Entry đã thay đổi theo chỉ báo mới nhất. Giá hiện tại đã nằm trong vùng này — có thể cân nhắc vào lệnh ngay.'}</p>
        `;
        if (targetContainer) {
          targetContainer.prepend(banner);
        }
        setLevelsOpacity('1');
      } else if (validity.status === 'zone_drifted') {
        const banner = document.createElement('div');
        banner.id = 'advice-stale-banner';
        banner.style.cssText = 'background: rgba(146, 64, 14, 0.4); border: 1px solid #f59e0b; border-radius: 8px; padding: 10px 12px; margin-bottom: 14px;';
        banner.innerHTML = `
          <div style="display:flex; align-items:center; gap:8px;">
            <span style="font-size:1rem;">⚠️</span>
            <span style="color:#fef08a; font-size:0.85rem; line-height:1.4;">${validity.reason || 'Vùng chờ Entry đã dịch chuyển theo EMA20 mới.'}</span>
          </div>
        `;
        if (targetContainer) {
          targetContainer.prepend(banner);
        }
        setLevelsOpacity('0.9');
      } else if (validity.status === 'stale') {
        const banner = document.createElement('div');
        banner.id = 'advice-stale-banner';
        banner.style.cssText = 'background: rgba(146, 64, 14, 0.4); border: 1px solid #f59e0b; border-radius: 8px; padding: 10px 12px; margin-bottom: 14px;';
        banner.innerHTML = `
          <div style="display:flex; align-items:center; gap:8px;">
            <span style="font-size:1rem;">⚠️</span>
            <span style="color:#fef08a; font-size:0.85rem; line-height:1.4;">${validity.reason || 'Giá đã trôi khá xa kể từ lúc tính khuyến nghị. Cân nhắc chờ cập nhật mới.'}</span>
          </div>
        `;
        if (targetContainer) {
          targetContainer.prepend(banner);
        }
        setLevelsOpacity('0.7');
      } else {
        setLevelsOpacity('1');
      }

      // Action Badge & Meta Profile
      const actionBadge = document.getElementById('adviceActionBadge');
      const actionText = document.getElementById('adviceActionText');
      if (actionBadge && actionText) {
        actionText.textContent = advice.action;
        actionBadge.className = 'advice-action-badge ' + (
          advice.action === 'BUY' ? 'action-buy' :
          advice.action === 'SELL' ? 'action-sell' : 'action-wait'
        );
      }
      if (metaBadge) {
        metaBadge.style.display = (advice.action !== 'WAIT') ? 'inline-block' : 'none';
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
      if (advice.display_action === 'HIDDEN') {
        if (actionBadge && actionText) {
          actionText.textContent = 'TRUNG LẬP';
          actionBadge.className = 'advice-action-badge action-wait';
        }
        if (metaBadge) metaBadge.style.display = 'none';
        setLevelsOpacity('1');
        if (levelsGrid) levelsGrid.style.display = 'none';
        if (swingTpBox) swingTpBox.style.display = 'none';
        if (waitMessage) {
          waitMessage.style.display = 'flex';
          waitMessage.innerHTML = '<span>⏸</span><span>' + (advice.display_label || 'Tín hiệu yếu – chưa đủ điều kiện khuyến nghị') + '</span>';
        }
      } else if (advice.action === 'WAIT') {
        setLevelsOpacity('1');
        if (levelsGrid) levelsGrid.style.display = 'none';
        if (swingTpBox) swingTpBox.style.display = 'none';
        if (waitMessage) {
          waitMessage.style.display = 'flex';
          waitMessage.innerHTML = '<span>⏸</span><span>Chưa có điểm vào lệnh phù hợp lúc này — hệ thống đang chờ tín hiệu rõ ràng hơn.</span>';
        }
      } else {
        if (waitMessage) waitMessage.style.display = 'none';
        if (levelsGrid) levelsGrid.style.display = 'grid';

        const liveClose = snapshot.indicators?.[asset]?.[targetTf]?.close;
        const livePriceStr = liveClose != null ? ('$' + Number(liveClose).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4})) : '--';

        const entryValEl = document.getElementById('adviceEntryVal');
        const entrySubEl = document.getElementById('adviceEntrySub');
        if (advice.entry_type === 'now') {
          entryValEl.textContent = '$' + Number(advice.entry_price).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          entryValEl.setAttribute('data-raw', advice.entry_price);
          entrySubEl.innerHTML = `📌 Entry tính tại nến đóng. Giá live: <span id="live-price-compare" style="color:var(--text-main); font-weight:600;">${livePriceStr}</span>`;
        } else {
          entryValEl.textContent = '$' + Number(advice.entry_zone).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          entryValEl.setAttribute('data-raw', advice.entry_zone);
          const liveEma = snapshot.indicators?.[asset]?.[targetTf]?.ema_fast;
          const liveEmaStr = (validity && validity.updated_entry_zone != null)
            ? ('$' + Number(validity.updated_entry_zone).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4}))
            : (liveEma != null ? ('$' + Number(liveEma).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4})) : '--');
          entrySubEl.innerHTML = `⏳ Chờ giá hồi về EMA20. Vùng EMA20 live: <span style="color:#6ee7b7; font-weight:600;">${liveEmaStr}</span> | Giá live: <span id="live-price-compare" style="color:var(--text-main); font-weight:600;">${livePriceStr}</span>`;
        }

        const slValEl = document.getElementById('adviceSlVal');
        const slSubEl = document.getElementById('adviceSlSub');
        if (slValEl) {
          slValEl.textContent = '$' + Number(advice.stop_loss).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          slValEl.setAttribute('data-raw', advice.stop_loss);
        }
        if (slSubEl) {
          if (advice.swing_mode && advice.sl_margin_loss_pct) {
            slSubEl.textContent = `Nếu dính SL: mất ~${advice.sl_margin_loss_pct}% vốn ký quỹ (x5)`;
            slSubEl.style.color = '#f87171';
          } else {
            slSubEl.textContent = 'R:R 1:1 bảo toàn vốn';
            slSubEl.style.color = 'var(--text-muted)';
          }
        }

        const tp1ValEl = document.getElementById('adviceTp1Val');
        const tp2SubEl = document.getElementById('adviceTp2Sub');
        if (tp1ValEl) {
          tp1ValEl.textContent = '$' + Number(advice.take_profit_1).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4});
          tp1ValEl.setAttribute('data-raw', advice.take_profit_1);
        }
        if (tp2SubEl) {
          if (advice.swing_mode && advice.tp_levels && advice.tp_levels.length > 0) {
            tp2SubEl.textContent = 'Multi-Tier TP (Xem chi tiết bảng bên dưới)';
          } else {
            tp2SubEl.textContent = `TP2: $${Number(advice.take_profit_2).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4})} (${advice.tp2_rule || 'Trailing EMA'})`;
          }
        }

        // Render Swing Multi-Tier TP Table
        if (swingTpBox) {
          if (advice.swing_mode && advice.tp_levels && advice.tp_levels.length > 0) {
            swingTpBox.style.display = 'flex';
            const tpTableEl = document.getElementById('adviceTpTable');
            if (tpTableEl) {
              const sign = (advice.action === 'SELL') ? '−' : '+';
              tpTableEl.innerHTML = advice.tp_levels.map(lvl => `
                <div class="tp-row">
                  <span class="tp-label">${lvl.label} (đóng ${lvl.close_pct}%)</span>
                  <span class="tp-price">$${Number(lvl.price).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4})} <small>(${sign}${Math.abs(lvl.price_move_pct)}% giá)</small></span>
                  <span class="tp-margin-profit">+${lvl.margin_profit_pct}% vốn ký quỹ</span>
                  <span class="tp-holding">~${lvl.holding_estimate}</span>
                  <button class="btn-copy-mini" onclick="copyRaw('${lvl.price}')">📋 Copy</button>
                </div>
              `).join('');
            }
            const feasEl = document.getElementById('adviceFeasibilityBox');
            if (feasEl) {
              feasEl.textContent = advice.target_feasibility;
              feasEl.className = 'feasibility-box' + (advice.target_feasibility.includes('⚠️') ? ' warn' : '');
            }
          } else {
            swingTpBox.style.display = 'none';
          }
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
        const targetTf = (asset !== 'BTC' && currentTimeframe === '15m') ? '15min' : currentTimeframe;
        if (cachedSnapshot.indicators) {
          updateCompositeGauge(cachedSnapshot.indicators[asset]?.[targetTf]);
        }
        renderAdviceCard(cachedSnapshot, asset, currentTimeframe);
      }
    }

    function selectTimeframe(tf) {
      currentTimeframe = tf;
      ['15m', '1h', '4h'].forEach(t => {
        const btn = document.getElementById('btnTf' + t);
        if (btn) btn.classList.toggle('active', t === tf);
      });
      document.getElementById('activeChartTitle').textContent = `[${currentAsset} · ${tf}]`;
      fetchCandleData();
      if (cachedSnapshot) {
        const targetTf = (currentAsset !== 'BTC' && tf === '15m') ? '15min' : tf;
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

    // --- PERFORMANCE TRACKER (PAPER-TRACKING FORWARD-TEST) ---
    const perfFilters = {
      asset: 'ALL',
      tf: 'ALL',
      conf: 'ALL',
      dir: 'ALL'
    };
    let cachedPerfJsonStr = '';

    function setPerfFilter(category, val) {
      perfFilters[category] = val;
      document.querySelectorAll(`.perf-filter-btn[data-filter="${category}"]`).forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-val') === val);
      });
      fetchPerformanceData();
    }

    async function fetchPerformanceData() {
      try {
        const query = new URLSearchParams({
          asset: perfFilters.asset,
          tf: perfFilters.tf,
          confidence: perfFilters.conf,
          limit: '50'
        });
        const res = await fetch(`/api/performance?${query.toString()}`);
        if (!res.ok) return;
        const data = await res.json();
        const jsonStr = JSON.stringify(data);
        if (jsonStr === cachedPerfJsonStr) return; // Không redraw nếu dữ liệu không đổi
        cachedPerfJsonStr = jsonStr;
        renderPerformance(data);
      } catch (err) {
        console.error('Error fetching performance metrics:', err);
      }
    }

    function renderPerformance(data) {
      if (!data || !data.metrics) return;
      const m = data.metrics;

      // Update timestamp
      const timeEl = document.getElementById('perfLastUpdated');
      if (timeEl) timeEl.textContent = data.last_updated || '';

      // Sample size warning badge
      const warnBadge = document.getElementById('sampleWarningBadge');
      if (warnBadge) {
        if (m.sample_size_warning) {
          warnBadge.style.display = 'inline-flex';
          warnBadge.textContent = '⚠️ ' + m.sample_size_warning;
        } else {
          warnBadge.style.display = 'none';
        }
      }

      // 1. Profit Factor
      const pfEl = document.getElementById('kpiProfitFactor');
      const pfSub = document.getElementById('kpiProfitFactorSub');
      if (pfEl) {
        if (m.closed_count < 30) {
          pfEl.textContent = m.profit_factor !== null ? m.profit_factor.toFixed(2) : '--';
          pfEl.className = 'kpi-val val-muted';
          if (pfSub) pfSub.textContent = 'Chưa đủ 30 mẫu để khẳng định';
        } else if (m.profit_factor === null) {
          pfEl.textContent = 'N/A';
          pfEl.className = 'kpi-val val-muted';
          if (pfSub) pfSub.textContent = 'Không có lệnh lỗ';
        } else {
          pfEl.textContent = m.profit_factor.toFixed(2);
          if (m.profit_factor > 1.2) pfEl.className = 'kpi-val val-green';
          else if (m.profit_factor >= 1.0) pfEl.className = 'kpi-val val-yellow';
          else pfEl.className = 'kpi-val val-red';
          if (pfSub) pfSub.textContent = m.profit_factor >= 1.0 ? 'Hệ thống có lợi thế thống kê' : 'Hệ thống đang âm vốn';
        }
      }

      // 2. Expectancy
      const expEl = document.getElementById('kpiExpectancy');
      if (expEl) {
        const sign = m.expectancy_r > 0 ? '+' : '';
        expEl.textContent = `${sign}${m.expectancy_r.toFixed(2)}R / lệnh`;
        expEl.className = 'kpi-val ' + (m.expectancy_r > 0 ? 'val-green' : (m.expectancy_r < 0 ? 'val-red' : 'val-muted'));
      }

      // 3. Max Drawdown
      const ddEl = document.getElementById('kpiMaxDrawdown');
      if (ddEl) {
        ddEl.textContent = `${m.max_drawdown_r.toFixed(2)}R`;
      }

      // 4. Win Rate
      const wrEl = document.getElementById('kpiWinRate');
      const wcSub = document.getElementById('kpiWinCountSub');
      if (wrEl) {
        wrEl.textContent = `${m.win_rate_pct.toFixed(1)}%`;
        if (wcSub) wcSub.textContent = `${m.win_count} thắng / ${m.win_count + m.loss_count} lệnh đóng`;
      }

      // 5. Entry Fill Rate
      const frEl = document.getElementById('kpiFillRate');
      if (frEl) {
        frEl.textContent = `${m.entry_fill_rate_pct.toFixed(1)}%`;
      }

      // 6. Sample Size
      const ssEl = document.getElementById('kpiSampleSize');
      const ssSub = document.getElementById('kpiSampleSizeSub');
      if (ssEl) {
        ssEl.textContent = `${m.closed_count} / 30`;
        if (m.closed_count >= 30) {
          ssEl.className = 'kpi-val val-green';
          if (ssSub) ssSub.textContent = 'Đủ điều kiện thống kê tin cậy';
        } else {
          ssEl.className = 'kpi-val val-yellow';
          if (ssSub) ssSub.textContent = `Cần thêm ${30 - m.closed_count} lệnh nữa`;
        }
      }

      // Render Equity Curve (SVG)
      renderEquitySvg(m.equity_curve_r || []);

      // Render Recent Tracks Table
      renderRecentTracks(data.recent_tracks || []);
    }

    function renderEquitySvg(curve) {
      const svg = document.getElementById('equitySvg');
      if (!svg) return;

      if (!curve || curve.length === 0) {
        svg.innerHTML = `
          <line x1="0" y1="110" x2="800" y2="110" stroke="#334155" stroke-dasharray="4,4" />
          <text x="400" y="115" fill="#94a3b8" font-size="13" text-anchor="middle">Chưa có lệnh đóng để vẽ biểu đồ tăng trưởng (cần ít nhất 1 lệnh kết thúc)</text>
        `;
        return;
      }

      const points = [{ cumulative_r: 0.0, track_id: 'Start' }, ...curve];
      let minR = 0.0;
      let maxR = 0.0;
      points.forEach(p => {
        if (p.cumulative_r < minR) minR = p.cumulative_r;
        if (p.cumulative_r > maxR) maxR = p.cumulative_r;
      });

      const range = (maxR - minR) || 2.0;
      const padY = range * 0.15;
      const yMin = minR - padY;
      const yMax = maxR + padY;

      const W = 800;
      const H = 220;
      const pL = 50;
      const pR = 20;
      const pT = 20;
      const pB = 30;

      function toX(idx) {
        if (points.length <= 1) return pL;
        return pL + (idx / (points.length - 1)) * (W - pL - pR);
      }
      function toY(val) {
        return pT + (1 - (val - yMin) / (yMax - yMin)) * (H - pT - pB);
      }

      const zeroY = toY(0.0);
      const coords = points.map((p, i) => `${toX(i).toFixed(1)},${toY(p.cumulative_r).toFixed(1)}`).join(' ');
      const areaCoords = `${toX(0)},${zeroY} ` + coords + ` ${toX(points.length - 1)},${zeroY}`;

      const isProfit = points[points.length - 1].cumulative_r >= 0;
      const strokeColor = isProfit ? '#10b981' : '#ef4444';
      const gradId = isProfit ? 'gradGreen' : 'gradRed';

      svg.innerHTML = `
        <defs>
          <linearGradient id="gradGreen" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#10b981" stop-opacity="0.35"/>
            <stop offset="100%" stop-color="#10b981" stop-opacity="0.0"/>
          </linearGradient>
          <linearGradient id="gradRed" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="#ef4444" stop-opacity="0.35"/>
            <stop offset="100%" stop-color="#ef4444" stop-opacity="0.0"/>
          </linearGradient>
        </defs>

        <line x1="${pL}" y1="${toY(yMax - padY)}" x2="${W - pR}" y2="${toY(yMax - padY)}" stroke="#1e293b" stroke-width="1" />
        <line x1="${pL}" y1="${zeroY}" x2="${W - pR}" y2="${zeroY}" stroke="#475569" stroke-width="1.5" stroke-dasharray="3,3" />
        <line x1="${pL}" y1="${toY(yMin + padY)}" x2="${W - pR}" y2="${toY(yMin + padY)}" stroke="#1e293b" stroke-width="1" />

        <text x="${pL - 8}" y="${zeroY + 4}" fill="#94a3b8" font-size="11" text-anchor="end">0.0R</text>
        <text x="${pL - 8}" y="${toY(maxR) + 4}" fill="#10b981" font-size="11" text-anchor="end">+${maxR.toFixed(1)}R</text>
        ${minR < 0 ? `<text x="${pL - 8}" y="${toY(minR) + 4}" fill="#ef4444" font-size="11" text-anchor="end">${minR.toFixed(1)}R</text>` : ''}

        <polygon points="${areaCoords}" fill="url(#${gradId})" />
        <polyline fill="none" stroke="${strokeColor}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" points="${coords}" />

        ${points.map((p, i) => `
          <circle cx="${toX(i)}" cy="${toY(p.cumulative_r)}" r="3" fill="${p.cumulative_r >= 0 ? '#10b981' : '#ef4444'}" stroke="#090d16" stroke-width="1.5">
            <title>Lệnh ${i}: ${p.cumulative_r >= 0 ? '+' : ''}${p.cumulative_r}R</title>
          </circle>
        `).join('')}

        <text x="${toX(points.length - 1)}" y="${H - 8}" fill="#94a3b8" font-size="11" text-anchor="middle">Lệnh #${points.length - 1}</text>
      `;
    }

    function renderRecentTracks(tracks) {
      const tbody = document.getElementById('recentTracksTbody');
      if (!tbody) return;

      let filtered = tracks;
      if (perfFilters.dir !== 'ALL') {
        filtered = filtered.filter(t => t.direction === perfFilters.dir);
      }

      if (!filtered || filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px;">Không có khuyến nghị nào phù hợp với bộ lọc.</td></tr>`;
        return;
      }

      const statusMap = {
        'pending_entry': { icon: '⏳', label: 'Chờ khớp entry', bg: 'rgba(245,158,11,0.15)', col: '#f59e0b' },
        'open':          { icon: '🔵', label: 'Đang mở',        bg: 'rgba(59,130,246,0.15)', col: '#3b82f6' },
        'partial_tp1':   { icon: '🟢', label: 'Đạt TP1, còn vị thế', bg: 'rgba(16,185,129,0.15)', col: '#10b981' },
        'partial_tp2':   { icon: '🟢', label: 'Đạt TP2, còn vị thế', bg: 'rgba(16,185,129,0.20)', col: '#10b981' },
        'closed_tp3':    { icon: '✅', label: 'Hoàn tất TP3',   bg: 'rgba(16,185,129,0.25)', col: '#10b981' },
        'closed_sl':     { icon: '🔴', label: 'Chạm Stop Loss', bg: 'rgba(239,68,68,0.15)', col: '#ef4444' },
        'expired':       { icon: '⌛', label: 'Hết thời hạn',   bg: 'rgba(148,163,184,0.15)', col: '#94a3b8' },
        'invalidated':   { icon: '🚫', label: 'Bị vô hiệu trước Entry', bg: 'rgba(239,68,68,0.1)', col: '#ef4444' },
        'ambiguous':     { icon: '⚠️', label: 'Không xác định TP/SL nến', bg: 'rgba(245,158,11,0.2)', col: '#f59e0b' },
      };

      tbody.innerHTML = filtered.map(t => {
        const st = statusMap[t.status] || { icon: '⚪', label: t.status, bg: 'rgba(255,255,255,0.05)', col: '#fff' };
        const dirCol = t.direction === 'BUY' ? 'var(--green)' : 'var(--red)';
        const rVal = t.realized_r || 0.0;
        const rSign = rVal > 0 ? '+' : '';
        const rCol = rVal > 0 ? 'var(--green)' : (rVal < 0 ? 'var(--red)' : 'var(--text-muted)');
        const entryStr = t.actual_entry ? Number(t.actual_entry).toLocaleString() : (t.planned_entry ? Number(t.planned_entry).toLocaleString() : '--');

        return `
          <tr>
            <td style="font-size:0.78rem;color:var(--text-muted);white-space:nowrap;">${t.created_at_str || ''}</td>
            <td><strong>${t.asset}</strong></td>
            <td><span class="conf-tag conf-mid">${t.timeframe}</span></td>
            <td><strong style="color:${dirCol};">${t.direction}</strong></td>
            <td><span style="font-size:0.8rem;">${t.confidence} (${t.confidence_score}/5)</span></td>
            <td style="text-align:right;font-family:monospace;font-weight:600;">$${entryStr}</td>
            <td style="font-size:0.8rem;color:#cbd5e1;max-width:260px;">${t.result_note || '--'}</td>
            <td style="text-align:right;font-family:monospace;font-weight:700;color:${rCol};">${rSign}${rVal.toFixed(2)}R</td>
            <td>
              <span class="status-badge-track" style="background:${st.bg};color:${st.col};border:1px solid ${st.col}40;">
                ${st.icon} ${st.label}
              </span>
            </td>
          </tr>
        `;
      }).join('');
    }

    // --- VÒNG LẶP CẬP NHẬT (PERFORMANCE GUARDRAILS) ---
    window.addEventListener('DOMContentLoaded', () => {
      initChart();
      updateDashboard();
      fetchCandleData();
      fetchPerformanceData();

      // Snapshot số liệu & tín hiệu: mỗi 3 giây
      setInterval(updateDashboard, 3000);

      // Biểu đồ nến: mỗi 15 giây (tiết kiệm băng thông & tải render canvas)
      setInterval(fetchCandleData, 15000);

      // Hiệu suất đề xuất (Paper-tracking): mỗi 30 giây (tiết kiệm CPU & I/O)
      setInterval(fetchPerformanceData, 30000);
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
        self._app.router.add_get("/api/performance", self.handle_performance)
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
        tf = request.query.get("tf", "1h").lower()
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
        cfg = getattr(self._orchestrator, "cfg", CONFIG)
        for a, tfs in self._orchestrator._latest_advice.items():
            advice_payload[a] = {}
            for tf, adv in tfs.items():
                adv_dict = adv.to_dict() if hasattr(adv, "to_dict") else dict(adv)

                # Kiểm tra tính hợp lệ của Advice (O(1) nhẹ)
                ind_snap = self._orchestrator._latest_indicators.get(a, {}).get(tf, {})
                buf = self._orchestrator._bufs.get(a, {}).get(tf)
                current_price = None
                if buf and len(buf) > 0 and buf.latest:
                    current_price = buf.latest.close
                elif ind_snap and "close" in ind_snap:
                    current_price = ind_snap["close"]

                atr = ind_snap.get("atr", 0.0) if ind_snap else 0.0
                current_ema_fast = ind_snap.get("ema_fast") if ind_snap else None
                if current_price is not None and atr is not None:
                    target_adv = adv
                    if not isinstance(adv, TradeAdvice) and isinstance(adv, dict):
                        target_adv = TradeAdvice(
                            action=adv.get("action", "WAIT"),
                            confidence=adv.get("confidence", "Thấp"),
                            confidence_score=adv.get("confidence_score", 0),
                            entry_type=adv.get("entry_type", "none"),
                            entry_price=adv.get("entry_price"),
                            entry_zone=adv.get("entry_zone"),
                            stop_loss=adv.get("stop_loss"),
                            take_profit_1=adv.get("take_profit_1"),
                            take_profit_2=adv.get("take_profit_2"),
                            tp2_rule=adv.get("tp2_rule", ""),
                            reasoning=adv.get("reasoning", []),
                            conflict_detected=adv.get("conflict_detected", False),
                            mtf_agreement=adv.get("mtf_agreement"),
                        )
                    if isinstance(target_adv, TradeAdvice):
                        validity = check_advice_validity(
                            target_adv,
                            current_price=float(current_price),
                            atr=float(atr),
                            cfg=cfg,
                            current_ema_fast=float(current_ema_fast) if current_ema_fast is not None else None,
                        )
                    else:
                        validity = {
                            "status": AdviceValidityStatus.FRESH.value,
                            "reason": None,
                            "price_drift_atr": 0.0,
                        }
                else:
                    validity = {
                        "status": AdviceValidityStatus.FRESH.value,
                        "reason": None,
                        "price_drift_atr": 0.0,
                    }

                adv_dict = build_advice_payload(adv)
                adv_dict["validity"] = validity
                advice_payload[a][tf] = adv_dict

        tracker = getattr(self._orchestrator, "outcome_tracker", None)
        tracker_summary = tracker.get_summary() if tracker else None

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
            "tracker_summary": tracker_summary,
        }
        for a in CONFIG.get("assets", {}):
            data[a] = {}
            for tf in ["15m", "1h", "4h"]:
                data[a][tf] = build_timeframe_payload(a, tf)
                if tf not in CONFIG.get("DEPRECATED_TIMEFRAMES", []):
                    data[a][tf]["indicators"] = self._orchestrator._latest_indicators.get(a, {}).get(tf)
                    data[a][tf]["advice"] = advice_payload.get(a, {}).get(tf)
        return web.json_response(data)

    async def handle_performance(self, request: web.Request) -> web.Response:
        """
        Paper-tracking / Forward-testing performance analytics endpoint.
        Returns metrics, breakdown, equity curve, and recent tracks.
        """
        tracker = getattr(self._orchestrator, "outcome_tracker", None)
        if not tracker:
            return web.json_response({"status": "error", "message": "Tracker not initialized"}, status=503)

        asset = request.query.get("asset", "ALL").upper()
        tf = request.query.get("tf", "ALL").lower()
        confidence = request.query.get("confidence", "ALL")
        limit_str = request.query.get("limit", "20")
        try:
            limit = int(limit_str)
        except ValueError:
            limit = 20

        metrics = tracker.get_metrics(asset=asset, timeframe=tf, confidence=confidence)
        recent_tracks = tracker.get_recent_tracks(asset=asset, timeframe=tf, confidence=confidence, limit=limit)

        data = {
            "status": "ok",
            "tracking_mode": "paper_forward_test",
            "last_updated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "filters": {
                "asset": asset,
                "timeframe": tf,
                "confidence": confidence,
            },
            "metrics": metrics,
            "recent_tracks": recent_tracks,
            "disclaimer": "Đây là kết quả paper-tracking dựa trên nến OHLC đã đóng. Kết quả chưa bao gồm spread, commission, funding, swap và slippage thực tế; không phải cam kết hiệu quả giao dịch.",
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
        self.outcome_tracker:    Optional[SignalOutcomeTracker] = SignalOutcomeTracker(
            db_path=CONFIG.get("TRACKER_DB_PATH", os.path.join(BASE_DIR, "data", "signal_tracker.db")),
            config=CONFIG,
        )
        self.migrate_deprecated_timeframe_trades()

    def migrate_deprecated_timeframe_trades(self, db_conn=None) -> int:
        """Close stale tracks on deprecated timeframes (15m) once at startup."""
        conn_context = None
        if db_conn is not None:
            conn = db_conn
        elif self.outcome_tracker:
            conn_context = self.outcome_tracker._get_connection()
            conn = conn_context.__enter__()
        else:
            return 0

        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE signal_tracks
                SET status = 'expired', result_note = 'timeframe_deprecated_migration'
                WHERE timeframe IN ('15m', '15min')
                  AND status IN ('pending_entry', 'open', 'partial_tp1', 'partial_tp2')
            """)
            try:
                cursor.execute("""
                    UPDATE trade_outcomes
                    SET status = 'expired', notes = 'timeframe_deprecated_migration'
                    WHERE timeframe IN ('15m', '15min')
                      AND status IN ('pending_entry', 'open', 'partial_tp1', 'partial_tp2')
                """)
            except Exception:
                pass
            conn.commit()
            closed_cnt = cursor.rowcount
            logger.info(f"[MIGRATION] Closed {closed_cnt} stale 15m tracks")
            return closed_cnt
        except Exception as e:
            logger.error(f"[MIGRATION] Failed to migrate 15m tracks: {e}", exc_info=True)
            return 0
        finally:
            if conn_context is not None:
                conn_context.__exit__(None, None, None)

    def _compute_advice_for_asset(self, asset: str, tf: str, ind: dict) -> None:
        """Compute trade advice using multi-timeframe indicators without triggering notifications."""
        cfg = CONFIG["assets"].get(asset, {})
        cfg_tfs = cfg.get("timeframes", [])
        
        # Chọn khung tham chiếu MTF phù hợp nhất (ưu tiên khung lớn hơn liền kề hoặc khung dưới liền kề)
        other_tf = None
        if tf in ("15m", "15min"):
            other_tf = "1h" if "1h" in cfg_tfs else ("4h" if "4h" in cfg_tfs else None)
        elif tf == "1h":
            other_tf = "4h" if "4h" in cfg_tfs else ("15m" if "15m" in cfg_tfs else ("15min" if "15min" in cfg_tfs else None))
        elif tf == "4h":
            other_tf = "1h" if "1h" in cfg_tfs else ("15m" if "15m" in cfg_tfs else ("15min" if "15min" in cfg_tfs else None))
        else:
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
        enabled_tfs = CONFIG.get("TRACKER_ENABLED_TIMEFRAMES", ["1h", "4h"])
        for a, cfg_a in CONFIG["assets"].items():
            for t in cfg_a.get("timeframes", []):
                ind_snap = self._latest_indicators.get(a, {}).get(t)
                if ind_snap:
                    self._compute_advice_for_asset(a, t, ind_snap)
                    if self.outcome_tracker and t in enabled_tfs:
                        init_adv = self._latest_advice.get(a, {}).get(t)
                        buf_init = self._bufs.get(a, {}).get(t)
                        last_c = buf_init.latest if buf_init else None
                        if init_adv and last_c:
                            logger.info(f"[Tracker] Initial advice track evaluated for [{a} {t}]: {init_adv.action} ({init_adv.confidence})")
                            self.outcome_tracker.maybe_create_track(
                                asset=a,
                                timeframe=t,
                                advice=init_adv,
                                candle_ts=int(last_c.ts),
                            )

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
            enabled_tfs = CONFIG.get("TRACKER_ENABLED_TIMEFRAMES", ["1h", "4h"])
            for tf in cfg["timeframes"]:
                ind_snap = self._latest_indicators.get(asset, {}).get(tf)
                if ind_snap:
                    self._compute_advice_for_asset(asset, tf, ind_snap)
                    if self.outcome_tracker and tf in enabled_tfs:
                        rel_adv = self._latest_advice.get(asset, {}).get(tf)
                        rel_buf = self._bufs.get(asset, {}).get(tf)
                        rel_last = rel_buf.latest if rel_buf else None
                        if rel_adv and rel_last:
                            logger.info(f"[Tracker] Reload advice track evaluated for [{asset} {tf}]: {rel_adv.action} ({rel_adv.confidence})")
                            self.outcome_tracker.maybe_create_track(
                                asset=asset,
                                timeframe=tf,
                                advice=rel_adv,
                                candle_ts=int(rel_last.ts),
                            )

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
        self._process_timeframe(asset, tf)
        cfg     = CONFIG["assets"].get(asset, {})
        if not cfg:
            return
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
        advice = self._latest_advice.get(asset, {}).get(tf)

        # Signal Outcome Tracker (Paper-tracking / Forward-testing)
        enabled_tfs = CONFIG.get("TRACKER_ENABLED_TIMEFRAMES", ["1h", "4h"])
        if self.outcome_tracker and tf in enabled_tfs:
            # A. Cập nhật các track cũ bằng candle vừa đóng
            self.outcome_tracker.process_closed_candle(
                asset=asset,
                timeframe=tf,
                candle=latest,
                latest_advice=advice,
            )
            # B. Sau đó mới xét tạo track mới từ advice hiện tại
            if advice:
                self.outcome_tracker.maybe_create_track(
                    asset=asset,
                    timeframe=tf,
                    advice=advice,
                    candle_ts=int(latest.ts),
                )

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
            if tf in CONFIG.get("SUPPORTED_TIMEFRAMES", ["1h", "4h"])
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

    def run_cycle(self) -> None:
        """Run one evaluation cycle for supported timeframes."""
        for asset in CONFIG.get("assets", {}):
            for tf in CONFIG.get("SUPPORTED_TIMEFRAMES", ["1h", "4h"]):
                self._process_timeframe(asset, tf)

    def _process_timeframe(self, asset: str, tf: str) -> None:
        """Hook called for each timeframe processed in an evaluation cycle."""
        pass


Orchestrator = IndicatorOrchestrator


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
