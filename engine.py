"""
HOSTING INDICATOR ENGINE v1.0
Low-Resource Quant Engine | 1.5vCore - 2GB RAM - 2GB Disk

Assets  : BTC/USDT - XAU/USD - WTI (OIL/USD)
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
            "name":       "Crude Oil (WTI)",
            "source":     "twelvedata",
            "symbol":     "WTI/USD",
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
    Used for XAU/USD (Gold) and WTI/USD (Crude Oil).
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

HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Indicator Engine - Live Dashboard</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: #111827;
      --card-border: #1f2937;
      --text: #f9fafb;
      --text-muted: #9ca3af;
      --accent: #3b82f6;
      --green: #10b981;
      --red: #ef4444;
      --gold: #f59e0b;
      --purple: #8b5cf6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      padding: 24px;
      line-height: 1.5;
    }
    .container { max-width: 1200px; margin: 0 auto; }
    header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 24px;
      gap: 16px;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand h1 { font-size: 1.4rem; font-weight: 700; letter-spacing: -0.02em; }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 600;
      background: rgba(16, 185, 129, 0.15);
      color: var(--green);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 8px var(--green);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.9); }
      100% { opacity: 1; transform: scale(1); }
    }
    .meta-info {
      display: flex;
      gap: 16px;
      font-size: 0.85rem;
      color: var(--text-muted);
      align-items: center;
    }
    .meta-item { display: flex; align-items: center; gap: 6px; }
    .meta-value { color: var(--text); font-weight: 600; font-family: monospace; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 16px 20px;
    }
    .card-title {
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 8px;
    }
    .card-metric { font-size: 1.5rem; font-weight: 700; font-family: monospace; }
    .card-sub { font-size: 0.8rem; color: var(--text-muted); margin-top: 4px; }
    .table-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow: hidden;
      margin-bottom: 24px;
    }
    .table-header {
      padding: 16px 20px;
      border-bottom: 1px solid var(--card-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .table-header h2 { font-size: 1.05rem; font-weight: 600; }
    .table-wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.9rem; }
    th {
      background: rgba(255, 255, 255, 0.02);
      color: var(--text-muted);
      font-weight: 600;
      font-size: 0.75rem;
      text-transform: uppercase;
      padding: 12px 16px;
      border-bottom: 1px solid var(--card-border);
    }
    td {
      padding: 14px 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      font-family: monospace;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(255, 255, 255, 0.02); }
    .asset-tag {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-weight: 700;
      color: var(--text);
    }
    .pill {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 0.8rem;
      font-weight: 600;
    }
    .pill-bull { background: rgba(16, 185, 129, 0.15); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); }
    .pill-bear { background: rgba(239, 68, 68, 0.15); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.3); }
    .pill-neutral { background: rgba(156, 163, 175, 0.15); color: var(--text-muted); border: 1px solid rgba(156, 163, 175, 0.2); }
    .pill-gold { background: rgba(245, 158, 11, 0.15); color: var(--gold); border: 1px solid rgba(245, 158, 11, 0.3); }
    .pill-purple { background: rgba(139, 92, 246, 0.15); color: var(--purple); border: 1px solid rgba(139, 92, 246, 0.3); }
    .signals-box {
      max-height: 380px;
      overflow-y: auto;
      padding: 12px 16px;
    }
    .signal-item {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      padding: 10px 12px;
      border-radius: 8px;
      margin-bottom: 8px;
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid rgba(255, 255, 255, 0.05);
      gap: 8px;
    }
    .signal-item:last-child { margin-bottom: 0; }
    .sig-left { display: flex; align-items: center; gap: 10px; }
    .sig-time { font-size: 0.75rem; color: var(--text-muted); font-family: monospace; }
    .sig-msg { font-size: 0.85rem; color: #d1d5db; }
    .sig-price { font-size: 0.85rem; font-weight: 700; font-family: monospace; }
    .empty-signals {
      padding: 32px 16px;
      text-align: center;
      color: var(--text-muted);
      font-size: 0.9rem;
    }
    footer {
      text-align: center;
      font-size: 0.8rem;
      color: var(--text-muted);
      padding: 20px 0;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <h1>Hosting Indicator Engine</h1>
        <div class="status-badge" id="engineStatusBadge">
          <span class="pulse-dot"></span>
          <span id="engineStatusText">ONLINE</span>
        </div>
      </div>
      <div class="meta-info">
        <div class="meta-item">
          <span>Uptime:</span>
          <span class="meta-value" id="uptimeDisplay">--:--:--</span>
        </div>
        <div class="meta-item">
          <span>Server Time:</span>
          <span class="meta-value" id="serverTimeDisplay">--:--:-- UTC</span>
        </div>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <div class="card-title">Assets Tracked</div>
        <div class="card-metric" style="color: var(--accent);">BTC • XAU • OIL</div>
        <div class="card-sub">Timeframes: 15m & 1h (6 Streams)</div>
      </div>
      <div class="card">
        <div class="card-title">Telegram Mode</div>
        <div class="card-metric" id="tgModeMetric">Detecting...</div>
        <div class="card-sub" id="tgModeSub">Optional Bot Fallback</div>
      </div>
      <div class="card">
        <div class="card-title">Signal Memory</div>
        <div class="card-metric" id="signalsCountMetric">0</div>
        <div class="card-sub">Circular ring buffer (Max 50)</div>
      </div>
      <div class="card">
        <div class="card-title">Resource Profile</div>
        <div class="card-metric" style="color: var(--green);">&lt; 120 MB RAM</div>
        <div class="card-sub">1.5 vCore • Low Overhead Engine</div>
      </div>
    </div>

    <div class="table-card">
      <div class="table-header">
        <h2>Live Technical Indicators</h2>
        <span style="font-size: 0.8rem; color: var(--text-muted);">Auto-refreshes every 3s</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Asset</th>
              <th>TF</th>
              <th>Close Price</th>
              <th>RSI (14)</th>
              <th>EMA Trend (20 / 50)</th>
              <th>Bollinger Bands (Low / Mid / High)</th>
              <th>ATR (14)</th>
              <th>Last Update</th>
            </tr>
          </thead>
          <tbody id="indicatorsTbody">
            <tr>
              <td colspan="8" style="text-align: center; color: var(--text-muted); padding: 24px;">
                Loading indicator streams...
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="table-card">
      <div class="table-header">
        <h2>Live Signals Feed</h2>
        <span style="font-size: 0.8rem; color: var(--text-muted);" id="signalsTotalLabel">0 triggered</span>
      </div>
      <div class="signals-box" id="signalsFeedBox">
        <div class="empty-signals">No signals triggered yet. Waiting for candle closes...</div>
      </div>
    </div>

    <footer>
      Hosting Indicator Engine v1.0 • Designed for low-resource hosting (Botkeep, VPS, Local)
    </footer>
  </div>

  <script>
    async function updateDashboard() {
      try {
        const res = await fetch('/api/snapshot');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();

        // 1. Header meta
        document.getElementById('uptimeDisplay').textContent = data.uptime_formatted || '--';
        document.getElementById('serverTimeDisplay').textContent = data.server_time || '--';
        document.getElementById('engineStatusText').textContent = data.status.toUpperCase();

        // 2. Telegram mode
        const tgMetric = document.getElementById('tgModeMetric');
        const tgSub = document.getElementById('tgModeSub');
        if (data.telegram_enabled) {
          tgMetric.textContent = 'ACTIVE';
          tgMetric.style.color = 'var(--green)';
          tgSub.textContent = 'Dispatching to Telegram Chat';
        } else {
          tgMetric.textContent = 'WEB / LOGS';
          tgMetric.style.color = 'var(--gold)';
          tgSub.textContent = 'Running Standalone (No Bot Token)';
        }

        // 3. Signals count
        const signals = data.signals || [];
        document.getElementById('signalsCountMetric').textContent = signals.length;
        document.getElementById('signalsTotalLabel').textContent = signals.length + ' stored';

        // 4. Render Indicators Table
        const tbody = document.getElementById('indicatorsTbody');
        const indMap = data.indicators || {};
        let rowsHtml = '';
        const assetsOrder = ['BTC', 'XAU', 'OIL'];

        assetsOrder.forEach(asset => {
          const tfs = indMap[asset] || {};
          Object.keys(tfs).forEach(tf => {
            const item = tfs[tf];
            if (!item) return;

            // Price format
            const price = item.close != null ? Number(item.close).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 4}) : '--';

            // RSI format & badge
            let rsiBadge = '<span class="pill pill-neutral">N/A</span>';
            if (item.rsi != null) {
              const r = Number(item.rsi);
              if (r >= 70) {
                rsiBadge = `<span class="pill pill-bear">${r.toFixed(1)} [OB]</span>`;
              } else if (r <= 30) {
                rsiBadge = `<span class="pill pill-bull">${r.toFixed(1)} [OS]</span>`;
              } else {
                rsiBadge = `<span class="pill pill-neutral">${r.toFixed(1)}</span>`;
              }
            }

            // EMA Trend
            let emaTrend = '<span class="pill pill-neutral">N/A</span>';
            if (item.ema_fast != null && item.ema_slow != null) {
              const ef = Number(item.ema_fast);
              const es = Number(item.ema_slow);
              if (ef > es) {
                emaTrend = `<span class="pill pill-bull">BULLISH ▲</span> <span style="color:var(--text-muted);font-size:0.75rem;">(${ef.toFixed(1)} / ${es.toFixed(1)})</span>`;
              } else {
                emaTrend = `<span class="pill pill-bear">BEARISH ▼</span> <span style="color:var(--text-muted);font-size:0.75rem;">(${ef.toFixed(1)} / ${es.toFixed(1)})</span>`;
              }
            }

            // Bollinger Bands
            let bbStr = '<span style="color:var(--text-muted);">N/A</span>';
            if (item.bb_lower != null && item.bb_mid != null && item.bb_upper != null) {
              bbStr = `${Number(item.bb_lower).toFixed(2)} | ${Number(item.bb_mid).toFixed(2)} | ${Number(item.bb_upper).toFixed(2)}`;
            }

            // ATR
            const atrStr = item.atr != null ? Number(item.atr).toFixed(4) : 'N/A';

            // Time
            const timeStr = item.ts ? new Date(item.ts * 1000).toLocaleTimeString() : '--';

            let assetIcon = '📊';
            if (asset === 'BTC') assetIcon = '₿';
            if (asset === 'XAU') assetIcon = '🟡';
            if (asset === 'OIL') assetIcon = '🛢️';

            rowsHtml += `
              <tr>
                <td><span class="asset-tag">${assetIcon} ${asset}</span></td>
                <td><span class="pill pill-neutral">${tf}</span></td>
                <td style="font-weight:700;color:var(--text);">$${price}</td>
                <td>${rsiBadge}</td>
                <td>${emaTrend}</td>
                <td style="font-size:0.8rem;color:var(--text-muted);">${bbStr}</td>
                <td>${atrStr}</td>
                <td style="font-size:0.8rem;color:var(--text-muted);">${timeStr}</td>
              </tr>
            `;
          });
        });

        if (rowsHtml) {
          tbody.innerHTML = rowsHtml;
        }

        // 5. Render Signals Feed
        const feedBox = document.getElementById('signalsFeedBox');
        if (signals.length === 0) {
          feedBox.innerHTML = '<div class="empty-signals">No signals triggered yet. Waiting for candle closes...</div>';
        } else {
          feedBox.innerHTML = signals.map(sig => {
            let pillClass = 'pill-gold';
            if (sig.signal_type.includes('UP') || sig.signal_type.includes('OVERSOLD')) pillClass = 'pill-bull';
            if (sig.signal_type.includes('DOWN') || sig.signal_type.includes('OVERBOUGHT')) pillClass = 'pill-bear';
            if (sig.signal_type.includes('SPIKE')) pillClass = 'pill-purple';

            const cleanMsg = (sig.message || '').replace(/\\*|_|`/g, '');
            return `
              <div class="signal-item">
                <div class="sig-left">
                  <span class="pill ${pillClass}">${sig.signal_type}</span>
                  <span style="font-weight:700;">${sig.asset} (${sig.timeframe})</span>
                  <span class="sig-msg">${cleanMsg}</span>
                </div>
                <div style="display:flex;align-items:center;gap:12px;">
                  <span class="sig-price">$${Number(sig.price).toFixed(4)}</span>
                  <span class="sig-time">${sig.time_str || ''}</span>
                </div>
              </div>
            `;
          }).join('');
        }
      } catch (err) {
        console.warn('Dashboard fetch error:', err);
      }
    }

    setInterval(updateDashboard, 3000);
    window.addEventListener('load', updateDashboard);
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
        self._app.router.add_get("/api/health",   self.handle_health)
        self._runner: Optional[web.AppRunner] = None
        self._site:   Optional[web.TCPSite]   = None

    async def handle_index(self, request: web.Request) -> web.Response:
        return web.Response(text=HTML_DASHBOARD, content_type="text/html", charset="utf-8")

    async def handle_snapshot(self, request: web.Request) -> web.Response:
        now = time.time()
        uptime_sec = int(now - self._orchestrator._start_time)
        data = {
            "status": "online",
            "uptime_seconds": uptime_sec,
            "uptime_formatted": str(datetime.timedelta(seconds=uptime_sec)),
            "server_time": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "telegram_enabled": self._orchestrator._tg.is_enabled if self._orchestrator._tg else False,
            "indicators": self._orchestrator._latest_indicators,
            "signals": list(self._orchestrator._recent_signals),
        }
        return web.json_response(data)

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
        self._persist:           Optional[StatePersistence]   = None
        self._tg:                Optional[TelegramDispatcher] = None
        self._session:           Optional[aiohttp.ClientSession] = None
        self._binance:           Optional[BinanceFetcher]     = None
        self._twelve:            Optional[TwelveDataFetcher]  = None
        self._dashboard:         Optional[DashboardServer]    = None
        self._start_time:        float                        = time.time()
        self._latest_indicators: dict[str, dict[str, dict]]   = {}
        self._recent_signals:    deque[dict]                  = deque(maxlen=50)

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
