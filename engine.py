"""
HOSTING INDICATOR ENGINE v1.0
Low-Resource Quant Engine | 1.5vCore - 2GB RAM - 2GB Disk

Assets  : BTC/USDT - XAU/USD - WTI (OIL/USD)
Signals : EMA crossover - RSI OB/OS - BB squeeze - ATR breakout
Stack   : Python 3.11+ - asyncio - aiohttp - stdlib only

Memory strategy:
  - CircularBuffer fixed 300 candles/asset  -> ~72 KB total RAM
  - No pandas/numpy (saves ~80MB baseline RAM)
  - All indicator functions are pure-math implementations
  - State persist = atomic JSON write every 15 min (<2MB)
  - Log rotating 5MB x 2 files = max 15MB on disk
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

import aiohttp  # pip install aiohttp


# =============================================================================
# SECTION 1: CONFIGURATION
# =============================================================================

CONFIG = {
    # ---- Telegram -------------------------------------------------------
    "telegram_bot_token": os.environ.get("TG_BOT_TOKEN", "YOUR_BOT_TOKEN"),
    "telegram_chat_id":   os.environ.get("TG_CHAT_ID",   "YOUR_CHAT_ID"),
    # ---- API Keys -------------------------------------------------------
    # TwelveData free tier: 800 requests/day (~33/hour). Used for XAU & OIL.
    "twelvedata_api_key": os.environ.get("TWELVE_API_KEY", "YOUR_TWELVE_KEY"),
    # ---- Asset definitions ----------------------------------------------
    "assets": {
        "BTC": {
            "source":     "binance",
            "symbol":     "BTCUSDT",
            "timeframes": ["15m", "1h"],
        },
        "XAU": {
            "source":     "twelvedata",
            "symbol":     "XAU/USD",
            "timeframes": ["15min", "1h"],
        },
        "OIL": {
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
    # ---- State persistence ----------------------------------------------
    "state_file":           "/opt/indicator/data/state.json",
    "state_max_bytes":      1_500_000,   # 1.5 MB ceiling before trim
    "state_flush_interval": 900,         # flush every 15 minutes
    # ---- Logging --------------------------------------------------------
    "log_file":        "/opt/indicator/logs/app.log",
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
    volume: float


class CircularBuffer:
    """
    Fixed-capacity FIFO using collections.deque(maxlen=N).
    Oldest candle is evicted automatically on append when full.
    RAM footprint stays constant after __init__ -- never grows.
    """

    __slots__ = ("_buf", "maxlen", "asset", "timeframe")

    def __init__(self, maxlen: int, asset: str, timeframe: str) -> None:
        self.maxlen    = maxlen
        self.asset     = asset
        self.timeframe = timeframe
        self._buf: deque[Candle] = deque(maxlen=maxlen)

    def push(self, candle: Candle) -> None:
        """Append a closed candle. Oldest entry evicted when at capacity."""
        self._buf.append(candle)

    def last_n_closes(self, n: int) -> list[float]:
        return [c.close for c in list(self._buf)[-n:]]

    def last_n_candles(self, n: int) -> list[Candle]:
        return list(self._buf)[-n:]

    def __len__(self) -> int:
        return len(self._buf)

    @property
    def latest(self) -> Optional[Candle]:
        return self._buf[-1] if self._buf else None


# =============================================================================
# SECTION 3: INDICATOR ENGINE  (pure math -- zero external dependencies)
# =============================================================================

class IndicatorEngine:
    """
    EMA, RSI, Bollinger Bands, ATR in pure Python.
    Avoids numpy/pandas to save ~80 MB of baseline RAM.
    All formulas are textbook implementations.
    """

    # ------------------------------------------------------------------
    @staticmethod
    def ema(closes: list[float], period: int) -> Optional[float]:
        """
        Exponential Moving Average (Wilder smoothing).
            k       = 2 / (period + 1)
            EMA_t   = close_t * k  +  EMA_{t-1} * (1 - k)
        Seed = simple average of first `period` values.
        """
        if len(closes) < period:
            return None
        k       = 2.0 / (period + 1)
        ema_val = sum(closes[:period]) / period   # seed
        for price in closes[period:]:
            ema_val = price * k + ema_val * (1.0 - k)
        return ema_val

    # ------------------------------------------------------------------
    @staticmethod
    def rsi(closes: list[float], period: int = 14) -> Optional[float]:
        """
        Relative Strength Index (Wilder smoothed).
            RS  = AvgGain / AvgLoss
            RSI = 100 - 100 / (1 + RS)
        Requires at least period + 1 bars.
        """
        if len(closes) < period + 1:
            return None
        deltas   = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains    = [d if d > 0 else 0.0 for d in deltas]
        losses   = [-d if d < 0 else 0.0 for d in deltas]
        avg_gain = sum(gains[:period]) / period   # seed
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    # ------------------------------------------------------------------
    @staticmethod
    def bollinger_bands(
        closes: list[float], period: int = 20, num_std: float = 2.0
    ) -> Optional[tuple[float, float, float]]:
        """
        Bollinger Bands -> (upper, middle, lower).
            middle = SMA(period)
            std    = population std-dev of last period values
            upper  = middle + num_std * std
            lower  = middle - num_std * std
        """
        if len(closes) < period:
            return None
        window   = closes[-period:]
        mean     = sum(window) / period
        variance = sum((x - mean) ** 2 for x in window) / period
        std      = math.sqrt(variance)
        return mean + num_std * std, mean, mean - num_std * std

    # ------------------------------------------------------------------
    @staticmethod
    def atr(candles: list[Candle], period: int = 14) -> Optional[float]:
        """
        Average True Range (Wilder smoothed).
            TR = max(H-L,  |H - prev_C|,  |L - prev_C|)
        Seed = simple average of first `period` TR values.
        """
        if len(candles) < period + 1:
            return None
        trs: list[float] = []
        for i in range(1, len(candles)):
            h, lo, pc = candles[i].high, candles[i].low, candles[i - 1].close
            trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
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
    message:     str   # human-readable text for Telegram
    price:       float
    ts:          int = field(default_factory=lambda: int(time.time()))


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
# SECTION 5: TELEGRAM DISPATCHER
# =============================================================================

class TelegramDispatcher:
    """
    Async Telegram message sender.
    Reuses one aiohttp session (persistent keep-alive connection).
    Retries up to 3 times with exponential backoff on failure.
    """

    _API_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, token: str, chat_id: str) -> None:
        self._token   = token
        self._chat_id = chat_id
        self._session: Optional[aiohttp.ClientSession] = None

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=10),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def send(self, text: str, retries: int = 3) -> bool:
        """Send a Markdown message. Returns True on success."""
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
        self._api_key      = api_key
        self._last_call    = 0.0
        self._min_gap      = 8.0   # seconds -- stays within 8 req/min

    async def fetch_candles(
        self, symbol: str, interval: str, limit: int = 300
    ) -> list[Candle]:
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
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

    def save(self, state: dict) -> None:
        try:
            payload   = json.dumps(state, indent=2)
            byte_size = len(payload.encode())
            if byte_size > self._max_bytes:
                logger.warning(f"State {byte_size}B > {self._max_bytes}B ceiling -- trimming")
                payload = json.dumps(self._trim(state), indent=2)
            dir_path = os.path.dirname(self._path)
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
                st       = os.statvfs("/")
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
# SECTION 10: MAIN ORCHESTRATOR
# =============================================================================

class IndicatorOrchestrator:
    """
    Top-level coordinator for the entire pipeline:
      _init_session  -> create shared aiohttp session + fetchers
      _init_buffers  -> create CircularBuffers and pre-fill with history
      _asset_loop    -> per-(asset, tf) infinite poll-compute-signal loop
      _flush_loop    -> periodic state snapshot to disk
      run            -> starts all asyncio tasks
    """

    def __init__(self) -> None:
        self._bufs:    dict[str, dict[str, CircularBuffer]] = {}
        self._rules    = RuleEngine()
        self._persist: Optional[StatePersistence]   = None
        self._tg:      Optional[TelegramDispatcher] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._binance: Optional[BinanceFetcher]     = None
        self._twelve:  Optional[TwelveDataFetcher]  = None

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
        # Compact log line
        rsi = f"{ind['rsi']:.1f}" if ind.get("rsi") is not None else "N/A"
        atr = f"{ind['atr']:.4f}" if ind.get("atr") is not None else "N/A"
        logger.info(f"[{asset} {tf}] close={latest.close:.4f} RSI={rsi} ATR={atr}")
        for sig in self._rules.evaluate(ind):
            logger.info(f"  >> {sig.signal_type}")
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
        await self._init_buffers()
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
            if self._session:
                await self._session.close()
            if self._tg:
                await self._tg.close()
            logger.info("Engine stopped cleanly")


# =============================================================================
# SECTION 11: LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Rotating file logger + stdout handler.
    5 MB per file, 2 backup files => max 15 MB logs on disk.
    """
    log_dir = os.path.dirname(CONFIG["log_file"])
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = RotatingFileHandler(
        CONFIG["log_file"],
        maxBytes=CONFIG["log_max_bytes"],
        backupCount=CONFIG["log_backup_count"],
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(fh)
    root.addHandler(ch)
    return logging.getLogger("indicator")


# =============================================================================
# ENTRY POINT
# =============================================================================

logger = setup_logging()


def main() -> None:
    logger.info("=" * 60)
    logger.info("  HOSTING INDICATOR ENGINE v1.0  (low-resource edition)")
    logger.info(f"  Python {sys.version.split()[0]}")
    logger.info("=" * 60)
    try:
        asyncio.run(IndicatorOrchestrator().run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt -- shutting down.")


if __name__ == "__main__":
    main()
