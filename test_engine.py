import asyncio
import os
import unittest
from engine import (
    Candle, CircularBuffer, IndicatorEngine, RuleEngine,
    Signal, TelegramDispatcher, DashboardServer, IndicatorOrchestrator,
    TwelveDataFetcher, CONFIG, _parse_port, _load_dotenv
)

class TestEngine(unittest.TestCase):
    def test_circular_buffer(self):
        buf = CircularBuffer(10, "BTC", "15m")
        for i in range(15):
            buf.push(Candle(ts=1000 + i, open=100.0, high=105.0, low=95.0, close=100.0 + i, volume=10.0))
        self.assertEqual(len(buf), 10)
        self.assertEqual(buf.latest.close, 114.0)

    def test_indicator_engine(self):
        buf = CircularBuffer(50, "BTC", "15m")
        for i in range(40):
            buf.push(Candle(ts=1000 + i, open=100.0, high=105.0, low=95.0, close=100.0 + (i % 5), volume=10.0))
        res = IndicatorEngine.compute_all(buf)
        self.assertIn("rsi", res)
        self.assertIn("ema_fast", res)
        self.assertIn("ema_slow", res)
        self.assertIn("bb_upper", res)
        self.assertIn("atr", res)

    def test_telegram_offline_mode(self):
        tg = TelegramDispatcher("YOUR_BOT_TOKEN", "YOUR_CHAT_ID")
        self.assertFalse(tg.is_enabled)
        # Dispatching signal in offline mode must not throw
        sig = Signal(asset="BTC", timeframe="15m", signal_type="TEST", message="Test", price=50000.0)
        asyncio.run(tg.dispatch_signal(sig))

    def test_signal_to_dict(self):
        sig = Signal(asset="BTC", timeframe="15m", signal_type="RSI_OVERSOLD", message="Buy", price=60000.0)
        d = sig.to_dict()
        self.assertEqual(d["asset"], "BTC")
        self.assertEqual(d["signal_type"], "RSI_OVERSOLD")
        self.assertIn("time_str", d)

    def test_dashboard_server_routes(self):
        orch = IndicatorOrchestrator()
        server = DashboardServer(orch, "127.0.0.1", 8080)
        self.assertIsNotNone(server._app)
        # Check routes exist: /, /health, /api/snapshot, /api/health
        routes = [r.resource.canonical for r in server._app.router.routes()]
        self.assertIn("/", routes)
        self.assertIn("/health", routes)
        self.assertIn("/api/health", routes)
        self.assertIn("/api/snapshot", routes)

    def test_parse_port_safety(self):
        # Empty string should fallback to default
        os.environ["SERVER_PORT"] = ""
        os.environ["PORT"] = ""
        self.assertEqual(_parse_port(8080), 8080)

        # Leading colon format should be parsed correctly
        os.environ["SERVER_PORT"] = ":3000"
        self.assertEqual(_parse_port(8080), 3000)

        # Standard number string
        os.environ["SERVER_PORT"] = "9000"
        self.assertEqual(_parse_port(8080), 9000)

        # Invalid string should fallback
        os.environ["SERVER_PORT"] = "invalid_port"
        self.assertEqual(_parse_port(8080), 8080)

        # Cleanup
        os.environ.pop("SERVER_PORT", None)
        os.environ.pop("PORT", None)

    def test_twelvedata_zero_env_bypass(self):
        fetcher = TwelveDataFetcher(None, "")
        self.assertFalse(fetcher.is_configured)
        candles = asyncio.run(fetcher.fetch_candles("XAU/USD", "15min"))
        self.assertEqual(candles, [])

        fetcher_placeholder = TwelveDataFetcher(None, "YOUR_TWELVE_KEY")
        self.assertFalse(fetcher_placeholder.is_configured)
        candles2 = asyncio.run(fetcher_placeholder.fetch_candles("WTI/USD", "1h"))
        self.assertEqual(candles2, [])

if __name__ == "__main__":
    unittest.main()
