import asyncio
import os
import tempfile
import unittest
from engine import (
    Candle, CircularBuffer, IndicatorEngine, RuleEngine,
    Signal, TelegramDispatcher, DashboardServer, IndicatorOrchestrator,
    TwelveDataFetcher, CONFIG, _parse_port, _load_dotenv, _update_dotenv
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
        # Check routes exist: /, /health, /api/snapshot, /api/candles, /api/health, /api/config
        routes = [r.resource.canonical for r in server._app.router.routes()]
        self.assertIn("/", routes)
        self.assertIn("/health", routes)
        self.assertIn("/api/health", routes)
        self.assertIn("/api/snapshot", routes)
        self.assertIn("/api/candles", routes)
        self.assertIn("/api/config", routes)

    def test_candles_endpoint_and_buffer_slice(self):
        orch = IndicatorOrchestrator()
        buf = CircularBuffer(50, "BTC", "15m")
        for i in range(20):
            buf.push(Candle(ts=1700000000 + i * 900, open=60000.0, high=60500.0, low=59500.0, close=60200.0 + i))
        orch._bufs["BTC"] = {"15m": buf}
        
        server = DashboardServer(orch, "127.0.0.1", 8080)
        
        # Mock request with query parameters
        class MockRequest:
            def __init__(self, query):
                self.query = query

        req = MockRequest({"asset": "BTC", "tf": "15m", "limit": "10"})
        resp = asyncio.run(server.handle_candles(req))
        self.assertEqual(resp.status, 200)
        import json
        data = json.loads(resp.text)
        self.assertEqual(len(data), 10)
        self.assertIn("time", data[0])
        self.assertIn("open", data[0])
        self.assertIn("close", data[0])
        self.assertEqual(data[-1]["close"], 60219.0)

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

    def test_twelvedata_zero_env_bypass_and_update(self):
        fetcher = TwelveDataFetcher(None, "")
        self.assertFalse(fetcher.is_configured)
        candles = asyncio.run(fetcher.fetch_candles("XAU/USD", "15min"))
        self.assertEqual(candles, [])

        # Test hot update via UI
        fetcher.update_key("real_api_key_12345")
        self.assertTrue(fetcher.is_configured)
        self.assertEqual(fetcher._api_key, "real_api_key_12345")

    def test_update_dotenv_file(self):
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as tmp:
            tmp.write("HOST=0.0.0.0\nPORT=8080\n")
            tmp_path = tmp.name

        try:
            _update_dotenv({"TWELVE_API_KEY": "my_new_key", "PORT": "9090"}, filepath=tmp_path)
            with open(tmp_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn("TWELVE_API_KEY=my_new_key", content)
            self.assertIn("PORT=9090", content)
            self.assertIn("HOST=0.0.0.0", content)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

if __name__ == "__main__":
    unittest.main()
