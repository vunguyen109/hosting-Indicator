import asyncio
import os
import unittest
from engine import (
    Candle, CircularBuffer, IndicatorEngine, RuleEngine,
    Signal, TelegramDispatcher, DashboardServer, IndicatorOrchestrator, CONFIG
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

if __name__ == "__main__":
    unittest.main()
