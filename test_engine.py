import asyncio
import os
import tempfile
import unittest
from engine import (
    Candle, CircularBuffer, IndicatorEngine, RuleEngine,
    Signal, TelegramDispatcher, DashboardServer, IndicatorOrchestrator,
    TwelveDataFetcher, CONFIG, _parse_port, _load_dotenv, _update_dotenv,
    ConfluenceDecisionEngine, TradeAdvice, TrendState, RsiZone, BbPosition, Action, Confidence,
    AdviceValidityStatus, check_advice_validity
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

    def test_confluence_decision_matrix_table_driven(self):
        engine = ConfluenceDecisionEngine(CONFIG)

        # 1. Trend mạnh xuống + RSI cân bằng + BB giữa/chạm biên dưới -> SELL
        snap_sell = {
            "close": 59800.0,
            "ema_fast": 60000.0,
            "ema_slow": 60500.0,  # diff = -500, with ATR=500 -> diff_in_atr = -1.0 (< -0.5) => STRONG_DOWN
            "rsi": 50.0,          # NEUTRAL
            "bb_upper": 61000.0,
            "bb_mid": 60250.0,
            "bb_lower": 59500.0,  # width=1500, edge=225, lower+edge=59725 => 59800 is MIDDLE
            "atr": 500.0,
        }
        adv1 = engine.decide("BTC", "15m", snap_sell, [])
        self.assertEqual(adv1.action, "SELL")
        self.assertIsNotNone(adv1.stop_loss)
        self.assertIsNotNone(adv1.take_profit_1)
        self.assertIsNotNone(adv1.take_profit_2)

        # 2. Trend mạnh xuống + BB vừa thủng đáy + RSI chưa quá bán sâu -> WAIT (quá đà)
        snap_break_lower = {
            "close": 59400.0,     # < bb_lower 59500 => BREAK_LOWER
            "ema_fast": 60000.0,
            "ema_slow": 60500.0,  # STRONG_DOWN
            "rsi": 35.0,          # LEAN_OVERSOLD (not EXTREME_OVERSOLD < 25)
            "bb_upper": 61000.0,
            "bb_mid": 60250.0,
            "bb_lower": 59500.0,
            "atr": 500.0,
        }
        adv2 = engine.decide("BTC", "15m", snap_break_lower, [])
        self.assertEqual(adv2.action, "WAIT")

        # 3. Sideway (trend neutral + BB giữa dải) -> WAIT
        snap_sideway = {
            "close": 60000.0,
            "ema_fast": 60010.0,
            "ema_slow": 60000.0,  # diff = +10, ATR=500 -> diff_in_atr = 0.02 => NEUTRAL
            "rsi": 50.0,          # NEUTRAL
            "bb_upper": 61000.0,
            "bb_mid": 60000.0,
            "bb_lower": 59000.0,  # width=2000 => MIDDLE
            "atr": 500.0,
        }
        adv3 = engine.decide("BTC", "15m", snap_sideway, [])
        self.assertEqual(adv3.action, "WAIT")

        # 4. Trend mạnh lên + RSI vừa phải + BB giữa/cận biên trên -> BUY
        snap_buy = {
            "close": 60600.0,
            "ema_fast": 60500.0,
            "ema_slow": 60000.0,  # diff = +500, ATR=500 -> diff_in_atr = +1.0 (> 0.5) => STRONG_UP
            "rsi": 55.0,          # NEUTRAL
            "bb_upper": 61000.0,
            "bb_mid": 60250.0,
            "bb_lower": 59500.0,  # width=1500, edge=225, upper-edge=60775 => MIDDLE
            "atr": 500.0,
        }
        adv4 = engine.decide("BTC", "15m", snap_buy, [])
        self.assertEqual(adv4.action, "BUY")
        self.assertIsNotNone(adv4.stop_loss)
        self.assertIsNotNone(adv4.take_profit_1)
        self.assertIsNotNone(adv4.take_profit_2)

        # 5. Có tín hiệu mâu thuẫn trong recent_signals -> luôn WAIT bất kể score
        conflicting_signals = [
            {"asset": "BTC", "timeframe": "15m", "signal_type": "EMA_CROSS_DOWN", "ts": 1700000000}
        ]
        adv5 = engine.decide("BTC", "15m", snap_buy, conflicting_signals)
        self.assertEqual(adv5.action, "WAIT")
        self.assertTrue(adv5.conflict_detected)

        # 6. MTF không đồng thuận -> giữ action nhưng hạ confidence
        other_tf_bearish = {
            "ema_fast": 59000.0,
            "ema_slow": 60000.0,
            "atr": 500.0,
        }
        adv6 = engine.decide("BTC", "15m", snap_buy, [], other_tf_snapshot=other_tf_bearish)
        self.assertEqual(adv6.action, "BUY")
        self.assertFalse(adv6.mtf_agreement)
        self.assertIn(adv6.confidence, ("Thấp", "Trung bình"))

    def test_confluence_no_telegram_reference_or_side_effects(self):
        engine = ConfluenceDecisionEngine(CONFIG)
        # Verify decide() is a pure synchronous function without network or dispatcher call
        snap = {
            "close": 60000.0, "ema_fast": 60500.0, "ema_slow": 60000.0,
            "rsi": 50.0, "bb_upper": 61000.0, "bb_mid": 60250.0, "bb_lower": 59500.0, "atr": 500.0
        }
        # Run decide() and verify it returns a TradeAdvice instance without throwing
        advice = engine.decide("BTC", "15m", snap, [])
        self.assertIsInstance(advice, TradeAdvice)
        self.assertFalse(hasattr(engine, "_tg"))
        self.assertFalse(hasattr(engine, "telegram"))

    def test_snapshot_api_returns_advice(self):
        orch = IndicatorOrchestrator()
        # Mock buffer and indicators for BTC 15m
        snap = {
            "close": 60000.0, "ema_fast": 60500.0, "ema_slow": 60000.0,
            "rsi": 50.0, "bb_upper": 61000.0, "bb_mid": 60250.0, "bb_lower": 59500.0, "atr": 500.0
        }
        orch._latest_indicators["BTC"] = {"15m": snap}
        orch._compute_advice_for_asset("BTC", "15m", snap)

        server = DashboardServer(orch, "127.0.0.1", 8080)
        
        class MockRequest:
            pass

        resp = asyncio.run(server.handle_snapshot(MockRequest()))
        self.assertEqual(resp.status, 200)
        import json
        payload = json.loads(resp.text)
        self.assertIn("advice", payload)
        self.assertIn("BTC", payload["advice"])
        self.assertIn("15m", payload["advice"]["BTC"])
        btc_advice = payload["advice"]["BTC"]["15m"]
        self.assertIn("action", btc_advice)
        self.assertIn("confidence", btc_advice)
        self.assertIn("disclaimer", btc_advice)

    def test_mtf_label_dynamic_not_hardcoded(self):
        """Lỗi 1: Khi đang ở khung 1h, label phải nói '15 phút (ngắn hạn hơn)', KHÔNG được nói 'khung lớn'."""
        engine = ConfluenceDecisionEngine(CONFIG)
        label = engine._get_relative_tf_label("1h", "15m")
        self.assertIn("ngắn hạn hơn", label)
        self.assertNotIn("lớn", label.lower())

    def test_counter_move_detected_on_vshape_bounce(self):
        """Lỗi 2 - Regression test: Tái hiện đúng case XAU 1h đã quan sát.
        Giá giảm sâu rồi bật mạnh ngược trend EMA giảm PHẢI được gắn cờ conflict, dẫn tới action=WAIT."""
        class FakeCandle:
            def __init__(self, close):
                self.close = close

        candles = [
            FakeCandle(close=4148.69),  # đáy
            FakeCandle(close=4163.00),
            FakeCandle(close=4188.83),  # bật mạnh ~3.7x ATR
        ]
        snapshot = {
            "close": 4188.83, "ema_fast": 4158.11, "ema_slow": 4162.64,
            "rsi": 40.4, "bb_upper": 4164.63, "bb_lower": 4151.95,
            "atr": 10.81,
        }
        engine = ConfluenceDecisionEngine(CONFIG)
        advice = engine.decide(
            asset="XAU", timeframe="1h", snapshot=snapshot,
            recent_signals=[], recent_candles=candles
        )

        self.assertTrue(advice.counter_move_detected)
        self.assertEqual(
            advice.action, "WAIT",
            "Phải chuyển thành WAIT khi phát hiện counter-move mạnh, không được giữ nguyên SELL như lỗi cũ"
        )
        self.assertTrue(any("biến động giá tăng mạnh bất thường" in r for r in advice.reasoning))

    def test_no_false_positive_counter_move_in_normal_trend(self):
        """Đảm bảo patch mới KHÔNG gây false positive khi trend đang diễn biến bình thường, không có biến động bất thường."""
        class FakeCandle:
            def __init__(self, close):
                self.close = close

        candles = [
            FakeCandle(close=4160.0), 
            FakeCandle(close=4158.0), 
            FakeCandle(close=4156.0),  # giảm đều, đúng với trend giảm
        ]
        snapshot = {
            "close": 4156.0, "ema_fast": 4158.0, "ema_slow": 4164.0,
            "rsi": 45.0, "bb_upper": 4170.0, "bb_lower": 4150.0,
            "atr": 10.0,
        }
        engine = ConfluenceDecisionEngine(CONFIG)
        advice = engine.decide(
            asset="XAU", timeframe="1h", snapshot=snapshot,
            recent_signals=[], recent_candles=candles
        )

        self.assertFalse(advice.counter_move_detected)
        self.assertEqual(advice.action, "SELL")

    def test_margin_to_price_conversion_correct(self):
        """Kiểm tra công thức cốt lõi: với leverage=5, margin_profit=10% PHẢI quy đổi đúng ra price_move=2.0%."""
        entry = 4159.30
        atr = 11.17
        engine = ConfluenceDecisionEngine(CONFIG)
        advice = engine.decide(
            asset="XAU", timeframe="1h",
            snapshot={"close": entry, "ema_fast": 4158.11, 
                      "ema_slow": 4162.64, "rsi": 38.3,
                      "bb_upper": 4164.63, "bb_lower": 4151.95, "atr": atr},
            recent_signals=[], recent_candles=[])

        self.assertTrue(advice.swing_mode)
        self.assertGreater(len(advice.tp_levels), 0)
        tp3 = advice.tp_levels[-1]
        self.assertEqual(tp3["margin_profit_pct"], 10.0)
        self.assertAlmostEqual(tp3["price_move_pct"], 2.0, places=2)
        # Với lệnh SELL: giá TP3 = entry - 2% = 4159.30 * 0.98 = 4076.114
        self.assertAlmostEqual(tp3["price"], 4076.114, places=2)

    def test_sl_margin_loss_calculated_with_leverage(self):
        """SL loss trên vốn ký quỹ phải được nhân với leverage, không phải % giá thuần."""
        entry = 4159.30
        atr = 11.17
        engine = ConfluenceDecisionEngine(CONFIG)
        advice = engine.decide(
            asset="XAU", timeframe="1h",
            snapshot={"close": entry, "ema_fast": 4158.11, 
                      "ema_slow": 4162.64, "rsi": 38.3,
                      "bb_upper": 4164.63, "bb_lower": 4151.95, "atr": atr},
            recent_signals=[], recent_candles=[])

        # Với SL = entry + 1.5 * atr = 4159.30 + 16.755 = 4176.055
        # risk_distance = 16.755, sl_price_move_pct = 16.755 / 4159.30 * 100 ≈ 0.4028%
        # Với leverage=5, sl_margin_loss_pct = 0.4028 * 5 ≈ 2.01%
        self.assertAlmostEqual(advice.sl_margin_loss_pct, 2.01, places=1)
        self.assertGreater(advice.sl_margin_loss_pct, 0.40)

    def test_ui_always_shows_both_units(self):
        """Đảm bảo mỗi tp_level LUÔN có cả 2 field price_move_pct VÀ margin_profit_pct, không được thiếu field nào."""
        entry = 4159.30
        atr = 11.17
        engine = ConfluenceDecisionEngine(CONFIG)
        advice = engine.decide(
            asset="XAU", timeframe="1h",
            snapshot={"close": entry, "ema_fast": 4158.11, 
                      "ema_slow": 4162.64, "rsi": 38.3,
                      "bb_upper": 4164.63, "bb_lower": 4151.95, "atr": atr},
            recent_signals=[], recent_candles=[])

        self.assertGreater(len(advice.tp_levels), 0)
        for level in advice.tp_levels:
            self.assertIn("price_move_pct", level)
            self.assertIn("margin_profit_pct", level)
            self.assertIn("close_pct", level)
            self.assertIn("holding_estimate", level)
            self.assertIn("price", level)
            self.assertIn("label", level)

    def test_4h_timeframe_integration(self):
        """Kiểm tra tích hợp khung nến 4h cho cả 3 tài sản và logic tính swing holding period."""
        # 1. Kiểm tra cấu hình assets có chứa 4h
        for asset in ("BTC", "XAU", "OIL"):
            self.assertIn("4h", CONFIG["assets"][asset]["timeframes"])

        # 2. Kiểm tra Confluence Engine nhận diện 4h là swing_mode
        engine = ConfluenceDecisionEngine(CONFIG)
        entry = 65000.0
        atr = 500.0
        adv_4h = engine.decide(
            asset="BTC", timeframe="4h",
            snapshot={"close": entry, "ema_fast": 65500.0, "ema_slow": 64000.0,
                      "rsi": 62.0, "bb_upper": 66000.0, "bb_mid": 65000.0, "bb_lower": 64000.0, "atr": atr},
            recent_signals=[], recent_candles=[]
        )
        self.assertTrue(adv_4h.swing_mode)
        self.assertGreater(len(adv_4h.tp_levels), 0)

        # 3. Kiểm tra ước lượng holding period cho 4h
        # Với khoảng cách 2 * (0.46 * atr) = 460 -> 2 nến 4h = 8h
        hold = engine.estimate_holding_period(price_distance=460.0, atr=atr, timeframe="4h")
        self.assertEqual(hold["candles"], 2)
        self.assertEqual(hold["hours"], 8.0)
        self.assertEqual(hold["time_label"], "8h")

    def test_advice_invalidated_when_price_crosses_sl(self):
        """Regression test: tái hiện ĐÚNG case XAU 1h đã quan sát — 
        SELL advice với SL=4132.74, giá hiện tại=4139.86 (đã vượt SL) 
        PHẢI trả về status=invalidated."""
        advice = TradeAdvice(
            action="SELL", entry_price=4112.6056, 
            stop_loss=4132.7379, confidence="Trung bình", 
            confidence_score=3, entry_type="now", entry_zone=None,
            take_profit_1=4071.4795, take_profit_2=4050.9165,
            tp2_rule="", reasoning=[], conflict_detected=False,
            mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=4139.86, atr=11.17, cfg=CONFIG
        )
        self.assertEqual(validity["status"], "invalidated")
        self.assertIn("vượt qua mức Cắt Lỗ", validity["reason"])
        self.assertGreater(validity["price_drift_atr"], 2.0)

    def test_advice_stale_but_not_invalidated(self):
        """Giá trôi > 1x ATR nhưng chưa chạm SL => status=stale, 
        không phải invalidated."""
        advice = TradeAdvice(
            action="BUY", entry_price=100.0, 
            stop_loss=95.0, confidence="Trung bình",
            confidence_score=3, entry_type="now", entry_zone=None,
            take_profit_1=105.0, take_profit_2=110.0,
            tp2_rule="", reasoning=[], conflict_detected=False,
            mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=105.5, atr=4.0, cfg=CONFIG
        )
        self.assertEqual(validity["status"], "stale")
        self.assertIn("đáng kể", validity["reason"])
        self.assertEqual(validity["price_drift_atr"], 1.38)

    def test_advice_fresh_when_price_barely_moved(self):
        """Giá gần như không đổi => status=fresh, không cảnh báo thừa."""
        advice = TradeAdvice(
            action="SELL", entry_price=100.0, 
            stop_loss=105.0, confidence="Trung bình",
            confidence_score=3, entry_type="now", entry_zone=None,
            take_profit_1=95.0, take_profit_2=90.0,
            tp2_rule="", reasoning=[], conflict_detected=False,
            mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=100.3, atr=4.0, cfg=CONFIG
        )
        self.assertEqual(validity["status"], "fresh")
        self.assertIsNone(validity["reason"])

    def test_wait_action_never_flagged_invalid(self):
        """Advice với action=WAIT không có Entry/SL thực nên luôn 
        PHẢI trả về fresh, không bao giờ invalidated/stale."""
        advice = TradeAdvice(
            action="WAIT", entry_price=None, 
            stop_loss=None, confidence="Thấp",
            confidence_score=0, entry_type="none", entry_zone=None,
            take_profit_1=None, take_profit_2=None,
            tp2_rule="", reasoning=[], conflict_detected=False,
            mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=100.0, atr=4.0, cfg=CONFIG
        )
        self.assertEqual(validity["status"], "fresh")
        self.assertIsNone(validity["reason"])

    def test_handle_snapshot_validity_field(self):
        """Kiểm tra endpoint /api/snapshot gắn field validity vào advice."""
        orch = IndicatorOrchestrator()
        server = DashboardServer(orch, "127.0.0.1", 8080)

        buf = CircularBuffer(50, "XAU", "1h")
        buf.push(Candle(ts=1700000000, open=4110.0, high=4145.0, low=4100.0, close=4139.86))
        orch._bufs["XAU"] = {"1h": buf}
        orch._latest_indicators["XAU"] = {"1h": {"close": 4139.86, "atr": 11.17}}

        adv = TradeAdvice(
            action="SELL", entry_price=4112.6056, stop_loss=4132.7379,
            confidence="Trung bình", confidence_score=3, entry_type="now", entry_zone=None,
            take_profit_1=4071.4795, take_profit_2=4050.9165, tp2_rule="",
            reasoning=[], conflict_detected=False, mtf_agreement=None
        )
        orch._latest_advice["XAU"] = {"1h": adv}

        class MockRequest:
            pass

        resp = asyncio.run(server.handle_snapshot(MockRequest()))
        self.assertEqual(resp.status, 200)
        import json
        payload = json.loads(resp.text)
        self.assertIn("advice", payload)
        self.assertIn("XAU", payload["advice"])
        self.assertIn("1h", payload["advice"]["XAU"])
        adv_json = payload["advice"]["XAU"]["1h"]
        self.assertIn("validity", adv_json)
        self.assertEqual(adv_json["validity"]["status"], "invalidated")
        self.assertIn("vượt qua mức Cắt Lỗ", adv_json["validity"]["reason"])


if __name__ == "__main__":
    unittest.main()
