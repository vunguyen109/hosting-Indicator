import asyncio
import os
import tempfile
import unittest
from engine import (
    Candle, CircularBuffer, IndicatorEngine, RuleEngine,
    Signal, TelegramDispatcher, DashboardServer, IndicatorOrchestrator,
    TwelveDataFetcher, CONFIG, _parse_port, _load_dotenv, _update_dotenv,
    ConfluenceDecisionEngine, TradeAdvice, TrendState, RsiZone, BbPosition, Action, Confidence,
    AdviceValidityStatus, check_advice_validity,
    TrackStatus, SignalTrack, SignalOutcomeTracker
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

    def test_entry_zone_updated_when_price_reaches_new_ema(self):
        """Tái hiện đúng case XAU: entry_zone cũ=4129.55, EMA20 mới=4135.19, 
        giá hiện tại=4135.19 (đã chạm EMA20 mới) => status=zone_updated."""
        advice = TradeAdvice(
            action="SELL", entry_type="wait_pullback", 
            entry_zone=4129.5504, entry_price=None,
            stop_loss=4151.1098, confidence="Trung bình",
            confidence_score=3, take_profit_1=4090.0,
            take_profit_2=4070.0, tp2_rule="", reasoning=[],
            conflict_detected=False, mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=4135.19, current_ema_fast=4135.19, 
            atr=14.37, cfg=CONFIG
        )
        
        self.assertEqual(validity["status"], "zone_updated")
        self.assertAlmostEqual(validity["updated_entry_zone"], 4135.19, places=2)
        self.assertIn("thay đổi theo chỉ báo mới nhất", validity["reason"])

    def test_entry_zone_drifted_but_price_not_there_yet(self):
        """EMA20 đã đổi nhiều nhưng giá hiện tại vẫn chưa chạm vùng mới 
        => status=zone_drifted (chỉ cảnh báo, chưa nâng cấp thành 'vào ngay')."""
        advice = TradeAdvice(
            action="SELL", entry_type="wait_pullback", entry_zone=4129.55,
            entry_price=None, stop_loss=4200.0, confidence="Trung bình",
            confidence_score=3, take_profit_1=4090.0, take_profit_2=4070.0,
            tp2_rule="", reasoning=[], conflict_detected=False, mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=4150.0, current_ema_fast=4140.0, 
            atr=14.37, cfg=CONFIG
        )
        self.assertEqual(validity["status"], "zone_drifted")
        self.assertIn("dịch chuyển", validity["reason"])

    def test_entry_now_type_unaffected_by_zone_check(self):
        """entry_type='now' (không phải wait_pullback) không được áp dụng 
        logic zone-drift mới này."""
        advice = TradeAdvice(
            action="SELL", entry_type="now", entry_price=100.0, 
            entry_zone=None, stop_loss=105.0, confidence="Trung bình",
            confidence_score=3, take_profit_1=95.0, take_profit_2=90.0,
            tp2_rule="", reasoning=[], conflict_detected=False, mtf_agreement=None,
        )
        validity = check_advice_validity(
            advice, current_price=100.5, current_ema_fast=102.0, 
            atr=4.0, cfg=CONFIG
        )
        self.assertEqual(validity["status"], "fresh")

    def test_tracker_creates_only_for_valid_buy_sell_advice(self):
        """WAIT, confidence thấp, invalidated advice không được tạo track."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            # 1. Action WAIT
            wait_adv = TradeAdvice(action="WAIT", confidence="Cao", confidence_score=4, entry_price=100.0, stop_loss=95.0, take_profit_1=105.0)
            self.assertFalse(tracker.should_create_track(wait_adv, "BTC", "1h"))

            # 2. Confidence Thấp
            low_conf = TradeAdvice(action="BUY", confidence="Thấp", confidence_score=1, entry_price=100.0, stop_loss=95.0, take_profit_1=105.0)
            self.assertFalse(tracker.should_create_track(low_conf, "BTC", "1h"))

            # 3. Validity Invalidated
            inv_adv = TradeAdvice(action="BUY", confidence="Cao", confidence_score=4, entry_price=100.0, stop_loss=95.0, take_profit_1=105.0, validity={"status": "invalidated"})
            self.assertFalse(tracker.should_create_track(inv_adv, "BTC", "1h"))

            # 4. Valid BUY advice
            valid_adv = TradeAdvice(action="BUY", confidence="Cao", confidence_score=4, entry_price=100.0, stop_loss=95.0, take_profit_1=105.0)
            self.assertTrue(tracker.should_create_track(valid_adv, "BTC", "1h"))

    def test_pending_pullback_entry_fills_only_when_candle_crosses_zone(self):
        """wait_pullback chỉ fill khi low <= entry <= high."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            adv = TradeAdvice(
                action="BUY", confidence="Cao", confidence_score=4,
                entry_type="wait_pullback", entry_price=100.0, stop_loss=95.0, take_profit_1=105.0
            )
            track = tracker.maybe_create_track("BTC", "1h", adv, candle_ts=1000)
            self.assertIsNotNone(track)
            self.assertEqual(track.status, TrackStatus.PENDING_ENTRY.value)

            # Candle 1: range [101, 105] không chứa entry 100
            c1 = Candle(ts=2000, open=102.0, high=105.0, low=101.0, close=104.0, volume=10.0)
            res1 = tracker.process_closed_candle("BTC", "1h", c1)
            self.assertEqual(res1.status, TrackStatus.PENDING_ENTRY.value)

            # Candle 2: range [99, 103] đi qua entry 100 -> Fill!
            c2 = Candle(ts=3000, open=101.0, high=103.0, low=99.0, close=102.0, volume=10.0)
            res2 = tracker.process_closed_candle("BTC", "1h", c2)
            self.assertEqual(res2.status, TrackStatus.OPEN.value)
            self.assertEqual(res2.actual_entry, 100.0)

    def test_entry_does_not_evaluate_tp_or_sl_on_same_candle(self):
        """Candle fill Entry không được đồng thời dùng để chốt TP/SL."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            adv = TradeAdvice(
                action="BUY", confidence="Cao", confidence_score=4,
                entry_type="now", entry_price=100.0, stop_loss=90.0, take_profit_1=110.0
            )
            track = tracker.maybe_create_track("BTC", "1h", adv, candle_ts=1000)

            # Nến này fill Entry và có high=120 (vượt TP 110) và low=85 (vượt SL 90)
            c1 = Candle(ts=2000, open=100.0, high=120.0, low=85.0, close=105.0, volume=10.0)
            res = tracker.process_closed_candle("BTC", "1h", c1)
            # Phải là OPEN, KHÔNG được chuyển thành CLOSED_TP, CLOSED_SL hay AMBIGUOUS ở cây nến fill này!
            self.assertEqual(res.status, TrackStatus.OPEN.value)
            self.assertEqual(res.entry_filled_at_ts, 2000)

    def test_sell_tp1_then_stop_loss_calculates_weighted_r(self):
        """SELL đạt TP1 40%, sau đó SL phần 60% còn lại:
        realized_r phải là weighted result, không được đơn giản là -1R."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            adv = TradeAdvice(
                action="SELL", confidence="Cao", confidence_score=4,
                entry_type="now", entry_price=100.0, stop_loss=102.0,
                tp_levels=[
                    {"label": "TP1", "price": 99.0, "close_pct": 0.40, "margin_profit_pct": 5.0},
                    {"label": "TP2", "price": 98.0, "close_pct": 0.35, "margin_profit_pct": 7.5},
                    {"label": "TP3", "price": 97.0, "close_pct": 0.25, "margin_profit_pct": 10.0},
                ]
            )
            track = tracker.maybe_create_track("BTC", "1h", adv, candle_ts=1000)

            # Nến 1: Fill Entry
            c1 = Candle(ts=2000, open=100.0, high=101.0, low=99.5, close=100.0, volume=1.0)
            tracker.process_closed_candle("BTC", "1h", c1)

            # Nến 2: Chạm TP1 (low=98.8 <= 99.0, high=100.5 < 102.0)
            # risk_per_unit = |100 - 102| = 2.0. R_tp1 = (100 - 99)/2.0 = 0.5R.
            # Weighted TP1: 0.5R * 0.40 = +0.2R.
            c2 = Candle(ts=3000, open=100.0, high=100.5, low=98.8, close=99.2, volume=1.0)
            res2 = tracker.process_closed_candle("BTC", "1h", c2)
            self.assertEqual(res2.status, TrackStatus.PARTIAL_TP1.value)
            self.assertAlmostEqual(res2.realized_r, 0.20, places=2)

            # Nến 3: Chạm SL (high=102.5 >= 102.0)
            # 60% còn lại bị -1.0R * 0.60 = -0.60R.
            # Tổng R: 0.20 - 0.60 = -0.40R.
            c3 = Candle(ts=4000, open=99.5, high=102.5, low=99.5, close=102.0, volume=1.0)
            res3 = tracker.process_closed_candle("BTC", "1h", c3)
            self.assertEqual(res3.status, TrackStatus.CLOSED_SL.value)
            self.assertAlmostEqual(res3.realized_r, -0.40, places=2)

    def test_ambiguous_candle_excluded_from_win_loss_metrics(self):
        """Cùng nến chạm TP và SL -> AMBIGUOUS, không tính vào win/loss."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            # Case BUY: SL=95, TP1=105
            adv = TradeAdvice(action="BUY", confidence="Cao", confidence_score=4, entry_type="now", entry_price=100.0, stop_loss=95.0, take_profit_1=105.0)
            tracker.maybe_create_track("BTC", "1h", adv, candle_ts=1000)
            c1 = Candle(ts=2000, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0)
            tracker.process_closed_candle("BTC", "1h", c1)

            # Nến tiếp theo vừa quét SL 95 vừa quét TP 105
            c2 = Candle(ts=3000, open=100.0, high=106.0, low=94.0, close=99.0, volume=1.0)
            res_buy = tracker.process_closed_candle("BTC", "1h", c2)
            self.assertEqual(res_buy.status, TrackStatus.AMBIGUOUS.value)

            # Case SELL: SL=105, TP1=95
            adv_sell = TradeAdvice(action="SELL", confidence="Cao", confidence_score=4, entry_type="now", entry_price=100.0, stop_loss=105.0, take_profit_1=95.0)
            tracker.maybe_create_track("XAU", "1h", adv_sell, candle_ts=1000)
            c3 = Candle(ts=2000, open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0)
            tracker.process_closed_candle("XAU", "1h", c3)

            c4 = Candle(ts=3000, open=100.0, high=106.0, low=94.0, close=99.0, volume=1.0)
            res_sell = tracker.process_closed_candle("XAU", "1h", c4)
            self.assertEqual(res_sell.status, TrackStatus.AMBIGUOUS.value)

            # Kiểm tra metrics: Cả 2 đều loại khỏi win_count và loss_count
            m = tracker.get_metrics("ALL", "ALL", "ALL")
            self.assertEqual(m["ambiguous_count"], 2)
            self.assertEqual(m["win_count"], 0)
            self.assertEqual(m["loss_count"], 0)

    def test_invalidated_advice_before_entry_is_not_counted_as_loss(self):
        """Advice invalidated trước Entry không được coi là lệnh thua."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            adv = TradeAdvice(action="BUY", confidence="Cao", confidence_score=4, entry_type="wait_pullback", entry_price=100.0, stop_loss=95.0, take_profit_1=105.0)
            tracker.maybe_create_track("BTC", "1h", adv, candle_ts=1000)

            # Nến chưa chạm entry nhưng latest_advice bị invalidated
            inv_advice = TradeAdvice(action="BUY", confidence="Cao", confidence_score=4, entry_price=100.0, stop_loss=95.0, take_profit_1=105.0, validity={"status": "invalidated"})
            c1 = Candle(ts=2000, open=102.0, high=104.0, low=101.0, close=103.0, volume=1.0)
            res = tracker.process_closed_candle("BTC", "1h", c1, latest_advice=inv_advice)

            self.assertEqual(res.status, TrackStatus.INVALIDATED.value)
            self.assertEqual(res.realized_r, 0.0)

            m = tracker.get_metrics("ALL", "ALL", "ALL")
            self.assertEqual(m["invalidated_count"], 1)
            self.assertEqual(m["loss_count"], 0)

    def test_pending_track_expires_after_configured_candles(self):
        """Pending entry quá hạn phải thành EXPIRED."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            cfg = dict(CONFIG)
            cfg["TRACK_PENDING_EXPIRY_CANDLES"] = {"1h": 2}
            tracker = SignalOutcomeTracker(db_path=db_path, config=cfg)

            adv = TradeAdvice(action="BUY", confidence="Cao", confidence_score=4, entry_type="wait_pullback", entry_price=100.0, stop_loss=95.0, take_profit_1=105.0)
            track = tracker.maybe_create_track("BTC", "1h", adv, candle_ts=1000)
            self.assertEqual(track.expiry_at_ts, 1000 + 2 * 3600)

            # Nến 1: ts=2000 (chưa quá 1000 + 7200 = 8200)
            c1 = Candle(ts=2000, open=105.0, high=106.0, low=104.0, close=105.0, volume=1.0)
            tracker.process_closed_candle("BTC", "1h", c1)
            self.assertEqual(track.status, TrackStatus.PENDING_ENTRY.value)

            # Nến 2: ts=8500 (vượt quá 8200)
            c2 = Candle(ts=8500, open=105.0, high=106.0, low=104.0, close=105.0, volume=1.0)
            res = tracker.process_closed_candle("BTC", "1h", c2)
            self.assertEqual(res.status, TrackStatus.EXPIRED.value)

    def test_metrics_profit_factor_and_expectancy(self):
        """Kiểm tra công thức Profit Factor, Expectancy và Total R."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            # Giả lập 2 track thắng (+2R, +1R) và 1 track thua (-1R)
            t1 = SignalTrack(track_id="t1", created_at_ts=100, created_at_str="", asset="BTC", timeframe="1h", direction="BUY", confidence="Cao", confidence_score=4, status=TrackStatus.CLOSED_TP3.value, entry_type="now", planned_entry=100.0, actual_entry=100.0, entry_filled_at_ts=100, stop_loss=95.0, tp_levels=[], initial_advice_validity="fresh", advice_reasoning=[], realized_r=2.0)
            t2 = SignalTrack(track_id="t2", created_at_ts=200, created_at_str="", asset="BTC", timeframe="1h", direction="BUY", confidence="Cao", confidence_score=4, status=TrackStatus.CLOSED_TP3.value, entry_type="now", planned_entry=100.0, actual_entry=100.0, entry_filled_at_ts=200, stop_loss=95.0, tp_levels=[], initial_advice_validity="fresh", advice_reasoning=[], realized_r=1.0)
            t3 = SignalTrack(track_id="t3", created_at_ts=300, created_at_str="", asset="BTC", timeframe="1h", direction="BUY", confidence="Cao", confidence_score=4, status=TrackStatus.CLOSED_SL.value, entry_type="now", planned_entry=100.0, actual_entry=100.0, entry_filled_at_ts=300, stop_loss=95.0, tp_levels=[], initial_advice_validity="fresh", advice_reasoning=[], realized_r=-1.0)

            tracker._save_track(t1)
            tracker._save_track(t2)
            tracker._save_track(t3)

            m = tracker.get_metrics("ALL", "ALL", "ALL")
            self.assertEqual(m["win_count"], 2)
            self.assertEqual(m["loss_count"], 1)
            self.assertEqual(m["closed_count"], 3)
            self.assertAlmostEqual(m["total_r"], 2.0, places=2)
            # Profit Factor = (2.0 + 1.0) / 1.0 = 3.0
            self.assertEqual(m["profit_factor"], 3.0)
            # Expectancy = 2.0 / 3 = 0.67R
            self.assertAlmostEqual(m["expectancy_r"], 0.67, places=2)

    def test_metrics_does_not_claim_profit_with_under_30_samples(self):
        """Sample < 30 phải trả sample_size_warning."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            t1 = SignalTrack(track_id="t1", created_at_ts=100, created_at_str="", asset="BTC", timeframe="1h", direction="BUY", confidence="Cao", confidence_score=4, status=TrackStatus.CLOSED_TP3.value, entry_type="now", planned_entry=100.0, actual_entry=100.0, entry_filled_at_ts=100, stop_loss=95.0, tp_levels=[], initial_advice_validity="fresh", advice_reasoning=[], realized_r=2.0)
            tracker._save_track(t1)

            m = tracker.get_metrics("ALL", "ALL", "ALL")
            self.assertIsNotNone(m["sample_size_warning"])
            self.assertIn("tối thiểu 30", m["sample_size_warning"])

    def test_tracker_database_is_bounded(self):
        """Khi vượt TRACKER_MAX_HISTORY_RECORDS, phải trim terminal records cũ."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.db")
            cfg = dict(CONFIG)
            cfg["TRACKER_MAX_HISTORY_RECORDS"] = 5
            tracker = SignalOutcomeTracker(db_path=db_path, config=cfg)

            # Insert 7 terminal tracks
            for i in range(7):
                t = SignalTrack(
                    track_id=f"t_{i}", created_at_ts=1000 + i, created_at_str="", asset="BTC", timeframe="1h",
                    direction="BUY", confidence="Cao", confidence_score=4, status=TrackStatus.CLOSED_SL.value,
                    entry_type="now", planned_entry=100.0, actual_entry=100.0, entry_filled_at_ts=1000 + i,
                    stop_loss=95.0, tp_levels=[], initial_advice_validity="fresh", advice_reasoning=[],
                    realized_r=-1.0
                )
                tracker._save_track(t)

            tracker._trim_database_if_needed()

            with tracker._get_connection() as conn:
                cnt = conn.execute("SELECT COUNT(*) as c FROM signal_tracks").fetchone()["c"]
                self.assertLessEqual(cnt, 5)


class TestRsiZoneConsistency(unittest.TestCase):
    def test_rsi_label_consistency_across_widgets(self):
        """Đảm bảo cùng 1 giá trị RSI cho cùng 1 nhãn ở cả Gauge và Advice Card."""
        from engine import build_market_mood_gauge, classify_rsi_zone, RSI_ZONE_LABEL_MAP
        rsi_value = 63.6
        gauge_label = build_market_mood_gauge(rsi=rsi_value)["rsi_label"]
        advice_reasoning_zone = classify_rsi_zone(rsi_value)
        self.assertEqual(RSI_ZONE_LABEL_MAP[advice_reasoning_zone], gauge_label)

    def test_rsi_boundary_60_is_lean_overbought(self):
        from engine import classify_rsi_zone
        self.assertEqual(classify_rsi_zone(60.0), "LEAN_OVERBOUGHT")
        self.assertEqual(classify_rsi_zone(59.9), "NEUTRAL")

    def test_gauge_uses_same_classify_function_as_confluence_engine(self):
        # Regression chống tái phát: đảm bảo Gauge không tự định nghĩa ngưỡng riêng
        import inspect
        from engine import build_market_mood_gauge
        gauge_source = inspect.getsource(build_market_mood_gauge)
        self.assertIn("classify_rsi_zone", gauge_source,
            "Gauge phải gọi classify_rsi_zone() dùng chung, không tự if/elif ngưỡng riêng")



# ============================================================================
# UNIT TESTS REGRESSION CHO PROMPT 4: LOẠI BỎ 15M, ẨN ACTION, ROWCOUNT & DỌN ORPHAN CODE
# ============================================================================

class TestPrompt4DeprecationAndActionHiding(unittest.TestCase):
    """
    Test suite regression cho Prompt 4: Loại bỏ 15m, ẩn Action khi confidence Thấp,
    kiểm tra rowcount migration chính xác và loại bỏ orphan code 15m.
    """

    def test_15m_stub_structure(self):
        """1. Stub 15m phải đúng schema: status=deprecated, available=False, candles=[], advice=None."""
        from engine import build_timeframe_payload
        payload = build_timeframe_payload("BTC", "15m")
        self.assertEqual(payload["status"], "deprecated")
        self.assertFalse(payload["available"])
        self.assertEqual(payload["timeframe"], "15m")
        self.assertEqual(payload["candles"], [])
        self.assertIsNone(payload["advice"])
        self.assertEqual(payload["replacement_timeframes"], ["1h", "4h"])
        self.assertGreater(len(payload["reason"]), 0)

    def test_15m_key_still_present_in_schema(self):
        """2. Key '15m' vẫn phải tồn tại trong snapshot schema của mọi asset dưới dạng stub."""
        from engine import build_snapshot
        snapshot = build_snapshot()
        self.assertIn("timeframes", snapshot)
        for asset in ("BTC", "XAU", "OIL"):
            self.assertIn(asset, snapshot["timeframes"], f"Thiếu asset {asset} trong timeframes")
            self.assertIn("15m", snapshot["timeframes"][asset], f"Thiếu key 15m cho {asset}")
            self.assertEqual(snapshot["timeframes"][asset]["15m"]["status"], "deprecated")

    def test_confidence_low_hides_action(self):
        """3. Khi confidence LOW/Thấp: display_action=HIDDEN, giấu Entry/SL/TP nhưng giữ reasoning."""
        from engine import make_advice, build_advice_payload
        adv = make_advice(
            action="BUY",
            confidence="LOW",
            entry=50000.0,
            sl=49000.0,
            tp1=51000.0,
            tp2=52000.0,
            reasoning=["Tín hiệu RSI phân kỳ yếu"]
        )
        payload = build_advice_payload(adv)
        self.assertEqual(payload["display_action"], "HIDDEN")
        self.assertIsNone(payload["entry_price"])
        self.assertIsNone(payload["tp1_price"])
        self.assertIsNone(payload["tp2_price"])
        self.assertIsNone(payload["sl_price"])
        self.assertGreater(len(payload["reasoning"]), 0)
        self.assertEqual(payload["display_label"], "Tín hiệu yếu – chưa đủ điều kiện khuyến nghị")

    def test_confidence_medium_shows_action(self):
        """4. Khi confidence MEDIUM/Trung bình: display_action giữ nguyên, hiển thị đầy đủ Entry/SL/TP."""
        from engine import make_advice, build_advice_payload
        adv = make_advice(
            action="BUY",
            confidence="MEDIUM",
            entry=50000.0,
            sl=49000.0,
            tp1=51000.0,
            tp2=52000.0,
            reasoning=["Xu hướng tăng rõ ràng"]
        )
        payload = build_advice_payload(adv)
        self.assertEqual(payload["display_action"], "BUY")
        self.assertEqual(payload["entry_price"], 50000.0)
        self.assertEqual(payload["sl_price"], 49000.0)
        self.assertEqual(payload["tp1_price"], 51000.0)
        self.assertEqual(payload["tp2_price"], 52000.0)

    def test_wait_action_never_hidden_regardless_confidence(self):
        """5. Action WAIT không bao giờ bị ẩn thành HIDDEN bất kể confidence là LOW hay HIGH."""
        from engine import make_advice, build_advice_payload
        for conf in ("LOW", "Thấp", "MEDIUM", "Trung bình", "HIGH", "Cao"):
            adv = make_advice(action="WAIT", confidence=conf)
            payload = build_advice_payload(adv)
            self.assertEqual(payload["display_action"], "WAIT", f"WAIT bị đổi thành {payload['display_action']} ở confidence {conf}")

    def test_fetcher_interval_map_has_no_15m(self):
        """6. INTERVAL_MAP của BinanceFetcher và TwelveDataFetcher tuyệt đối không chứa key 15m."""
        from engine import BinanceFetcher, TwelveDataFetcher
        self.assertNotIn("15m", BinanceFetcher.INTERVAL_MAP)
        self.assertNotIn("15m", TwelveDataFetcher.INTERVAL_MAP)
        self.assertNotIn("15min", TwelveDataFetcher.INTERVAL_MAP)

    def test_orchestrator_loop_skips_15m(self):
        """7. Orchestrator.run_cycle() chỉ xử lý 1h và 4h, bỏ qua hoàn toàn 15m."""
        from engine import Orchestrator
        from unittest.mock import patch
        orch = Orchestrator()
        processed_timeframes = []

        def mock_process(self_orch, asset, tf):
            processed_timeframes.append(tf)

        with patch.object(Orchestrator, "_process_timeframe", mock_process):
            orch.run_cycle()

        self.assertGreater(len(processed_timeframes), 0)
        self.assertNotIn("15m", processed_timeframes)
        self.assertTrue(set(processed_timeframes).issubset({"1h", "4h"}))

    def test_migration_closes_stale_15m_tracks(self):
        """8. migrate_deprecated_timeframe_trades đóng tất cả track 15m chưa đóng thành 'expired' với result_note chuẩn."""
        import sqlite3
        from unittest.mock import MagicMock
        from engine import migrate_deprecated_timeframe_trades, IndicatorOrchestrator

        # Case 1: Gọi trực tiếp hàm standalone với conn
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = os.path.join(tmp_dir, "test_mig_standalone.db")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE signal_tracks (
                    track_id TEXT PRIMARY KEY,
                    asset TEXT,
                    timeframe TEXT,
                    status TEXT,
                    result_note TEXT
                )
            """)
            cursor.execute("INSERT INTO signal_tracks VALUES ('t1', 'BTC', '15m', 'open', NULL)")
            cursor.execute("INSERT INTO signal_tracks VALUES ('t2', 'BTC', '15m', 'pending_entry', NULL)")
            cursor.execute("INSERT INTO signal_tracks VALUES ('t3', 'BTC', '1h', 'open', NULL)")
            cursor.execute("INSERT INTO signal_tracks VALUES ('t4', 'BTC', '15m', 'tp_hit', 'closed_tp')")
            conn.commit()

            closed_cnt = migrate_deprecated_timeframe_trades(conn)
            self.assertEqual(closed_cnt, 2, "Hàm standalone phải đóng chính xác 2 track 15m chưa chốt")

            cursor.execute("SELECT track_id, status, result_note FROM signal_tracks ORDER BY track_id")
            rows = {r[0]: (r[1], r[2]) for r in cursor.fetchall()}
            conn.close()

            self.assertEqual(rows["t1"], ("expired", "timeframe_deprecated_migration"))
            self.assertEqual(rows["t2"], ("expired", "timeframe_deprecated_migration"))
            self.assertEqual(rows["t3"], ("open", None))
            self.assertEqual(rows["t4"], ("tp_hit", "closed_tp"))

        # Case 2: Gọi qua wrapper orch.migrate_deprecated_timeframe_trades(db_conn=conn)
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = os.path.join(tmp_dir, "test_mig_orch_conn.db")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE signal_tracks (
                    track_id TEXT PRIMARY KEY,
                    asset TEXT,
                    timeframe TEXT,
                    status TEXT,
                    result_note TEXT
                )
            """)
            cursor.execute("INSERT INTO signal_tracks VALUES ('o1', 'BTC', '15m', 'open', NULL)")
            cursor.execute("INSERT INTO signal_tracks VALUES ('o2', 'BTC', '15m', 'partial_tp1', NULL)")
            cursor.execute("INSERT INTO signal_tracks VALUES ('o3', 'BTC', '4h', 'open', NULL)")
            conn.commit()

            orch = IndicatorOrchestrator()
            closed_cnt = orch.migrate_deprecated_timeframe_trades(db_conn=conn)
            self.assertEqual(closed_cnt, 2, "Wrapper orch(db_conn) phải đóng chính xác 2 track")

            cursor.execute("SELECT track_id, status, result_note FROM signal_tracks ORDER BY track_id")
            rows = {r[0]: (r[1], r[2]) for r in cursor.fetchall()}
            conn.close()

            self.assertEqual(rows["o1"], ("expired", "timeframe_deprecated_migration"))
            self.assertEqual(rows["o2"], ("expired", "timeframe_deprecated_migration"))
            self.assertEqual(rows["o3"], ("open", None))

        # Case 3: Nhánh fallback qua self.outcome_tracker.db_path khi không truyền db_conn
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = os.path.join(tmp_dir, "test_mig_fallback.db")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE signal_tracks (
                    track_id TEXT PRIMARY KEY,
                    asset TEXT,
                    timeframe TEXT,
                    status TEXT,
                    result_note TEXT
                )
            """)
            cursor.execute("INSERT INTO signal_tracks VALUES ('fb1', 'BTC', '15m', 'pending_entry', NULL)")
            conn.commit()
            conn.close()

            orch = IndicatorOrchestrator()
            orch.outcome_tracker = MagicMock(db_path=db_file)
            closed_cnt = orch.migrate_deprecated_timeframe_trades()
            self.assertEqual(closed_cnt, 1, "Fallback qua outcome_tracker.db_path phải đóng đúng 1 track")

            verify_conn = sqlite3.connect(db_file)
            v_cur = verify_conn.cursor()
            v_cur.execute("SELECT status, result_note FROM signal_tracks WHERE track_id='fb1'")
            row = v_cur.fetchone()
            verify_conn.close()
            self.assertEqual(row, ("expired", "timeframe_deprecated_migration"))

    def test_btc_1h_case_regression_still_matches_verified_numbers(self):
        """9. Replay case BTC 1h đã verify: tính toán ra đúng chính xác các giá trị Entry, TP1, TP2, TP3, SL."""
        from engine import replay_case_btc_1h, build_advice_payload
        adv = replay_case_btc_1h()
        payload = build_advice_payload(adv)

        self.assertEqual(payload["action"], "SELL")
        self.assertAlmostEqual(payload["entry_price"], 81698.1219, places=2)
        self.assertAlmostEqual(payload["tp1_price"], 80881.1407, places=2)
        self.assertAlmostEqual(payload["tp2_price"], 80472.6501, places=2)
        self.assertAlmostEqual(payload["tp3_price"], 80064.1594, places=2)
        self.assertAlmostEqual(payload["sl_price"], 82594.2353, places=2)

    def test_performance_filters_case_insensitivity_and_defaults(self):
        """10. Đảm bảo query với timeframe='ALL', 'all', hoặc không truyền param đều không bị lọc nhầm."""
        import json
        from engine import SignalOutcomeTracker, SignalTrack, TrackStatus, DashboardServer, IndicatorOrchestrator

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "tracker_test.db")
            tracker = SignalOutcomeTracker(db_path=db_path)

            t1 = SignalTrack(
                track_id="track_1", created_at_ts=1000, created_at_str="2026-10-16 01:00:00 UTC",
                asset="BTC", timeframe="1h", direction="SELL", confidence="Trung bình", confidence_score=3,
                status=TrackStatus.OPEN.value, entry_type="now", planned_entry=80000.0, actual_entry=80000.0,
                entry_filled_at_ts=1000, stop_loss=81000.0, tp_levels=[], initial_advice_validity="fresh",
                advice_reasoning=["Test reasoning"], realized_r=0.0
            )
            t2 = SignalTrack(
                track_id="track_2", created_at_ts=2000, created_at_str="2026-10-16 02:00:00 UTC",
                asset="XAU", timeframe="4h", direction="BUY", confidence="Cao", confidence_score=4,
                status=TrackStatus.PENDING_ENTRY.value, entry_type="wait_pullback", planned_entry=4150.0, actual_entry=None,
                entry_filled_at_ts=None, stop_loss=4130.0, tp_levels=[], initial_advice_validity="fresh",
                advice_reasoning=["Test reasoning"], realized_r=0.0
            )
            tracker._save_track(t1)
            tracker._save_track(t2)

            # 1. Kiểm tra trực tiếp tracker.get_metrics
            self.assertEqual(tracker.get_metrics(timeframe="ALL")["total_tracks"], 2)
            self.assertEqual(tracker.get_metrics(timeframe="all")["total_tracks"], 2)
            self.assertEqual(tracker.get_metrics(timeframe="1h")["total_tracks"], 1)
            self.assertEqual(tracker.get_metrics(timeframe="4h")["total_tracks"], 1)

            # 2. Kiểm tra trực tiếp tracker.get_recent_tracks
            self.assertEqual(len(tracker.get_recent_tracks(timeframe="ALL")), 2)
            self.assertEqual(len(tracker.get_recent_tracks(timeframe="all")), 2)
            self.assertEqual(len(tracker.get_recent_tracks(timeframe="1h")), 1)

            # 3. Kiểm tra thông qua endpoint /api/performance của DashboardServer
            orch = IndicatorOrchestrator()
            orch.outcome_tracker = tracker
            server = DashboardServer(orch, "127.0.0.1", 8080)

            class MockRequest:
                def __init__(self, query):
                    self.query = query

            req1 = MockRequest({"asset": "ALL", "tf": "ALL", "confidence": "ALL"})
            res1 = asyncio.run(server.handle_performance(req1))
            data1 = json.loads(res1.text)
            self.assertEqual(len(data1["recent_tracks"]), 2)
            self.assertEqual(data1["metrics"]["total_tracks"], 2)

            req2 = MockRequest({})
            res2 = asyncio.run(server.handle_performance(req2))
            data2 = json.loads(res2.text)
            self.assertEqual(len(data2["recent_tracks"]), 2)
            self.assertEqual(data2["metrics"]["total_tracks"], 2)

    def test_orchestrator_migration_exact_count_and_status(self):
        """11. Kiểm tra migrate_deprecated_timeframe_trades đóng chính xác 1 track 15m thành 'expired'."""
        import sqlite3
        from engine import IndicatorOrchestrator
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test_mig_exact.db")
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE signal_tracks (
                    track_id TEXT PRIMARY KEY,
                    asset TEXT,
                    timeframe TEXT,
                    status TEXT,
                    result_note TEXT
                )
            """)
            conn.execute("INSERT INTO signal_tracks VALUES ('tr_15m', 'BTC', '15m', 'pending_entry', NULL)")
            conn.execute("INSERT INTO signal_tracks VALUES ('tr_1h', 'BTC', '1h', 'pending_entry', NULL)")
            conn.commit()

            orch = IndicatorOrchestrator()
            closed_cnt = orch.migrate_deprecated_timeframe_trades(db_conn=conn)
            self.assertEqual(closed_cnt, 1, "closed_cnt phải đúng bằng 1 track được cập nhật")

            cursor = conn.cursor()
            cursor.execute("SELECT track_id, status, result_note FROM signal_tracks WHERE track_id='tr_15m'")
            row = cursor.fetchone()
            self.assertEqual(row[1], "expired")
            self.assertEqual(row[2], "timeframe_deprecated_migration")
            conn.close()

    def test_html_dashboard_no_15m_alias_ternary(self):
        """12. Xác nhận HTML_DASHBOARD không còn bất kỳ ternary alias 15m -> 15min nào."""
        from engine import HTML_DASHBOARD
        self.assertNotIn("'15min' :", HTML_DASHBOARD)
        self.assertNotIn('"15min" :', HTML_DASHBOARD)

    def test_handle_candles_15m_request_returns_empty_no_alias_lookup(self):
        """13. Gọi /api/candles?asset=BTC&tf=15m trả về [] do buffer 15m không còn tồn tại."""
        import json
        orch = IndicatorOrchestrator()
        server = DashboardServer(orch, "127.0.0.1", 8080)
        class MockRequest:
            def __init__(self, query):
                self.query = query
        req = MockRequest({"asset": "BTC", "tf": "15m"})
        resp = asyncio.run(server.handle_candles(req))
        self.assertEqual(resp.status, 200)
        self.assertEqual(json.loads(resp.text), [])


class TestOutcomeTrackerCandidateCheckLog(unittest.TestCase):
    """Prompt 1 / Check 1.6 — regression cho log chẩn đoán candidate_check.
    Không kiểm tra logic should_create_track() (đã có coverage riêng nếu tồn tại),
    chỉ xác nhận log được phát đúng, đúng field, và không làm đổi hành vi gốc."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.tmp_db = os.path.join(self.tmp_dir, "test_tracker_candidate.db")
        self.tracker = SignalOutcomeTracker(db_path=self.tmp_db)

    def test_candidate_check_log_true_and_track_persisted(self):
        """is_candidate=True → log đúng field, track ghi vào DB."""
        advice = TradeAdvice(
            action="BUY",
            confidence="Cao",
            confidence_score=5,
            entry_type="now",
            entry_price=4150.0,
            stop_loss=4100.0,
            take_profit_1=4190.0,
            take_profit_2=4210.0,
            tp_levels=[
                {"label": "TP1", "price": 4190.0, "close_pct": 0.4, "margin_profit_pct": 5.0},
            ],
            reasoning=["unit test candidate_check true"],
        )
        with self.assertLogs(level="INFO") as cm:
            track = self.tracker.maybe_create_track(
                asset="XAU_TEST",
                timeframe="1h",
                advice=advice,
                candle_ts=1700000000,
            )

        self.assertIsNotNone(track, "Advice hợp lệ phải tạo được track")

        joined = "\n".join(cm.output)
        self.assertIn("[OUTCOME_TRACKER_DEBUG] candidate_check", joined)
        self.assertIn("asset=XAU_TEST", joined)
        self.assertIn("tf=1h", joined)
        self.assertIn("action=BUY", joined)
        self.assertIn("confidence=Cao", joined)
        self.assertIn("is_candidate=True", joined)

        import sqlite3
        conn = sqlite3.connect(self.tracker.db_path)
        rows = conn.execute(
            "SELECT * FROM signal_tracks WHERE asset='XAU_TEST'"
        ).fetchall()
        conn.close()
        self.assertEqual(len(rows), 1, "Track phải được ghi vào DB khi is_candidate=True")

    def test_candidate_check_log_false_low_confidence_no_track(self):
        """is_candidate=False (confidence=Thấp) → log đúng field, không ghi DB."""
        advice = TradeAdvice(
            action="BUY",
            confidence="Thấp",    # LOW → should_create_track() reject
            confidence_score=1,
            entry_type="now",
            entry_price=4150.0,
            stop_loss=4100.0,
            take_profit_1=4190.0,
        )
        with self.assertLogs(level="INFO") as cm:
            track = self.tracker.maybe_create_track(
                asset="XAU_TEST2",
                timeframe="1h",
                advice=advice,
                candle_ts=1700000000,
            )

        self.assertIsNone(track, "Confidence Thấp không được tạo track")

        joined = "\n".join(cm.output)
        self.assertIn("[OUTCOME_TRACKER_DEBUG] candidate_check", joined)
        self.assertIn("confidence=Thấp", joined)
        self.assertIn("is_candidate=False", joined)

        import sqlite3
        conn = sqlite3.connect(self.tracker.db_path)
        rows = conn.execute(
            "SELECT * FROM signal_tracks WHERE asset='XAU_TEST2'"
        ).fetchall()
        conn.close()
        self.assertEqual(len(rows), 0, "Không được ghi track khi is_candidate=False")


class TestRsiZoneSSOTAcrossAllIndicatorSources(unittest.TestCase):
    """Prompt 3 (mở rộng) — đảm bảo rsi_zone nhất quán ở cả 3 điểm ghi
    _latest_indicators: init (_init_buffers), hot-reload (reload_asset_buffers),
    nến đóng (_process). Tái hiện đúng lớp bug quan sát: RSI=63.6 từng bị
    phân loại khác nhau do nhiều bộ ngưỡng JS độc lập (70/30 vs 60/40)."""

    def _make_buf(self, n=40):
        buf = CircularBuffer(50, "BTC", "1h")
        for i in range(n):
            buf.push(Candle(
                ts=1000 + i * 3600,
                open=100.0 + i * 0.3,
                high=105.0 + i * 0.3,
                low=95.0 + i * 0.3,
                close=100.0 + i * 0.5,
                volume=10.0,
            ))
        return buf

    def test_helper_adds_rsi_zone_matching_classify_rsi_zone(self):
        from engine import classify_rsi_zone, _compute_indicators_with_zone
        buf = self._make_buf()
        ind = _compute_indicators_with_zone(buf)
        self.assertIn("rsi", ind)
        self.assertIn("rsi_zone", ind)
        expected_zone = classify_rsi_zone(ind["rsi"], cfg=CONFIG)
        self.assertEqual(ind["rsi_zone"], expected_zone)

    def test_rsi_field_and_all_other_fields_unchanged(self):
        """Constraint #6: chỉ thêm field rsi_zone, không đổi field nào khác."""
        from engine import _compute_indicators_with_zone
        buf_plain = self._make_buf()
        buf_wrapped = self._make_buf()
        ind_plain = IndicatorEngine.compute_all(buf_plain)
        ind_wrapped = _compute_indicators_with_zone(buf_wrapped)

        for key, value in ind_plain.items():
            self.assertIn(key, ind_wrapped)
            self.assertEqual(ind_wrapped[key], value, f"Field '{key}' bị thay đổi")

        self.assertNotIn("rsi_zone", ind_plain)
        self.assertIn("rsi_zone", ind_wrapped)

    def test_zone_boundaries_59_9_60_0_69_9(self):
        from engine import classify_rsi_zone
        self.assertEqual(classify_rsi_zone(59.9, cfg=CONFIG), "NEUTRAL")
        self.assertEqual(classify_rsi_zone(60.0, cfg=CONFIG), "LEAN_OVERBOUGHT")
        self.assertEqual(classify_rsi_zone(69.9, cfg=CONFIG), "LEAN_OVERBOUGHT")

    def test_no_duplicate_direct_compute_all_calls_outside_helper(self):
        """Chặn tái phát lệch pha: chỉ được có đúng 1 lời gọi
        IndicatorEngine.compute_all(buf) trong toàn bộ engine.py —
        bên trong _compute_indicators_with_zone(). Nếu có call site khác
        bỏ qua helper, rsi_zone sẽ lại lệch pha như bug gốc."""
        with open("engine.py", encoding="utf-8") as f:
            src = f.read()
        direct_calls = src.count("IndicatorEngine.compute_all(buf)")
        self.assertEqual(
            direct_calls, 1,
            "Phát hiện có call site tính indicators KHÔNG qua "
            "_compute_indicators_with_zone() — nguy cơ rsi_zone lệch pha."
        )

class TestHandleIndexSSOT(unittest.TestCase):
    """
    Kiểm thử bảo đảm handle_index() dùng HTML_DASHBOARD làm SSOT duy nhất,
    không đọc file dashboard.html và không bao giờ bị override bởi file trên đĩa.
    """

    def test_handle_index_contains_all_ssot_signatures(self):
        """Chiều 1: response của handle_index phải chứa đủ cả 3 chữ ký của Prompt 3 & Prompt 4."""
        from engine import HTML_DASHBOARD
        server = DashboardServer(orchestrator=None, host="127.0.0.1", port=8080)
        resp = asyncio.run(server.handle_index(None))
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content_type, "text/html")
        self.assertEqual(resp.charset, "utf-8")
        self.assertEqual(resp.text, HTML_DASHBOARD)

        # Chữ ký Prompt 3
        self.assertIn("RSI_GAUGE_LABELS", resp.text)
        self.assertIn("RSI_TABLE_LABELS", resp.text)
        # Chữ ký Prompt 4
        self.assertIn("display_action === 'HIDDEN'", resp.text)
        self.assertIn('id="btnTf15m" style="display:none;"', resp.text)

    def test_handle_index_ignores_dummy_dashboard_html_on_disk(self):
        """Chiều 2: Nếu có file dashboard.html giả lập rác trên đĩa, handle_index vẫn trả về HTML_DASHBOARD nguyên bản."""
        from engine import HTML_DASHBOARD
        dummy_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        dummy_content = "<html><body>DUMMY_CORRUPTED_DASHBOARD_CONTENT</body></html>"
        server = DashboardServer(orchestrator=None, host="127.0.0.1", port=8080)

        # Đảm bảo dọn dẹp bằng try/finally
        try:
            with open(dummy_path, "w", encoding="utf-8") as f:
                f.write(dummy_content)

            resp = asyncio.run(server.handle_index(None))
            # Response phải khớp 100% với HTML_DASHBOARD, không bị nội dung rác override
            self.assertEqual(resp.text, HTML_DASHBOARD)
            self.assertNotIn("DUMMY_CORRUPTED_DASHBOARD_CONTENT", resp.text)
            self.assertIn("RSI_GAUGE_LABELS", resp.text)
            self.assertIn("RSI_TABLE_LABELS", resp.text)
            self.assertIn("display_action === 'HIDDEN'", resp.text)
        finally:
            if os.path.exists(dummy_path):
                os.remove(dummy_path)

        # Sau khi dọn dẹp, xác nhận lại file dummy không còn sót lại
        self.assertFalse(os.path.exists(dummy_path))


if __name__ == "__main__":
    unittest.main()



