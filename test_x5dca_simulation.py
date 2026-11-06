"""
Self-contained Unit Tests for X5-DCA and TP1 Breakeven-SL Simulation Module.
Hoàn toàn in-memory, zero lookahead, không dùng CSV lịch sử, không gọi API ngoài.
"""

import math
import pytest
from x5dca_simulation import (
    X5DCASimulator,
    SimConfig,
    Candle,
    Direction,
    PositionStatus,
)


class TestX5DCAMechanics:
    def test_dca_trigger_and_fill_slippage_long(self):
        """Kiểm tra công thức trigger DCA và trượt giá mua bất lợi (+0.02%)."""
        cfg = SimConfig(ENABLE_X5_DCA=True)
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 102.0, 102.0, 1000)
        assert pos is not None
        assert math.isclose(pos.qty, 75.0 / 102.0, rel_tol=1e-9)

        # Trigger = 102 - 0.80 * (15 / (75/102)) = 102 * 0.84 = 85.68
        trigger = sim.calc_dca_trigger(pos)
        assert math.isclose(trigger, 85.68, rel_tol=1e-9)

        # Chạy nến chạm trigger (nến ts=2000, sau nến vào lệnh ts=1000)
        # Proxy thanh lý là 102 - 0.975 * (15 / (75/102)) = 82.11
        # Cho low = 85.60 (chạm 85.68 nhưng > 82.11)
        c = Candle(ts=2000, open=90.0, high=90.0, low=85.60, close=86.0)
        sim.step_candle(c)

        # Khớp tại 85.68 * 1.0002 = 85.697136
        assert pos.dca_count == 1
        expected_dca_qty = 18.75 / (85.68 * 1.0002)
        expected_total_qty = (75.0 / 102.0) + expected_dca_qty
        expected_avg = 93.75 / expected_total_qty

        assert math.isclose(pos.qty, expected_total_qty, rel_tol=1e-9)
        assert math.isclose(pos.avg_entry, expected_avg, rel_tol=1e-9)
        assert math.isclose(expected_avg, 98.261387235729, rel_tol=1e-9)

    def test_entry_bar_suppression(self):
        """Vị thế vừa khớp không được xử lý TP/DCA/Liquidation trên chính nến vào lệnh."""
        cfg = SimConfig(ENABLE_X5_DCA=True)
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000)

        # Nến có cùng ts=1000, giá giảm cực sâu xuyên qua proxy thanh lý
        c_same_bar = Candle(ts=1000, open=100.0, high=105.0, low=50.0, close=55.0)
        sim.step_candle(c_same_bar)

        assert pos.is_open
        assert pos.dca_count == 0
        assert pos.status == PositionStatus.OPEN

    def test_dca_max_one_per_candle_and_hysteresis(self):
        """Tối đa 1 DCA mỗi nến và cơ chế reset trigger."""
        cfg = SimConfig(ENABLE_X5_DCA=True)
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000)

        # Nến 1 giảm chạm trigger (low=79.5), Close=80.0 để tỷ lệ lỗ >= 80% (không reset trigger)
        # Liq proxy sau DCA là ~77.44, nên 79.5 không bị thanh lý
        c1 = Candle(ts=2000, open=90.0, high=90.0, low=79.5, close=80.0)
        sim.step_candle(c1)
        assert pos.dca_count == 1
        assert not pos.dca_trigger_reset  # Đang khóa trigger vì loss_ratio >= 80%

        # Nến 2 tiếp tục ở mức thấp 79.0, chưa hồi phục -> Không được DCA tiếp vì trigger chưa reset
        c2 = Candle(ts=3000, open=80.0, high=80.0, low=78.5, close=79.0)
        sim.step_candle(c2)
        assert pos.dca_count == 1  # Vẫn là 1

        # Nến 3 giá hồi phục lên 98.0 -> Tỷ lệ lỗ giảm về < 80% -> Trigger được reset
        c3 = Candle(ts=4000, open=80.0, high=99.0, low=80.0, close=98.0)
        sim.step_candle(c3)
        assert pos.dca_trigger_reset is True


class TestLedgerAndFundingSettlement:
    def test_funding_positive_and_negative_settlement(self):
        """Funding Long trả khi rate > 0, nhận khi rate < 0."""
        cfg = SimConfig()
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000)

        # Rate > 0: Long trả
        sim.apply_funding({"BTCUSDT": 100.0}, {"BTCUSDT": 0.01}, funding_ts=2000)
        expected_fee = (75.0 / 100.0) * 100.0 * 0.01  # 0.75
        assert math.isclose(pos.ledger.funding_net, -0.75, rel_tol=1e-9)
        assert math.isclose(pos.ledger.margin_balance, 15.0 - 0.75, rel_tol=1e-9)

        # Rate < 0: Long nhận
        sim.apply_funding({"BTCUSDT": 100.0}, {"BTCUSDT": -0.02}, funding_ts=3000)
        expected_rec = (75.0 / 100.0) * 100.0 * 0.02  # 1.50
        assert math.isclose(pos.ledger.funding_net, -0.75 + 1.50, rel_tol=1e-9)
        assert math.isclose(pos.ledger.margin_balance, 14.25 + 1.50, rel_tol=1e-9)

    def test_funding_exceeds_margin_non_negative(self):
        """Funding vượt quá margin: margin không âm, kích hoạt thanh lý do cạn margin."""
        cfg = SimConfig()
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000)
        pos.ledger.margin_balance = 2.0  # Giả định margin chỉ còn 2.0

        # Phí funding danh nghĩa là 75 * 0.1 = 7.5 > 2.0
        # Thực trả 2.0, deficit do funding là 5.5
        sim.apply_funding({"BTCUSDT": 100.0}, {"BTCUSDT": 0.10}, funding_ts=2000)

        assert math.isclose(pos.ledger.funding_net, -2.0, rel_tol=1e-9)
        assert math.isclose(pos.ledger.margin_balance, 0.0, abs_tol=1e-9)
        assert pos.ledger.insurance_deficit >= 5.5
        assert pos.status == PositionStatus.LIQUIDATED

    def test_liquidation_margin_refund_when_surplus(self):
        """Thanh lý có thặng dư: hoàn trả phần dư về free_cash, không tính trùng."""
        cfg = SimConfig()
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000)
        # Cash sau mở: 100 - 15 - 0.0375 = 84.9625
        assert math.isclose(sim.free_cash, 84.9625, rel_tol=1e-9)

        # Thanh lý tại giá 95.0 (giả định)
        sim.liquidate_position(pos, 95.0, is_gap=False)

        # Exec price = 95 * 0.9995 = 94.9525
        p_exec = 95.0 * (1.0 - 0.0005)
        gross_loss = 0.75 * (100.0 - p_exec)
        fee = 0.75 * p_exec * 0.0005
        expected_refund = 15.0 - (gross_loss + fee)

        assert expected_refund > 0
        assert math.isclose(pos.ledger.margin_refund, expected_refund, rel_tol=1e-9)
        assert math.isclose(sim.free_cash, 84.9625 + expected_refund, rel_tol=1e-9)
        assert pos.status == PositionStatus.LIQUIDATED


class TestTP1BreakevenSLVariant:
    def test_tp1_activates_breakeven_sl_and_blocks_dca(self):
        """TP1 khớp kích hoạt Breakeven SL, SL đóng toàn bộ lượng còn lại và chặn DCA."""
        cfg = SimConfig(ENABLE_X5_DCA=True, ENABLE_TP1_BREAKEVEN_SL=True)
        sim = X5DCASimulator(cfg)
        pos = sim.open_position("P1", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000)

        # Nến 1: Giá tăng lên đúng 101.2 -> Chỉ chạm TP1 (+1.0% = 101.0), chưa chạm TP2 (+1.5% = 101.5)
        c1 = Candle(ts=2000, open=100.0, high=101.2, low=99.5, close=101.1)
        sim.step_candle(c1)

        assert pos.tp_hit_flags[0] is True
        assert pos.tp_hit_flags[1] is False
        assert pos.sl_active is True
        assert math.isclose(pos.sl_price, 100.0, rel_tol=1e-9)
        # Đã chốt 40% (0.3 unit), còn 60% (0.45 unit)
        assert math.isclose(pos.qty, 0.45, rel_tol=1e-9)
        assert pos.status == PositionStatus.OPEN

        # Nến 2: Giá rớt mạnh về 80 -> Không được DCA vì SL đã active, mà bị SL tại 100
        c2 = Candle(ts=3000, open=101.0, high=101.0, low=80.0, close=81.0)
        sim.step_candle(c2)

        assert pos.dca_count == 0  # Bị chặn
        assert pos.status == PositionStatus.CLOSED_SL
        assert pos.qty == 0.0


class TestSYN04AnalyticalOracle:
    def test_syn04_independent_exact_tolerance(self):
        """
        Đối soát độc lập SYN-04 với dung sai tuyệt đối <= 1e-8.
        So sánh nhánh DCA-first vs Entry-first theo đúng công thức giải tích.
        """
        # Công thức giải tích độc lập:
        q_dca = 12.5 / 84.0168
        q_total = 0.5 + q_dca
        avg = 62.5 / q_total
        p_liq = avg - 0.975 * 12.5 / q_total
        upnl_close = q_total * (82.0 - avg)

        cash_dca = 16.0 - 2.5 - 0.00625
        margin_dca = 12.5
        equity_dca_first = cash_dca + margin_dca + upnl_close

        # Branch Entry-first:
        p_exec = 80.5 * (1.0 - 0.0005)
        gross_loss_a = 0.5 * (100.0 - p_exec)
        liq_fee_a = 0.5 * p_exec * 0.0005
        margin_refund_a = 10.0 - gross_loss_a - liq_fee_a
        upnl_b = (75.0 / 2000.0) * (1950.0 - 2000.0)  # -1.875
        cash_after_b = 16.0 - 15.0 - 0.0375
        equity_entry_first = cash_after_b + margin_refund_a + 15.0 + upnl_b

        # Kiểm tra giá trị giải tích:
        assert abs(equity_dca_first - 16.693690964188) <= 1e-8
        assert abs(equity_entry_first - 14.297260062500) <= 1e-8
        assert equity_entry_first < equity_dca_first

        # Chênh lệch chính xác:
        diff = equity_dca_first - equity_entry_first
        assert abs(diff - 2.396430901688) <= 1e-8

        # Mô phỏng trong simulator:
        # Nhánh 1: Entry B trước
        sim1 = X5DCASimulator(SimConfig(INITIAL_EQUITY=16.0, ENABLE_X5_DCA=True))
        # Nạp trạng thái A (dùng fixed_margin=10.0 để khớp chính xác fixture)
        pos_a = sim1.open_position("A", "BTCUSDT", "1h", Direction.BUY, 100.0, 100.0, 1000, fixed_margin=10.0)
        # Giả lập nạp vốn ví lại đúng 16.0
        sim1.free_cash = 16.0
        pos_a.qty = 0.5
        pos_a.current_notional = 50.0
        pos_a.initial_notional = 50.0
        pos_a.ledger.initial_margin = 10.0
        pos_a.ledger.margin_balance = 10.0

        # Khớp B trước tại Open (fixed_margin=15.0)
        pos_b = sim1.open_position("B", "ETHUSDT", "1h", Direction.BUY, 2000.0, 2000.0, 2000, fixed_margin=15.0)
        assert pos_b is not None
        assert math.isclose(sim1.free_cash, 16.0 - 15.0375, abs_tol=1e-8)

        # A xét DCA tại Open -> Thiếu vốn
        dca_ok = sim1.execute_dca(pos_a, 84.0, 2000)
        assert dca_ok is False

        # BTC giảm chạm 79 -> Thanh lý A intrabar tại proxy 80.5
        sim1.liquidate_position(pos_a, 80.5, is_gap=False)
        assert pos_a.status == PositionStatus.LIQUIDATED
        assert abs(pos_a.ledger.margin_refund - margin_refund_a) <= 1e-8

        # Tính equity cuối của sim1 tại Close
        eq1 = sim1.get_total_equity({"BTCUSDT": 82.0, "ETHUSDT": 1950.0})
        assert abs(eq1 - 14.297260062500) <= 1e-8
