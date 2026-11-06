"""
X5-DCA and TP1 Breakeven-SL Simulation Module (DEV / Paper / Simulation-Only)

CẢNH BÁO KIỂM SOÁT:
- Module này CHỈ dùng cho mô phỏng in-memory, paper testing, và nghiên cứu thuật toán.
- KHÔNG gửi lệnh ra sàn thực tế, KHÔNG kết nối API ngoài, KHÔNG đọc credentials/API keys.
- Hai tính năng X5-DCA và TP1 Breakeven-SL mặc định là OFF (False).
- Baseline của repo giữ nguyên 100% khi các cờ tính năng ở trạng thái mặc định OFF.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "wait_pullback"


class PositionStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED_TP = "CLOSED_TP"
    CLOSED_SL = "CLOSED_SL"
    LIQUIDATED = "LIQUIDATED"


@dataclass
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    asset: str = "BTCUSDT"
    timeframe: str = "1h"


@dataclass
class PositionLedger:
    initial_margin: float = 0.0
    total_dca_margin_added: float = 0.0
    margin_balance: float = 0.0
    gross_realized_pnl: float = 0.0
    fees_paid_by_account: float = 0.0
    funding_net: float = 0.0
    forfeited_margin: float = 0.0
    margin_refund: float = 0.0
    insurance_deficit: float = 0.0


@dataclass
class Position:
    pos_id: str
    asset: str
    timeframe: str
    direction: Direction
    entry_ts: int
    entry_price: float
    planned_entry: float
    qty: float
    current_notional: float
    initial_notional: float
    ladder_base_qty: float

    # Trạng thái TP / SL
    tp_hit_flags: List[bool] = field(default_factory=lambda: [False, False, False])
    sl_active: bool = False
    sl_price: Optional[float] = None

    # Trạng thái DCA
    dca_count: int = 0
    last_dca_candle_ts: Optional[int] = None
    dca_trigger_reset: bool = True

    # Ledger
    ledger: PositionLedger = field(default_factory=PositionLedger)
    status: PositionStatus = PositionStatus.OPEN

    @property
    def avg_entry(self) -> float:
        return self.current_notional / self.qty if self.qty > 0 else self.entry_price

    @property
    def is_open(self) -> bool:
        return self.status == PositionStatus.OPEN and self.qty > 0


@dataclass
class SimConfig:
    # Cờ tính năng (Mặc định đều OFF)
    ENABLE_X5_DCA: bool = False
    ENABLE_TP1_BREAKEVEN_SL: bool = False

    # Vốn và đòn bẩy
    INITIAL_EQUITY: float = 100.0
    LEVERAGE: float = 5.0
    INITIAL_MARGIN_PCT: float = 0.15  # 15% equity = 15.0 USDT
    MAX_ACTIVE_POSITIONS: int = 5

    # DCA config
    DCA_MARGIN_LOSS_TRIGGER: float = 0.80  # 80% current margin loss
    DCA_NOTIONAL_PCT: float = 0.25  # 25% initial notional (18.75 USDT)
    DCA_SLIPPAGE: float = 0.0002  # 0.02% bất lợi

    # Phí và trượt giá
    TAKER_FEE: float = 0.0005  # 0.05%
    MAKER_FEE: float = 0.0002  # 0.02%
    SL_SLIPPAGE: float = 0.0005  # 0.05%
    LIQ_SLIPPAGE: float = 0.0005  # 0.05%

    # Liquidation threshold
    LIQ_BUFFER: float = 0.975  # 97.5% margin loss proxy

    # TP Ladder percentages
    TP_TIERS: Tuple[float, float, float] = (0.010, 0.015, 0.020)  # +1.0%, +1.5%, +2.0%
    TP_PCTS: Tuple[float, float, float] = (0.40, 0.35, 0.25)


class X5DCASimulator:
    def __init__(self, config: Optional[SimConfig] = None):
        self.config = config or SimConfig()
        self.free_cash = self.config.INITIAL_EQUITY
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []
        self.audit_logs: List[str] = []

    def log(self, msg: str) -> None:
        self.audit_logs.append(msg)

    # -------------------------------------------------------------------------
    # HÀM TOÁN HỌC & PROXY
    # -------------------------------------------------------------------------
    def calc_liquidation_proxy(self, pos: Position) -> float:
        if pos.qty <= 0:
            return 0.0
        margin_term = self.config.LIQ_BUFFER * (pos.ledger.margin_balance / pos.qty)
        if pos.direction == Direction.BUY:
            return pos.avg_entry - margin_term
        else:
            return pos.avg_entry + margin_term

    def calc_dca_trigger(self, pos: Position) -> float:
        if pos.qty <= 0:
            return 0.0
        trigger_term = self.config.DCA_MARGIN_LOSS_TRIGGER * (pos.ledger.margin_balance / pos.qty)
        if pos.direction == Direction.BUY:
            return pos.avg_entry - trigger_term
        else:
            return pos.avg_entry + trigger_term

    def calc_unrealized_pnl(self, pos: Position, mark_price: float) -> float:
        if pos.qty <= 0 or not pos.is_open:
            return 0.0
        if pos.direction == Direction.BUY:
            return pos.qty * (mark_price - pos.avg_entry)
        else:
            return pos.qty * (pos.avg_entry - mark_price)

    def get_total_equity(self, mark_prices: Dict[str, float]) -> float:
        total = self.free_cash
        for pos in self.positions.values():
            if pos.is_open:
                total += pos.ledger.margin_balance
                mp = mark_prices.get(pos.asset, pos.avg_entry)
                total += self.calc_unrealized_pnl(pos, mp)
        return total

    # -------------------------------------------------------------------------
    # MỞ VỊ THẾ
    # -------------------------------------------------------------------------
    def open_position(
        self,
        pos_id: str,
        asset: str,
        timeframe: str,
        direction: Direction,
        planned_entry: float,
        actual_entry: float,
        entry_ts: int,
        fixed_margin: Optional[float] = None,
    ) -> Optional[Position]:
        active_count = sum(1 for p in self.positions.values() if p.is_open)
        if active_count >= self.config.MAX_ACTIVE_POSITIONS:
            self.log(f"REJECTED_SLOT_LIMIT: {pos_id}")
            return None

        for p in self.positions.values():
            if p.is_open and p.asset == asset and p.direction == direction:
                self.log(f"REJECTED_CONCURRENT_SAME_ASSET: {pos_id}")
                return None

        # Vốn ban đầu (nếu truyền fixed_margin thì dùng fixed_margin, ngược lại dùng 15.0 cố định theo baseline 100 equity)
        margin = fixed_margin if fixed_margin is not None else 15.0
        notional = margin * self.config.LEVERAGE  # e.g. 75.0
        entry_fee = notional * self.config.TAKER_FEE  # 0.0375

        if self.free_cash < margin + entry_fee:
            self.log(f"REJECTED_INSUFFICIENT_FUNDS: {pos_id}")
            return None

        qty = notional / actual_entry
        self.free_cash -= (margin + entry_fee)

        ledger = PositionLedger(
            initial_margin=margin,
            margin_balance=margin,
            fees_paid_by_account=entry_fee,
        )

        pos = Position(
            pos_id=pos_id,
            asset=asset,
            timeframe=timeframe,
            direction=direction,
            entry_ts=entry_ts,
            entry_price=actual_entry,
            planned_entry=planned_entry,
            qty=qty,
            current_notional=notional,
            initial_notional=notional,
            ladder_base_qty=qty,
            ledger=ledger,
        )
        self.positions[pos_id] = pos
        self.log(f"OPENED_POSITION: {pos_id} at {actual_entry}, Qty={qty}, Margin={margin}")
        return pos

    # -------------------------------------------------------------------------
    # THỰC THI DCA
    # -------------------------------------------------------------------------
    def execute_dca(self, pos: Position, trigger_price: float, candle_ts: int) -> bool:
        if not self.config.ENABLE_X5_DCA:
            return False

        if pos.sl_active:
            self.log(f"DCA_BLOCKED_SL_ACTIVE: {pos.pos_id}")
            return False

        if pos.last_dca_candle_ts == candle_ts:
            self.log(f"DCA_BLOCKED_MAX_1_PER_CANDLE: {pos.pos_id}")
            return False

        if not pos.dca_trigger_reset:
            self.log(f"DCA_BLOCKED_HYSTERESIS: {pos.pos_id}")
            return False

        # DCA notional cố định 25% initial notional
        dca_notional = pos.initial_notional * self.config.DCA_NOTIONAL_PCT
        dca_margin = dca_notional / self.config.LEVERAGE
        dca_fee = dca_notional * self.config.TAKER_FEE

        if self.free_cash < dca_margin + dca_fee:
            self.log(f"DCA_REJECTED_INSUFFICIENT_FUNDS: {pos.pos_id}")
            pos.dca_trigger_reset = False
            return False

        # Slippage bất lợi: Long mua cao hơn (+0.02%), Short bán thấp hơn (-0.02%)
        if pos.direction == Direction.BUY:
            dca_fill = trigger_price * (1.0 + self.config.DCA_SLIPPAGE)
        else:
            dca_fill = trigger_price * (1.0 - self.config.DCA_SLIPPAGE)

        q_dca = dca_notional / dca_fill
        self.free_cash -= (dca_margin + dca_fee)

        # Cập nhật position
        pos.qty += q_dca
        pos.current_notional += dca_notional
        pos.dca_count += 1
        pos.last_dca_candle_ts = candle_ts
        pos.dca_trigger_reset = False

        # Cập nhật ledger
        pos.ledger.total_dca_margin_added += dca_margin
        pos.ledger.margin_balance += dca_margin
        pos.ledger.fees_paid_by_account += dca_fee

        self.log(f"DCA_FILLED: {pos.pos_id} at {dca_fill}, AddedQty={q_dca}, NewAvg={pos.avg_entry}")
        return True

    # -------------------------------------------------------------------------
    # THANH LÝ (LIQUIDATION SETTLEMENT)
    # -------------------------------------------------------------------------
    def liquidate_position(self, pos: Position, raw_price: float, is_gap: bool = False) -> None:
        if not pos.is_open:
            return

        # Giá khớp thanh lý kèm slippage bất lợi 0.05%
        if pos.direction == Direction.BUY:
            exec_price = raw_price * (1.0 - self.config.LIQ_SLIPPAGE)
            gross_loss = pos.qty * (pos.avg_entry - exec_price)
        else:
            exec_price = raw_price * (1.0 + self.config.LIQ_SLIPPAGE)
            gross_loss = pos.qty * (exec_price - pos.avg_entry)

        fee_liq_nominal = pos.qty * exec_price * self.config.TAKER_FEE
        m_pre_liq = pos.ledger.margin_balance
        total_liability = gross_loss + fee_liq_nominal

        if total_liability < m_pre_liq:
            # Thặng dư ký quỹ -> Hoàn trả phần dư về free_cash
            margin_refund = m_pre_liq - total_liability
            self.free_cash += margin_refund

            pos.ledger.forfeited_margin = gross_loss
            pos.ledger.fees_paid_by_account += fee_liq_nominal
            pos.ledger.margin_refund = margin_refund
            pos.ledger.margin_balance = 0.0
        else:
            # Thâm hụt ký quỹ -> Cạn toàn bộ margin, bảo hiểm chịu phần vượt
            fee_account = min(fee_liq_nominal, m_pre_liq)
            forfeited = m_pre_liq - fee_account
            deficit = total_liability - m_pre_liq

            pos.ledger.forfeited_margin = forfeited
            pos.ledger.fees_paid_by_account += fee_account
            pos.ledger.insurance_deficit += deficit
            pos.ledger.margin_refund = 0.0
            pos.ledger.margin_balance = 0.0

        pos.qty = 0.0
        pos.current_notional = 0.0
        pos.status = PositionStatus.LIQUIDATED
        self.closed_positions.append(pos)
        self.log(f"LIQUIDATED: {pos.pos_id} at {exec_price}, Loss={gross_loss}, Fee={fee_liq_nominal}")

    # -------------------------------------------------------------------------
    # TAKE PROFIT & BREAKEVEN SL
    # -------------------------------------------------------------------------
    def close_partial_tp(self, pos: Position, tp_index: int, target_price: float) -> None:
        if not pos.is_open or pos.tp_hit_flags[tp_index]:
            return

        # Lưu lại avg_entry TRƯỚC KHI đóng partial để gán sl_price nếu là TP1
        pre_tp_avg = pos.avg_entry

        pct = self.config.TP_PCTS[tp_index]
        close_qty = pos.ladder_base_qty * pct
        if close_qty > pos.qty:
            close_qty = pos.qty

        # PnL gộp
        if pos.direction == Direction.BUY:
            pnl = close_qty * (target_price - pre_tp_avg)
        else:
            pnl = close_qty * (pre_tp_avg - target_price)

        fee = close_qty * target_price * self.config.MAKER_FEE
        released_margin = pos.ledger.margin_balance * (close_qty / pos.qty)
        pos.ledger.margin_balance -= released_margin

        self.free_cash += (released_margin + pnl - fee)

        pos.ledger.gross_realized_pnl += pnl
        pos.ledger.fees_paid_by_account += fee
        pos.qty -= close_qty
        pos.current_notional = pos.qty * pre_tp_avg
        pos.tp_hit_flags[tp_index] = True

        self.log(f"TP_HIT_{tp_index+1}: {pos.pos_id} at {target_price}, Qty={close_qty}, PnL={pnl}")

        # Kích hoạt Breakeven SL khi TP1 khớp (Variant TP1-SL)
        if tp_index == 0 and self.config.ENABLE_TP1_BREAKEVEN_SL:
            pos.sl_active = True
            pos.sl_price = pre_tp_avg
            self.log(f"BREAKEVEN_SL_ACTIVATED: {pos.pos_id} at {pos.sl_price}")

        if pos.qty <= 1e-12:
            pos.status = PositionStatus.CLOSED_TP
            pos.qty = 0.0
            pos.current_notional = 0.0
            self.closed_positions.append(pos)

    def close_stop_loss(self, pos: Position, raw_price: float, is_gap: bool = False) -> None:
        if not pos.is_open:
            return

        if pos.direction == Direction.BUY:
            exec_price = raw_price * (1.0 - self.config.SL_SLIPPAGE)
            pnl = pos.qty * (exec_price - pos.avg_entry)
        else:
            exec_price = raw_price * (1.0 + self.config.SL_SLIPPAGE)
            pnl = pos.qty * (pos.avg_entry - exec_price)

        fee = pos.qty * exec_price * self.config.TAKER_FEE
        released_margin = pos.ledger.margin_balance
        pos.ledger.margin_balance = 0.0

        self.free_cash += (released_margin + pnl - fee)

        pos.ledger.gross_realized_pnl += pnl
        pos.ledger.fees_paid_by_account += fee
        pos.qty = 0.0
        pos.current_notional = 0.0
        pos.status = PositionStatus.CLOSED_SL
        self.closed_positions.append(pos)
        self.log(f"SL_CLOSED: {pos.pos_id} at {exec_price}, PnL={pnl}, Fee={fee}")

    # -------------------------------------------------------------------------
    # FUNDING SETTLEMENT (MỐC UTC)
    # -------------------------------------------------------------------------
    def apply_funding(self, mark_prices: Dict[str, float], funding_rates: Dict[str, float], funding_ts: int) -> None:
        for pos in list(self.positions.values()):
            if not pos.is_open or pos.entry_ts >= funding_ts:
                continue

            rate = funding_rates.get(pos.asset, 0.0)
            if rate == 0.0:
                continue

            mark_p = mark_prices.get(pos.asset, pos.avg_entry)
            funding_notional = pos.qty * mark_p

            if pos.direction == Direction.BUY:
                is_paying = rate > 0
            else:
                is_paying = rate < 0

            nominal_amt = abs(funding_notional * rate)

            if not is_paying:
                pos.ledger.margin_balance += nominal_amt
                pos.ledger.funding_net += nominal_amt
                self.log(f"FUNDING_RECEIVED: {pos.pos_id}, Amt={nominal_amt}")
            else:
                max_payable = max(0.0, pos.ledger.margin_balance)
                actual_paid = min(nominal_amt, max_payable)

                pos.ledger.margin_balance -= actual_paid
                pos.ledger.funding_net -= actual_paid

                if nominal_amt > actual_paid:
                    deficit = nominal_amt - actual_paid
                    pos.ledger.insurance_deficit += deficit
                    self.log(f"FUNDING_DEFICIT_INSURED: {pos.pos_id}, Deficit={deficit}")

                self.log(f"FUNDING_PAID: {pos.pos_id}, ActualPaid={actual_paid}")

                # Nếu margin balance về 0 -> kích hoạt thanh lý do cạn margin
                if pos.ledger.margin_balance <= 1e-12:
                    self.log(f"MARGIN_EXHAUSTED_BY_FUNDING: {pos.pos_id}")
                    self.liquidate_position(pos, mark_p, is_gap=False)

    # -------------------------------------------------------------------------
    # CHU TRÌNH XỬ LÝ NẾN (LIFECYCLE)
    # -------------------------------------------------------------------------
    def step_candle(self, candle: Candle) -> None:
        for pos in list(self.positions.values()):
            if not pos.is_open or pos.asset != candle.asset or pos.timeframe != candle.timeframe:
                continue

            if pos.entry_ts == candle.ts:
                continue

            # -----------------------------------------------------------------
            # BƯỚC 1: XỬ LÝ GAP TẠI OPEN
            # -----------------------------------------------------------------
            liq_proxy = self.calc_liquidation_proxy(pos)
            liq_gap = (candle.open <= liq_proxy) if pos.direction == Direction.BUY else (candle.open >= liq_proxy)

            sl_gap = False
            if pos.sl_active and pos.sl_price is not None:
                sl_gap = (candle.open <= pos.sl_price) if pos.direction == Direction.BUY else (candle.open >= pos.sl_price)

            if liq_gap:
                self.liquidate_position(pos, candle.open, is_gap=True)
                continue
            elif sl_gap:
                self.close_stop_loss(pos, candle.open, is_gap=True)
                continue

            # Kiểm tra TP Gap tại Open
            for i, delta in enumerate(self.config.TP_TIERS):
                if not pos.tp_hit_flags[i]:
                    target_p = pos.avg_entry * (1.0 + delta) if pos.direction == Direction.BUY else pos.avg_entry * (1.0 - delta)
                    tp_gap = (candle.open >= target_p) if pos.direction == Direction.BUY else (candle.open <= target_p)
                    if tp_gap:
                        self.close_partial_tp(pos, i, target_p)

            if not pos.is_open:
                continue

            # Kiểm tra DCA Gap tại Open (chỉ khi SL chưa active)
            if self.config.ENABLE_X5_DCA and not pos.sl_active:
                dca_trigger = self.calc_dca_trigger(pos)
                dca_gap = (candle.open <= dca_trigger) if pos.direction == Direction.BUY else (candle.open >= dca_trigger)
                if dca_gap:
                    self.execute_dca(pos, candle.open, candle.ts)

            # -----------------------------------------------------------------
            # BƯỚC 2: XỬ LÝ INTRABAR
            # -----------------------------------------------------------------
            if pos.direction == Direction.BUY:
                # 1. Nhịp bất lợi trước (Open -> Low)
                dca_trigger = self.calc_dca_trigger(pos)
                if self.config.ENABLE_X5_DCA and not pos.sl_active and candle.low <= dca_trigger:
                    self.execute_dca(pos, dca_trigger, candle.ts)

                # Nếu SL đã active: SL kiểm tra trước khi rớt tới mức liquidation!
                if pos.sl_active and pos.sl_price is not None and candle.low <= pos.sl_price:
                    self.close_stop_loss(pos, pos.sl_price, is_gap=False)
                    continue

                current_liq = self.calc_liquidation_proxy(pos)
                if candle.low <= current_liq:
                    self.liquidate_position(pos, current_liq, is_gap=False)
                    continue

                # 2. Nhịp thuận lợi sau (Low -> High)
                for i, delta in enumerate(self.config.TP_TIERS):
                    if not pos.tp_hit_flags[i]:
                        target_p = pos.avg_entry * (1.0 + delta)
                        if candle.high >= target_p:
                            self.close_partial_tp(pos, i, target_p)

                # 3. Nhịp hồi quy sau TP1 (High -> Close)
                if pos.sl_active and pos.sl_price is not None and candle.close < pos.sl_price:
                    self.close_stop_loss(pos, pos.sl_price, is_gap=False)
                    continue

            else:
                # Short: Open -> High
                dca_trigger = self.calc_dca_trigger(pos)
                if self.config.ENABLE_X5_DCA and not pos.sl_active and candle.high >= dca_trigger:
                    self.execute_dca(pos, dca_trigger, candle.ts)

                if pos.sl_active and pos.sl_price is not None and candle.high >= pos.sl_price:
                    self.close_stop_loss(pos, pos.sl_price, is_gap=False)
                    continue

                current_liq = self.calc_liquidation_proxy(pos)
                if candle.high >= current_liq:
                    self.liquidate_position(pos, current_liq, is_gap=False)
                    continue

                # Short: High -> Low
                for i, delta in enumerate(self.config.TP_TIERS):
                    if not pos.tp_hit_flags[i]:
                        target_p = pos.avg_entry * (1.0 - delta)
                        if candle.low <= target_p:
                            self.close_partial_tp(pos, i, target_p)

                # Short: Low -> Close
                if pos.sl_active and pos.sl_price is not None and candle.close > pos.sl_price:
                    self.close_stop_loss(pos, pos.sl_price, is_gap=False)
                    continue

            # Hysteresis reset check cuối nến
            loss_ratio = 0.0
            if pos.direction == Direction.BUY and pos.avg_entry > candle.close:
                loss_ratio = (pos.qty * (pos.avg_entry - candle.close)) / pos.ledger.margin_balance
            elif pos.direction == Direction.SELL and candle.close > pos.avg_entry:
                loss_ratio = (pos.qty * (candle.close - pos.avg_entry)) / pos.ledger.margin_balance

            if loss_ratio < self.config.DCA_MARGIN_LOSS_TRIGGER:
                pos.dca_trigger_reset = True
