
[ROLE]
Bạn là Senior Python Backend Engineer kiêm Quant Developer, được giao tiếp 
quản codebase "Hosting Indicator Engine" (repo: vunguyen109/hosting-Indicator, 
v1.0, file lõi engine.py ~2,439 dòng, chạy production trên Botkeep PaaS 
1.5 vCore/2GB RAM/2GB Disk). Bạn PHẢI tuân thủ nghiêm ngặt triết lý thiết kế 
đã được thiết lập trong PROJECT_ARCHITECTURE.md, đặc biệt là nguyên tắc 
"Zero Heavy Dependencies" và "Bounded Memory Strategy".

Quy tắc bất di bất dịch khi code:
1. KHÔNG thêm bất kỳ package nào vào requirements.txt (hiện chỉ có 
   aiohttp==3.9.5). Mọi logic mới chỉ dùng Python Standard Library.
2. KHÔNG được sửa đổi hành vi của IndicatorEngine, RuleEngine, 
   CircularBuffer hiện có — module mới phải là ADDITIVE (đọc dữ liệu 
   từ các module đó, không ghi đè/thay đổi logic của chúng).
3. KHÔNG được sửa đổi `TelegramDispatcher` (Section 5) dưới bất kỳ 
   hình thức nào. Tính năng khuyến nghị BUY/SELL/WAIT trong prompt 
   này CHỈ phục vụ hiển thị trên Web Dashboard, KHÔNG dispatch qua 
   Telegram. Nếu cần mở rộng Telegram sau này, đó là một yêu cầu 
   RIÊNG BIỆT sẽ được đặc tả ở lần cập nhật khác.
4. KHÔNG được tăng tần suất polling hiện tại (/api/snapshot client 
   poll 3s, /api/candles poll 15s, TwelveData throttle 8s giữa 
   2 request). Mọi tính toán mới phải chạy đồng bộ với nhịp đóng 
   nến hiện có (seconds_until_next_close), KHÔNG tính lại mỗi lần 
   có HTTP request tới /api/snapshot.
5. KHÔNG tăng kích thước state.json vượt trần 1.5MB đã quy định 
   trong StatePersistence. Dữ liệu Advice KHÔNG BẮT BUỘC phải 
   persist xuống disk (có thể chỉ tồn tại trong RAM, tính lại ngay 
   sau mỗi lần backfill/restart).
6. PHẢI giữ nguyên tương thích ngược (backward compatible) với 
   schema JSON hiện tại của /api/snapshot — chỉ được THÊM field 
   mới (`advice`), không đổi tên/xóa field cũ mà dashboard.html 
   đang đọc (indicators, signals, status...).
7. Toàn bộ ngưỡng số (threshold) dùng trong logic phải là hằng số 
   đặt tập trung trong CONFIG (Section 1), không hard-code rải rác 
   trong hàm, để sau này dễ hiệu chỉnh qua backtest.
8. Mọi khuyến nghị BUY/SELL hiển thị trên UI bắt buộc phải kèm dòng 
   cảnh báo "đây là gợi ý thuật toán, không phải lời khuyên đầu tư" 
   — hiển thị rõ ràng, không được ẩn hoặc thu nhỏ quá mức.
9. Tuân thủ triết lý "Single Source of Truth" đã nêu trong Tech Debt 
   #1 của PROJECT_ARCHITECTURE.md: toàn bộ logic phân loại/quyết 
   định (classify + decide) PHẢI nằm ở backend Python. Phần 
   JavaScript trong dashboard.html CHỈ ĐƯỢC PHÉP render dữ liệu đã 
   tính sẵn từ API, TUYỆT ĐỐI không viết lại logic matrix bằng JS 
   (tránh lặp lại sai lầm "Dual Frontend Code" đã ghi nhận trong 
   tài liệu kiến trúc).

[CONTEXT]
## Kiến trúc hiện tại (tóm tắt từ PROJECT_ARCHITECTURE.md):
- Section 3 (IndicatorEngine): tính EMA20/50, RSI14, BB(20,2), ATR14 — 
  pure math, lưu kết quả per asset/timeframe.
- Section 4 (RuleEngine + Signal): phát hiện sự kiện (EMA Cross, 
  RSI OB/OS, BB Breakout, ATR Spike) bằng cách so sánh snapshot hiện 
  tại với snapshot trước đó, lưu tối đa 50 tín hiệu gần nhất.
- Section 5 (TelegramDispatcher): GIỮ NGUYÊN, không chạm vào.
- Section 9 (Scheduler): seconds_until_next_close — tính toán chính 
  xác thời điểm nến đóng, đây CHÍNH LÀ nhịp mà IndicatorEngine và 
  RuleEngine được trigger chạy lại (không phải mỗi 3s).
- /api/snapshot: trả về indicators{} theo asset→timeframe và 
  signals[] (50 tín hiệu gần nhất), client poll mỗi 3s NHƯNG dữ liệu 
  bên trong chỉ thay đổi khi có nến mới đóng.
- Dashboard hiện tại tính "Composite Gauge 0-100" và "Story feed" 
  100% ở CLIENT (JavaScript) — đây là ngoại lệ lịch sử đã tồn tại, 
  KHÔNG cần sửa lại phần này, nhưng tính năng MỚI (Advice) phải 
  tuân theo nguyên tắc "tính ở backend" để tránh lặp lại vấn đề 
  Dual Logic trong tương lai.

## Yêu cầu nghiệp vụ cần triển khai (nguồn: Bảng Ma Trận Quyết Định 
## đã thống nhất với người dùng ở phiên trước):

| Gauge/Xu hướng | EMA Trend | RSI Zone | Vị trí BB | → Hành động |
|---|---|---|---|---|
| Bán/Bán mạnh | Giảm rõ rệt | Cân bằng hoặc Quá bán | Trong dải/chạm biên dưới (chưa thủng) | SELL |
| Bán mạnh | Giảm rõ rệt | Cân bằng | Vừa thủng đáy biên | WAIT (chờ xác nhận) |
| Bán mạnh | Giảm rõ rệt | Quá bán sâu (<25) | Thủng đáy biên | WAIT (rủi ro quá đà) |
| Trung lập | Nghiêng nhẹ 2 chiều | Cân bằng | Trong dải (vùng giữa) | WAIT (sideway) |
| Mua/Mua mạnh | Tăng rõ rệt | Cân bằng hoặc chưa quá mua | Trong dải/chạm biên trên | BUY |
| Bất kỳ | Bất kỳ | Bất kỳ | Có tín hiệu RuleEngine NGƯỢC CHIỀU trong 1-2 nến gần nhất | WAIT (nhiễu/mâu thuẫn) |
| Bất kỳ (BUY/SELL hợp lệ) | — | — | Khung lớn (1h) KHÔNG đồng thuận hướng với khung nhỏ (15m) | Hạ confidence xuống "Thấp", vẫn giữ action nhưng cảnh báo |

## Vấn đề kỹ thuật cần giải quyết thêm:
- Cần công thức PHÂN LOẠI khách quan cho "EMA Trend" (Tăng rõ rệt/
  Nghiêng tăng/Cân bằng/Nghiêng giảm/Giảm rõ rệt) — hiện engine chỉ 
  tính giá trị EMA thô, chưa có lớp phân loại trạng thái.
- Cần công thức phân loại "Vị trí BB" (Trong dải/Chạm biên/Vượt biên).
- Cần công thức phân loại "RSI Zone" chi tiết hơn 2 ngưỡng 30/70 hiện có.
- Cần cơ chế phát hiện "tín hiệu mâu thuẫn" bằng cách tra cứu lại 
  RuleEngine.recent_signals của chính asset/timeframe đó.
- Cần cơ chế so sánh đa khung thời gian (15m vs 1h) cho CÙNG 1 asset — 
  dữ liệu này ĐÃ CÓ SẴN trong cùng 1 orchestrator process (cả 2 
  timeframe cùng chạy trong 1 Python process), chỉ cần truy cập 
  đối tượng IndicatorEngine của timeframe còn lại.

[INSTRUCTION]
Thực hiện các bước sau theo đúng thứ tự, mỗi bước phải có code hoàn 
chỉnh (không phải placeholder) kèm giải thích ngắn gọn lý do thiết kế:

## BƯỚC 1 — Thêm hằng số cấu hình (Section 1: Configuration)

Mở rộng CONFIG với các ngưỡng mới, có giá trị mặc định hợp lý 
(đính kèm comment giải thích đơn vị đo):

```python
CONFIG.update({
    # Ngưỡng phân loại EMA Trend (đơn vị: số lần ATR giữa EMA20-EMA50)
    "EMA_TREND_STRONG_ATR_MULT": 0.5,   # > 0.5*ATR => "rõ rệt"
    "EMA_TREND_WEAK_ATR_MULT": 0.15,    # > 0.15*ATR => "nghiêng"
    
    # Ngưỡng phân loại RSI Zone
    "RSI_EXTREME_OVERSOLD": 25,
    "RSI_LEAN_OVERSOLD": 40,
    "RSI_LEAN_OVERBOUGHT": 60,
    "RSI_EXTREME_OVERBOUGHT": 75,
    
    # Ngưỡng phân loại vị trí Bollinger Band (% độ rộng dải)
    "BB_EDGE_ZONE_PCT": 0.15,   # 15% sát biên => "chạm biên"
    
    # Số nến gần nhất để kiểm tra tín hiệu mâu thuẫn
    "CONFLICT_LOOKBACK_CANDLES": 2,
    
    # Hệ số ATR cho Stop Loss / Take Profit
    "SL_ATR_MULTIPLIER": 1.5,
    "TP1_RR_RATIO": 1.0,       # R:R 1:1 cho TP1
    "TP2_ATR_MULTIPLIER": 3.0, # TP2 tham khảo (song song rule "trailing theo EMA")
    
    # Ngưỡng coi là "giá đã gần EMA20, có thể entry ngay" 
    # thay vì "chờ pullback"
    "ENTRY_PULLBACK_ATR_THRESHOLD": 0.3,
})
```

## BƯỚC 2 — Tạo module mới: `ConfluenceDecisionEngine` 
## (Section mới, chèn giữa Section 4 "Rules" và Section 5 "Telegram")

Class này CHỈ ĐỌC dữ liệu từ IndicatorEngine và RuleEngine hiện có, 
không sửa đổi chúng và KHÔNG có bất kỳ import/reference nào tới 
TelegramDispatcher. Toàn bộ tính toán là O(1) — thực hiện xong 
trong vài micro-giây.

```python
# ==================== Section 4.5: Confluence Decision Engine ====================

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


@dataclass
class TradeAdvice:
    action: str                    # BUY / SELL / WAIT
    confidence: str                # Cao / Trung bình / Thấp
    confidence_score: int          # 0-5, số yếu tố đồng thuận
    entry_type: str                # "now" / "wait_pullback" / "none"
    entry_price: float | None
    entry_zone: float | None       # giá EMA20 (vùng chờ pullback)
    stop_loss: float | None
    take_profit_1: float | None
    take_profit_2: float | None
    tp2_rule: str                  # mô tả quy tắc trailing bằng text
    reasoning: list[str]           # checklist lý do (để hiển thị UI)
    conflict_detected: bool
    mtf_agreement: bool | None     # None nếu không có dữ liệu khung kia
    disclaimer: str = (
        "Đây là khuyến nghị dựa trên thuật toán kỹ thuật, "
        "KHÔNG PHẢI lời khuyên đầu tư. Người dùng tự chịu "
        "trách nhiệm với quyết định giao dịch của mình."
    )


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

    def __init__(self, config: dict):
        self.cfg = config

    def classify_ema_trend(self, ema_fast: float, ema_slow: float, 
                            atr: float) -> TrendState:
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
        c = self.cfg
        if rsi <= c["RSI_EXTREME_OVERSOLD"]:
            return RsiZone.EXTREME_OVERSOLD
        if rsi <= c["RSI_LEAN_OVERSOLD"]:
            return RsiZone.LEAN_OVERSOLD
        if rsi >= c["RSI_EXTREME_OVERBOUGHT"]:
            return RsiZone.EXTREME_OVERBOUGHT
        if rsi >= c["RSI_LEAN_OVERBOUGHT"]:
            return RsiZone.LEAN_OVERBOUGHT
        return RsiZone.NEUTRAL

    def classify_bb_position(self, close: float, bb_upper: float, 
                              bb_lower: float) -> BbPosition:
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

    def detect_conflict(self, recent_signals: list, asset: str, 
                         timeframe: str, trend: TrendState) -> bool:
        """Kiểm tra N tín hiệu gần nhất của asset/timeframe này có 
        loại NGƯỢC CHIỀU với xu hướng EMA hiện tại không."""
        lookback = self.cfg["CONFLICT_LOOKBACK_CANDLES"]
        bullish_types = {"EMA_CROSS_UP", "BB_BREAKOUT_UP", "RSI_OVERSOLD"}
        bearish_types = {"EMA_CROSS_DOWN", "BB_BREAKOUT_DOWN", "RSI_OVERBOUGHT"}
        
        relevant = [
            s for s in recent_signals 
            if s.asset == asset and s.timeframe == timeframe
        ][-lookback:]
        
        trend_is_up = trend in (TrendState.STRONG_UP, TrendState.LEAN_UP)
        trend_is_down = trend in (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN)
        
        for s in relevant:
            if trend_is_up and s.signal_type in bearish_types:
                return True
            if trend_is_down and s.signal_type in bullish_types:
                return True
        return False

    def check_mtf_agreement(self, trend_current_tf: TrendState, 
                             trend_other_tf: TrendState | None) -> bool | None:
        if trend_other_tf is None:
            return None
        up_states = (TrendState.STRONG_UP, TrendState.LEAN_UP)
        down_states = (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN)
        if trend_current_tf in up_states and trend_other_tf in up_states:
            return True
        if trend_current_tf in down_states and trend_other_tf in down_states:
            return True
        return False

    def decide(self, asset: str, timeframe: str, snapshot: dict, 
                recent_signals: list, 
                other_tf_snapshot: dict | None = None) -> TradeAdvice:
        """
        snapshot: dict với các key close, ema_fast, ema_slow, rsi, 
                  bb_upper, bb_mid, bb_lower, atr (đúng format hiện 
                  có trong IndicatorEngine output)
        other_tf_snapshot: snapshot của timeframe còn lại CÙNG asset 
                  (để check MTF agreement), None nếu chưa có dữ liệu
        """
        close = snapshot["close"]
        atr = snapshot["atr"]
        trend = self.classify_ema_trend(
            snapshot["ema_fast"], snapshot["ema_slow"], atr)
        rsi_zone = self.classify_rsi(snapshot["rsi"])
        bb_pos = self.classify_bb_position(
            close, snapshot["bb_upper"], snapshot["bb_lower"])
        conflict = self.detect_conflict(recent_signals, asset, timeframe, trend)

        trend_other = None
        if other_tf_snapshot:
            trend_other = self.classify_ema_trend(
                other_tf_snapshot["ema_fast"], 
                other_tf_snapshot["ema_slow"], 
                other_tf_snapshot["atr"])
        mtf_agree = self.check_mtf_agreement(trend, trend_other)

        reasoning = []
        score = 0

        max_factors = 0
        max_factors += 1
        if trend in (TrendState.STRONG_UP, TrendState.LEAN_UP):
            score += 2 if trend == TrendState.STRONG_UP else 1
            reasoning.append(f"Xu hướng EMA: {trend.value} (ủng hộ MUA)")
        elif trend in (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN):
            score -= 2 if trend == TrendState.STRONG_DOWN else 1
            reasoning.append(f"Xu hướng EMA: {trend.value} (ủng hộ BÁN)")
        else:
            reasoning.append(f"Xu hướng EMA: {trend.value} (trung lập)")

        max_factors += 1
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

        max_factors += 1
        if bb_pos == BbPosition.BREAK_UPPER:
            reasoning.append("Giá vượt đỉnh dải Bollinger (biến động mạnh, "
                              "cẩn trọng mua đuổi)")
        elif bb_pos == BbPosition.BREAK_LOWER:
            reasoning.append("Giá thủng đáy dải Bollinger (biến động mạnh, "
                              "cẩn trọng bán đuổi)")
        elif bb_pos == BbPosition.NEAR_UPPER:
            score += 1
            reasoning.append("Giá áp sát biên trên (ủng hộ xu hướng tăng)")
        elif bb_pos == BbPosition.NEAR_LOWER:
            score -= 1
            reasoning.append("Giá áp sát biên dưới (ủng hộ xu hướng giảm)")
        else:
            reasoning.append("Giá trong vùng dao động bình thường")

        max_factors += 1
        if conflict:
            reasoning.append("⚠️ Phát hiện tín hiệu MÂU THUẪN gần đây "
                              "trên chart — cần thận trọng")
        else:
            reasoning.append("Không có tín hiệu mâu thuẫn gần đây")

        if mtf_agree is not None:
            max_factors += 1
            if mtf_agree:
                score += 1 if score > 0 else (-1 if score < 0 else 0)
                reasoning.append("Khung thời gian lớn ĐỒNG THUẬN xu hướng")
            else:
                reasoning.append("⚠️ Khung thời gian lớn KHÔNG đồng thuận")

        is_sideway = (trend == TrendState.NEUTRAL and 
                      bb_pos == BbPosition.MIDDLE)
        is_extreme_overextended = (
            (bb_pos == BbPosition.BREAK_LOWER and 
             rsi_zone != RsiZone.EXTREME_OVERSOLD and score <= -3) or
            (bb_pos == BbPosition.BREAK_UPPER and 
             rsi_zone != RsiZone.EXTREME_OVERBOUGHT and score >= 3)
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

            ref_price = entry_price if entry_price else ema20
            sl_mult = self.cfg["SL_ATR_MULTIPLIER"]
            rr = self.cfg["TP1_RR_RATIO"]
            tp2_mult = self.cfg["TP2_ATR_MULTIPLIER"]

            if action == Action.BUY:
                sl = ref_price - sl_mult * atr
                tp1 = ref_price + sl_mult * atr * rr
                tp2 = ref_price + tp2_mult * atr
                tp2_rule = ("Giữ lệnh đến khi Xu hướng EMA chuyển từ "
                             "Tăng sang Cân bằng/Giảm")
            else:
                sl = ref_price + sl_mult * atr
                tp1 = ref_price - sl_mult * atr * rr
                tp2 = ref_price - tp2_mult * atr
                tp2_rule = ("Giữ lệnh đến khi Xu hướng EMA chuyển từ "
                             "Giảm sang Cân bằng/Tăng")

        return TradeAdvice(
            action=action.value,
            confidence=confidence.value,
            confidence_score=confidence_score,
            entry_type=entry_type,
            entry_price=round(entry_price, 4) if entry_price else None,
            entry_zone=round(entry_zone, 4) if entry_zone else None,
            stop_loss=round(sl, 4) if sl else None,
            take_profit_1=round(tp1, 4) if tp1 else None,
            take_profit_2=round(tp2, 4) if tp2 else None,
            tp2_rule=tp2_rule,
            reasoning=reasoning,
            conflict_detected=conflict,
            mtf_agreement=mtf_agree,
        )
```

## BƯỚC 3 — Tích hợp vào Orchestrator (Section 11)

Trong `IndicatorOrchestrator`, sau mỗi lần `IndicatorEngine` cập nhật 
snapshot MỚI (tức đúng tại thời điểm nến đóng), gọi 
`ConfluenceDecisionEngine.decide()` MỘT LẦN và cache kết quả vào 
`self.latest_advice[asset][timeframe]`. 

**Không có bất kỳ lời gọi nào tới `telegram_dispatcher` trong đoạn 
code này.** Advice chỉ được lưu trong RAM để API đọc ra, không 
kích hoạt bất kỳ hành động gửi thông báo nào.

```python
advice = self.confluence_engine.decide(
    asset=asset,
    timeframe=timeframe,
    snapshot=current_indicator_snapshot,
    recent_signals=self.rule_engine.recent_signals,
    other_tf_snapshot=self.indicators.get(asset, {}).get(other_tf),
)
self.latest_advice.setdefault(asset, {})[timeframe] = advice
# Lưu ý: Không gọi self.telegram.dispatch(...) ở đây.
# RuleEngine.evaluate() vẫn tiếp tục dispatch Signal qua Telegram 
# như logic CŨ, hoàn toàn độc lập với luồng advice này.
```

## BƯỚC 4 — Mở rộng `/api/snapshot` (Section 10/5 — ADDITIVE ONLY)

Thêm field mới `"advice"` vào response JSON, giữ nguyên toàn bộ 
field cũ. Đây là NƠI DUY NHẤT dữ liệu khuyến nghị được "xuất ra" — 
không có đường dẫn code nào khác (Telegram, log file riêng...) 
tiêu thụ `TradeAdvice`.

```json
{
  "status": "online",
  "indicators": { "...": "giữ nguyên như cũ" },
  "signals": [ "...": "giữ nguyên như cũ" ],
  "advice": {
    "BTC": {
      "15m": {
        "action": "BUY",
        "confidence": "Trung bình",
        "confidence_score": 3,
        "entry_type": "wait_pullback",
        "entry_price": null,
        "entry_zone": 83420.36,
        "stop_loss": 83051.82,
        "take_profit_1": 83789.9,
        "take_profit_2": 84158.36,
        "tp2_rule": "Giữ lệnh đến khi Xu hướng EMA chuyển từ Tăng sang Cân bằng/Giảm",
        "reasoning": [
          "Xu hướng EMA: Tăng rõ rệt (ủng hộ MUA)",
          "RSI nghiêng mua, còn dư địa tăng",
          "Giá vượt đỉnh dải Bollinger (biến động mạnh, cẩn trọng mua đuổi)",
          "Không có tín hiệu mâu thuẫn gần đây",
          "Khung thời gian lớn ĐỒNG THUẬN xu hướng"
        ],
        "conflict_detected": false,
        "mtf_agreement": true,
        "disclaimer": "Đây là khuyến nghị dựa trên thuật toán kỹ thuật, KHÔNG PHẢI lời khuyên đầu tư..."
      }
    }
  }
}
```

## BƯỚC 5 — Hiển thị trên `dashboard.html` (TRỌNG TÂM của prompt này)

Thêm 1 card mới **"🎯 Khuyến Nghị Giao Dịch"** vào layout hiện có, 
đặt ngay cạnh/dưới card "Thước Đo Tâm Lý Thị Trường" (Gauge) đã có 
sẵn trong UI — vì về mặt luồng đọc, người dùng đã quen xem Gauge 
trước rồi mới xem chi tiết. Card mới đóng vai trò "kết luận hành 
động cụ thể" tiếp nối ngay sau Gauge.

**Yêu cầu chi tiết UI:**

1. **Khối Action chính** (nổi bật nhất, ngay đầu card):
   - Chữ to: `BUY` (nền xanh lá) / `SELL` (nền đỏ) / `WAIT` (nền vàng/xám)
   - Badge độ tin cậy cạnh bên: "Độ tin cậy: Cao/Trung bình/Thấp" 
     kèm progress bar nhỏ thể hiện `confidence_score/5`

2. **Khối Entry/SL/TP** (dạng bảng hoặc 3 cột thẻ):
   ```
   ┌─────────────────┬─────────────────┬─────────────────┐
   │   📍 ENTRY       │   🛑 STOP LOSS   │   🎯 TAKE PROFIT │
   ├─────────────────┼─────────────────┼─────────────────┤
   │ Nếu entry_type   │   83,051.82     │ TP1: 83,789.90  │
   │ = "now":         │                 │ TP2: 84,158.36  │
   │   Vào ngay:      │                 │ (+ ghi chú      │
   │   83,650.01      │                 │  tp2_rule)      │
   │                  │                 │                 │
   │ Nếu = "wait_     │                 │                 │
   │ pullback":       │                 │                 │
   │   Chờ giá về     │                 │                 │
   │   vùng: 83,420.36│                 │                 │
   └─────────────────┴─────────────────┴─────────────────┘
   ```
   - Mỗi số liệu có nút icon "📋 Copy" nhỏ cạnh bên để người dùng 
     copy nhanh vào lệnh trên sàn giao dịch.
   - Nếu `action == "WAIT"`, ẩn toàn bộ khối Entry/SL/TP, thay bằng 
     dòng chữ: "⏸ Chưa có điểm vào lệnh phù hợp lúc này — hệ thống 
     đang chờ tín hiệu rõ ràng hơn."

3. **Khối Lý Do (Reasoning Checklist)** — render từ mảng `reasoning[]`:
   ```
   📋 Vì sao có khuyến nghị này?
   ✅ Xu hướng EMA: Tăng rõ rệt (ủng hộ MUA)
   ✅ RSI nghiêng mua, còn dư địa tăng
   ⚠️ Giá vượt đỉnh dải Bollinger (biến động mạnh, cẩn trọng mua đuổi)
   ✅ Không có tín hiệu mâu thuẫn gần đây
   ✅ Khung thời gian lớn ĐỒNG THUẬN xu hướng
   ```
   - Dòng có chứa "⚠️" trong text gốc → style màu vàng/cam khác 
     biệt với các dòng "✅" thông thường (phân biệt bằng cách check 
     substring, không cần thêm field mới vào schema).

4. **Dòng Disclaimer** — LUÔN hiển thị cuối card, cỡ chữ nhỏ hơn 
   nhưng KHÔNG được ẩn:
   ```
   ⚠️ Đây là khuyến nghị dựa trên thuật toán kỹ thuật, không phải 
   lời khuyên đầu tư. Bạn tự chịu trách nhiệm với quyết định của mình.
   ```

5. **Vị trí trong layout tổng thể** — mô tả bằng wireframe text:
   ```
   ┌──────────────────────────────┬───────────────────────────┐
   │   Biểu Đồ Nến Trực Quan       │  Thước Đo Tâm Lý (Gauge)  │
   │   (TradingView Chart)        ├───────────────────────────┤
   │                               │  🎯 Khuyến Nghị Giao Dịch │
   │                               │    (CARD MỚI — đặt ngay   │
   │                               │     dưới Gauge)           │
   └──────────────────────────────┴───────────────────────────┘
   │        Bảng Theo Dõi Sức Khỏe Thị Trường (giữ nguyên)      │
   └─────────────────────────────────────────────────────────┘
   ```

6. **Cơ chế lấy dữ liệu (JavaScript)** — CHỈ đọc, không tính toán:
   ```javascript
   // Trong hàm fetch snapshot hiện có (poll mỗi 3s), 
   // thêm đoạn đọc field advice và render — KHÔNG thêm fetch() mới
   function renderAdviceCard(snapshot, currentAsset, currentTimeframe) {
     const advice = snapshot.advice?.[currentAsset]?.[currentTimeframe];
     if (!advice) {
       document.getElementById('advice-card').innerHTML = 
         '<p class="text-gray-400">Đang thu thập đủ dữ liệu...</p>';
       return;
     }
     
     const actionColors = {
       BUY: 'bg-green-600 text-white',
       SELL: 'bg-red-600 text-white', 
       WAIT: 'bg-yellow-600 text-black'
     };
     
     // Render action badge
     const actionEl = document.getElementById('advice-action');
     actionEl.textContent = advice.action;
     actionEl.className = `advice-badge ${actionColors[advice.action]}`;
     
     // Render confidence
     document.getElementById('advice-confidence').textContent = 
       `Độ tin cậy: ${advice.confidence} (${advice.confidence_score}/5)`;
     
     // Render Entry/SL/TP block (ẩn nếu action = WAIT)
     const entrySlTpBlock = document.getElementById('advice-entry-sl-tp');
     if (advice.action === 'WAIT') {
       entrySlTpBlock.classList.add('hidden');
       document.getElementById('advice-wait-message').classList.remove('hidden');
     } else {
       entrySlTpBlock.classList.remove('hidden');
       document.getElementById('advice-wait-message').classList.add('hidden');
       
       const entryText = advice.entry_type === 'now' 
         ? `Vào ngay: ${advice.entry_price}`
         : `Chờ giá về vùng: ${advice.entry_zone}`;
       document.getElementById('advice-entry').textContent = entryText;
       document.getElementById('advice-sl').textContent = advice.stop_loss;
       document.getElementById('advice-tp1').textContent = advice.take_profit_1;
       document.getElementById('advice-tp2').textContent = 
         `${advice.take_profit_2} (${advice.tp2_rule})`;
     }
     
     // Render reasoning checklist
     const reasoningList = document.getElementById('advice-reasoning');
     reasoningList.innerHTML = advice.reasoning.map(r => {
       const isWarning = r.includes('⚠️');
       return `<li class="${isWarning ? 'text-yellow-400' : 'text-green-400'}">${r}</li>`;
     }).join('');
   }
   
   // Gọi hàm này bên trong callback fetch snapshot hiện có 
   // (nơi đang update Gauge/bảng chỉ báo), KHÔNG tạo setInterval mới
   ```

7. **Nút Copy nhanh** (progressive enhancement, không bắt buộc 
   nhưng khuyến khích):
   ```javascript
   function copyToClipboard(value) {
     navigator.clipboard.writeText(value.toString());
     // Hiện toast nhỏ "Đã copy!" trong 1.5s
   }
   ```

## BƯỚC 6 — Unit Test (test_engine.py)

Viết test dạng table-driven, feed snapshot giả lập đại diện ĐÚNG 
từng dòng trong Bảng Ma Trận Quyết Định ở phần [CONTEXT], assert 
action/entry/sl/tp output đúng kỳ vọng. Tối thiểu các case:
1. Trend mạnh xuống + RSI cân bằng + BB giữa dải → SELL
2. Trend mạnh xuống + BB vừa thủng đáy + RSI chưa quá bán sâu → WAIT
3. Sideway (trend neutral + BB giữa dải) → WAIT
4. Trend mạnh lên + RSI vừa phải + BB giữa/cận biên trên → BUY
5. Có tín hiệu mâu thuẫn trong recent_signals → luôn WAIT bất kể score
6. MTF không đồng thuận → giữ action nhưng hạ confidence xuống thấp hơn

Thêm 1 test riêng xác nhận: `ConfluenceDecisionEngine` và 
`TradeAdvice` KHÔNG import/reference bất kỳ thành phần nào của 
`TelegramDispatcher` (kiểm tra bằng cách assert module không có 
side-effect nào gọi network/HTTP khi chạy `decide()`).

## BƯỚC 7 — Cập nhật tài liệu

- Thêm dòng mới vào bảng Section 4 của PROJECT_ARCHITECTURE.md: 
  `"Section 4.5: ConfluenceDecisionEngine"` — ghi rõ trong cột 
  "Chức năng chi tiết": *"Chỉ phục vụ hiển thị UI qua /api/snapshot, 
  KHÔNG tích hợp với TelegramDispatcher."*
- Cập nhật Mục 5 (API Specifications) thêm field `advice` vào ví dụ 
  response của `/api/snapshot`.
- Thêm vào Mục 6 (Technical Debt) ghi chú mới: "Ngưỡng phân loại 
  trong ConfluenceDecisionEngine (EMA_TREND_*, RSI_*, BB_EDGE_ZONE_PCT) 
  là giá trị heuristic ban đầu, CẦN backtest định lượng trên dữ liệu 
  lịch sử thực tế của từng asset trước khi coi là đáng tin cậy cho 
  giao dịch thật." và thêm ghi chú: "Nếu trong tương lai cần mở rộng 
  Advice sang Telegram, đây sẽ là một tính năng riêng cần đặc tả 
  cơ chế chống spam/dedup bổ sung."

[OUTPUT FORMAT]
- Trả về code hoàn chỉnh cho từng file bị thay đổi (engine.py diff, 
  dashboard.html diff, test_engine.py bổ sung, PROJECT_ARCHITECTURE.md 
  bổ sung) — không dùng placeholder "// TODO".
- Mỗi thay đổi phải nêu rõ: file nào, vị trí chèn (theo đúng Section 
  đã đánh số trong tài liệu kiến trúc gốc), và lý do tại sao vị trí 
  đó là đúng.
- Bắt buộc có 1 bảng "Cam kết tài nguyên" ở cuối, liệt kê cụ thể:
  - Số dòng code thêm vào engine.py (ước tính)
  - Số dòng code thêm vào dashboard.html (ước tính)
  - RAM tăng thêm (ước tính theo KB, do giữ thêm dict advice/asset/tf)
  - CPU tăng thêm (ước tính theo micro-giây/lần tính, nhân với tần 
    suất đóng nến thực tế — KHÔNG phải tần suất poll 3s)
  - Disk tăng thêm (nếu có — mặc định là 0 vì advice không bắt buộc 
    persist)
  - Khẳng định KHÔNG có package mới nào được thêm vào requirements.txt
  - Khẳng định KHÔNG có dòng code nào trong TelegramDispatcher bị 
    chỉnh sửa (liệt kê diff count = 0 cho Section 5)
- Kết thúc bằng checklist xác nhận đã tuân thủ đủ 9 quy tắc bất di 
  bất dịch đã nêu trong [ROLE], đặc biệt nhấn mạnh xác nhận riêng 
  cho quy tắc #3 (không chạm Telegram) và #9 (Single Source of Truth 
  — không viết logic JS trùng lặp).

