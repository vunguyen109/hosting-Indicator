[ROLE]
Bạn là Senior Python Backend Engineer kiêm Quant Developer, tiếp tục 
mở rộng ConfluenceDecisionEngine (Section 4.5) trong engine.py của 
dự án "Hosting Indicator Engine" (1.5 vCore/2GB RAM/2GB Disk, Zero 
Heavy Dependencies). Đây là bản sửa lại hoàn toàn công thức Take 
Profit đã thiết kế ở lần trước, do phát hiện NHẦM LẪN quan trọng 
giữa "% biến động giá" và "% lợi nhuận trên vốn ký quỹ (margin)".

Quy tắc bất di bất dịch (kế thừa đầy đủ từ các lần cập nhật trước):
1. KHÔNG thêm package mới vào requirements.txt.
2. KHÔNG sửa TelegramDispatcher.
3. KHÔNG tính toán lại theo nhịp poll 3s — chạy đồng bộ với nhịp 
   đóng nến.
4. Mọi threshold/tham số phải đặt trong CONFIG.
5. Giữ tương thích ngược schema JSON — CHỈ THÊM field mới.
6. BẮT BUỘC phân biệt RÕ RÀNG trên UI giữa 2 khái niệm:
   - "% biến động giá" (price move %) — con số thực tế giá phải đi
   - "% lợi nhuận trên vốn ký quỹ" (margin profit %) — con số sau 
     khi đã nhân với đòn bẩy
   Đây là yêu cầu bắt buộc vì sự nhầm lẫn giữa 2 khái niệm này đã 
   từng xảy ra ở vòng thiết kế trước, gây sai lệch tới 5 lần (vì 
   leverage=5) nếu không làm rõ.
7. Công thức TP phải tính XUÔI từ mục tiêu lợi nhuận trên vốn ký quỹ 
   mong muốn → quy đổi ra khoảng cách giá cần thiết bằng phép chia 
   cho đòn bẩy → KHÔNG được tính ngược từ ATR/swing-structure rồi 
   suy ra lợi nhuận (cách làm cũ đã gây sai lệch kỳ vọng).

[CONTEXT]
## Công thức quy đổi cốt lõi cần hiểu đúng:
```
margin_profit_pct = price_move_pct × leverage
  =>  price_move_pct = margin_profit_pct / leverage

Ví dụ với leverage = 5, mục tiêu margin_profit = 5% đến 10%:
  price_move_pct cần thiết = 1.0% đến 2.0%
```

## Hồ sơ rủi ro người dùng (đã đính chính):
- Vốn sử dụng mỗi lệnh: 10% tổng tài sản
- Đòn bẩy: x5
- Kỳ vọng TP: **5-10% lợi nhuận trên VỐN KÝ QUỸ** (tương đương chỉ 
  1-2% biến động giá thực tế — con số này HOÀN TOÀN khả thi trong 
  vài giờ đến 1-2 ngày trên khung 1h/4h, KHÔNG cần nhiều ngày)
- Khung thời gian mong muốn: 1h và 4h

## Số liệu minh chứng (dùng case XAU thực tế để kiểm tra công thức):
Entry (SELL) = $4,159.30, ATR(1h) ≈ 11.17
- TP ứng với 5% margin profit  → price move 1.0% → ~$4,117.7
- TP ứng với 7.5% margin profit → price move 1.5% → ~$4,096.9  
- TP ứng với 10% margin profit → price move 2.0% → ~$4,076.1
Các mức này tương đương 3.7x–7.4x ATR — nằm trong tầm với hợp lý 
của biến động thông thường trên khung 1h, không cần tìm vùng swing 
xa như thiết kế trước.

## Lưu ý về SL (không thay đổi mục tiêu, chỉ cần khớp R:R hợp lý):
Giữ nguyên SL = 1.5-2.0× ATR như thiết kế cũ. Với TP giờ đã xa hơn 
đáng kể (3.7-7.4x ATR thay vì 1.5-3x ATR trước đây), tỷ lệ R:R tự 
nhiên sẽ cải thiện lên khoảng 2:1 đến 4:1 — tốt hơn nhiều so với 
thiết kế 1:1 ban đầu.

[INSTRUCTION]

## BƯỚC 1 — Sửa lại CONFIG (thay thế hoàn toàn SWING_TP_LEVELS cũ)

```python
CONFIG.update({
    # Đòn bẩy và % vốn mặc định — dùng để quy đổi và hiển thị minh họa
    "DEFAULT_LEVERAGE": 5,
    "DEFAULT_ACCOUNT_RISK_PCT": 0.10,
    
    # SL vẫn giữ nguyên tinh thần cũ — nới nhẹ cho khung 1h/4h
    "SWING_SL_ATR_MULTIPLIER": 1.5,
    
    # ĐÃ SỬA: Multi-tier TP định nghĩa trực tiếp bằng % LỢI NHUẬN 
    # TRÊN VỐN KÝ QUỸ mong muốn, KHÔNG dùng R:R ratio hay ATR nhân 
    # tùy ý như thiết kế cũ.
    "SWING_TP_LEVELS_MARGIN_PCT": [
        {"margin_profit_pct": 5.0,  "close_pct": 0.40, "label": "TP1 - Chốt lời sớm"},
        {"margin_profit_pct": 7.5,  "close_pct": 0.35, "label": "TP2 - Mục tiêu chính"},
        {"margin_profit_pct": 10.0, "close_pct": 0.25, "label": "TP3 - Mục tiêu tối đa"},
    ],
    
    # Ngưỡng đánh giá tính khả thi — tính theo % BIẾN ĐỘNG GIÁ thực 
    # tế (đã quy đổi từ margin %), KHÔNG phải % margin
    "PRICE_MOVE_FEASIBLE_MAX_PCT": 0.03,  # price move > 3% mới coi là "tham vọng"
    "PRICE_MOVE_FEASIBLE_MIN_ATR_MULT": 1.0,  # price move < 1x ATR thì "quá gần, dễ đạt nhưng R:R kém"
})
```

## BƯỚC 2 — Sửa lại hoàn toàn logic tính TP trong `decide()`

```python
if action in (Action.BUY, Action.SELL) and timeframe in ("1h", "4h"):
    leverage = self.cfg["DEFAULT_LEVERAGE"]
    sl_mult = self.cfg["SWING_SL_ATR_MULTIPLIER"]
    entry_ref = entry_price if entry_price else entry_zone
    
    sl = (entry_ref - sl_mult*atr if action == Action.BUY 
          else entry_ref + sl_mult*atr)
    risk_distance = abs(entry_ref - sl)
    sl_price_move_pct = risk_distance / entry_ref * 100
    sl_margin_loss_pct = sl_price_move_pct * leverage  # % vốn mất nếu dính SL
    
    tp_levels_result = []
    for level in self.cfg["SWING_TP_LEVELS_MARGIN_PCT"]:
        margin_target = level["margin_profit_pct"]
        
        # === CÔNG THỨC CỐT LÕI ĐÃ SỬA ĐÚNG ===
        price_move_pct_needed = margin_target / leverage   # VD: 5%/5 = 1.0%
        price_distance = entry_ref * (price_move_pct_needed / 100)
        
        tp_price = (entry_ref + price_distance if action == Action.BUY 
                    else entry_ref - price_distance)
        
        rr_ratio = price_distance / risk_distance if risk_distance > 0 else 0
        holding = self.estimate_holding_period(price_distance, atr, timeframe)
        
        tp_levels_result.append({
            "label": level["label"],
            "price": round(tp_price, 4),
            "price_move_pct": round(price_move_pct_needed, 2),      # % GIÁ
            "margin_profit_pct": margin_target,                      # % VỐN KÝ QUỸ
            "rr_ratio": round(rr_ratio, 2),
            "close_pct": int(level["close_pct"]*100),
            "holding_estimate": holding["time_label"],
        })
    
    # Đánh giá tính khả thi dựa trên % GIÁ thực tế (không phải % margin)
    farthest_price_move = tp_levels_result[-1]["price_move_pct"] / 100
    max_feasible = self.cfg["PRICE_MOVE_FEASIBLE_MAX_PCT"]
    
    if farthest_price_move <= max_feasible:
        feasibility = ("✅ Hoàn toàn khả thi — biến động giá cần thiết "
                        "nằm trong biên độ dao động thông thường của "
                        f"khung {timeframe}")
    else:
        feasibility = ("⚠️ Cần biến động giá lớn hơn bình thường — "
                        "có thể mất nhiều thời gian hơn ước tính hoặc "
                        "không đạt nếu thị trường đi ngang")
    
    advice.swing_mode = True
    advice.tp_levels = tp_levels_result
    advice.sl_margin_loss_pct = round(sl_margin_loss_pct, 2)
    advice.target_feasibility = feasibility
    advice.estimated_holding_time = tp_levels_result[-1]["holding_estimate"]
```

## BƯỚC 3 — Cập nhật TradeAdvice (field mới, additive)

```python
@dataclass
class TradeAdvice:
    # ... giữ nguyên toàn bộ field cũ ...
    swing_mode: bool = False
    tp_levels: list = field(default_factory=list)
    # Mỗi phần tử tp_levels giờ có CẢ 2 con số để tránh nhầm lẫn:
    #   price_move_pct    -> % giá thực tế cần di chuyển
    #   margin_profit_pct -> % lợi nhuận trên vốn ký quỹ tương ứng
    sl_margin_loss_pct: float = 0.0   # % vốn mất nếu dính SL (đã nhân leverage)
    target_feasibility: str = ""
    estimated_holding_time: str = ""
```

## BƯỚC 4 — Cập nhật UI (dashboard.html) — hiển thị SONG SONG 2 đơn vị

Đây là phần quan trọng nhất để không lặp lại nhầm lẫn — LUÔN hiển 
thị cả 2 con số cạnh nhau, không bao giờ chỉ hiển thị 1 loại %:

```
┌───────────────────────────────────────────────────────────────┐
│  🎯 Khuyến Nghị Giao Dịch [XAU · 1h]       Độ tin cậy: Cao     │
│                                                                 │
│   SELL    Đòn bẩy: x5 | Vốn/lệnh: 10%                         │
│                                                                 │
│  ĐIỂM VÀO: $4,159.30     CẮT LỖ: $4,142.55                     │
│  (Nếu dính SL: mất ~1.4% vốn ký quỹ)                           │
│                                                                 │
│  ┌───────────────────────────────────────────────────────┐   │
│  │           Giá cần di chuyển   │   Lãi trên vốn ký quỹ  │   │
│  │ TP1 (40%): $4,117.70 (−1.0%)  │        +5.0%     ~8h   │   │
│  │ TP2 (35%): $4,096.90 (−1.5%)  │        +7.5%     ~12h  │   │
│  │ TP3 (25%): $4,076.10 (−2.0%)  │       +10.0%     ~16h  │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                                 │
│  ✅ Hoàn toàn khả thi — biến động giá cần thiết nằm trong      │
│     biên độ dao động thông thường của khung 1h                │
│                                                                 │
│  ⚠️ Lưu ý: % "Lãi trên vốn ký quỹ" đã bao gồm đòn bẩy x5.      │
│     Đây KHÔNG phải % biến động giá thực tế của XAU (chỉ 1-2%). │
└───────────────────────────────────────────────────────────────┘
```

Yêu cầu kỹ thuật cho đoạn JS render:
```javascript
// Luôn hiển thị CẢ 2 giá trị cạnh nhau trong cùng 1 dòng, 
// KHÔNG được chỉ hiển thị 1 trong 2 để tránh tái diễn nhầm lẫn
function renderTpLevel(level) {
  return `
    <div class="tp-row">
      <span>${level.label} (đóng ${level.close_pct}%)</span>
      <span class="price">${level.price} 
        <small>(giá ${level.price_move_pct > 0 ? '+' : ''}${level.price_move_pct}%)</small>
      </span>
      <span class="margin-profit">+${level.margin_profit_pct}% vốn ký quỹ</span>
      <span class="holding">~${level.holding_estimate}</span>
    </div>`;
}
```

## BƯỚC 5 — Unit Test xác nhận công thức quy đổi ĐÚNG

```python
def test_margin_to_price_conversion_correct():
    """Kiểm tra công thức cốt lõi: với leverage=5, margin_profit=10% 
    PHẢI quy đổi đúng ra price_move=2.0%, không được lệch 5 lần 
    như lỗi thiết kế trước."""
    entry = 4159.30
    atr = 11.17
    engine = ConfluenceDecisionEngine(CONFIG)
    advice = engine.decide(
        asset="XAU", timeframe="1h",
        snapshot={"close": entry, "ema_fast": 4158.11, 
                  "ema_slow": 4162.64, "rsi": 38.3,
                  "bb_upper": 4164.63, "bb_lower": 4151.95, "atr": atr},
        recent_signals=[], recent_candles=[...])
    
    tp3 = advice.tp_levels[-1]
    assert tp3["margin_profit_pct"] == 10.0
    assert abs(tp3["price_move_pct"] - 2.0) < 0.01, (
        "Lỗi quy đổi: price_move_pct phải = margin_profit_pct / leverage"
    )

def test_sl_margin_loss_calculated_with_leverage():
    """SL loss trên vốn ký quỹ phải được nhân với leverage, 
    không phải % giá thuần."""
    # Với SL cách Entry 0.4% giá, leverage=5 => mất 2.0% vốn, 
    # không phải 0.4%
    assert advice.sl_margin_loss_pct == pytest.approx(
        advice.tp_levels[0]["price_move_pct"]... , rel=0.1)  
    # (chi tiết assert theo đúng fixture cụ thể)

def test_ui_always_shows_both_units():
    """Đảm bảo mỗi tp_level LUÔN có cả 2 field price_move_pct 
    VÀ margin_profit_pct, không được thiếu field nào."""
    for level in advice.tp_levels:
        assert "price_move_pct" in level
        assert "margin_profit_pct" in level
```

[OUTPUT FORMAT]
- Code diff đầy đủ, chỉ rõ đây là bản THAY THẾ cho CONFIG 
  "SWING_TP_LEVELS" và logic tính TP đã thiết kế ở lần cập nhật 
  trước (không phải thêm mới song song — tránh tồn tại 2 bộ công 
  thức gây nhầm lẫn).
- Bảng minh họa dùng ĐÚNG số liệu XAU trong ảnh gốc, trình bày rõ 
  2 cột "% giá" và "% vốn ký quỹ" cạnh nhau để kiểm chứng công thức 
  trước khi merge.
- Bảng "Cam kết tài nguyên": xác nhận thay đổi này KHÔNG làm tăng 
  tải tính toán so với thiết kế trước (thực chất ĐƠN GIẢN HƠN vì 
  bỏ được bước quét swing-structure phức tạp, chỉ còn phép nhân 
  chia cơ bản).
- Checklist xác nhận test_margin_to_price_conversion_correct() 
  PASS — đây là test quan trọng nhất để đảm bảo không tái diễn sai 
  số 5 lần do nhầm lẫn đơn vị.
```
