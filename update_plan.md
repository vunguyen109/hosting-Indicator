
[ROLE]
Bạn là Senior Python Backend Engineer, tiếp tục bảo trì module 
ConfluenceDecisionEngine (Section 4.5) vừa được triển khai trong 
engine.py của dự án "Hosting Indicator Engine" (1.5 vCore/2GB RAM/
2GB Disk, Zero Heavy Dependencies, Telegram KHÔNG được chạm vào).

Đây là bản VÁ LỖI (patch), không phải thiết kế lại. Áp dụng nguyên 
tắc "Minimal Diff" — chỉ sửa đúng phần code liên quan đến 3 lỗi đã 
xác định bên dưới, KHÔNG refactor các phần đang hoạt động đúng, 
KHÔNG đổi tên field/function đã có (sẽ phá vỡ dashboard.html đang 
đọc dữ liệu từ các field đó).

Quy tắc bất di bất dịch (kế thừa từ bản cập nhật trước):
1. KHÔNG thêm package mới vào requirements.txt.
2. KHÔNG sửa TelegramDispatcher (Section 5) dưới bất kỳ hình thức nào.
3. KHÔNG tăng tần suất polling/tính toán — mọi logic mới vẫn chạy 
   đúng 1 lần/nến đóng, không chạy theo nhịp poll 3s của client.
4. Mọi threshold mới phải đặt trong CONFIG, không hard-code.
5. Giữ nguyên tương thích ngược schema JSON của /api/snapshot — 
   chỉ được THÊM field mới nếu cần, không đổi/xóa field cũ.
6. Mọi thay đổi phải có Unit Test tương ứng chứng minh lỗi cũ đã 
   được sửa (regression test dựa trên đúng số liệu thực tế của 
   case XAU 1h đã quan sát).

[CONTEXT]
## 3 lỗi đã xác định qua kiểm thử thực tế (đính kèm số liệu gốc):

### LỖI 1 (🔴 Critical — Sai logic, dễ gây hiểu nhầm nghiêm trọng):
Khi người dùng xem ở khung thời gian LỚN NHẤT hệ thống hỗ trợ (1h), 
module vẫn hard-code hiển thị reasoning text "Khung thời gian lớn 
ĐỒNG THUẬN xu hướng" — trong khi thực tế logic đang so sánh với 
khung NHỎ HƠN (15m). Về bản chất ngữ nghĩa, "khung lớn hơn" không 
tồn tại khi đang đứng ở khung lớn nhất, nên câu reasoning đang nói 
ngược hướng so với phép so sánh thực sự diễn ra trong code.

Dữ liệu minh chứng: Case XAU timeframe="1h", mtf_agree được tính 
từ việc so sánh với timeframe="15m" (nhỏ hơn), nhưng text output 
vẫn ghi "Khung thời gian lớn ĐỒNG THUẬN" — gây hiểu lầm cho người 
dùng về hướng so sánh.

### LỖI 2 (🟠 High — Điểm mù nghiêm trọng nhất, ảnh hưởng chất 
### lượng khuyến nghị thực chiến):
Hàm detect_conflict() hiện tại CHỈ phát hiện mâu thuẫn bằng cách 
quét RuleEngine.recent_signals tìm các Signal RỜI RẠC đã được định 
nghĩa sẵn (EMA_CROSS_UP/DOWN, BB_BREAKOUT_UP/DOWN, RSI_OVERBOUGHT/
OVERSOLD) trong N nến gần nhất. Hàm này KHÔNG có khả năng phát hiện 
một cú biến động giá mạnh dạng V-shape/counter-move nếu cú biến 
động đó không trùng khớp chính xác với 1 trong các loại Signal đã 
định nghĩa.

Dữ liệu minh chứng thực tế: Asset XAU, timeframe 1h. Giá giảm sâu 
xuống chạm BB Lower (~$4,148), sau đó bật tăng mạnh trở lại trong 
vài nến liên tiếp lên đến $4,188.83 (mức tăng ước tính ~3.7x ATR14 
hiện tại, ATR≈10.81). Mặc dù biến động giá rất rõ ràng mâu thuẫn 
với kết luận EMA "Giảm rõ rệt", hệ thống KHÔNG gắn cờ conflict_
detected=true, dẫn đến khuyến nghị SELL với entry_zone thấp hơn 
giá hiện tại ~25 điểm — một kịch bản rủi ro mà một trader có kinh 
nghiệm sẽ nhận ra ngay bằng mắt nhưng thuật toán hiện tại bỏ sót.

### LỖI 3 (🟡 Medium — UX gây nhiễu nhận thức, không phải lỗi logic):
Dashboard đồng thời hiển thị 2 thang đo khác nhau không có chú 
giải liên kết:
- Gauge "Thước Đo Tâm Lý Thị Trường": thang 0-100 (tính ở CLIENT, 
  theo logic cũ đã có từ trước khi có ConfluenceDecisionEngine)
- Card "Khuyến Nghị Giao Dịch": field confidence_score thang 0-5 
  (tính ở SERVER, từ ConfluenceDecisionEngine)
Người dùng phổ thông (không rành kỹ thuật) nhìn thấy "19 - BÁN 
MẠNH" ở Gauge nhưng "Trung bình (3/5)" ở Card Khuyến Nghị, dễ 
hiểu lầm đây là 2 kết luận trái ngược nhau, trong khi thực chất 
chỉ là 2 thang đo khác nhau đo cùng 1 xu hướng.

[INSTRUCTION]

## PATCH 1 — Sửa reasoning text theo đúng hướng so sánh thực tế (Lỗi 1)

Trong hàm `decide()` của ConfluenceDecisionEngine, sửa đoạn sinh 
reasoning text cho MTF agreement. Thay vì hard-code "lớn/nhỏ", 
xác định ĐỘNG dựa trên thứ tự thực tế của timeframe đang xét so 
với timeframe còn lại:

```python
# Định nghĩa thứ tự timeframe (thêm vào CONFIG hoặc hằng số module)
TIMEFRAME_ORDER = {"15m": 1, "1h": 2}

def _get_relative_tf_label(self, current_tf: str, other_tf: str) -> str:
    """Trả về mô tả tương đối chính xác về mặt ngữ nghĩa, 
    không giả định cố định 'khung lớn' hay 'khung nhỏ'."""
    current_rank = TIMEFRAME_ORDER.get(current_tf, 0)
    other_rank = TIMEFRAME_ORDER.get(other_tf, 0)
    other_tf_display = {"15m": "15 phút", "1h": "1 giờ"}.get(other_tf, other_tf)
    if other_rank > current_rank:
        return f"khung {other_tf_display} (dài hạn hơn)"
    elif other_rank < current_rank:
        return f"khung {other_tf_display} (ngắn hạn hơn)"
    else:
        return f"khung {other_tf_display}"
```

Sửa lại đoạn gọi trong `decide()`:

```python
# THAY THẾ đoạn cũ:
# reasoning.append("Khung thời gian lớn ĐỒNG THUẬN xu hướng")
# reasoning.append("⚠️ Khung thời gian lớn KHÔNG đồng thuận")

# BẰNG đoạn mới, cần truyền thêm other_timeframe vào hàm decide():
if mtf_agree is not None:
    tf_label = self._get_relative_tf_label(timeframe, other_timeframe)
    if mtf_agree:
        reasoning.append(f"{tf_label} cũng ĐỒNG THUẬN xu hướng")
    else:
        reasoning.append(f"⚠️ {tf_label} KHÔNG đồng thuận xu hướng")
```

Cập nhật signature hàm `decide()` để nhận thêm tham số `other_
timeframe: str` (tên khung còn lại, VD "15m"), truyền từ Orchestrator 
nơi đã biết rõ cặp timeframe nào đang được so sánh với nhau — đây 
là field CÓ SẴN trong vòng lặp hiện tại, không cần tính toán mới.

## PATCH 2 — Bổ sung phát hiện "Counter-Move" (Lỗi 2 — ƯU TIÊN CAO NHẤT)

Thêm hằng số mới vào CONFIG:
```python
CONFIG.update({
    # Số nến gần nhất dùng để phát hiện biến động ngược dòng mạnh
    "COUNTER_MOVE_LOOKBACK_CANDLES": 3,
    # Ngưỡng: biến động giá (đơn vị lần ATR) trong N nến gần nhất 
    # được coi là "counter-move" đủ mạnh để nghi ngờ xu hướng EMA
    "COUNTER_MOVE_ATR_THRESHOLD": 2.0,
})
```

Thêm phương thức mới vào ConfluenceDecisionEngine, nhận trực tiếp 
dữ liệu nến thô từ CircularBuffer (đã có sẵn trong Orchestrator, 
qua `buffer.last_n_candles(limit)` — method này ĐÃ TỒN TẠI theo 
Section 2, không cần tạo mới):

```python
def detect_counter_move(self, recent_candles: list, atr: float, 
                          trend: TrendState) -> tuple[bool, float]:
    """
    Phát hiện biến động giá ngược chiều với xu hướng EMA hiện tại 
    trong N nến gần nhất, kể cả khi không có Signal rời rạc nào 
    được RuleEngine kích hoạt.
    
    Trả về: (có_phát_hiện: bool, độ_lớn_theo_ATR: float) 
    để phục vụ cả logic quyết định lẫn hiển thị reasoning chi tiết.
    """
    n = self.cfg["COUNTER_MOVE_LOOKBACK_CANDLES"]
    if len(recent_candles) < n or atr <= 0:
        return False, 0.0
    
    window = recent_candles[-n:]
    price_change = window[-1].close - window[0].close
    change_in_atr = price_change / atr
    threshold = self.cfg["COUNTER_MOVE_ATR_THRESHOLD"]
    
    trend_is_down = trend in (TrendState.STRONG_DOWN, TrendState.LEAN_DOWN)
    trend_is_up = trend in (TrendState.STRONG_UP, TrendState.LEAN_UP)
    
    # Counter-move = giá tăng mạnh NGƯỢC xu hướng giảm, hoặc ngược lại
    if trend_is_down and change_in_atr > threshold:
        return True, change_in_atr
    if trend_is_up and change_in_atr < -threshold:
        return True, change_in_atr
    return False, change_in_atr
```

Sửa lại signature và logic của `decide()` để nhận thêm tham số 
`recent_candles: list` và tích hợp kết quả counter-move vào quyết 
định cuối cùng:

```python
def decide(self, asset: str, timeframe: str, snapshot: dict, 
            recent_signals: list, recent_candles: list,
            other_tf_snapshot: dict | None = None,
            other_timeframe: str | None = None) -> TradeAdvice:
    # ... (giữ nguyên toàn bộ logic classify_* như cũ) ...
    
    conflict_from_signals = self.detect_conflict(
        recent_signals, asset, timeframe, trend)
    counter_move_detected, counter_move_magnitude = self.detect_counter_move(
        recent_candles, atr, trend)
    
    # Gộp 2 nguồn phát hiện mâu thuẫn thành 1 cờ tổng hợp
    conflict = conflict_from_signals or counter_move_detected
    
    # ... (giữ nguyên scoring logic EMA/RSI/BB như cũ) ...
    
    # Thêm reasoning riêng cho counter-move (khác với conflict từ Signal)
    if counter_move_detected:
        direction = "tăng" if counter_move_magnitude > 0 else "giảm"
        reasoning.append(
            f"⚠️ Phát hiện biến động giá {direction} mạnh bất thường "
            f"trong {self.cfg['COUNTER_MOVE_LOOKBACK_CANDLES']} nến gần nhất "
            f"(~{abs(counter_move_magnitude):.1f}x ATR) — ngược chiều với "
            f"xu hướng EMA hiện tại, cần thận trọng"
        )
    elif conflict_from_signals:
        reasoning.append("⚠️ Phát hiện tín hiệu MÂU THUẪN gần đây trên chart")
    else:
        reasoning.append("Không có tín hiệu mâu thuẫn gần đây")
    
    # ... (phần is_sideway, is_extreme_overextended, action quyết định 
    #      GIỮ NGUYÊN logic cũ, vì biến `conflict` đã tự động bao gồm 
    #      cả 2 nguồn phát hiện — không cần sửa thêm điều kiện nào) ...
```

Cập nhật lời gọi trong Orchestrator (Section 11) để truyền thêm 
`recent_candles` và `other_timeframe`:

```python
advice = self.confluence_engine.decide(
    asset=asset,
    timeframe=timeframe,
    snapshot=current_indicator_snapshot,
    recent_signals=self.rule_engine.recent_signals,
    recent_candles=self.buffers[asset][timeframe].last_n_candles(
        self.cfg["COUNTER_MOVE_LOOKBACK_CANDLES"]),
    other_tf_snapshot=self.indicators.get(asset, {}).get(other_tf),
    other_timeframe=other_tf,
)
```

**Thêm field mới vào TradeAdvice** (additive, không phá schema cũ) 
để frontend có thể hiển thị chi tiết hơn nếu muốn:
```python
@dataclass
class TradeAdvice:
    # ... các field cũ giữ nguyên ...
    counter_move_detected: bool = False
    counter_move_magnitude_atr: float = 0.0
```

## PATCH 3 — Chú giải liên kết giữa Gauge (0-100) và Confidence (0-5) (Lỗi 3)

Đây là thay đổi CHỈ Ở FRONTEND (dashboard.html), không đụng Python 
backend. Thêm 1 dòng text nhỏ ngay dưới Gauge hiện có:

```html
<!-- Chèn ngay sau phần tử hiển thị số Gauge 0-100 và nhãn 
     "BÁN MẠNH/MUA MẠNH", TRƯỚC phần progress bar màu gradient -->
<p class="text-xs text-gray-400 mt-1">
  ℹ️ Đây là chỉ số tâm lý thị trường tổng quát. Xem khuyến nghị 
  hành động cụ thể kèm độ tin cậy riêng ở khung 
  <strong>"Khuyến Nghị Giao Dịch"</strong> bên dưới.
</p>
```

Đồng thời, trong card "Khuyến Nghị Giao Dịch", thêm tooltip 
(giống cơ chế icon ⓘ đã có sẵn ở các cột bảng "Sức Khỏe Thị Trường" 
theo đúng pattern UI hiện tại) giải thích ngắn gọn ý nghĩa thang 
đo 0-5:

```html
<span class="info-icon" data-tooltip="Độ tin cậy được tính từ số 
lượng yếu tố kỹ thuật (Xu hướng, RSI, Vị trí dải giá, Tín hiệu 
mâu thuẫn, Đa khung thời gian) đang đồng thuận với nhau. Đây là 
thang đo RIÊNG, khác với Thước Đo Tâm Lý Thị Trường ở trên, nhưng 
cả hai đều phản ánh cùng một xu hướng thị trường.">ⓘ</span>
```

## PATCH 4 — Unit Test cho cả 3 lỗi đã sửa

Viết test case dựa TRỰC TIẾP trên số liệu thực tế đã quan sát để 
đảm bảo không tái diễn:

```python
def test_mtf_label_dynamic_not_hardcoded():
    """Lỗi 1: Khi đang ở khung 1h, label phải nói '15 phút 
    (ngắn hạn hơn)', KHÔNG được nói 'khung lớn'."""
    engine = ConfluenceDecisionEngine(CONFIG)
    label = engine._get_relative_tf_label("1h", "15m")
    assert "ngắn hạn hơn" in label
    assert "lớn" not in label.lower()

def test_counter_move_detected_on_vshape_bounce():
    """Lỗi 2 - Regression test: Tái hiện đúng case XAU 1h đã 
    quan sát. Giá giảm sâu rồi bật mạnh ngược trend EMA giảm 
    PHẢI được gắn cờ conflict, dẫn tới action=WAIT."""
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
        recent_signals=[], recent_candles=candles)
    
    assert advice.counter_move_detected is True
    assert advice.action == "WAIT", (
        "Phải chuyển thành WAIT khi phát hiện counter-move mạnh, "
        "không được giữ nguyên SELL như lỗi cũ"
    )

def test_no_false_positive_counter_move_in_normal_trend():
    """Đảm bảo patch mới KHÔNG gây false positive khi trend đang 
    diễn biến bình thường, không có biến động bất thường."""
    candles = [
        FakeCandle(close=4160.0), 
        FakeCandle(close=4158.0), 
        FakeCandle(close=4156.0),  # giảm đều, đúng với trend giảm
    ]
    snapshot = {
        "close": 4156.0, "ema_fast": 4158.0, "ema_slow": 4162.0,
        "rsi": 45.0, "bb_upper": 4170.0, "bb_lower": 4150.0,
        "atr": 10.0,
    }
    engine = ConfluenceDecisionEngine(CONFIG)
    advice = engine.decide(
        asset="XAU", timeframe="1h", snapshot=snapshot,
        recent_signals=[], recent_candles=candles)
    
    assert advice.counter_move_detected is False
    assert advice.action == "SELL"  # vẫn giữ nguyên hành vi đúng cũ
```

## PATCH 5 — Cập nhật tài liệu

Trong PROJECT_ARCHITECTURE.md, Section 4.5, thêm ghi chú changelog:

```markdown
### Changelog v1.1 (Patch sau kiểm thử thực tế)
- [Fixed] Reasoning text MTF agreement giờ mô tả ĐÚNG hướng so 
  sánh thực tế (ngắn hạn/dài hạn) thay vì hard-code "khung lớn".
- [Fixed] Bổ sung detect_counter_move() để bắt các cú biến động 
  giá mạnh dạng V-shape không trùng khớp với Signal rời rạc nào 
  của RuleEngine — khắc phục điểm mù khiến hệ thống bỏ sót mâu 
  thuẫn rõ ràng giữa EMA Trend và diễn biến giá tức thời.
- [UX] Thêm chú giải liên kết giữa Gauge (thang 0-100, tính client) 
  và Confidence Score (thang 0-5, tính server) để tránh người dùng 
  hiểu lầm đây là 2 kết luận trái ngược.
- Nguồn gốc patch: phát hiện qua live testing case thực tế 
  XAU/USD khung 1h ngày quan sát, giá bật từ $4,148 lên $4,188 
  (~3.7x ATR) mà không được hệ thống gắn cờ cảnh báo.
```

[OUTPUT FORMAT]
- Code diff đầy đủ cho từng patch (PATCH 1 → PATCH 5), chỉ rõ vị 
  trí chèn/sửa theo đúng dòng/hàm đã tồn tại trong engine.py.
- Bảng "Trước/Sau" minh họa output thực tế của case XAU 1h đã nêu, 
  so sánh reasoning text VÀ action trước và sau khi áp dụng patch.
- Bảng "Cam kết tài nguyên" xác nhận patch này KHÔNG làm tăng đáng 
  kể RAM/CPU (ước tính cụ thể: thêm bao nhiêu byte do field 
  counter_move_detected/magnitude, thêm bao nhiêu micro-giây do 
  vòng lặp 3 nến trong detect_counter_move).
- Checklist xác nhận: tất cả Unit Test ở PATCH 4 phải PASS trước 
  khi coi bản vá hoàn tất, đặc biệt test_counter_move_detected_
  on_vshape_bounce() vì đây là test tái hiện đúng lỗi thực tế 
  nghiêm trọng nhất đã phát hiện.
```

