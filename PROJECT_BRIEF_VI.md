# Hosting Indicator Engine — bàn giao kỹ thuật và công việc còn mở

Ngày rà soát 18/11/2026. Mốc mã nguồn là `main@c8a284b`. Thông tin vận hành dựa trên snapshot Botkeep ngày 17/11, nhật ký triển khai ngày 18/11 và hai báo cáo audit trong workspace. Trạng thái server sau các mốc này cần được xác minh riêng.

## 1. Mục đích và môi trường

Engine chạy 24/7, đọc nến OHLC, tính chỉ báo, hiển thị khuyến nghị BUY/SELL/WAIT và theo dõi kết quả giả lập của các khuyến nghị đủ điều kiện. Tiến trình chạy `engine.py` chỉ truy cập dữ liệu thị trường và các kênh hiển thị/thông báo. Mã đặt lệnh lên sàn không có trong luồng này. Telegram nhận các sự kiện chỉ báo, còn khuyến nghị hội tụ và kết quả paper-tracking xem qua Web/API.

| Thành phần | Triển khai tại mốc rà soát |
|---|---|
| Danh mục | Binance Spot REST công khai cho BTCUSDT, ETHUSDT. TwelveData cho XAU/USD, XAG/USD và USO. OIL đại diện ETF USO. XAG có trong cấu hình nhưng key của đợt audit bị nhà cung cấp từ chối quyền truy cập. |
| Khung thời gian | 1h và 4h cho mỗi tài sản, tối đa 10 bộ đệm × 300 nến. 15m/15min đã ngừng tạo khuyến nghị và track mới. API còn trường tương thích cho 15m. |
| Backend | Python, asyncio, aiohttp.web, SQLite WAL và thư viện chuẩn. Phụ thuộc backend trực tiếp duy nhất là `aiohttp==3.9.5`. Các phép tính chỉ báo không dùng pandas/numpy. |
| Frontend | HTML/CSS/JavaScript nhúng trong `engine.py`, TradingView Lightweight Charts 4.1.3 qua CDN. Snapshot mỗi 3 giây, biểu đồ nến mỗi 15 giây, hiệu suất mỗi 30 giây. Giá dùng để kiểm tra advice là close mới nhất trong cache. |
| Hosting | Botkeep/Linux. Snapshot cho thấy Python 3.11. Ngân sách thiết kế 1,5 vCore, RAM 2 GB, đĩa 2 GB. Dashboard mở trước khi nạp đủ nến. |
| Khởi động | Cài `pip install -r requirements.txt`, chạy `python -u engine.py`. Bind mặc định `0.0.0.0`, cổng lấy theo `SERVER_PORT`, rồi `PORT`, mặc định 8080. |
| Cấu hình | `TWELVE_API_KEY` mở nguồn TwelveData, `TG_BOT_TOKEN` và `TG_CHAT_ID` mở Telegram. Thiếu các khóa này vẫn chạy Web/console và có thể lấy dữ liệu Binance. Trên main, `ADMIN_TOKEN` bảo vệ cấu hình nóng bằng Bearer. Khi thiếu token, endpoint ghi trả 403. |
| Lưu trữ | SQLite `data/signal_tracker.db` lưu vòng đời track. Chính sách dọn lịch sử dùng ngưỡng 500 bản ghi và 20 MB. `data/state.json` lưu tóm tắt buffer mỗi 15 phút. Log xoay vòng 5 MiB × 3 file. |
| API | `/`, `/health`, `/api/health`, `/api/snapshot`, `/api/candles`, `/api/performance` dùng GET. `POST /api/config` cập nhật nóng ba trường cấu hình TwelveData/Telegram được phép. |

Các yêu cầu thiết kế đã ghi gồm giữ dependency nhẹ, xử lý quyết định theo nhịp đóng nến, giữ tương thích JSON khi bổ sung trường, tập trung tham số vào CONFIG và hiển thị riêng % giá với % ký quỹ. Cấu hình có mức 10% tài khoản cho hồ sơ vốn minh họa. Tracker chính ghi kết quả theo R và % ký quỹ, chưa có sổ vốn thực thi sizing 10% cho từng lệnh. [Cấu hình và engine](./engine.py) · [Ràng buộc kiến trúc](./PROJECT_ARCHITECTURE.md) · [Dependency](./requirements.txt)

## 2. Từ chỉ báo tới điểm vào

`RuleEngine` phát sự kiện khi EMA20/50 cắt nhau, RSI14 vượt ngưỡng 70/30, close nằm ngoài Bollinger Bands (20, 2σ), hoặc ATR14 lớn hơn 1,5 lần ATR trước. `ConfluenceDecisionEngine.decide` dùng bảng điểm riêng để tạo advice.

| Yếu tố | Tác động lên điểm |
|---|---|
| EMA | Lấy (EMA20 − EMA50) / ATR. Trên +0,5 cho +2, trên +0,15 còn lại cho +1. Chiều giảm đối xứng cho −2/−1. Vùng giữa cho 0. |
| RSI | RSI ≤25 cho +1. Trên 25 đến 40 cho −1. Từ 60 đến dưới 75 cho +1. Từ 75 trở lên cho −1. Vùng giữa cho 0. |
| Bollinger | Bên trong dải và sát biên trên/dưới trong vùng 15% độ rộng dải cho +1/−1. Phá dải đưa ra cảnh báo, không cộng điểm biên. |
| Hai khung 1h và 4h | EMA hai khung đồng hướng làm độ lớn của điểm khác 0 tăng 1. Có dữ liệu nhưng không đồng hướng làm điểm tin cậy giảm 1. Thiếu dữ liệu khung kia thì giữ nguyên. |

- **BUY** khi điểm ≥2, **SELL** khi điểm ≤−2. **WAIT** khi thiếu dữ liệu, không đủ điểm, EMA trung tính đồng thời giá ở giữa BB, hoặc có điều kiện chặn.
- **Điều kiện chặn** gồm tín hiệu ngược xu hướng trong hai *sự kiện* gần nhất cùng tài sản/khung, hoặc close đầu đến close cuối trong ba nến đi ngược xu hướng hơn 2 ATR. Engine cũng chặn mua đuổi khi phá BB trên, điểm ≥3 và RSI chưa ở vùng cực quá mua. Chiều bán áp dụng đối xứng.
- **Độ tin cậy** tính từ trị tuyệt đối của điểm sau điều chỉnh bất đồng khung. Từ 4 là Cao, 2–3 là Trung bình, dưới 2 là Thấp. Tracker chỉ nhận BUY/SELL Cao hoặc Trung bình, có entry, SL, TP1, qua kiểm tra trạng thái vô hiệu và còn trống cặp tài sản/khung.
- **Điểm vào dự kiến** là close nếu khoảng cách từ close tới EMA20 ≤0,3 ATR (`now`). Xa hơn thì chờ tại EMA20 (`wait_pullback`). Công thức dùng khoảng cách tuyệt đối nên có thể tạo vùng chờ sai phía. Dashboard có các trạng thái chạm/vượt SL, lệch entry ≥1 ATR và dịch vùng EMA20 ≥0,3 ATR. Poll dashboard chỉ đọc cache và kiểm tra hiệu lực.

Nguồn triển khai gồm [RuleEngine và ConfluenceDecisionEngine](./engine.py), các ca quyết định và tracker trong [test_engine.py](./test_engine.py).

## 3. SL, TP và khớp lệnh của tracker chính

Giá tham chiếu là close với `now`, EMA20 với `wait_pullback`. BUY đặt SL dưới giá tham chiếu 1,5 ATR, SELL đặt SL phía trên 1,5 ATR. TP được tính từ mục tiêu lợi nhuận trên ký quỹ chia đòn bẩy mặc định 5. R:R thay đổi theo tài sản và ATR.

| Mốc | Lợi nhuận mục tiêu trên ký quỹ | Biên độ giá thuận chiều | Tỷ trọng chốt |
|---|---:|---:|---:|
| TP1 | 5% | 1% | 40% |
| TP2 | 7,5% | 1,5% | 35% |
| TP3 | 10% | 2% | 25% |

Ví dụ BUY có giá tham chiếu 100, ATR bằng 1 thì SL 98,5 và ba TP lần lượt 101, 101,5, 102. Nếu khớp đúng 100 và đạt đủ ba TP, lợi nhuận gộp theo tỷ trọng là 7,125% ký quỹ trước chi phí. TP3 là mục tiêu của 25% cuối vị thế.

- Tracker cập nhật track cũ rồi mới xét tạo track. Một cặp tài sản/khung có tối đa một track đang hoạt động. Cùng tài sản có thể có track ở cả 1h và 4h.
- `now` xét khớp ở nến tiếp theo, BUY dùng giá lớn hơn giữa giá kế hoạch và open, SELL dùng giá nhỏ hơn. `wait_pullback` hiện cần low ≤ entry ≤ high và khớp tại giá kế hoạch. Nến vừa khớp được bỏ qua khi xét TP/SL.
- Tracker chốt từng phần, giữ SL gốc sau TP1, tính R thực hiện theo tỷ trọng. Khi nến cùng chạm SL và TP kế tiếp, track chuyển `ambiguous` và được loại khỏi thống kê thắng/thua chính. Hiệu suất chưa tính đầy đủ chi phí giao dịch, funding hay trượt giá thực tế.
- Hạn chờ 1h/4h tương ứng 12/8 đơn vị khung, hạn sau khớp 72/30. Code cộng số giây vào timestamp, nên khoảng nghỉ thị trường vẫn tính vào hạn. Với vị thế đã mở, nhánh hết hạn được xét trước SL/TP và đóng phần còn lại tại close.

## 4. DCA x5 trong simulator độc lập

Entry của simulator do bên gọi truyền vào. Hai cờ `ENABLE_X5_DCA` và `ENABLE_TP1_BREAKEVEN_SL` mặc định đều tắt. Tiến trình `engine.py` không gọi simulator. [Mã mô phỏng](./x5dca_simulation.py) · [Test mô phỏng](./test_x5dca_simulation.py)

- Vốn mô hình mặc định 100, margin mở lệnh mặc định cố định 15, notional 75 tại x5. Bên gọi có thể truyền `fixed_margin`. Đổi `INITIAL_MARGIN_PCT` riêng lẻ chưa thay đổi số 15 trong hàm mở lệnh. Tối đa 5 vị thế mở, chặn trùng tài sản cùng hướng qua các khung.
- Khi bật DCA, mức kích hoạt là lỗ chưa thực hiện bằng 80% margin hiện tại. Mỗi lần thêm notional bằng 25% notional **ban đầu**, có trượt giá bất lợi 0,02% và phí taker 0,05%. Giới hạn một lần mỗi nến. Để tái kích hoạt, mức lỗ tại close phải xuống dưới ngưỡng 80%. Cần đủ tiền khả dụng và SL hòa vốn chưa kích hoạt.
- Trước TP1, vị thế dùng ngưỡng thanh lý mô phỏng ở mức mất 97,5% margin. Ví dụ BUY tại 100, x5, chưa DCA hoặc funding thì DCA đầu ở 84 và thanh lý mô phỏng ở 80,5. Đây là hai mức của simulator, khác với SL 1,5 ATR của tracker.
- TP đặt cách giá vốn bình quân 1% / 1,5% / 2%, phí maker 0,02%. Khối lượng chốt mỗi tầng dựa vào lượng mở ban đầu. Nếu bật cờ hòa vốn, TP1 đặt SL ở giá vốn bình quân ngay trước lần chốt và khóa DCA. Phí và trượt giá làm PnL ròng của phần dừng tại đó có thể âm. Funding dùng giá mark và rate do bên gọi cấp.

## 5. Danh sách việc còn mở

Thứ tự dưới đây phản ánh phụ thuộc kỹ thuật và tác động khi đọc kết quả. Các thay đổi chiến lược vẫn cần quyết định của chủ dự án.

| Việc | Bằng chứng và tiêu chí hoàn tất |
|---|---|
| 1. Xác nhận hoặc triển khai WP-S trên Botkeep | Bearer auth, giới hạn tần suất, kiểm tra đầu vào và che bí mật đã có trên main. Byte của engine trong snapshot khớp bản trước WP-S. Lần triển khai SFTP được ghi ngày 18/11 dừng ở xác thực. Cần xác định revision thực chạy và kiểm tra hành vi bảo vệ cấu hình trên revision đó. |
| 2. WP-1, chuẩn hóa nến TwelveData | Fetcher parse giờ thiếu offset, thiếu lọc nến đang chạy. Luồng xử lý giữ bản đầu khi timestamp trùng. Audit ghi nhận USO 1h lúc mới chạy nửa nến. Cần thống nhất timestamp đóng theo UTC, xử lý phiên USO, bao gồm nến cuối phiên ngắn, tại cả boot, reload và poll. |
| 3. Quyết định số phận XAG | Probe lịch sử bị từ chối theo gói. Nhánh local `chore/remove-xag@8acd986` đã bỏ XAG nhưng chưa merge vào main. Nếu giữ, cần nguồn/quyền dữ liệu hoạt động và sửa hot-reload vốn chỉ nạp lại XAU/OIL. |
| 4. WP-4, khôi phục sau gián đoạn và đồng bộ hai khung | Boot/reload có thể sinh lại track cùng nến khi track cũ đã kết thúc. Sau downtime, tracker bỏ qua các nến lỡ. Hai khung đóng cùng lúc có thể đọc snapshot theo thứ tự scheduler. Cần khóa định danh theo nến, phát lại nến thiếu đúng thứ tự và dùng khung tham chiếu đã đóng tại thời điểm quyết định. |
| 5. Chốt quy tắc entry, gap, SL và expiry | Audit ghi 52/358 advice pullback bị sai phía trong log. Pullback có thể bỏ lỡ cả nến gap ở giá tốt hơn. Cần đặc tả market/limit, xử lý giá mở cửa vượt SL, ưu tiên SL/TP trên nến hết hạn và đếm nến giao dịch qua cuối tuần. |
| 6. Sửa mẫu thống kê hiệu suất | Snapshot có 137 track, trong đó 83 hết hạn chưa khớp và 4 hết hạn sau khớp. Cả hai nhóm vào mẫu số expectancy và ngưỡng cảnh báo 30 mẫu. Cần tách vòng đời tín hiệu khỏi kết quả các vị thế đã khớp và thống nhất cách báo cáo nến ambiguous. |
| 7. WP-3/Q1, hoàn tất replay trước khi chốt TP/SL | Đã có harness sơ bộ. Báo cáo A6 ghi việc chạy Q1 cuối đang chờ Auditor nghiệm thu. Cần chứng minh replay tái tạo được quyết định với nến đóng đúng thời điểm, sau đó so TP hiện hành với phương án theo ATR/R hoặc lọc R:R. Kết quả cũ trong scratch chưa phải quyết định đổi chiến lược. |
| 8. Chốt quản trị rủi ro và phần dư của DCA | Chưa có trần số lần DCA hoặc tổng margin mỗi vị thế. Đã tái hiện BUY tại 100, DCA một lần rồi chạm cả ba TP nhưng vẫn OPEN với khoảng 0,22317 đơn vị tài sản. Cần quyết định cách chốt lượng mua thêm, sizing khi đổi vốn và ý nghĩa hòa vốn trước/sau phí. |
| 9. Đồng bộ ý nghĩa các trường API và nhãn UI | Với swing, `take_profit_2` trỏ tầng cuối và `tp2_price` lấy lại trường này, khiến `tp2_price` và `tp3_price` có thể trùng dù `tp_levels` đúng. Snapshot còn tính cờ official trước khi gắn validity vừa kiểm tra. Cần test qua endpoint thật, phân biệt advice mới với track đang mở và làm rõ nhãn giá lấy từ cache. |
| 10. Cập nhật tài liệu vận hành | README còn 3 tài sản và 15m. `.env.example` thiếu `ADMIN_TOKEN`. Kế hoạch UI cũ vẫn ghi chờ triển khai dù endpoint nến, chart, gauge và trang hiệu suất đã có. `scratch/` được loại bằng quy tắc Git riêng của checkout, nên tài liệu/bằng chứng cần bàn giao phải được chọn lọc trước khi chia sẻ. |

Auditor có thể dùng các ca trên làm tiêu chí nghiệm thu và quyết định các lựa chọn ảnh hưởng kết quả chiến lược. Executor có thể kiểm mã, chạy test/replay, đọc SQLite, kiểm UI qua trình duyệt và gọi API khi có quyền. Đối chiếu bản đang chạy hoặc lấy snapshot mới từ server phụ thuộc quyền Botkeep/SFTP và kết nối hợp lệ. WebSocket/SSE, Discord/Slack, bot hai chiều và chỉ báo mới xuất hiện trong roadmap dài hạn, chưa có căn cứ xếp cùng các việc đang chặn nghiệm thu.

## 6. Bằng chứng kiểm tra

- Đã chạy hai file test từ bản sao cô lập của mã tại HEAD, trên Windows/Python 3.12.9 với aiohttp 3.9.5. Kết quả **89 passed**. Kiểm thử dùng server HTTP cục bộ cho các ca API.
- Đã đếm SQLite ở chế độ chỉ đọc. Snapshot server có 31 `closed_sl`, 16 `closed_tp3`, 87 `expired` và 3 `pending_entry`. DB trong `data/` của workspace chỉ có 3 track. Hai bộ dữ liệu có phạm vi khác nhau.
- Đã đối chiếu file engine trong snapshot với Git và tái hiện lượng dư sau DCA bằng `step_candle`. Việc rà soát này không đo lại RAM/CPU trên hosting hay truy cập server đang chạy.
- Bằng chứng lịch sử lưu cục bộ tại [báo cáo Pha A](./scratch/pha_a/BAO_CAO_PHA_A.md), [báo cáo A6](./scratch/pha_a/BAO_CAO_A6.md) và [khung replay WP-3](./scratch/wp3/harness.py). Các đường dẫn trong `scratch/` chỉ dùng được ở workspace có lưu hồ sơ audit.
