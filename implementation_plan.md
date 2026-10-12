# KẾ HOẠCH NÂNG CẤP: THIẾT LẬP TWELVE_API_KEY & TELEGRAM TRỰC TIẾP TRÊN WEB UI
**Dự án**: Hosting Indicator Engine (`vunguyen109/hosting-Indicator`)  
**Tác giả**: CTO AI Assistant  
**Trạng thái**: Chờ phê duyệt (Pending Approval)  

---

## 1. Mục tiêu & Bài toán thực tế

Trên giao diện Botkeep (và nhiều nền tảng container khác), người dùng không thể nhập biến môi trường ở bước tạo server ban đầu, hoặc việc cấu hình biến môi trường trên panel đòi hỏi phải restart lại container.

**Giải pháp**: Tích hợp một bảng điều khiển **"⚙️ System Configuration"** trực tiếp trên Web Dashboard UI:
- Người dùng chỉ cần mở Web Dashboard bằng trình duyệt (hoặc qua domain/IP Botkeep cấp).
- Nhập `TWELVE_API_KEY` (và token Telegram nếu muốn) trực tiếp vào form trên giao diện web.
- Bấm nút **"Save & Connect"**.
- Backend tự động áp dụng ngay trong runtime mà không cần restart server, nạp nến Vàng (XAU) & Dầu (WTI) ngay lập tức, đồng thời lưu cấu hình vào file để duy trì sau mỗi lần restart container!

---

## 2. Thiết kế chi tiết hệ thống

### 2.1. Phía Frontend (Web Dashboard UI - `HTML_DASHBOARD`)
1. **Thêm Card "⚙️ API & Notifications Settings"**:
   - Trường nhập `TwelveData API Key` (có nút ẩn/hiện key).
   - Badge trạng thái kết nối realtime:
     - 🟡 `NOT CONFIGURED (Running BTC only)` nếu chưa có key.
     - 🟢 `CONNECTED (Tracking XAU & OIL)` khi đã có key.
   - Trường nhập `Telegram Bot Token` & `Chat ID` (Tùy chọn).
   - Nút hành động: **"Save & Apply"** với animation loading và Toast thông báo kết quả.
2. **Cập nhật JS Fetcher**:
   - Gọi API `POST /api/config` gửi dữ liệu cấu hình.
   - Cập nhật trạng thái badge theo dữ liệu trả về từ `/api/snapshot`.

### 2.2. Phía Backend (`engine.py`)
1. **Endpoint mới: `POST /api/config`**:
   - Tiếp nhận JSON payload: `{ "twelvedata_api_key": "...", "telegram_bot_token": "...", "telegram_chat_id": "..." }`.
   - Cập nhật trực tiếp vào đối tượng `TwelveDataFetcher`:
     - Gán API Key mới.
     - Đặt `is_configured = True`.
     - Kích hoạt tác vụ nền nạp nến lịch sử cho XAU và OIL (`_reload_assets(["XAU", "OIL"])`).
   - Cập nhật `TelegramDispatcher` nếu có token mới.
   - **Ghi bền vững (Persistence)**: Tự động ghi/cập nhật vào file `.env` trên đĩa để khi container khởi động lại trong tương lai, cấu hình vẫn được giữ nguyên 100%!
2. **Endpoint `GET /api/snapshot`**:
   - Bổ sung trường `twelvedata_configured: bool` và `twelvedata_key_masked: str` để frontend hiển thị đúng trạng thái.

---

## 3. Các bước thực hiện

1. **Cập nhật [`engine.py`](file:///d:/project/hosting-Indicator/engine.py)**:
   - Thêm hàm `_save_env_file(key, value)`.
   - Thêm method `_reload_assets()` trong `IndicatorOrchestrator` để nạp nến ngay khi có key mới.
   - Thêm route handler `handle_save_config` cho `POST /api/config` trong `DashboardServer`.
   - Cập nhật `HTML_DASHBOARD` với giao diện Settings Card & form xử lý AJAX.
2. **Cập nhật [`test_engine.py`](file:///d:/project/hosting-Indicator/test_engine.py)**:
   - Viết test case kiểm tra endpoint `POST /api/config` và kiểm tra nạp key động trong runtime.
3. **Kiểm thử tự động & Đẩy code lên GitHub**:
   - Chạy toàn bộ test suite.
   - Commit và push lên nhánh `main` để Botkeep tự động kéo bản mới nhất.
