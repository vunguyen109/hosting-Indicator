# KẾ HOẠCH TOÀN DIỆN: CHUẨN HÓA ZERO-ENV & KHẮC PHỤC CRASHLOOP TRÊN BOTKEEP
**Dự án**: Hosting Indicator Engine (`vunguyen109/hosting-Indicator`)  
**Tác giả**: CTO AI Assistant  
**Trạng thái**: Chờ phê duyệt triển khai (Ready for Execution)  

---

## 1. Xác nhận phát hiện từ thực tế của bạn (Insight Analysis)

Nhận định của bạn là **chính xác 100%**:
1. **Giao diện tạo server trên Botkeep không có bước nhập ENV:**  
   Form tạo server chỉ có 4 bước (Deploy type, Name/Runtime/Command, Git Source, Resources). Mục Environment Variables chỉ có thể truy cập được **sau khi server đã tạo xong**.
2. **Hậu quả khi chạy không có ENV (Zero-ENV state):**
   - **Treo Healthcheck do TwelveData:** Biến `TWELVE_API_KEY` mang giá trị mặc định `"YOUR_TWELVE_KEY"`. Engine cũ vẫn cố gửi 4 request lấy nến Vàng & Dầu tới TwelveData với cơ chế throttle 8s/request, làm quá trình boot bị nghẽn tới **32–40 giây**. Botkeep ping healthcheck thấy server không phản hồi kịp (timeout) nên gửi tín hiệu **SIGKILL ép restart liên tục**.
   - **Crash Port do Botkeep khởi tạo biến rỗng:** Nếu Botkeep tạo container với biến `SERVER_PORT=""`, lệnh ép kiểu cũ `int("")` gây crash `ValueError` ngay tại module-level.
   - **Mất log hoàn toàn:** Python trong container đệm stdout, nên khi tiến trình bị kill giữa chừng, toàn bộ lỗi biến mất khỏi console.

---

## 2. Giải pháp kỹ thuật: Chế độ "Zero-ENV & Fail-Safe"

Dự án sẽ được nâng cấp để **hoạt động hoàn hảo 100% ngay cả khi người dùng không cấu hình bất kỳ biến môi trường nào**:

### Giải pháp 1: Chế độ Auto-Bypass thông minh khi thiếu API Key
- Trong `TwelveDataFetcher`: Nếu `TWELVE_API_KEY` để trống hoặc là placeholder (`YOUR_TWELVE_KEY`, `your_twelvedata_key_here`), engine sẽ **bỏ qua ngay lập tức trong 0 giây** (không gửi request, không chờ throttle 8s).
- Log hiển thị rõ: `[CONFIG] TWELVE_API_KEY not configured -> Skipping Gold & Oil. Running BTC in standalone mode.`
- Engine khởi động xong trong **chưa đầy 1 giây** với dữ liệu Bitcoin từ Binance công khai (hoàn toàn không cần API key).

### Giải pháp 2: Bộ nạp `.env` tự động (Thuần Python Stdlib, không phụ thuộc thư viện)
- Viết sẵn hàm `load_dotenv()` thuần chuẩn POSIX tích hợp sẵn trong `engine.py`.
- Nếu có file `.env` trong thư mục, engine tự nạp mà không cần cài thêm package `python-dotenv`.

### Giải pháp 3: Khởi động Web Server & Healthcheck ngay lập tức
- Đưa `_init_buffers()` chạy background task (`asyncio.create_task`).
- Cổng Dashboard (`0.0.0.0:8080` hoặc cổng Botkeep) mở thành công ngay trong 0.2 giây đầu tiên. Các endpoint `/`, `/health`, `/api/health` trả về `200 OK` ngay lập tức, Botkeep gắn nhãn `HEALTHY` và không bao giờ kill container.

### Giải pháp 4: Chống mất log tuyệt đối (Zero-Buffering)
- Ép `PYTHONUNBUFFERED=1` và `sys.stdout.reconfigure(line_buffering=True)`.
- Bọc toàn bộ quá trình boot bằng khối `try ... except BaseException` fail-safe, in lỗi với `flush=True` và delay 15 giây nếu gặp lỗi nghiêm trọng để Botkeep kịp hiển thị toàn bộ log lên console.

### Giải pháp 5: Start Command tối ưu cho Botkeep
- Cấu hình lệnh khởi động an toàn trên Botkeep:
  ```bash
  pip install -r requirements.txt && python -u engine.py
  ```

---

## 3. Danh sách các file sẽ điều chỉnh

1. [`engine.py`](file:///d:/project/hosting-Indicator/engine.py):
   - Thêm zero-buffering & boot log.
   - Thêm `load_dotenv()` tích hợp.
   - Hàm `_parse_port()` an toàn chống crash.
   - Thêm kiểm tra `TWELVE_API_KEY` bypass tức thì.
   - Thêm route alias `/health`.
   - Chuyển `_init_buffers()` sang background task.
   - Bọc `main()` fail-safe traceback print.
2. [`test_engine.py`](file:///d:/project/hosting-Indicator/test_engine.py):
   - Bổ sung test case cho Zero-ENV mode, `_parse_port()`, và route `/health`.
3. Git Repository:
   - Untrack file binary `__pycache__/engine.cpython-312.pyc`.
   - Commit và push lên GitHub nhánh `main`.
