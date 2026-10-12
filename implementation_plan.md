# KẾ HOẠCH DỌN DẸP CÁC XỬ LÝ & FILE CŨ KHÔNG CÒN SỬ DỤNG
**Dự án**: Hosting Indicator Engine (`vunguyen109/hosting-Indicator`)  
**Tác giả**: CTO AI Assistant  
**Trạng thái**: Chờ phê duyệt (Pending Approval)  

---

## 1. Rà soát hiện trạng (Audit Findings)

Dự án hiện tại đã chuyển đổi hoàn toàn sang kiến trúc **Container PaaS / Botkeep-first** với Web Dashboard tích hợp và cơ chế Zero-ENV / UI Config. Tuy nhiên, trong repository vẫn còn tồn đọng 4 file di tích cũ từ mô hình triển khai trên VPS Linux truyền thống (`/opt/indicator`):

| File cần dọn dẹp | Mục đích cũ | Lý do không còn dùng đến trên Botkeep |
|---|---|---|
| [`setup.sh`](file:///d:/project/hosting-Indicator/setup.sh) | Script cài đặt root user, venv, systemd trên Ubuntu VPS | Botkeep tự động chạy container độc lập, không dùng root setup script này |
| [`indicator-engine.service`](file:///d:/project/hosting-Indicator/indicator-engine.service) | File unit systemd để quản lý daemon trên VPS vật lý | Botkeep/Docker tự quản lý vòng đời tiến trình (Process Lifecycle), không có systemd |
| [`logrotate.conf`](file:///d:/project/hosting-Indicator/logrotate.conf) | Cấu hình xoay vòng log của hệ điều hành Linux | Trong [`engine.py`](file:///d:/project/hosting-Indicator/engine.py) đã có sẵn `RotatingFileHandler` (max 15MB) tự động, không cần OS logrotate |
| [`crontab.example`](file:///d:/project/hosting-Indicator/crontab.example) | Cron job dọn dẹp file tạm và kiểm tra tiến trình định kỳ | Engine tự dọn và giám sát bằng `asyncio` 24/7 bên trong tiến trình |

---

## 2. Kế hoạch hành động chi tiết

### Bước 1: Xóa các file thừa khỏi Git và Workspace
- Xóa 4 file di tích cũ:
  - `setup.sh`
  - `indicator-engine.service`
  - `logrotate.conf`
  - `crontab.example`
- Lệnh thực hiện: `git rm setup.sh indicator-engine.service logrotate.conf crontab.example`

### Bước 2: Rà soát và chuẩn hóa mã nguồn [`engine.py`](file:///d:/project/hosting-Indicator/engine.py)
- Đảm bảo không còn bất kỳ import thừa hoặc đường dẫn tuyệt đối nào liên quan đến `/opt/indicator`.
- Giữ nguyên các hàm cốt lõi và các test cases.

### Bước 3: Kiểm thử tự động (Unit Test Verification)
- Chạy toàn bộ test suite [`test_engine.py`](file:///d:/project/hosting-Indicator/test_engine.py) để khẳng định 100% chức năng không bị ảnh hưởng sau khi dọn dẹp.

### Bước 4: Commit và Push lên GitHub
- Tạo commit dọn dẹp: `chore: remove legacy VPS scripts (systemd, setup.sh, logrotate, crontab)`
- Đẩy lên nhánh `main` để đồng bộ hoàn toàn với GitHub.

---

## 3. Cấu trúc thư mục tinh gọn sau dọn dẹp
```
hosting-Indicator/
├── engine.py              ← Core Quant Engine + Web Dashboard + UI Config
├── requirements.txt       ← Dependency siêu nhẹ (aiohttp==3.9.5)
├── test_engine.py         ← Bộ kiểm thử tự động
├── .env.example           ← Biến môi trường mẫu
├── .gitignore             ← Chặn các file rác/cache
├── implementation_plan.md ← Tài liệu kế hoạch phát triển
└── README.md              ← Hướng dẫn triển khai chuẩn cho Botkeep
```
