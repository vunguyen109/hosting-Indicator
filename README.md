# Hosting Indicator Engine

**Low-Resource Quant Signal Service & Web Dashboard** — Python 3.11 | asyncio | aiohttp  
Tài sản: BTC/USDT · XAU/USD (Gold) · Dầu thô (USO ETF Proxy) | Khung thời gian: M15 · H1  
Tín hiệu: EMA Cross · RSI OB/OS · Bollinger Bands · ATR Volatility Spike  
Giao diện: **Web UI Dashboard (Dark Mode)** · Live Console · Telegram Bot *(Tùy chọn)*

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Yêu cầu tối thiểu](#2-yêu-cầu-tối-thiểu)
3. [Giao diện Web UI & Chế độ không cần Telegram](#3-giao-diện-web-ui--chế-độ-không-cần-telegram)
4. [Chuẩn bị Repository trước khi deploy](#4-chuẩn-bị-repository-trước-khi-deploy)
5. [Deploy lên Botkeep qua GitHub](#5-deploy-lên-botkeep-qua-github)
6. [Cấu hình Secrets & Environment Variables](#6-cấu-hình-secrets--environment-variables)
7. [Lệnh khởi động (Start Command)](#7-lệnh-khởi-động-start-command)
8. [Cấu hình Web Domain & Giám sát trên Botkeep](#8-cấu-hình-web-domain--giám-sát-trên-botkeep)
9. [Cập nhật code (Deploy revision mới)](#9-cập-nhật-code-deploy-revision-mới)
10. [Xử lý sự cố thường gặp](#10-xử-lý-sự-cố-thường-gặp)
11. [Giới hạn tài nguyên & Hiệu năng](#11-giới-hạn-tài-nguyên--hiệu-năng)
12. [Bảng tham chiếu tín hiệu (Signals Reference)](#12-bảng-tham-chiếu-tín-hiệu-signals-reference)

---

## 1. Tổng quan kiến trúc

```
Data Sources          RAM (Circular Buffers)        Processing Engine           Output Destinations
Binance REST  ──────► BTC M15 (300 nến) ──┐                                  ┌─► Web Dashboard (HTTP :8080/SERVER_PORT)
                      BTC  H1 (300 nến) ──┤                                  │   (Bảng chỉ báo realtime + 50 signals)
TwelveData    ──────► XAU M15 (300 nến) ──┼─► IndicatorEngine (RSI/EMA/BB/ATR)├─► REST API (/api/snapshot)
                      XAU  H1 (300 nến) ──┤        │                         ├─► Live Console Log (Stdout)
                      OIL M15 (300 nến) ──┤        ▼                         │
                      OIL  H1 (300 nến) ──┘   RuleEngine (Phát tín hiệu) ────┴─► Telegram Bot (Tùy chọn)
                                                   │
                                                   └─► state.json (Lưu mỗi 15 phút vào ./data/)
```

| Tài nguyên | Sử dụng thực tế | Giới hạn Botkeep (Free Plan) | Tỷ lệ sử dụng |
|---|---|---|---|
| **RAM** | **~115 MB** | 2 GB | ~5.8% |
| **Disk** | **~18 MB** | 2 GB | ~0.9% |
| **CPU** | **~5 - 10% avg** | 1.5 vCore | Rất nhẹ |
| **Web Server** | Nhúng `aiohttp.web` | Tích hợp sẵn trong runtime | **0 dependency mới** |

---

## 2. Yêu cầu tối thiểu

Trước khi deploy, bạn cần chuẩn bị:

- [x] **Tài khoản Botkeep** tại [botkeep.cloud/app](https://botkeep.cloud/app)
- [x] **Tài khoản GitHub** đã fork hoặc push repository này lên branch `main`
- [x] **TwelveData API Key** *(Bắt buộc cho Vàng & Dầu)*: Đăng ký miễn phí tại [twelvedata.com](https://twelvedata.com) (800 req/ngày miễn phí).
- [ ] **Telegram Bot Token & Chat ID** *(TÙY CHỌN)*: Nếu chưa tạo bot Telegram, engine vẫn chạy bình thường 100% và hiển thị đầy đủ thông tin trên **Web UI** và **Console log**.

---

## 3. Giao diện Web UI & Chế độ không cần Telegram

### 3.1. Chưa có Telegram Bot thì xem thông tin ở đâu?
Hệ thống được tích hợp sẵn **Single-page Web Dashboard** và **REST API** bằng `aiohttp.web`:
1. **Bảng chỉ báo Realtime (Live Indicators Table):** Cập nhật giá đóng cửa, RSI (14), Trend EMA (20/50), Bollinger Bands (Lower / Mid / Upper), ATR (14) của cả 3 tài sản (BTC, Vàng XAU, Dầu thô USO) trên khung 15m và 1h.
2. **Dòng tín hiệu trực tiếp (Live Signals Feed):** Lưu bộ đệm 50 tín hiệu gần nhất trong RAM, phân loại rõ ràng:
   - `BULLISH ▲ / BEARISH ▼`
   - `RSI OVERBOUGHT / RSI OVERSOLD`
   - `BB BREAKOUT UP / DOWN`
   - `ATR VOLATILITY SPIKE`
3. **Cơ chế tự động làm mới (Auto-refresh):** Giao diện tự động fetch API `/api/snapshot` mỗi 3 giây.
4. **Console Output:** Khi chưa cấu hình Telegram, tín hiệu sẽ tự động in ra màn hình console dạng `>> [SIGNAL DETECTED]` thay vì báo lỗi.

---

## 4. Chuẩn bị Repository trước khi deploy

### 4.1. Cấu trúc thư mục chuẩn
```
hosting-Indicator/
├── engine.py          ← Core engine + Web Dashboard tích hợp
├── requirements.txt   ← Chỉ duy nhất: aiohttp==3.9.5
├── test_engine.py     ← Bộ kiểm thử tự động
├── .env.example       ← Mẫu biến môi trường
└── README.md          ← Tài liệu hướng dẫn
```

### 4.2. File `requirements.txt`
```txt
aiohttp==3.9.5
```
*(Chỉ cần duy nhất `aiohttp`, dashboard dùng `aiohttp.web` có sẵn, không cần cài Flask/FastAPI hay React).*

### 4.3. Đảm bảo `.env` KHÔNG commit vào GitHub
File `.gitignore` đã chặn các file nhạy cảm:
```gitignore
.env
*.env
data/
logs/
__pycache__/
*.pyc
```

---

## 5. Deploy lên Botkeep qua GitHub

### Bước 1 — Đăng nhập Botkeep
Truy cập [botkeep.cloud/app](https://botkeep.cloud/app) và đăng nhập bằng tài khoản GitHub hoặc Discord.

### Bước 2 — Tạo Server mới
1. Click **"New Server"** (hoặc **"Create"**) trên dashboard.
2. Chọn loại: **Application** (Python app).
3. Chọn nguồn triển khai: **GitHub**.

### Bước 3 — Kết nối Repository
1. Click **"Connect GitHub"** → cấp quyền cho Botkeep truy cập repo của bạn.
2. Chọn repo `vunguyen109/hosting-Indicator` (hoặc tên repo bạn fork về).
3. Cấu hình thông số:
   - **Branch:** `main`
   - **Project root:** `/` (thư mục gốc)
   - **Runtime:** Python 3.11 hoặc 3.12 (Botkeep tự nhận diện qua `requirements.txt`)

---

## 6. Cấu hình Secrets & Environment Variables

Trong giao diện quản lý server trên Botkeep, vào mục **Settings → Environment** (hoặc **Secrets**) để thêm các biến:

| Biến môi trường | Bắt buộc? | Giá trị mẫu | Mô tả |
|---|---|---|---|
| `TWELVE_API_KEY` | **Bắt buộc** | `abc123xyz...` | Lấy miễn phí tại [twelvedata.com](https://twelvedata.com) |
| `SERVER_PORT` | Tự động | *Botkeep tự cấp* | Botkeep tự gán port; engine tự động nhận diện |
| `TG_BOT_TOKEN` | *Tùy chọn* | `123456:AAxxxx...` | Để trống nếu chưa có bot Telegram |
| `TG_CHAT_ID` | *Tùy chọn* | `-100123456789` | Để trống nếu chưa có bot Telegram |

> 💡 **Mẹo:** Nếu bạn để trống `TG_BOT_TOKEN`, engine tự động chuyển sang chế độ **Web UI & Console Mode**, không phát sinh bất kỳ lỗi kết nối nào!

---

## 7. Lệnh khởi động (Start Command)

Trong ô **Start Command** trên Botkeep, nhập lệnh khuyến nghị để đảm bảo tự động cài thư viện và hiển thị log trực tiếp (unbuffered):

```bash
pip install -r requirements.txt && python -u engine.py
```

*(Hoặc nếu Botkeep đã có sẵn aiohttp: `python -u engine.py`)*

> 💡 **Cờ `-u` (Unbuffered):** Cực kỳ quan trọng trên container PaaS/Botkeep, giúp toàn bộ log và traceback được đẩy trực tiếp ra màn hình console ngay lập tức, không bị giữ lại trong bộ đệm.
> 💡 **Chế độ Zero-ENV:** Khi vừa tạo server trên Botkeep, bạn chưa thể nhập ENV ngay. Engine đã tích hợp sẵn chế độ an toàn: tự động chạy BTC (Binance không cần key), bỏ qua nến Vàng/Dầu trong 0 giây, và mở Dashboard ngay lập tức để vượt qua Health Check của Botkeep mà không bao giờ bị restart liên tục! Sau khi server chạy ổn định, bạn có thể vào mục Settings để thêm `TWELVE_API_KEY` nếu muốn kích hoạt thêm Vàng & Dầu.

### Chạy thử nghiệm trên máy tính cá nhân (Local Test)
```bash
# 1. Cài đặt thư viện
pip install aiohttp==3.9.5

# 2. Chạy engine (không cần token Telegram)
python engine.py

# 3. Mở trình duyệt xem Dashboard:
# http://localhost:8080
```

Log khởi động thành công:
```
============================================================
  HOSTING INDICATOR ENGINE v1.0  (low-resource edition)
  Python 3.12.x
  Dashboard available at http://0.0.0.0:8080
============================================================
2026-10-11T23:30:00 [INFO    ] indicator: Telegram bot not configured -> Running in Web UI & Console mode.
2026-10-11T23:30:00 [INFO    ] indicator: Web Dashboard listening on http://0.0.0.0:8080
2026-10-11T23:30:00 [INFO    ] indicator: Pre-filling circular buffers with historical data...
2026-10-11T23:30:02 [INFO    ] indicator:   [BTC 15m] loaded 300 candles
2026-10-11T23:30:04 [INFO    ] indicator:   [BTC 1h] loaded 300 candles
2026-10-11T23:30:06 [INFO    ] indicator:   [XAU 15min] loaded 300 candles
2026-10-11T23:30:08 [INFO    ] indicator:   [OIL 15min] loaded 300 candles
2026-10-11T23:30:10 [INFO    ] indicator: All 8 tasks started -- engine running 24/7
```

---

## 8. Cấu hình Web Domain & Giám sát trên Botkeep

### 8.1. Mở Web UI qua Internet (Public Access)
Theo tài liệu chuẩn của Botkeep ([botkeep.cloud/docs/hosting](https://botkeep.cloud/docs/hosting)):
1. Vào tab **Network** của server: Xem port được hệ thống gán.
2. Vào tab **Domains**:
   - Thêm một domain (hoặc subdomain miễn phí do Botkeep cung cấp dạng `ten-app.botkeep.cloud`).
   - Trỏ domain vào port của service.
3. **Mở trình duyệt:** Truy cập `https://ten-app.botkeep.cloud` để xem Dashboard realtime từ bất kỳ đâu (điện thoại, máy tính bảng, PC).

### 8.2. Xem qua Tab Console (Nếu không cấu hình Domain)
Nếu bạn không muốn public domain ra ngoài internet:
- Vào tab **Console / Output** trên Botkeep.
- Toàn bộ giá đóng cửa, RSI, ATR và tín hiệu `>> [SIGNAL DETECTED]` sẽ được in rõ ràng theo từng chu kỳ nến (15 phút, 1 giờ).

---

## 9. Cập nhật code (Deploy revision mới)

Khi bạn chỉnh sửa code hoặc logic chỉ báo:
```bash
git add engine.py README.md
git commit -m "feat: enhance web dashboard indicators"
git push origin main
```

Trên Botkeep dashboard:
1. Vào tab **GitHub** → Click **"Check for updates"**.
2. Click **"Apply update"** → Chọn **"Restart"**.
3. Service sẽ khởi động lại với code mới nhất trong vòng 10 giây.

---

## 10. Xử lý sự cố thường gặp

### ❌ Web Dashboard không truy cập được trên Botkeep
- **Kiểm tra Host binding:** Code đã cấu hình mặc định bind vào `0.0.0.0` và tự đọc `SERVER_PORT`.
- **Kiểm tra Domains:** Đảm bảo bạn đã cấu hình mục **Domains** trên Botkeep trỏ vào đúng port tại tab **Network**.

### ❌ Lỗi TwelveData `status: error`
- Kiểm tra đã nhập biến `TWELVE_API_KEY` trong mục Environment chưa.
- Gói Free của TwelveData giới hạn 8 req/phút: engine đã tích hợp sẵn throttle 8 giây giữa các request để đảm bảo không bao giờ bị rate-limit.

### ❌ File state và log báo lỗi Permission
- Code đã chuyển sang dùng đường dẫn tương đối (`./data/state.json` và `./logs/app.log`), tự động tạo thư mục con mà không cần quyền root `/opt`.

---

## 11. Giới hạn tài nguyên & Hiệu năng

| Thành phần | Tiêu thụ RAM | Tiêu thụ Disk |
|---|---|---|
| Python Runtime + aiohttp + Web Server | ~25 MB | ~5 MB |
| 6 Circular Buffers (300 nến x 6) | ~0.5 MB | 0 MB |
| Bộ đệm 50 tín hiệu gần nhất | ~0.1 MB | 0 MB |
| File state (`./data/state.json`) | — | < 1.5 MB |
| File logs xoay vòng (Rotating 5MB x 2) | — | < 15 MB |
| Vùng đệm an toàn (Safety Buffer) | ~85 MB | — |
| **Tổng cộng** | **~115 MB / 2 GB** | **~21.5 MB / 2 GB** |

=> **Chiếm chưa tới 6% tài nguyên của gói Botkeep Free**.

---

## 12. Bảng tham chiếu tín hiệu (Signals Reference)

| Mã Tín Hiệu | Điều kiện kích hoạt | Phân loại |
|---|---|---|
| `EMA_CROSS_UP` | EMA 20 cắt lên trên EMA 50 | **Golden Cross (Tăng giá)** |
| `EMA_CROSS_DOWN` | EMA 20 cắt xuống dưới EMA 50 | **Death Cross (Giảm giá)** |
| `RSI_OVERSOLD` | RSI(14) giảm xuống dưới 30 | **Quá bán (Cơ hội Mua)** |
| `RSI_OVERBOUGHT` | RSI(14) tăng vượt 70 | **Quá mua (Cơ hội Chốt lời)** |
| `BB_BREAKOUT_UP` | Giá đóng cửa vượt dải trên Bollinger Band | **Biến động bứt phá tăng** |
| `BB_BREAKOUT_DOWN` | Giá đóng cửa thủng dải dưới Bollinger Band | **Biến động bứt phá giảm** |
| `ATR_SPIKE` | ATR hiện tại tăng đột biến > 1.5 lần ATR trước | **Cảnh báo biến động mạnh** |

---

*Hỗ trợ & Tài liệu Botkeep:* [botkeep.cloud/docs](https://botkeep.cloud/docs)  
*Repository GitHub:* [github.com/vunguyen109/hosting-Indicator](https://github.com/vunguyen109/hosting-Indicator)