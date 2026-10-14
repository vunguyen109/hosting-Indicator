# Hosting Indicator Engine — Kiến Trúc & Tổng Quan Hệ Thống

> **Dành cho:** AI Models, Developers & Kỹ sư triển khai tiếp quản dự án.  
> **Repository:** `vunguyen109/hosting-Indicator`  
> **Phiên bản:** `v1.0` (Production Ready trên Botkeep PaaS)  
> **Cập nhật lần cuối:** 2026-10-14  

---

## 1. Mục Tiêu & Triết Lý Thiết Kế (Design Philosophy)

Dự án **Hosting Indicator Engine** là một dịch vụ phát tín hiệu định lượng (Quant Signal Service) kết hợp Web Dashboard giám sát tài chính thời gian thực. Hệ thống được tối ưu hóa đặc biệt để chạy 24/7 trên môi trường hosting tài nguyên cực thấp (Cloud PaaS / Container Free Tier: **1.5 vCore, 2GB RAM, 2GB Disk**).

### Ràng buộc kỹ thuật cốt lõi (Core Constraints):

1. **Zero Heavy Dependencies:**
   - Phụ thuộc duy nhất gói ngoài: `aiohttp==3.9.5` (trong [requirements.txt](file:///d:/vunq/hosting-Indicator/requirements.txt)).
   - Toàn bộ backend được viết bằng Python Standard Library (`asyncio`, `math`, `collections.deque`, `dataclasses`, `logging`, `json`, `tempfile`).
   - **Tuyệt đối không dùng `pandas` hay `numpy`** để tiết kiệm ~80MB baseline RAM.
2. **Quản lý bộ nhớ giới hạn nghiêm ngặt (Bounded Memory Strategy):**
   - Bộ đệm nến sử dụng `collections.deque(maxlen=300)` cố định.
   - 3 tài sản x 2 khung thời gian = 6 buffers, mỗi buffer 300 nến ~24 KB => Toàn bộ dữ liệu RAM chỉ tốn ~150 KB.
   - Mức tiêu thụ thực tế khi chạy 24/7: **~112 - 115 MB RAM** (<6% giới hạn 2GB của server) và **5 - 10% CPU**.
3. **Frontend Zero-Build & Client-side Processing:**
   - Không dùng framework nặng (React, Vue), không build tool (Webpack, Vite, npm).
   - Single Page App (SPA) dark mode với HTML/CSS/JS thuần, nhúng thư viện đồ thị chuyên dụng **TradingView Lightweight Charts v4.x** (~45KB gzipped) trực tiếp qua CDN.
   - Tất cả tác vụ dịch thuật ngôn ngữ tự nhiên, tính thước đo tâm lý tổng hợp (Composite Gauge 0-100), giải nghĩa câu chuyện tín hiệu (Story feed) diễn ra **100% tại Client (Browser)**, đảm bảo **0% CPU overhead** cho server.
4. **Khả năng tự phục hồi & Chế độ Zero-ENV (Fail-safe Boot):**
   - Khởi động an toàn ngay cả khi chưa cấu hình API Key hay Telegram Token.
   - Tự động kích hoạt luồng BTC (Binance Public API không cần key), bỏ qua nến Vàng/Dầu trong 0s và mở ngay Web Server để vượt qua cơ chế Health Check của PaaS mà không bị restart loop.
   - Cho phép nạp nóng `TWELVE_API_KEY` và `TG_BOT_TOKEN` ngay trên giao diện Web UI (lưu tự động vào `.env` và kích hoạt ngay không cần khởi động lại tiến trình).

---

## 2. Cấu Trúc Thư Mục & Vai Trò Các Tệp Tin

```
hosting-Indicator/
├── engine.py              # Core Engine nguyên khối: Data Fetcher, Math, Rules, API, Web Server (2,439 dòng)
├── dashboard.html         # Giao diện Web Dashboard độc lập (được server ưu tiên nạp nếu tồn tại)
├── requirements.txt       # Danh sách thư viện phụ thuộc (duy nhất aiohttp==3.9.5)
├── test_engine.py         # Bộ test suite tự động kiểm thử toàn bộ module lõi
├── implementation_plan.md # Kế hoạch triển khai đợt nâng cấp UX/Dashboard gần nhất
├── PROJECT_ARCHITECTURE.md# Tài liệu này (Tổng hợp kiến trúc phục vụ bàn giao và cập nhật)
├── README.md              # Tài liệu vận hành, cấu hình biến môi trường và deploy lên Botkeep
└── .env.example           # File mẫu biến môi trường
```

---

## 3. Kiến Trúc Luồng Dữ Liệu (Data Flow Diagram)

```
 [Nguồn Dữ Liệu]                     [Vùng Nhớ RAM]                      [Bộ Xử Lý & Đánh Giá]                  [Đầu Ra (Outputs)]
┌──────────────────┐               ┌───────────────────────┐           ┌──────────────────────┐               ┌─────────────────────────────────────┐
│  Binance REST    │──(BTC M15/H1)─►   CircularBuffer      │           │   IndicatorEngine    │               │  Embedded aiohttp.web Dashboard     │
│  (No API Key)    │               │   (deque maxlen=300)  ├───────────►   (Pure-math Python) │               │  - Web UI (TradingView Chart Canvas)│
└──────────────────┘               └───────────┬───────────┘           └──────────┬───────────┘               │  - REST API (/api/snapshot, candles)│
                                               │                                  │                           └──────────────────▲──────────────────┘
┌──────────────────┐                           │                                  ▼                                              │
│  TwelveData REST │──(XAU/OIL)────┐           │                       ┌──────────────────────┐                                  │
│  (8s Throttle)   │               ▼           │                       │     RuleEngine       │──────────────────────────────────┘
└──────────────────┘          [Deduplication]  │                       │ (Phát hiện tín hiệu) │
                                               ▼                       └──────────┬───────────┘
                                   ┌───────────────────────┐                      │                   ┌─────────────────────────────────────┐
                                   │   StatePersistence    │                      ├──────────────────►│  TelegramDispatcher (Async Retry x3) │
                                   │ (Atomic JSON flush)   │                      │                   └─────────────────────────────────────┘
                                   │ ./data/state.json     │                      │                   ┌─────────────────────────────────────┐
                                   └───────────────────────┘                      └──────────────────►│  Stdout / logs/app.log (Rotating)    │
                                                                                                      └─────────────────────────────────────┘
```

---

## 4. Chi Tiết Các Phân Vùng Mã Nguồn trong `engine.py`

| Phân vùng | Lớp / Hàm chính | Chức năng chi tiết |
|---|---|---|
| **Section 1: Configuration** | `CONFIG`, `_parse_port`, `_load_dotenv`, `_update_dotenv` | Quản lý cấu hình tập trung. Xử lý an toàn port từ ENV (`SERVER_PORT` hoặc `PORT`), hỗ trợ cập nhật biến môi trường trực tiếp từ runtime vào file `.env`. |
| **Section 2: Buffer** | `Candle`, `CircularBuffer` | Mô hình dữ liệu nến OHLCV (`~80B/candle`). Cung cấp `push()` O(1), `last_n_candles(limit)` phục vụ API vẽ đồ thị. |
| **Section 3: Indicators** | `IndicatorEngine` | Pure-math không dùng numpy: <br>• **EMA**: Hệ số $k = 2 / (period + 1)$<br>• **RSI**: Chu kỳ 14 theo thuật toán làm mượt Wilder<br>• **Bollinger Bands**: Chu kỳ 20, độ lệch chuẩn 2.0<br>• **ATR**: Chu kỳ 14 theo thuật toán Wilder. |
| **Section 4: Rules** | `RuleEngine`, `Signal` | So sánh snapshot hiện tại với snapshot trước đó:<br>• **Golden/Death Cross**: EMA20 cắt lên/cắt xuống EMA50<br>• **RSI Overbought/Oversold**: Vượt ngưỡng 70 / Thủng ngưỡng 30<br>• **Bollinger Breakout**: Giá đóng cửa vượt Upper Band hoặc thủng Lower Band<br>• **ATR Volatility Spike**: ATR hiện tại > 1.5x ATR trước đó. |
| **Section 4.5: Confluence Engine** | `ConfluenceDecisionEngine`, `TradeAdvice` | Phân tích hợp lưu đa khung thời gian (EMA Trend, RSI Zone, Vị trí BB, Tín hiệu mâu thuẫn gần đây, MTF Agreement) để sinh khuyến nghị BUY/SELL/WAIT kèm điểm Entry, Stop Loss, Take Profit 1 & 2.<br>**Phạm vi:** Chỉ phục vụ hiển thị UI qua `/api/snapshot`, **KHÔNG tích hợp với TelegramDispatcher**. |
| **Section 5: Telegram** | `TelegramDispatcher` | Gửi tin nhắn bất đồng bộ qua Telegram Bot API, tự retry tối đa 3 lần với exponential backoff (1s, 2s, 4s). Tự động chuyển chế độ Console Mode nếu không có token. |
| **Section 6: Fetchers** | `BinanceFetcher`, `TwelveDataFetcher` | • Binance: Lấy klines công khai, tự động loại bỏ cây nến chưa đóng cuối cùng.<br>• TwelveData: Lấy nến Vàng & Dầu, tích hợp cơ chế throttle tối thiểu **8.0 giây giữa 2 request** để tránh vượt giới hạn 8 req/min của gói Free. |
| **Section 7: Persistence** | `StatePersistence` | Ghi trạng thái định kỳ 15 phút một lần xuống `./data/state.json`. Dùng kỹ thuật ghi ra temp file rồi `os.replace` để đảm bảo tính nguyên tử (atomic) trên POSIX. Tự động trim dữ liệu nếu vượt ngưỡng trần 1.5MB. |
| **Section 8: Watchdog** | `DiskWatchdog` | Định kỳ mỗi 6 giờ kiểm tra dung lượng ổ cứng. Gửi cảnh báo Telegram nếu dung lượng sử dụng vượt quá 70%. |
| **Section 9: Scheduler** | `seconds_until_next_close` | Tính toán chính xác số giây cần `sleep` cho đến thời điểm đóng nến tiếp theo + 5 giây bù trễ mạng của sàn giao dịch. |
| **Section 10: Web Server** | `DashboardServer`, `HTML_DASHBOARD` | Nhúng server HTTP bằng `aiohttp.web`, tiêu thụ < 5MB RAM. Cung cấp API và Dashboard SPA. Ưu tiên nạp từ `dashboard.html` nếu file tồn tại. |
| **Section 11: Orchestrator** | `IndicatorOrchestrator` | Bộ điều phối chính: Khởi tạo HTTP session dùng chung, pre-fill lịch sử 300 nến vào RAM, quản lý 8 tasks chạy nền song song, hỗ trợ `reload_asset_buffers` khi người dùng nạp API key mới từ UI. |
| **Section 12: Logging** | `setup_logging`, `main` | Rotating log (5MB x 2 file backup = tối đa 15MB disk). Cấu hình unbuffered stdout để log hiển thị tức thì trên bảng điều khiển cloud container. |

### Section 4.5: Confluence Decision Engine
Phân tích hợp lưu đa khung thời gian để sinh khuyến nghị BUY/SELL/WAIT:
- **Heuristics:** Phân loại Trend EMA (20/50), RSI (14), Bollinger Bands, Tín hiệu mâu thuẫn (RuleEngine) và Biến động ngược chiều (Counter-Move).
- **Phạm vi:** Chỉ phục vụ hiển thị UI qua `/api/snapshot`, **KHÔNG tích hợp với TelegramDispatcher**.

### Changelog v1.1 (Patch sau kiểm thử thực tế)
- [Fixed] Reasoning text MTF agreement giờ mô tả ĐÚNG hướng so sánh thực tế (ngắn hạn/dài hạn) thay vì hard-code "khung lớn".
- [Fixed] Bổ sung `detect_counter_move()` để bắt các cú biến động giá mạnh dạng V-shape không trùng khớp với Signal rời rạc nào của RuleEngine — khắc phục điểm mù khiến hệ thống bỏ sót mâu thuẫn rõ ràng giữa EMA Trend và diễn biến giá tức thời.
- [UX] Thêm chú giải liên kết giữa Gauge (thang 0-100, tính client) và Confidence Score (thang 0-5, tính server) để tránh người dùng hiểu lầm đây là 2 kết luận trái ngược.
- Nguồn gốc patch: phát hiện qua live testing case thực tế XAU/USD khung 1h ngày quan sát, giá bật từ $4,148 lên $4,188 (~3.7x ATR) mà không được hệ thống gắn cờ cảnh báo.

### Changelog v1.2 (Chuẩn hóa TP/SL Multi-Tier theo Đòn Bẩy)
- [Fixed & Upgraded] **Chuẩn hóa công thức Take Profit / Stop Loss theo Đòn bẩy (Leverage):**
  - Khắc phục triệt để sự nhầm lẫn giữa `% biến động giá tài sản` (Price Move %) và `% lãi trên vốn ký quỹ` (Margin Profit %).
  - Công thức tính xuôi: `price_move_pct = margin_profit_pct / leverage` (với `DEFAULT_LEVERAGE = 5`: 5% margin -> 1.0% giá; 7.5% margin -> 1.5% giá; 10% margin -> 2.0% giá).
  - Áp dụng cấu trúc Multi-Tier TP cho swing mode (1h, 4h): TP1 (đóng 50%), TP2 (đóng 30%), TP3 (đóng 20%).
  - Stop Loss bảo toàn vốn cố định `1.5 * ATR`, bổ sung tính toán `% rủi ro trên vốn ký quỹ`: `sl_margin_loss_pct = (risk_distance / ref_price * 100) * leverage`.
  - Bổ sung ước lượng thời gian giữ lệnh (`estimate_holding_period`): dựa trên tốc độ di chuyển giá định lượng `0.46 * ATR / nến` (~8h cho TP1, ~12h cho TP2, ~16h cho TP3 trên XAU 1h).
  - Tự động đánh giá tính khả thi mục tiêu (`target_feasibility`): so sánh mục tiêu giá với ATR phiên.
  - UI Web Dashboard: Hiển thị song song cả 2 đơn vị ("% giá" và "% vốn ký quỹ"), có badge đòn bẩy và nút copy tiện dụng. Đồng bộ 100% giữa file tĩnh `dashboard.html` và chuỗi nhúng `HTML_DASHBOARD` trong `engine.py`.

---

## 5. Đặc Tả Giao Diện API (API Specifications)

Server lắng nghe mặc định tại `0.0.0.0:${SERVER_PORT:-8080}`:

### 1. `GET /`
- **Chức năng:** Trả về trang Web Dashboard Single Page App.
- **Thứ tự nạp:** Ưu tiên đọc nội dung file [dashboard.html](file:///d:/vunq/hosting-Indicator/dashboard.html). Nếu không tìm thấy, fallback sang biến `HTML_DASHBOARD` nhúng sẵn trong `engine.py`.

### 2. `GET /health` & `GET /api/health`
- **Chức năng:** Health check phục vụ PaaS Router / Load Balancer.
- **Response:**
  ```json
  { "status": "ok", "time": 1728892800 }
  ```

### 3. `GET /api/snapshot`
- **Chức năng:** Cung cấp toàn bộ chỉ báo, trạng thái hệ thống và 50 tín hiệu gần nhất. Frontend tự động polling mỗi **3 giây**.
- **Response:**
  ```json
  {
    "status": "online",
    "uptime_seconds": 3600,
    "uptime_formatted": "1:00:00",
    "server_time": "2026-10-14 04:00:00 UTC",
    "telegram_enabled": false,
    "twelvedata_configured": true,
    "indicators": {
      "BTC": {
        "15m": {
          "close": 68500.5,
          "ema_fast": 68420.0,
          "ema_slow": 68100.2,
          "rsi": 62.4,
          "bb_upper": 68900.0,
          "bb_mid": 68400.0,
          "bb_lower": 67900.0,
          "atr": 240.5
        }
      }
    },
    "signals": [
      {
        "asset": "BTC",
        "timeframe": "15m",
        "signal_type": "EMA_CROSS_UP",
        "message": "[GOLDEN CROSS] BTC 15m...",
        "price": 68500.5,
        "ts": 1728892800,
        "time_str": "04:00:00 UTC"
      }
    ],
    "advice": {
      "BTC": {
        "15m": {
          "action": "BUY",
          "confidence": "Trung bình",
          "confidence_score": 3,
          "entry_type": "wait_pullback",
          "entry_price": null,
          "entry_zone": 68420.0,
          "stop_loss": 68059.25,
          "take_profit_1": 68780.75,
          "take_profit_2": 69141.5,
          "tp2_rule": "Giữ lệnh đến khi Xu hướng EMA chuyển từ Tăng sang Cân bằng/Giảm",
          "reasoning": [
            "Xu hướng EMA: Tăng rõ rệt (ủng hộ MUA)",
            "RSI nghiêng mua, còn dư địa tăng",
            "Giá áp sát biên trên (ủng hộ xu hướng tăng)",
            "Không có tín hiệu mâu thuẫn gần đây",
            "khung 1 giờ (dài hạn hơn) cũng ĐỒNG THUẬN xu hướng"
          ],
          "conflict_detected": false,
          "mtf_agreement": true,
          "counter_move_detected": false,
          "counter_move_magnitude_atr": 0.0,
          "swing_mode": false,
          "tp_levels": null,
          "sl_margin_loss_pct": null,
          "target_feasibility": null,
          "estimated_holding_time": null,
          "disclaimer": "Đây là khuyến nghị dựa trên thuật toán kỹ thuật, KHÔNG PHẢI lời khuyên đầu tư..."
        }
      },
      "XAU": {
        "1h": {
          "action": "BUY",
          "confidence": "Cao",
          "confidence_score": 4,
          "entry_type": "now",
          "entry_price": 4159.3,
          "entry_zone": null,
          "stop_loss": 4142.54,
          "take_profit_1": 4200.89,
          "take_profit_2": 4221.69,
          "tp2_rule": "Trailing EMA",
          "reasoning": [
            "Xu hướng EMA: Tăng rõ rệt (ủng hộ MUA)",
            "RSI = 58.2 (còn dư địa tăng)",
            "Mục tiêu TP1 (+1.0% giá) khả thi cao trong phiên giao dịch (~3.7 ATR)"
          ],
          "conflict_detected": false,
          "mtf_agreement": true,
          "counter_move_detected": false,
          "counter_move_magnitude_atr": 0.0,
          "swing_mode": true,
          "tp_levels": [
            {"label": "TP1", "close_pct": 50, "price": 4200.89, "price_move_pct": 1.0, "margin_profit_pct": 5.0, "atr_multiple": 3.72, "holding_estimate": "8 giờ (8 nến)"},
            {"label": "TP2", "close_pct": 30, "price": 4221.69, "price_move_pct": 1.5, "margin_profit_pct": 7.5, "atr_multiple": 5.58, "holding_estimate": "12 giờ (12 nến)"},
            {"label": "TP3", "close_pct": 20, "price": 4242.49, "price_move_pct": 2.0, "margin_profit_pct": 10.0, "atr_multiple": 7.45, "holding_estimate": "16 giờ (16 nến)"}
          ],
          "sl_margin_loss_pct": 2.01,
          "target_feasibility": "✅ Khả thi cao: Mục tiêu TP1 (+1.0% giá) tương đương 3.72x ATR (nằm trong biên độ dao động thông thường của phiên).",
          "estimated_holding_time": "8 - 16 giờ",
          "disclaimer": "Đây là khuyến nghị dựa trên thuật toán kỹ thuật, KHÔNG PHẢI lời khuyên đầu tư..."
        }
      }
    }
  }
  ```

### 4. `GET /api/candles`
- **Chức năng:** Trả về mảng nến OHLC trực tiếp từ `CircularBuffer` trong RAM (O(1) slice, không tốn tài nguyên mạng/disk). Frontend polling mỗi **15 giây** để vẽ biểu đồ nến.
- **Query Params:**
  - `asset`: Tên tài sản (`BTC`, `XAU`, `OIL`). Mặc định: `BTC`.
  - `tf`: Khung giờ (`15m`, `1h`). Tự động xử lý alias `15m` <-> `15min`. Mặc định: `15m`.
  - `limit`: Số nến cần lấy (10 đến 300). Mặc định: `100`.
- **Response:**
  ```json
  [
    {
      "time": 1728889200,
      "open": 68200.0,
      "high": 68650.0,
      "low": 68150.0,
      "close": 68500.5
    }
  ]
  ```

### 5. `POST /api/config`
- **Chức năng:** Cập nhật cấu hình nóng từ giao diện Web mà không cần restart server.
- **Payload:**
  ```json
  {
    "twelvedata_api_key": "YOUR_API_KEY",
    "telegram_bot_token": "YOUR_BOT_TOKEN",
    "telegram_chat_id": "YOUR_CHAT_ID"
  }
  ```
- **Hành vi:** Cập nhật biến trong `CONFIG`, ghi cập nhật vào `.env`, kích hoạt `TwelveDataFetcher` / `TelegramDispatcher` và kích hoạt tác vụ nền tải nến lịch sử cho Vàng & Dầu ngay lập tức.

---

## 6. Những Điểm Cần Chú Ý & Nợ Kỹ Thuật (Technical Debt & Gotchas)

Khi tiếp nhận dự án để thực hiện nâng cấp, cần đặc biệt chú ý:

1. **Vấn đề mã nguồn kép (Dual Frontend Code):**
   - Mã HTML/CSS/JS đang tồn tại đồng thời ở cả file [dashboard.html](file:///d:/vunq/hosting-Indicator/dashboard.html) và biến chuỗi `HTML_DASHBOARD` bên trong [engine.py](file:///d:/vunq/hosting-Indicator/engine.py#L1277).
   - Cơ chế `DashboardServer.handle_index` ưu tiên đọc file [dashboard.html](file:///d:/vunq/hosting-Indicator/dashboard.html) nếu tồn tại. Khi chỉnh sửa frontend, cần đảm bảo đồng bộ cả hai nơi hoặc thống nhất loại bỏ chuỗi nhúng trong `engine.py`.
2. **Kích thước file `engine.py` (Monolithic ~3,300 lines):**
   - File bao trùm toàn bộ ứng dụng nhằm mục đích "single file execution". Nếu dự án mở rộng thêm nhiều chỉ báo và chiến lược mới, cần cân nhắc cấu trúc lại thành các package nhỏ hơn (`indicators/`, `fetchers/`, `server/`) nếu không bị ràng buộc triển khai 1 file.
3. **Giới hạn Rate Limit của TwelveData:**
   - Free tier có hạn mức **8 req/phút** và **800 req/ngày**. Cơ chế throttle hiện tại cố định 8.0s giữa các lượt gọi. Nếu thêm tài sản mới hoặc thêm khung thời gian ngắn hơn (ví dụ M5, M1), hạn ngạch sẽ nhanh chóng bị cạn kiệt.
4. **Cơ chế Polling REST:**
   - Hệ thống hiện lấy dữ liệu khi đóng nến (`seconds_until_next_close`). Giá trên biểu đồ và bảng chỉ báo không nhảy từng giây như sàn giao dịch mà cập nhật theo chu kỳ nến.
5. **Ngưỡng Heuristic của Confluence Decision Engine:**
   - Các ngưỡng phân loại trong `ConfluenceDecisionEngine` (`EMA_TREND_*`, `RSI_*`, `BB_EDGE_ZONE_PCT`) là giá trị heuristic ban đầu, **CẦN backtest định lượng** trên dữ liệu lịch sử thực tế của từng asset trước khi coi là đáng tin cậy cho giao dịch thật.
6. **Mở rộng kênh thông báo Advice (Telegram/Discord):**
   - Hiện tại `TradeAdvice` **CHỈ phục vụ hiển thị trên Web Dashboard** qua `/api/snapshot`. Nếu trong tương lai cần mở rộng phát tín hiệu Advice sang Telegram, đây sẽ là một tính năng riêng cần đặc tả cơ chế chống spam/dedup bổ sung.

---

## 7. Gợi Ý Lộ Trình Nâng Cấp Khả Thi (Upgrade Roadmap)

Dưới đây là các định hướng phát triển tối ưu đã được phân tích sẵn để model tiếp theo có thể lập kế hoạch thực hiện ngay:

### Giai đoạn 1: Chuẩn hóa Kiến trúc & Dọn dẹp Code (Refactoring)
- Thống nhất cơ chế nạp giao diện: Tách hẳn `dashboard.html` thành file template chuẩn, chuyển `engine.py` sang đọc template sạch sẽ thay vì giữ một biến chuỗi 1,100 dòng.
- Bổ sung cấu hình linh hoạt cho danh mục chỉ báo (chu kỳ EMA, ngưỡng RSI) cho phép tùy chỉnh qua Web UI hoặc ENV.

### Giai đoạn 2: Nâng cấp Năng lực Định lượng (Quant Capabilities)
- **Bổ sung chỉ báo mới (Pure-math):**
  - **MACD (12, 26, 9)**: Phân kỳ hội tụ trung bình động.
  - **Stochastic RSI (14, 14, 3, 3)**: Xác định vùng quá mua/quá bán nhạy bén hơn.
  - **Supertrend**: Xác định xu hướng đảo chiều rõ ràng cho non-trader.
- **Hợp lưu đa khung thời gian (Multi-timeframe Confluence):**
  - Kết hợp H1 và M15: Chỉ kích hoạt tín hiệu Mua M15 khi xu hướng H1 nằm trên EMA50 (bộ lọc giảm thiểu tín hiệu giả - whipsaw).

### Giai đoạn 3: Realtime Streaming qua WebSocket
- Thay thế cơ chế REST Polling nến BTC bằng **Binance Kline WebSocket Stream** (`wss://stream.binance.com:9443/ws/btcusdt@kline_15m`).
- Tích hợp WebSocket hoặc Server-Sent Events (SSE) giữa Backend và Web Dashboard để cập nhật giá tức thời từng giây mà không tốn công polling HTTP liên tục.

### Giai đoạn 4: Đa dạng hóa Kênh Thông Báo & Tương Tác
- Bổ sung **Discord Webhook** hoặc **Slack Webhook** (gửi tín hiệu vào channel chuyên biệt, không cần tạo bot phức tạp).
- Hỗ trợ Telegram Bot 2 chiều (Command Polling): Người dùng có thể chat các lệnh `/price`, `/signals`, `/status` để bot phản hồi ngay tức thì.

### Giai đoạn 5: Thống Kê & Đánh Giá Hiệu Quả Tín Hiệu (Performance Tracker)
- Lưu lại snapshot giá khi tín hiệu được kích hoạt vào SQLite hoặc JSON nhẹ.
- Sau $N$ cây nến tiếp theo (ví dụ 5, 10, 20 nến), tự động đo đạc tỷ lệ lợi nhuận/thua lỗ (Win/Loss Rate) để hiển thị bảng xếp hạng độ tin cậy thực tế trên Dashboard.
