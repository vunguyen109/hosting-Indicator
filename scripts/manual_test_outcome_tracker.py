"""
Manual Test Script for SignalOutcomeTracker (Paper-tracking & Forward-testing)
Chạy độc lập để xác minh chu trình Ghi -> Lưu SQLite -> Đọc lại từ DB.
"""

import os
import sys
import sqlite3
import time

# Ensure project root is in sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from engine import (
    SignalOutcomeTracker,
    TradeAdvice,
    Candle,
    TrackStatus,
    CONFIG,
)

def run_test():
    print("=" * 65)
    print("🚀 BẮT ĐẦU TEST THỦ CÔNG: SignalOutcomeTracker End-to-End")
    print("=" * 65)

    tracker = SignalOutcomeTracker()
    print(f"[CHECK 1.1 & 1.2] Database path thực tế: {tracker.db_path}")
    print(f"[CHECK 1.2] Working Directory hiện tại: {os.getcwd()}")
    print(f"[CHECK 1.2] File DB tồn tại: {os.path.exists(tracker.db_path)}")

    # Dọn dẹp bản ghi TEST_BTC cũ (nếu có từ lần chạy trước) để bảo đảm idempotent
    with tracker._get_connection() as conn:
        conn.execute("DELETE FROM signal_tracks WHERE asset = 'TEST_BTC'")
        conn.commit()
    tracker._active_tracks.pop("TEST_BTC_1h", None)

    # 1. Giả lập một TradeAdvice chuẩn BUY TEST_BTC 1h
    fake_advice = TradeAdvice(
        action="BUY",
        confidence="Cao",
        confidence_score=5,
        entry_type="now",
        entry_price=65000.0,
        entry_zone=None,
        stop_loss=64000.0,
        take_profit_1=66000.0,
        take_profit_2=67500.0,
        tp2_rule="Trailing theo EMA20",
        reasoning=["EMA20 cắt lên EMA50", "RSI vượt 50 xác nhận đà tăng"],
        conflict_detected=False,
        mtf_agreement=True,
        tp_levels=[
            {"label": "TP1", "price": 66000.0, "close_pct": 0.40, "margin_profit_pct": 5.0},
            {"label": "TP2", "price": 67500.0, "close_pct": 0.35, "margin_profit_pct": 7.5},
            {"label": "TP3", "price": 69000.0, "close_pct": 0.25, "margin_profit_pct": 10.0},
        ],
    )

    now_ts = int(time.time())
    print("\n--- BƯỚC 1: Gọi maybe_create_track() ---")
    track = tracker.maybe_create_track(
        asset="TEST_BTC",
        timeframe="1h",
        advice=fake_advice,
        candle_ts=now_ts,
    )

    assert track is not None, "❌ LỖI: maybe_create_track() trả về None"
    print(f"✅ Đã tạo track thành công: track_id = {track.track_id} [{track.asset} {track.timeframe} {track.direction}]")
    print(f"   Trạng thái: {track.status}, Planned Entry: {track.planned_entry}, SL: {track.stop_loss}")

    # 2. Truy vấn trực tiếp bằng raw SQLite query
    print("\n--- BƯỚC 2: Kiểm tra trực tiếp qua sqlite3 connection ---")
    conn = sqlite3.connect(tracker.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT track_id, asset, timeframe, direction, status, planned_entry, stop_loss FROM signal_tracks WHERE track_id = ?", (track.track_id,))
    row = cur.fetchone()
    conn.close()

    assert row is not None, f"❌ LỖI: Không tìm thấy bản ghi {track.track_id} trong database!"
    print(f"✅ Đọc lại trực tiếp từ SQLite thành công:")
    print(f"   track_id     : {row['track_id']}")
    print(f"   asset/tf     : {row['asset']} {row['timeframe']}")
    print(f"   direction    : {row['direction']}")
    print(f"   status       : {row['status']}")
    print(f"   planned_entry: {row['planned_entry']}")
    print(f"   stop_loss    : {row['stop_loss']}")

    # 3. Kiểm tra API query: get_recent_tracks và get_metrics
    print("\n--- BƯỚC 3: Kiểm tra hàm get_recent_tracks() & get_metrics() ---")
    recent = tracker.get_recent_tracks(asset="TEST_BTC", timeframe="1h", limit=5)
    metrics = tracker.get_metrics(asset="TEST_BTC", timeframe="1h")
    print(f"✅ get_recent_tracks(asset='TEST_BTC') trả về {len(recent)} bản ghi.")
    print(f"✅ get_metrics trả về: total_tracks={metrics.get('total_tracks')}, active_tracks={metrics.get('active_tracks')}")
    assert len(recent) == 1, "❌ LỖI: get_recent_tracks() không trả về đúng 1 bản ghi của TEST_BTC!"

    # 4. Giả lập nến khớp Entry
    print("\n--- BƯỚC 4: Mô phỏng nến đóng tiếp theo khớp Entry ---")
    candle_entry = Candle(ts=now_ts + 3600, open=65100.0, high=65500.0, low=64800.0, close=65200.0)
    tracker.process_closed_candle(asset="TEST_BTC", timeframe="1h", candle=candle_entry, latest_advice=fake_advice)
    assert track.status == TrackStatus.OPEN.value, f"❌ LỖI: Track chưa chuyển sang OPEN, status={track.status}"
    print(f"✅ Track đã chuyển trạng thái thành OPEN: actual_entry = {track.actual_entry}")

    # 5. Giả lập nến tiếp theo chạm TP1 (high >= 66000)
    print("\n--- BƯỚC 5: Mô phỏng nến đạt TP1 ---")
    candle_tp1 = Candle(ts=now_ts + 7200, open=65300.0, high=66200.0, low=65100.0, close=66100.0)
    tracker.process_closed_candle(asset="TEST_BTC", timeframe="1h", candle=candle_tp1, latest_advice=fake_advice)
    assert track.status == TrackStatus.PARTIAL_TP1.value, f"❌ LỖI: Track chưa chuyển sang PARTIAL_TP1, status={track.status}"
    print(f"✅ Track đạt TP1 thành công: status = {track.status}, realized_r = {track.realized_r}R")

    # Dọn dẹp bản ghi TEST_BTC sau khi test xong
    with tracker._get_connection() as conn:
        conn.execute("DELETE FROM signal_tracks WHERE asset = 'TEST_BTC'")
        conn.commit()
    tracker._active_tracks.pop("TEST_BTC_1h", None)
    print("\n🧹 Đã dọn dẹp dữ liệu test sạch sẽ khỏi SQLite.")

    print("\n" + "=" * 65)
    print("🎉 TẤT CẢ CÁC BƯỚC TEST THỦ CÔNG ĐỀU THÀNH CÔNG PASS 100%!")
    print("   Module SignalOutcomeTracker đã hoạt động chính xác từ tầng ghi,")
    print("   tầng lưu trữ SQLite, tầng tính toán đến tầng truy vấn dữ liệu.")
    print("=" * 65)

if __name__ == "__main__":
    run_test()
