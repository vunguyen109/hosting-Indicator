"""Giả lập 1 TradeAdvice hợp lệ, gọi trực tiếp maybe_create_track(), sau đó query
lại DB để xác nhận ghi/đọc hoạt động — tách biệt khỏi việc chờ hệ thống thật
phát sinh tín hiệu tự nhiên."""
import sys, os, io, sqlite3, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import SignalOutcomeTracker, TradeAdvice

def main():
    tracker = SignalOutcomeTracker()
    
    # Xoa record test cu cua XAU de dam bao tinh idempotent khi chay lap lai
    conn = sqlite3.connect(tracker.db_path)
    conn.execute("DELETE FROM signal_tracks WHERE asset='XAU'")
    conn.commit()
    conn.close()
    tracker._active_tracks.pop("XAU_1h", None)
    
    fake_advice = TradeAdvice(
        action="BUY",
        confidence="Cao",
        confidence_score=5,
        entry_type="now",
        entry_price=4150.0,
        stop_loss=4100.0,
        take_profit_1=4190.0,
        take_profit_2=4210.0,
        tp_levels=[
            {"label": "TP1", "price": 4190.0, "close_pct": 0.4, "margin_profit_pct": 5.0},
            {"label": "TP2", "price": 4210.0, "close_pct": 0.35, "margin_profit_pct": 7.5},
            {"label": "TP3", "price": 4230.0, "close_pct": 0.25, "margin_profit_pct": 10.0},
        ],
        reasoning=["Test reasoning manual verify"],
    )
    track = tracker.maybe_create_track(
        asset="XAU",
        timeframe="1h",
        advice=fake_advice,
        candle_ts=int(time.time()),
    )
    assert track is not None, "FAIL: maybe_create_track returned None"
    
    conn = sqlite3.connect(tracker.db_path)
    rows = conn.execute("SELECT * FROM signal_tracks WHERE asset='XAU' ORDER BY created_at_ts DESC LIMIT 1").fetchall()
    assert len(rows) == 1, "FAIL: không ghi được track test"
    print("PASS:", rows[0])
    conn.close()

if __name__ == "__main__":
    main()
