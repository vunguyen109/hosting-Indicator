"""
Script test thu cong: gia lap 1 advice BUY, goi maybe_create_track(),
sau do query lai DB de xac nhan ban ghi da ton tai.
Chay: python scripts/manual_test_outcome_tracker.py
"""
import sys, io, os, sqlite3, datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import SignalOutcomeTracker, TradeAdvice

print("=" * 60)
print("Manual test: SignalOutcomeTracker.maybe_create_track()")
print("=" * 60)

# 1. Khoi tao tracker voi DB rieng (tranh anh huong production)
import tempfile
tmp_dir = tempfile.mkdtemp()
test_db = os.path.join(tmp_dir, "test_manual.db")
print(f"\nTest DB: {test_db}")

tracker = SignalOutcomeTracker(db_path=test_db)
print(f"Tracker initialized at: {tracker.db_path}")

# 2. Tao fake advice BUY hop le
fake_advice = TradeAdvice(
    action="BUY",
    entry_price=65000.0,
    entry_zone=None,
    stop_loss=64000.0,
    take_profit_1=66000.0,
    take_profit_2=67500.0,
    tp_levels=[
        {"label": "TP1", "price": 66000.0, "close_pct": 0.4, "margin_profit_pct": 5.0},
        {"label": "TP2", "price": 67500.0, "close_pct": 0.35, "margin_profit_pct": 7.5},
        {"label": "TP3", "price": 69000.0, "close_pct": 0.25, "margin_profit_pct": 10.0},
    ],
    confidence="Cao",
    confidence_score=5,
    entry_type="now",
    reasoning=["EMA crossover", "RSI > 50"],
    conflict_detected=False,
    mtf_agreement=True,
)
print(f"\nFake advice: action={fake_advice.action}, conf={fake_advice.confidence}, entry={fake_advice.entry_price}")

# 3. Goi maybe_create_track
import time
candle_ts = int(time.time())
track = tracker.maybe_create_track(
    asset="TEST_BTC",
    timeframe="1h",
    advice=fake_advice,
    candle_ts=candle_ts,
)

if track is None:
    print("\nX maybe_create_track() tra ve None - FAIL")
    sys.exit(1)

print(f"\nTrack tao ra: id={track.track_id[:8]}, status={track.status}")
print(f"  direction={track.direction}, entry={track.planned_entry}, sl={track.stop_loss}")

# 4. Doc lai tu DB
conn = sqlite3.connect(test_db)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
result = cur.execute(
    "SELECT * FROM signal_tracks WHERE track_id=?", (track.track_id,)
).fetchone()
conn.close()

if result is None:
    print("\nX LOI: Ghi khong thanh cong hoac khong doc lai duoc")
    sys.exit(1)

print(f"\nDoc lai tu DB thanh cong:")
print(f"  track_id: {result['track_id'][:8]}...")
print(f"  asset: {result['asset']}, tf: {result['timeframe']}")
print(f"  direction: {result['direction']}, status: {result['status']}")
print(f"  planned_entry: {result['planned_entry']}, stop_loss: {result['stop_loss']}")

print("\n[OK] Test thu cong THANH CONG - SignalOutcomeTracker hoat dong dung")
print("     DB ghi/doc binh thuong, path resolved dung.")
print(f"\n[NOTE] Production DB: {os.path.abspath('./data/signal_tracker.db')}")
print("       Service chua chay = DB trong la BINH THUONG, khong phai loi code.")

# Cleanup
import shutil
shutil.rmtree(tmp_dir, ignore_errors=True)
print(f"\nTest DB da duoc xoa: {test_db}")
