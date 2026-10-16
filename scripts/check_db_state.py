"""
Script verify state cua signal_tracker.db
"""
import sys, io, sqlite3, os, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

db_path = os.path.abspath("./data/signal_tracker.db")
print(f"DB path: {db_path}")
print(f"File exists: {os.path.exists(db_path)}, size: {os.path.getsize(db_path) if os.path.exists(db_path) else 0} bytes")

if not os.path.exists(db_path):
    print("FILE NOT FOUND!")
    sys.exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Count by status
print("\n--- Count by status ---")
rows = cur.execute("SELECT status, COUNT(*) as cnt FROM signal_tracks GROUP BY status").fetchall()
for r in rows:
    print(f"  {r['status']}: {r['cnt']}")

# All records
print("\n--- All records ---")
rows = cur.execute("SELECT track_id, asset, timeframe, direction, status, confidence, planned_entry, created_at_ts FROM signal_tracks ORDER BY created_at_ts DESC").fetchall()
for r in rows:
    import datetime
    ts_str = datetime.datetime.fromtimestamp(r['created_at_ts'], datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"  [{r['asset']} {r['timeframe']} {r['direction']}] status={r['status']} conf={r['confidence']} entry={r['planned_entry']} created={ts_str}")

conn.close()
print("\nDone.")
