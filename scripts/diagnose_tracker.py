"""
Script chan doan OutcomeTracker - Giai doan 1 cua update_plan.md
Chay: python scripts/diagnose_tracker.py
"""
# -*- coding: utf-8 -*-
import sqlite3
import os
import sys

# Fix encoding for Windows console
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

print("=" * 60)
print("CHECK 1.1 -- Xac minh file database")
print("=" * 60)

db_paths = [
    "./data/signal_tracker.db",
    "./data/trade_history.db",
]

for p in db_paths:
    abs_p = os.path.abspath(p)
    exists = os.path.exists(abs_p)
    size = os.path.getsize(abs_p) if exists else 0
    print(f"\nFile: {abs_p}")
    print(f"  Ton tai: {exists}, Kich thuoc: {size} bytes")
    if exists:
        try:
            conn = sqlite3.connect(abs_p)
            cur = conn.cursor()
            tables = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
            print(f"  Bang: {[t[0] for t in tables]}")
            for (tname,) in tables:
                count = cur.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
                print(f"  {tname} -> {count} records")
                if count > 0:
                    rows = cur.execute(f"SELECT * FROM {tname} LIMIT 3").fetchall()
                    for r in rows:
                        print(f"    Sample: {r}")
            conn.close()
        except Exception as e:
            print(f"  LOI khi truy van: {e}")
    else:
        print("  FILE KHONG TON TAI!")

print("\n" + "=" * 60)
print("CHECK 1.2 -- Working directory & paths")
print("=" * 60)
print(f"  CWD: {os.getcwd()}")
print(f"  engine.py abs path: {os.path.abspath('engine.py')}")
print(f"  ./data/ dir exists: {os.path.isdir('./data')}")

print("\n" + "=" * 60)
print("CHECK -- Tim class OutcomeTracker / SignalOutcomeTracker trong engine.py")
print("=" * 60)

try:
    with open("engine.py", "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    tracker_lines = []
    keywords = [
        "OutcomeTracker", "SignalOutcomeTracker", 
        "start_trade", "outcome_tracker",
        "trade_history", "trade_outcomes",
        "signal_track", "maybe_create_track",
        "last_advice_state", "prev_advice",
        "TRACKER_ENABLED", "db_path",
    ]
    for i, line in enumerate(lines, 1):
        if any(kw in line for kw in keywords):
            tracker_lines.append((i, line.rstrip()))
    
    print(f"  Tim thay {len(tracker_lines)} dong lien quan den tracker:")
    for lineno, content in tracker_lines[:100]:
        print(f"  {lineno:5d}: {content}")

except Exception as e:
    print(f"  Khong the doc engine.py: {e}")

print("\n" + "=" * 60)
print("DONE - Xem ket qua tren de chan doan")
print("=" * 60)
