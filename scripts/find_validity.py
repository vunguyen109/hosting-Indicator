import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
with open('engine.py', encoding='utf-8') as f:
    lines = f.readlines()
kws = ['validity', 'INVALIDAT', 'stale', 'drift', 'invalidat', 'Validity', 'advice_validity',
       'AdviceValidity', 'val_status', 'Tăng đáng kể', 'price_drift', 'entry_drift']
found = []
for i, line in enumerate(lines, 1):
    if any(k.lower() in line.lower() for k in kws):
        found.append((i, line.rstrip()))
print(f'Total matches: {len(found)}')
for lineno, content in found[:120]:
    print(f'{lineno:5d}: {content}')
