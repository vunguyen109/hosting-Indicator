#!/usr/bin/env bash
set -euo pipefail

MAX_ROUNDS="${MAX_ROUNDS:-3}"
PAUSE="${PAUSE:-1}"   # 1 = dừng chờ người duyệt sau mỗi vòng audit
BASE="$(git rev-parse HEAD)"
mkdir -p handoff/history

for i in $(seq 1 "$MAX_ROUNDS"); do
  echo "================ ROUND $i: EXECUTOR (Gemini) ================"
  gemini --yolo -p "Bạn là EXECUTOR. Làm theo GEMINI.md. Đọc handoff/TASK.md, và handoff/AUDIT.md nếu có. Thực hiện hoặc sửa code, sau đó ghi handoff/REPORT.md."

  git add -A
  git commit -m "executor: round $i" || echo "(không có thay đổi để commit)"

  echo "================ ROUND $i: AUDITOR (Codex) ================"
  rm -f handoff/AUDIT.md
  codex exec --full-auto "Bạn là AUDITOR. Làm theo AGENTS.md. Review bằng lệnh: git diff $BASE..HEAD, cùng handoff/TASK.md và handoff/REPORT.md. Chỉ ghi file handoff/AUDIT.md, dòng đầu là VERDICT."

  if [ ! -f handoff/AUDIT.md ]; then
    echo "LỖI: auditor không tạo AUDIT.md. Dừng."; exit 1
  fi
  cp handoff/AUDIT.md "handoff/history/AUDIT_round_$i.md"
  cp handoff/REPORT.md "handoff/history/REPORT_round_$i.md" 2>/dev/null || true
  git add -A && git commit -m "auditor: round $i" || true

  if head -n1 handoff/AUDIT.md | grep -q "^VERDICT: APPROVED"; then
    echo "✅ APPROVED ở vòng $i"; exit 0
  fi

  echo "⚠️  CHANGES_REQUESTED. Xem handoff/AUDIT.md"
  if [ "$PAUSE" = "1" ]; then
    read -rp "Xem xong thì Enter để chạy vòng tiếp, Ctrl+C để dừng: "
  fi
done

echo "❌ Hết $MAX_ROUNDS vòng mà chưa APPROVED. Cần người can thiệp."
exit 2
