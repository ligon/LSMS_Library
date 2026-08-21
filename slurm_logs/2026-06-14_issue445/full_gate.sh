#!/usr/bin/env bash
# GH #445: authoritative all-country cold regression gate after the fix commit.
# Matches the prior release gate (release_gate_FINAL.out): --rebuild-caches,
# --dist=loadfile, parallel.  Run via .venv/bin/python (not poetry) to avoid the
# keyring hang noted in the release skill.
set -uo pipefail
cd /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
LOG=slurm_logs/2026-06-14_issue445/full_gate.out

echo "HEAD: $(git rev-parse --short HEAD) on $(git branch --show-current)"
.venv/bin/python -m pytest -n 24 --dist=loadfile --rebuild-caches -q -rfE \
  > "$LOG" 2>&1
code=$?
echo "=== pytest exit: $code ==="
echo "=== summary line ==="
tail -1 "$LOG"
echo "=== FAILED / ERROR lines ==="
grep -E "^(FAILED|ERROR)" "$LOG" || echo "  (none)"
exit $code
