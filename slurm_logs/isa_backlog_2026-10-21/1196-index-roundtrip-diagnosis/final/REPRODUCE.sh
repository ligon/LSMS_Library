#!/bin/bash
set -euo pipefail
# Historical executed commands. Evidence is frozen: do not replay into this root.
# A new reviewer must create a new owner-only root and metadata target, adapt
# only launcher/private bindings, verify manifests/copies and terminal gates,
# then execute dependent stages serially. No acquisition/source-cold stage here.
R=/local/job40255479/worktrees/isa-1196-index-diagnosis
P=/local/job40255479/control/isa-backlog-20261021/shared1196-diagnosis
cd "$R"
bash "$P/run.sh" prepare.py
TASK_DATA=data-warm bash "$P/run.sh" gate-v2.py
TASK_DATA=data-warm bash "$P/run.sh" artifact-stages.py
TASK_DATA=data-warm bash "$P/run.sh" warm.py warm-observed
TASK_DATA=data-warm bash "$P/run.sh" warm.py warm-fresh
# This exact focused command exits1: eight intended regression failures,
# seven GH645 passes, no error/skip. Preserve the red result.
if bash "$P/run.sh" run-controls.py; then
  echo 'Unexpected green result on f8 regression draft' >&2
  exit 1
else
  status=$?
  test "$status" -eq 1
fi
TASK_DATA=data-warm bash "$P/run.sh" analyze-controls.py
bash "$P/run.sh" hash-boundary.py
TASK_DATA=data-warm bash "$P/run.sh" native-stage-account.py
TASK_DATA=data-warm bash "$P/run.sh" attrs-and-boundary.py
bash "$P/run.sh" route-and-isolation-account.py
# All imports/unpickling/APIs use mounted Python through guarded entry.py,
# exact cwd/import/config/data roots, unshare-rn and numerical/build/Make1;
# tests use exactly2 loadfile workers. Original stopped gate.py is retained.
