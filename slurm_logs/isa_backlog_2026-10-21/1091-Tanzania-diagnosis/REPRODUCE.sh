#!/bin/bash
# Runtime: e75be9c56fa6c318ce32cea5a8be6d448578bed1; ledger-only descendants.
# Frozen evidence must never be rerun in place. For an independent replay,
# prepare a NEW source/root/ignored tmp exception and adapt run.sh guards.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Tanzania/1091
R=/local/job40255479/worktrees/isa-Tanzania-1091
# Actual command record, dependency ordered for a new private replay:
# bash "$P/run.sh" prepare "$P/prepare.py" #2heldmodules/40copies TERMINAL
# bash "$P/run.sh" default-all "$P/discovery.py" #real Make -n, no builds
# bash "$P/matrix.sh" #20target-cold+20fresh-process/new-objectwarm
# bash "$P/run.sh" default-all "$P/source-layers.py" #correctedrealcountrycwd
# bash "$P/run.sh" default-all "$P/extras.py" first
# bash "$P/run.sh" default-all "$P/extras.py" fresh
# bash "$P/run.sh" make-all "$P/extras.py" first
# bash "$P/run.sh" make-all "$P/extras.py" fresh
# bash "$P/run.sh" tests "$P/test-run.py"
#   pytest selection58pass/0skip; subsequent make api-audit failed2 because
#   Poetry attempted guarded environment operations. Preserve that attempt.
# bash "$P/run.sh" audit "$P/api-audit-direct.py" #same existing script,image0
# bash "$P/run.sh" prepare "$P/populated-prepare.py" #fresh/currentcopygate
# bash "$P/populated-matrix.sh" #20actualpopulatedcalls
# bash "$P/run.sh" populated-default "$P/warm-dag.py" #read-onlyactualwarmDAG
# bash "$P/run.sh" empty-contract "$P/empty-contract.py" #purehelper,NOTcandidate
# bash "$P/run.sh" default-all "$R/bench/feature_audit/scan.py" \
#   --features community_prices --countries Tanzania Mali --phases 1,2 \
#   --out "$P/scanner-private.jsonl"
# bash "$P/run.sh" analysis "$P/analyze-existing.py" #existingartifacts
# Excluded launches/incidents remain private; do not reproduce them to seek
# green or zero-write outcomes. See DIAGNOSIS and validation limits.
# Read-only source checks safe on the frozen snapshot:
git -C "$R" rev-parse HEAD
git -C "$R" status --short
git -C "$R" diff --stat e75be9c56fa6c318ce32cea5a8be6d448578bed1 HEAD
