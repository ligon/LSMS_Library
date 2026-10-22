#!/bin/bash
# Frozen evidence is immutable. Replay only in NEW physical roots after adapting
# run.sh/sitecustomize.py and preparing the exact candidate/base metadata links.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Tanzania/1091-fix
R=/local/job40255479/worktrees/isa-Tanzania-1091-fix
# Actual dependency-ordered command record (all data logs remain private):
# bash "$P/run.sh" base hash "$P/hash_inventory.py" "$P/base-hashes-private.json"
# bash "$P/run.sh" candidate hash "$P/hash_inventory.py" "$P/candidate-hashes-private.json"
# Gate actual549/39 comparison in hash-scope.json before any source rebuild.
# bash "$P/run.sh" base prepare "$P/prepare.py"
# bash "$P/run.sh" candidate prepare "$P/prepare-v2.py"
#   Preserve initial candidate snapshot stop; v2 records the approved deletion.
# bash "$P/run.sh" candidate tests "$P/new-tests-run.py" #27 pass
# bash "$P/matrix.sh" base
# bash "$P/matrix.sh" candidate
# bash "$P/after-matrix.sh" base #same-file parent20fail/7pass/0errors
# bash "$P/after-matrix.sh" candidate #original CLI path stop preserved
# bash "$P/run.sh" candidate default-all "$P/resume-first-cli.py" first
# bash "$P/continue-candidate.sh" #no replay of prior first public/Feature calls
# Both sequences include source oracle, gated populated physical copies,
#20populated requests, default/None Features, actual CLI/Arrow controls,
#11existing held assertion bodies, direct mounted audit and phases1/2 scanner.
# Candidate also runs existing58focused cases with2loadfileworkers/threads1.
# bash "$P/run.sh" candidate analysis "$P/analyze-composition.py"
# Read-only checks below are safe on the frozen checkout:
git -C "$R" rev-parse HEAD
git -C "$R" status --short
git -C "$R" diff --stat 99c824079da480ab2646e499c06ac70e457ff271 HEAD
