#!/bin/bash
# Exact source: /local/job40255479/worktrees/isa-Niger-1091 @ d602f8d63520e521a730f85551b366c4d45d9a35
# Preparation was run ONCE into newly empty roots; existing roots are frozen evidence.
# Do not rerun preparation in place or reuse them for a later candidate.
# Root must prepare a new ignored metadata exception/physical evidence root for independent replay.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Niger/1091
# Historical commands, each routed through exact identity/offline/write guards:
# "$P/run.sh" prepare "$P/prepare.py"
# bash "$P/matrix.sh"  # twelve normal cold + twelve fresh-process warm calls
# "$P/run.sh" default-all "$P/extra.py" cold
# "$P/run.sh" default-all "$P/extra.py" warm
# "$P/run.sh" default-all "$P/layers.py"
# for b in default make; do for w in all 2011 2014 2018 2021 absent; do "$P/run.sh" default-all "$P/populated.py" "$b" "$w"; done; done
# bash "$P/test-run.sh" #97pytest passes,0skips; explicit two loadfile workers
# "$P/run.sh" default-all "$P/built-checks.py" #12existing real built assertions
# "$P/run.sh" default-all bench/feature_audit/scan.py --features community_prices --countries Niger Mali --phases 1,2 --out "$P/scanner-private.jsonl"
# "$P/run.sh" make-all "$P/make-cli-feature.py"
# "$P/run.sh" prepare "$P/empty-contract.py" #pure helper measurement, NOT candidate route
# "$P/run.sh" prepare "$P/final-analyze.py"
# Source-only read checks, no Git/ref mutation:
git -C /local/job40255479/worktrees/isa-Niger-1091 rev-parse HEAD
git -C /local/job40255479/worktrees/isa-Niger-1091 status --short
