#!/bin/bash
# Command record: runtime/test pin 6be6576607f1862b603d123acfb74ace4026fd3f.
# The evidence roots below are frozen; do not rerun writes into them.
# Independent replay needs a NEW root, physical held-source copies and a new
# exact ignored-metadata exception, then adapted run.sh/run-base.sh guards.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Niger/1091-fix
R=/local/job40255479/worktrees/isa-Niger-1091-fix
# Ordered historical commands (logs and full frames private):
# "$P/run-base.sh" hash-base "$P/hash_inventory.py" "$P/baseline-hashes.json"
# "$P/run.sh" hash-final "$P/hash_inventory.py" "$P/candidate-hashes.json"
# Census comparison and hash-scope.json PASS preceded ALL source builds.
# "$P/run.sh" prepare "$P/prepare.py" #11sources/44copies TERMINAL_SUCCESS
# bash "$P/matrix.sh" #12cold+12independent fresh-process warm
# "$P/run.sh" default-all "$P/extra.py" cold
# "$P/run.sh" default-all "$P/extra.py" warm
# "$P/run.sh" default-all "$P/layers.py"
# for b in default make; do
#   for w in all 2011 2014 2018 2021 absent; do
#     "$P/run.sh" default-all "$P/populated.py" "$b" "$w"
#   done
# done
# "$P/run.sh" make-all "$P/make-cli-feature.py"
# "$P/run.sh" default-all "$P/test-launch.py" -n 1 --dist=loadfile -q \
#   tests/test_community_prices_variety.py \
#   tests/test_gh1039_make_backend_no_country_rule.py \
#   tests/test_gh1091_niger_community_builder.py -k 'not TestBuilt'
# "$P/run-base.sh" baseline-newtests "$P/test-launch.py" -n 1 \
#   --dist=loadfile -q "$P/parent-test/test_gh1091_niger_community_builder.py"
# "$P/run.sh" default-all "$P/built-checks.py" #12actualbuiltassertions
# "$P/run.sh" default-all bench/feature_audit/scan.py \
#   --features community_prices --countries Niger Mali --phases 1,2 \
#   --out "$P/scanner-private.jsonl"
# "$P/run-base.sh" parent-feature "$P/parent-feature.py" cold
# "$P/run-base.sh" parent-feature "$P/parent-feature.py" warm
# "$P/run.sh" default-all "$P/verify.py" #72strict+72axis+3Arrow
# Auxiliary original/repeats are preserved separately; do not repeat them
# merely to seek zero writes. See REPORT.org for the lost-delta limitation.
# "$P/run.sh" default-all "$P/final-metadata.py" #read-onlyfooters
# Read-only source inspection is safe on the frozen roots:
git -C "$R" rev-parse HEAD
git -C "$R" status --short
git -C "$R" diff --stat d602f8d63520e521a730f85551b366c4d45d9a35 HEAD
