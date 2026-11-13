#!/bin/bash
# Frozen evidence roots: NEVER replay builds/tests into them.
# Replay uses NEW physical roots/checkouts with the exact source pins and own
# prepared DVCtmp links. Copy the private code physically; substitute only new
# root constants in run.sh/sitecustomize.py and bind matching metadata receipts.
# Every launcher asserts mounted Python/cwd/PYTHONPATH/import/config/data,
# threads1, namespace lo-only, socket/write guard. Never stamp/bypass/acquire.
set -euo pipefail
P=/local/job40255479/control/isa-backlog-20261021/Malawi/218
R=/local/job40255479/worktrees/isa-Malawi-218
B=/local/job40255479/worktrees/isa-Malawi-218-base
# Actual successful dependency order, with original failures retained separately:
# TASK_CHECKOUT="$B" bash "$P/run.sh" inventory.py "$P/parent-inventory-private.json"
# bash "$P/run.sh" inventory.py "$P/candidate-inventory-private.json"
# bash "$P/run.sh" metadata-check.py          # measured549/39/97 before builds
# bash "$P/run.sh" stage.py                   # terminal19-source/four-root physical gate
# bash "$P/run.sh" healthy-gate.py            # current COUNTRY/YAML wave gates; legit hashless companions
# bash "$P/run.sh" test-runner.py focused-candidate tests/test_gh218_derived_registrations.py tests/test_food_prices_units_kwarg.py tests/test_food_expenditures_basis.py tests/test_feature_vocabulary.py tests/test_feature.py::TestFeatureDiscovery
# TASK_CHECKOUT="$B" bash "$P/run.sh" test-runner.py new-parent "$P/parent-test_gh218_derived_registrations.py"
# The parent's private test copy is byte-exact to the candidate test;1FAIL/1PASS expected.
# bash "$P/run.sh" audit-run-v2.py            # actual strict api_surface_audit.py --strict
# bash "$P/run.sh" comparator-controls.py     # eight meaningful rejected mutations
# For each head, use its separate Default and Make root, one consumer/root:
# TASK_LABEL=base-default TASK_DATA=base-default/data TASK_CHECKOUT="$B" bash "$P/run.sh" probe.py cold default
# TASK_LABEL=candidate-default TASK_DATA=candidate-default/data bash "$P/run.sh" probe.py cold default
# TASK_LABEL=base-make TASK_DATA=base-make/data TASK_CHECKOUT="$B" bash "$P/run.sh" probe.py cold make
# TASK_LABEL=candidate-make TASK_DATA=candidate-make/data bash "$P/run.sh" probe.py cold make
# Make source cold/fresh warm both preserve RuntimeError; do not substitute empty frames.
# TASK_LABEL=base-make TASK_DATA=base-make/data TASK_CHECKOUT="$B" bash "$P/run.sh" probe.py warm make
# TASK_LABEL=candidate-make TASK_DATA=candidate-make/data bash "$P/run.sh" probe.py warm make
# TASK_LABEL=base-make TASK_DATA=base-make/data TASK_CHECKOUT="$B" bash "$P/run.sh" Make-derived-controls.py
# TASK_LABEL=candidate-make TASK_DATA=candidate-make/data bash "$P/run.sh" Make-derived-controls.py
# bash "$P/run.sh" native-cache-oracle.py     # artifact-only5native/10byte bindings; no source replay
# bash "$P/run.sh" compare.py                 # cold29 matched frames, before new phase artifacts
# TASK_LABEL=base-default TASK_DATA=base-default/data TASK_CHECKOUT="$B" bash "$P/run.sh" warm-probe-v2.py warm default
# TASK_LABEL=candidate-default TASK_DATA=candidate-default/data bash "$P/run.sh" warm-probe-v2.py warm default
# bash "$P/run.sh" exact-stage-check.py
# TASK_LABEL=base-default TASK_DATA=base-default/data TASK_CHECKOUT="$B" bash "$P/run.sh" feature-probe-v2.py feature-cold
# TASK_LABEL=candidate-default TASK_DATA=candidate-default/data bash "$P/run.sh" feature-probe-v2.py feature-cold
# bash "$P/run.sh" composition-check.py feature-cold
# TASK_LABEL=base-default TASK_DATA=base-default/data TASK_CHECKOUT="$B" bash "$P/run.sh" feature-probe-v2.py feature-warm
# TASK_LABEL=candidate-default TASK_DATA=candidate-default/data bash "$P/run.sh" feature-probe-v2.py feature-warm
# bash "$P/run.sh" composition-check.py feature-warm
# TASK_LABEL=base-default TASK_DATA=base-default/data TASK_CHECKOUT="$B" bash "$P/run.sh" scanner-run.py
# TASK_LABEL=candidate-default TASK_DATA=candidate-default/data bash "$P/run.sh" scanner-run.py
# bash "$P/run.sh" scanner-assessment.py       # all205 raw records, no normalization
# TASK_LABEL=base-default TASK_DATA=base-default/data TASK_CHECKOUT="$B" bash "$P/run.sh" cli-probe-v2.py
# TASK_LABEL=candidate-default TASK_DATA=candidate-default/data bash "$P/run.sh" cli-probe-v2.py
# bash "$P/run.sh" cli-artifact-check.py
# bash "$P/run.sh" nested-footer-check.py       # exact serialized hash-only reconstruction
# bash "$P/run.sh" cache-artifact-check-v3.py   # retains prior strict footer stop
# bash "$P/run.sh" warning-account.py
# bash "$P/run.sh" warning-path-diagnostic.py   # raw inequality stays explicit
# Run final-metadata-gate.py separately at each of the four identity-bound roots.
# Private original cold script hashes all L1/L2 around each call. Subsequent
# phases bracket complete L1/L2, with per-call L2 receipts; no weakened warm claim.
# All raw frames/warnings/stderr remain private. Safe exports contain aggregates only.
# Exact read-only source inspection (safe to run on frozen source):
GIT_OPTIONAL_LOCKS=0 git -C "$R" rev-parse HEAD
GIT_OPTIONAL_LOCKS=0 git -C "$R" status --short
GIT_OPTIONAL_LOCKS=0 git -C "$R" diff --stat f8d236694c26fe9e709c5548f02c70f74ea72c33 HEAD
