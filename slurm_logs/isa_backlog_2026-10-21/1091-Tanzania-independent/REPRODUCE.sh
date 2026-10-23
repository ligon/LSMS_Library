#!/bin/bash
# Frozen roots are evidence. Do not rerun into them.
# Replay requires NEW physical roots, exact prepared metadata links and the same
# source pins; adapt run.sh/sitecustomize.py and the private-root constants first.
set -euo pipefail
P=/local/job40255479/control/isa-backlog-20261021/review-Tanzania-1091
R=/local/job40255479/worktrees/isa-review-Tanzania-1091
B=/local/job40255479/worktrees/isa-review-Tanzania-1091-base
# Actual dependency order (private logs, not pipelines):
# bash "$P/run.sh" bootstrap.py
# bash "$P/run.sh" preflight.py               # preserved manifest-shape stop
# bash "$P/run.sh" prepare-tools.py
# bash "$P/run.sh" preflight-v2.py
# TASK_LABEL=base TASK_CHECKOUT="$B" TASK_DATA=base/data-hash bash "$P/run.sh" hash_inventory.py "$P/base-hashes-private.json"
# TASK_LABEL=candidate TASK_DATA=candidate/data-hash bash "$P/run.sh" hash_inventory.py "$P/candidate-hashes-private.json"
# bash "$P/run.sh" hash-gate.py                # terminal549/39 gate before builds
# TASK_LABEL=base TASK_CHECKOUT="$B" TASK_DATA=base/data-static bash "$P/run.sh" preparation.py
# TASK_LABEL=candidate TASK_DATA=candidate/data-static bash "$P/run.sh" preparation.py
# Gate both preparation-terminal.json files before dependent source decoding.
# bash "$P/run.sh" tighten-probe.py
# bash "$P/matrix.sh" base
# bash "$P/matrix.sh" candidate
# TASK_LABEL=candidate TASK_DATA=candidate/data-tests bash "$P/run.sh" tests.py new
# TASK_LABEL=base TASK_CHECKOUT="$B" TASK_DATA=base/data-tests bash "$P/run.sh" tests.py new
# TASK_LABEL=candidate TASK_DATA=candidate/data-existing-tests bash "$P/run.sh" tests.py existing
# Each tests.py invocation runs -n2 --dist=loadfile, no cacheprovider,
# private unique --basetemp/--junitxml, exact -c pyproject.toml.
# Existing selection: test_community_prices_variety.py +
# test_gh1039_make_backend_no_country_rule.py -k 'not TestBuilt'.
# TASK_LABEL=base TASK_CHECKOUT="$B" TASK_SCENARIO=default-all TASK_DATA=base/data-default-all bash "$P/run.sh" oracle.py
# TASK_LABEL=candidate TASK_SCENARIO=default-all TASK_DATA=candidate/data-default-all bash "$P/run.sh" oracle.py
# bash "$P/post.sh" candidate                 # preserved final redirect stop
# bash "$P/post.sh" base                      # preserved final redirect stop
# Only the missing scanner step resumed; none of the completed prefix replayed:
# TASK_LABEL=candidate TASK_SCENARIO=default-all TASK_DATA=candidate/data-default-all bash "$P/run.sh" scanner-run.py
# TASK_LABEL=base TASK_CHECKOUT="$B" TASK_SCENARIO=default-all TASK_DATA=base/data-default-all bash "$P/run.sh" scanner-run.py
# bash "$P/run.sh" comparator-controls.py
# TASK_DATA=candidate/data-artifacts bash "$P/run.sh" evidence-gaps.py
# TASK_DATA=candidate/data-analysis bash "$P/run.sh" assess.py
# TASK_DATA=candidate/data-artifact-final bash "$P/run.sh" artifact-final-checks.py
# TASK_DATA=candidate/data-terminal-proof bash "$P/run.sh" terminal-proof.py # preserved label-oracle stop
# bash "$P/run.sh" coverage-stop.py           # artifact-only saved canonical operands
# TASK_DATA=candidate/data-terminal-proof bash "$P/run.sh" terminal-proof-v2.py
# Packaging/final-freeze are stdlib/artifact-only, not an additional API stage.
# Read-only source checks:
GIT_OPTIONAL_LOCKS=0 git -C "$R" rev-parse HEAD
GIT_OPTIONAL_LOCKS=0 git -C "$R" status --short
GIT_OPTIONAL_LOCKS=0 git -C "$R" diff --stat 99c824079da480ab2646e499c06ac70e457ff271 HEAD
