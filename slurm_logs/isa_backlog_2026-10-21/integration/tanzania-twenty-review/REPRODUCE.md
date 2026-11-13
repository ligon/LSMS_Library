Review source: de38fe9a29da5b24ff70bef11f2e37bfd7f73dc2; actual19 control:2566a3e490c0aefe03ab31d538d7d04e9b77c77e.

Frozen evidence must remain immutable. Replay only in NEW physical roots after adapting exact checkout/config/data/guard paths and obtaining root metadata preparation. Never run these launchers against the released root.

The actual dependency-ordered commands use `P=/local/job40255479/control/isa-backlog-20261021/integration/tanzania-twenty-review` and mounted Python through `bash "$P/run.sh"`:

```bash
TASK_LABEL=candidate bash "$P/run.sh" preflight.py
TASK_LABEL=base TASK_CHECKOUT=/local/job40255479/worktrees/isa-integration-twenty-base bash "$P/run.sh" hash_inventory.py "$P/base-hashes-private.json"
TASK_LABEL=candidate bash "$P/run.sh" hash_inventory.py "$P/candidate-hashes-private.json"
# Wait for BOTH terminal exit0; independently read scope before dependent preparation.
TASK_LABEL=candidate bash "$P/run.sh" hash_gate.py
TASK_LABEL=base TASK_CHECKOUT=/local/job40255479/worktrees/isa-integration-twenty-base bash "$P/run.sh" preparation-v2.py
TASK_LABEL=candidate bash "$P/run.sh" preparation-v2.py
# Wait for BOTH preparation terminals before decode/API/pickle.
bash "$P/matrix.sh" base
bash "$P/matrix.sh" candidate
TASK_LABEL=candidate TASK_SCENARIO=tests TASK_DATA=candidate/data-tests bash "$P/run.sh" tests.py new
TASK_LABEL=base TASK_CHECKOUT=/local/job40255479/worktrees/isa-integration-twenty-base TASK_SCENARIO=tests TASK_DATA=base/data-tests bash "$P/run.sh" tests.py new
TASK_LABEL=candidate TASK_SCENARIO=tests-existing TASK_DATA=candidate/data-tests-existing bash "$P/run.sh" tests-existing.py existing
# Oracle per head uses TASK_SCENARIO=default-all, TASK_DATA=<label>/data-default-all.
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" reference_prep.py
# Actual post-v2.sh preserved; it overwrote earlier populated child logs.
# A NEW replay must version its driver and use unique backend+key log names.
bash "$P/post-v2.sh" candidate
bash "$P/post-v2.sh" base
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" comparator-controls.py
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" check_cache_artifacts.py
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" assess-v2.py
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" warnings_scanner_check.py
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" freeze_check.py
TASK_LABEL=candidate TASK_SCENARIO=static bash "$P/run.sh" final_stats.py
```

The two oracles each ran once after their default-all cold/warm outputs existed. Base/candidate matrix roots were independent; pytest cohorts were sequential with two loadfile workers total. Preserve premature missing-terminal and absent-author-layout stops, shell metadata qualifications and populated child-log gap; do not replay completed prefixes to reconstruct them.

All actual runner, guard, oracle, exact comparator, probes, checker and sealing sources are manifest-pinned in checker-sources.json. Logs and raw operands stay private. Final packaging/sealing is metadata/artifact-only and invokes no Country/Feature/data API.

Sue
