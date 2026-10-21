The reviewed checkout is frozen at `55ec81b5f04aed8239128d8453aa7dd5ee0f2d25` in `/local/job40255479/worktrees/isa-integration`.
All scripts and outputs below are private. Do not replay preparation or materialization over preserved evidence.

The commands executed were:

```sh
P=/local/job40255479/control/isa-backlog-20261021/integration/uganda-twelve-review
bash "$P/run.sh" "$P/prepare.py" > "$P/preparation-private.log" 2>&1
bash "$P/run.sh" "$P/materialize.py" > "$P/materialization-private.log" 2>&1
bash "$P/run.sh" "$P/api.py" initial > "$P/initial-api-private.log" 2>&1
bash "$P/run.sh" "$P/tests.py" scoped > "$P/scoped-tests-private.log" 2>&1
bash "$P/run.sh" "$P/tests.py" support > "$P/support-tests-private.log" 2>&1
bash "$P/run.sh" "$P/api.py" warm > "$P/warm-api-private.log" 2>&1
bash "$P/run.sh" "$P/finish.py" > "$P/final-proof-private.log" 2>&1
```

Initial API and scoped tests ran concurrently; fresh-process warm API and supporting tests ran concurrently, after both earlier processes finished. Each pytest selection used `-n 3 --dist=loadfile`; the API process used one numerical thread.

For a fresh replay, first physically copy only these private harness files (`run.sh`, `sitecustomize.py`, `common.py`, `prepare.py`, `materialize.py`, `api.py`, `tests.py`, `finish.py`) into a new empty sibling directory under `integration/`, and update `TASK_EVIDENCE` in its private `run.sh` to that directory. Keep `TASK_ROOT` pinned to the reviewed immutable checkout. `prepare.py` computes the same sibling API11 source and independent review reference roots and makes physical, verified copies; it intentionally refuses an existing data/reference destination. Run preparation, inspect `initial-grades.json` and the held-source receipts, then run materialization. Before probes, `assert_ready()` requires all eight country hashes and eight-wave manifests; no manual stamp or bypass is permitted.

`run.sh` contains the exact mounted-image interpreter, realpath environment, private data/config/DVC-site roots, network namespace and active socket guard, and numerical/build/Make thread limits. `tests.py` contains the exact 25- and 41-case selections. `api.py` performs complete logical frame/attrs/matrix comparisons to both physically copied f8 and independently reviewed #1191 references. Full raw output remains private.

The preserved `warning-accounting.json` proves the only warning-count difference from the prior one-object API probe: four new Region Country objects each replay the same eight existing grain messages. Its four named production function ASTs are exact to f8. This accounting neither filters warnings nor normalizes data.

Sue.
