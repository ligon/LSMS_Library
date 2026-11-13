# Completed commands and replay scope

Run directory: /local/job40255479/worktrees/isa-Uganda-1198.
Mounted interpreter: /local/job40255479/venv_img/bin/python.
Private root: /local/job40255479/control/isa-backlog-20261021/Uganda/1198.
Ledger FIRST4332afede, then private prepare.py and bootstrap.py. prepare.py
verified frozen-manifest input receipts before any unpickle or normal API.

The successful bounded invocations were:

```sh
P=/local/job40255479/control/isa-backlog-20261021/Uganda/1198
TASK_SCRIPT=prototype.py "$P/run.sh" diagnostic.py > "$P/prototype-v4-private.log" 2>&1
TASK_SCRIPT=downstream.py "$P/run.sh" diagnostic.py > "$P/downstream-v2-private.log" 2>&1
TASK_SCRIPT=controls.py "$P/run.sh" diagnostic.py > "$P/controls-v2-private.log" 2>&1
TASK_SCRIPT=alignment_audit.py "$P/run.sh" diagnostic.py > "$P/alignment-v3-private.log" 2>&1
TASK_SCRIPT=attrs_controls.py "$P/run.sh" diagnostic.py > "$P/attrs-private.log" 2>&1
```

run.sh sets exact cwd/PYTHONPATH/countries/config/data/XDG/temp/DVC paths,
PYTHONDONTWRITEBYTECODE, offline socket and source-write guard through
sitecustomize, unshare-rn, and all numeric/build/Make threads1. Each script
asserts package/config/data identity before reading pickle inputs. No concurrent
consumer shares its cache. Full output/frames remain private. The safe
exception wrapper must remain in use; never print a raw failure log.

These are completed commands, not permission to write the now-frozen root.
Replay requires a NEW physical evidence/cache copy, verified SHA/size/mode/
mtime/nlink receipts, a new approved metadata target/root receipt and updated
run/guard constants. Retain original frozen roots. No source acquisition,
manual hash stamp or assume-fresh/read-gate bypass is part of a replay.

The future real-yield test is executable with the existing API once an approved
current/full-coverage plot_features prerequisite is physically staged:
load each saved public projection under the same identity guard; call
harvest_kg and yield_kg(projection, actual_plot_features, on='parcel') and
on='plot'; independently reconcile source area keys/units/masks and compare
complete frames at check_exact=True,rtol=atol=0 plus typed attrs. Report
coverage/denominators, without substituting synthetic areas for survey areas.
