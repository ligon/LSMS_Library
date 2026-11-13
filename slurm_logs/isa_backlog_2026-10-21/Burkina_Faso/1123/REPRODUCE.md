# Reproduction commands

The actual before/after run used mounted image
`/local/job40255479/venv_img/bin/python`, baseline worktree
`/local/job40255479/worktrees/isa-baseline` at f8 and candidate
`/local/job40255479/worktrees/isa-Burkina_Faso-1123`.
Private scripts, complete frames, warning bodies, full hash inventories and
logs live at:

```sh
P=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1123/implementation
```

`run.sh` asserts matching real import/config paths in each script, changes cwd
to the selected checkout, sets exact PYTHONPATH/LSMS_COUNTRIES_ROOT, private
LSMS_DATA_DIR/CONFIG_DIR/TMPDIR/issues log, threads/build/Make workers to one,
adds the mounted image to PATH and executes under `unshare -rn`. The namespace
contains only loopback. Raw sources use sanctioned helpers and pre-copied held
blobs only. No in-tree raw file or parquet is used.

Initial physical preparation ran once. It refuses a Burkina L2 tree, verifies
eight source sidecar MD5s and physically copies held L1. It copies only verified
Benin/Togo companion/plot caches with current hashes/full coverage. No Burkina
cache is seeded. The 12 comparator file SHA256s and all source MD5s are
recorded privately; source-safe source receipts are in validation.json.

Actual commands, each redirected to its own private log:

```sh
bash "$P/run.sh" candidate "$P/prepare.py"
bash "$P/run.sh" baseline "$P/build.py" baseline cold
bash "$P/run.sh" candidate "$P/build.py" candidate cold
bash "$P/run.sh" baseline "$P/build.py" baseline warm
bash "$P/run.sh" candidate "$P/build.py" candidate warm
bash "$P/run.sh" candidate "$P/compare.py"
bash "$P/run.sh" candidate "$P/run_tests.py" candidate
bash "$P/run.sh" baseline "$P/run_tests.py" baseline  # expected exit 1: 8fail/1pass
bash "$P/run.sh" candidate "$P/run_tests.py" comparators
bash "$P/run.sh" baseline "$P/hashes.py" "$P/baseline-hashes.json"
bash "$P/run.sh" candidate "$P/hashes.py" "$P/candidate-hashes.json"
bash "$P/run.sh" baseline "$P/feature.py" baseline
bash "$P/run.sh" candidate "$P/feature.py" candidate
bash "$P/run.sh" candidate "$P/accounting.py"
bash "$P/run.sh" candidate "$P/api_audit.py"
```

The API audit calls the actual Make target with a private `POETRY` adapter that
accepts only `run python` and invokes the mounted image. It creates/adopts no
venv and changes no dependencies. All pytest commands were serial, with private
cache and temporary directories. New tests run the actual wave script with a
write spy for mutations and compare real source payloads by native identities;
they neither skip real build failures nor rely on post-collapse uniqueness.

A portable focused invocation once a private held-L1 root is prepared is:

```sh
cd /path/to/checkout
export PYTHONPATH="$PWD" LSMS_COUNTRIES_ROOT="$PWD/lsms_library/countries"
export LSMS_DATA_DIR=/path/to/private-data-root
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1
unshare -rn /path/to/mounted-image/bin/python -m pytest -q \
  tests/test_burkina_faso_plot_inputs_identity.py
```

Supply the usual read-credential path for the data-test gate; held blobs serve
all source reads and the namespace prevents acquisition. The report's cold
claims refer to the first physically absent Burkina roots, not a repeated warm
call. Do not clear shared caches or stamp a stale parquet to repeat them.

Sue
