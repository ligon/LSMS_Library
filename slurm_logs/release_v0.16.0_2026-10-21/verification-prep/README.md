# Release 0.16.0 verification preparation

Prepared by Sue. Source, tags, GitHub and original caches are unchanged.
No package import, project test, build or dependency installation has run in this preparation.
Only the metadata-gate synthetic controls and physical-copy verification execute now.

## Parent-owned Makefile gate

Wait for the parent's final merged-master full SHA and automatic **master push** `ci.yml` run ID.
The parent reviews this invocation, substitutes those two values plus the exact master checkout path,
and runs the actual Makefile release target **once**. This command creates the annotated tag only after
both the local suite and exact remote workflow succeed. It is deliberately not an executable launcher here.

```bash
RELEASE_PREP=/local/job40255479/control/release-v016/verification-prep-seed1519
export RELEASE_REPO=/local/job40255479/worktrees/REPLACE_WITH_FINAL_MASTER_CHECKOUT
export RELEASE_EXPECTED_SHA=REPLACE_WITH_40_CHARACTER_FINAL_MASTER_SHA
export RELEASE_CI_RUN_ID=REPLACE_WITH_EXACT_MASTER_PUSH_RUN_ID
export RELEASE_OUT=/local/job40255479/control/release-v016/make-release-final
export RELEASE_COPY_MANIFEST="$RELEASE_PREP/copy-manifest-private.json"
export RELEASE_CI_WAIT_SECONDS=10800
export PYTHONPATH="$RELEASE_REPO:$RELEASE_PREP"
export LSMS_COUNTRIES_ROOT="$RELEASE_REPO/lsms_library/countries"
export LSMS_DATA_DIR=/local/job40255479/release-v016-seed-data
export LSMS_EXPECTED_ROOT="$RELEASE_REPO"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1 MAKEFLAGS=-j1
export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1
unset LSMS_NO_CACHE LSMS_SKIP_AUTH PYTEST_ADDOPTS PYTEST_PLUGINS LSMS_ALLOW_INTREE_SOURCES
cd "$RELEASE_REPO"
taskset -c 0-7,20-27 make release v=0.16.0 \
  POETRY="$RELEASE_PREP/poetry-pytest-only" \
  PYTEST_ARGS='-n 16 --dist=loadfile --timeout=600 --timeout-method=thread --maxfail=1 --tb=short -ra --durations=25' \
  > /local/job40255479/control/release-v016/make-release-final.log 2>&1
```

The real `release` recipe has no `setup` prerequisite. The adapter accepts **only** `run pytest`;
it never invokes Poetry, setup, install, build or an environment-adoption mechanism.
It uses `/local/job40255479/venv_img/bin/python`, asserts checkout/package/config/data identity
in the same process before `pytest.main`, and repeats those assertions in all 16 xdist workers.
It checks the copied file set/hashes before launch. The current copied cache has 1,519 files /
341,221,428 bytes from the original 1,519-file seed, checked against the exact manifest used at
the 532 suite START (SHA256 `c54c8efa4a64da4d7eca8d24d4b5f838b2cbb1d9c7b8413b794e427a27de2e1d`).
The source `/local/job40255479/followup-farm-data` remains unchanged. The first preparation and
its separate 1,535-file cache copy are preserved untouched and are not used by this controller.

The full suite runs with the ordinary Makefile semantics (no `--no-purge`, no rebuild override),
16 loadfile workers and 600-second thread timeout. Private log/JUnit/outcomes capture every result.
The adapter requires all worker identities and no outcome/reason changes against the completed
exact-532 suite, including directly observed XPASS identity. Any change needs parent inspection;
it does not authorize tagging because pytest alone returned zero.

After pytest returns and xdist shuts down, the adapter writes its complete local summary and performs
read-only `gh api .../actions/runs/<id>` GETs every 45 seconds. It validates run ID, full SHA,
`push` event, `master`, repository and `ci.yml` path, printing only status changes. Only completed
workflow `success` permits a zero adapter exit and the Makefile tag line. Failed/cancelled/timed-out
CI, identity mismatch, wait expiry or five consecutive metadata-read errors fail closed.
The tests are not rerun while waiting. The default wait cap is three hours; the parent must ensure
the allocation has enough time. Tag push, GitHub release and publishing remain parent-owned.

## Published artifact verification (future, parent-authorized execution)

After the exact tag has been published to PyPI, use a new output directory:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  taskset -c 16-19 /local/job40255479/venv_img/bin/python \
  "$RELEASE_PREP/verify_published.py" \
  --repo "$RELEASE_REPO" --expected-sha "$RELEASE_EXPECTED_SHA" \
  --out /local/job40255479/control/release-v016/published-0.16.0
```

This reads the exact 0.16.0 PyPI JSON, downloads the one wheel and one sdist, checks their lengths
and SHA256 digests, then compares all 11,753 tracked package files byte-for-byte to **tagged Git
objects**. It verifies both versions and the `lsms-library = lsms_library.cli:main` entrypoint,
and refuses raw binary survey/cache payload. It does not build the sdist or claim a wheel/sdist
build relationship beyond comparing their independently downloaded payloads.

The wheel installs with `--no-index --no-deps --no-compile --target` into a new private target.
The configuration smoke runs outside all checkouts using that target and the image's existing
dependencies, with package/config/data identities asserted, network refused and data-build APIs
blocked. It does not run any Country/Feature data method or assess installed microdata. It makes
no fresh dependency-resolution, all optional-import, or complete installed-wheel test claim.

## Copy and stop

`COPYLIST.txt` lists the preparation/controller and aggregate files. The cache and full per-file
manifest remain private; no original root is modified. Keep raw future logs/JUnit/outcomes private.
Preparation stops before any final-master tests, builds, import, tag or publication.
