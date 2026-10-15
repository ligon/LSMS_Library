SCOPE DEVIATIONS

None. The preparation phase made no library API calls. The subsequent final
validation uses ordinary APIs only on the authorized physical copy; source,
original corpus, and shared L2 remain unchanged. Diagnostic and test calls use
their own separate physical copies.
The copied files are data artifacts, not publishable survey evidence; control
reports remain aggregate-only. Raw warning logs must remain private.

Preparation status
------------------

The preserved cold corpus is at `runtime/data-corpus`; the new independent
copy is `runtime/data-final-promotion`. All 1,519 parquets / 340,885,785 bytes
have equal SHA-256 and distinct inodes. Both before and after copying, the
original fileset, SHA-256, size, mtime, device, and inode matched its preserved
manifest. Only `dvc-cache` is symlinked, to the shared immutable L1. The frozen
7fa05179c8519f44ea329ba6387f5d6d6441a8ea worktree is still clean and contains
no parquets. Evidence: copy-summary.json and copy-manifest.json.

The coordinator supplied final source 3914d8804d7b2936d75041cea98bbc9ddb6d4c9f.
The final metadata preflight, all 549 ordinary requests, trailing reads,
1,519-file raw comparison and 158 targeted tests completed. REPORT.md records
the four reviewed served exceptions and the single hashless nutrition exclusion;
423 country files finish fresh, rather than the hoped-for 424. The harness refuses a source other than the supplied exact SHA,
a dirty source checkout, the wrong package/config/data paths, unbounded CPU
affinity, cache bypass variables, or a duplicate completed run. Its source
diff from the completed cold runner is runner-adaptation.diff. No publication
or promotion script is armed.

Reusable evidence and final-source scope
---------------------------------------

The completed cold run enumerated every declared `Country.data_scheme` pair:
38 countries, 549 unique pairs, 530 successes, no empty results, 19 recorded
errors (Armenia 2 absent microdata; Nepal 11 unacquired licensed sources;
Peru 6 unwired declarations). Its data/config identity, per-table served
digests, wave coverage, raw-cache fingerprints, and final physical manifest
remain usable subject to final-source/hash comparison. The completed Feature
audit and its independent review remain evidence with their recorded findings;
they are not recast as a clean audit. Source-neutral fixture/docs/CI changes
do not erase those findings or require another full Feature sweep by themselves.

The user's explicit nutrition baseline approval supersedes historical hold
language in the archived corpus report and handoff. The two-field fixture
patch is hash-neutral. The proposed Uganda country assets script correction
is different: both `_input_hash` and `_table_cache_hash` fold **every** country
build input. Even one `_ / assets.py` edit therefore invalidates all Uganda
table hashes, including wave caches. A framework build-transform/import-closure
edit could broaden that to the full corpus. The final hash preflight measures
the exact scope before any table API call. Do not infer it from filenames alone.

Uganda's named idiosyncrasy is its two competing builders: canonical wave
concatenation and the country script reached through make/prerequisites.
CONTENTS says "assets still disagrees with its own script" and records
different output column/index layouts. The observed 790,688-row country-script
result versus 116,808 canonical wave rows corroborates this live caveat; an
ordinary warm API read alone is not the discriminating regression. The fix
owner must demonstrate the actual script/make route and inspect the raw output
before an API read can rebuild it. Final targeted tests belong on a separate
test cache so a test that clears or poisons caches cannot contaminate promotion.

Uganda CONTENTS also says the current country read gate "treats hashless
country parquets as stale and rebuilds" and names earnings/FCT after sibling
income/nutrition writes. This overrides the generic `cache_freshness` helper's
older `legacy` terminology. The original remains evidence of those two
hashless, manifest-less outputs; never manually stamp or normalize it.

Exact final-gate sequence (execution status in REPORT.md)
---------------------------------------

The coordinator supplies FINAL_SHA only after the assets fix and approved
nutrition fixture have landed on development. Substitute that exact 40-digit
SHA below. Worktree creation is the first source operation; no source edits.

```bash
export LSMS_FINAL_SHA=FINAL_SHA_FROM_COORDINATOR
git -C /local/job39919092/mirrors/LSMS_Library worktree add --detach \
  /local/job39919092/release-work/runtime/final-candidate "$LSMS_FINAL_SHA"
source /local/job39919092/release-work/control/runtime/final-promotion/final_env.sh

taskset -c 0-7 /local/job39919092/venv_img/bin/python \
  /local/job39919092/release-work/control/runtime/final-promotion/check_final_hashes.py \
  > /local/job39919092/release-work/control/runtime/final-promotion/hash-preflight.log 2>&1
```

Inspect `run/final-hash-impact.json` and `run/source-diff.txt` before proceeding.
Preflight enumerates all 549 declarations, compares every country expected
hash and previously observed wave expected hash to the cold run, inspects the
copied footers, and proves no original/copy file changed during this metadata
phase. A changed declaration/wave inventory is an explicit stop for review.

```bash
taskset -c 0-7 /local/job39919092/venv_img/bin/python \
  /local/job39919092/release-work/control/runtime/final-promotion/run_final.py \
  > /local/job39919092/release-work/control/runtime/final-promotion/final-run.log 2>&1
```

The reused runner invokes ordinary APIs in declaration order, two country
processes maximum, serial tables, build/make/BLAS pools fixed at one. The prior
cold snapshot supplies its comparison baseline; it never calls an API against
the original or shared root. Exit 1 is expected while the same 19 source gaps
remain, but must not be treated as success without checking the exact pairs,
error categories and all 549 records. A new error, missing/duplicate request,
measurement error, or source/cache identity issue stops promotion. Interrupted
runs are not blindly retried or appended; preserve diagnostics and plan an
explicit continuation. No full-suite repetition is implied by this cache pass.

After all country processes finish, run the legitimate trailing reads:

```bash
taskset -c 0-7 /local/job39919092/venv_img/bin/python \
  /local/job39919092/release-work/control/runtime/final-promotion/trailing_uganda.py \
  > /local/job39919092/release-work/control/runtime/final-promotion/trailing-uganda.log 2>&1

taskset -c 0-7 /local/job39919092/venv_img/bin/python \
  /local/job39919092/release-work/control/runtime/final-promotion/summarize_final.py \
  > /local/job39919092/release-work/control/runtime/final-promotion/final-summary.log 2>&1
```

The trailing sequence is `fct(), earnings(), fct(), earnings()` after the full
inventory has built nutrition/income. It compares served and raw fingerprints
to the preserved corpus, checks freshness and full-wave manifests after each
call, and snapshots all Uganda outputs again after the sequence. The repeated
reads check immediate stability; they do not claim future income/nutrition
builds cannot rewrite siblings again. If either output stays hashless, develops
a coverage gap, or changes values, report it and exclude it from promotion
until understood. No direct metadata stamping or bypass is used.

Review final served digests/shape/columns/index/dtypes/wave rows against the
cold run and every changed raw cache fingerprint, not merely row counts.
The expected final outcomes are the same 530 successful pairs / 19 documented
gaps and no unexplained value changes; these are checks, not assumed results.
Check every country parquet again at final quiescence, not only at call time.
If the trailing reads succeed, all 424 country files may finish fresh with
coverage, but that number must be measured. Preserve any exceptions explicitly.

All known hashes are pre-finalize cache identities. API outputs include
post-read transformations, so compare API-to-API and raw-to-raw separately.
Source/config equality proves only the code inputs; it does not substitute
for the final value and route checks. LSMS_SKIP_AUTH only skips initial auth
setup (#1001); existing credentials and shared immutable L1 remain available.

Final targeted tests use a physical copy of the completed final cache, with
the same final checkout and environment. The recorded prior 127-test selection
is exactly `test_962_unit_sentinel.py`, `test_food_kg_inference.py`, and
`test_uganda_invariance.py`; the guessed food_quantities filename is not used.
Add the reviewed assets/earnings modules as directed by the coordinator:

```bash
# First physically copy the quiescent final cache to a separate test root,
# retaining only the immutable dvc-cache symlink and verifying distinct inodes.
LSMS_DATA_DIR=/local/job39919092/release-work/runtime/data-final-targeted \
taskset -c 0-7 /local/job39919092/venv_img/bin/python -m pytest \
  -n 6 --dist=loadfile --timeout=600 --timeout-method=thread --durations=10 -q -ra \
  tests/test_962_unit_sentinel.py tests/test_food_kg_inference.py \
  tests/test_uganda_invariance.py tests/test_uganda_assets_script.py \
  tests/test_assets_additive.py tests/test_gh772_uganda_earnings_shape.py \
  > /local/job39919092/release-work/control/runtime/final-promotion/final-targeted.log 2>&1
```

Actual Makefile/raw structural evidence is owned by the framework agent on
the frozen fix. Reuse it if merge runtime is byte-identical; do not repeat
the destructive route just to duplicate evidence. The required evidence is
the country script's raw Value/(t,i,j) layout and duplicate/sanity checks,
exact canonical frame/attribute comparison, and the approved nutrition fields.

Promotion manifest and execution plan
-------------------------------------

1. At final quiescence, freeze the private root and produce a physical file
   manifest: relative path, SHA-256, bytes, mtime/device/inode, parquet row/column
   counts, stored/expected hash, hash policy, built-wave manifest, observed wave
   coverage, and pre-finalize fingerprint. Include final source SHA, declared
   inventory identity, source-diff/hash-impact identities, successful/failed
   pair counts, and the exact trailing-read evidence hashes.
2. Classify each candidate explicitly: country fresh/full coverage; wave fresh;
   script-wave hashless with verified cold/final-build provenance and legacy
   policy; companion/unclassified. A generic `legacy` label is not country
   approval. Do not promote a stale, unexplained, partial, or unverifiable
   country output. Unobserved wave/companion files require provenance review;
   never invent their expected hash. Keep exclusions in the manifest.
3. Re-verify the preserved original manifest and source cleanliness. No test or
   library call may mutate the final candidate after its manifest is signed
   off. Any later code change requires a new input/hash-impact check and the
   affected validation; do not silently relabel evidence with a newer SHA.
4. Parent reviews the manifest and the source/value/route evidence. Shared
   writing remains a separate final action under the authorized release scope.
   Before it, quiesce other builders/readers for the short replacement window
   and take a **new** read-only shared L2 inventory/hash snapshot. Earlier shared
   cache observations are not a concurrency lock or assumed truth.
5. Stage approved parquet files physically on the shared root's filesystem,
   outside the active country paths; verify staged SHA-256 against the signed
   manifest. No symlink/hardlink writable parquets, auth files, SQLite, locks,
   DVC metadata, or source files enter staging. The private root currently
   contains only parquets plus its L1 symlink; never copy that symlink over L1.
6. Use a reviewed per-file atomic replacement journal with recoverable backups
   and a precondition check against the just-taken shared inventory. Abort on
   concurrent changes. Retain explicit rollback mapping and original hashes.
   Omitted or obsolete files are not silently deleted: list them separately
   for retention or backed-up retirement. If a hashless country sibling must
   be excluded, do not leave an older shared counterpart masquerading as the
   promoted result; give it an explicit backed-up retirement disposition.
7. Do **not** swap the whole shared root. Its dvc-cache is an actual directory,
   whereas the private copy's link points into that same root; a whole-root
   swap would risk replacing L1 with a self-referencing link. Keep L1 in place.
   The current shared root is a real directory on device 4079531836; stage on
   that same filesystem so individual `os.replace` operations are atomic.
8. Verify promoted bytes, footers, coverage and journal completeness read-only.
   Ordinary API smoke checks, if requested, must use final source and are
   recorded separately because they may rebuild. Check the original corpus
   preservation once more. Retain backup/staging/evidence until the parent
   decides the release rollback window is over.

The coordinator subsequently authorized preparation of the promotion
implementation. `promote_cache.py` now implements this journalled procedure;
it has been exercised only with synthetic local bytes. The six checks cover
normal apply/retire/rollback, interrupted apply, changes before apply, changed
source, concurrent rollback conflict, and path/symlink rejection. L1 was
preserved in every synthetic case. It has not read or written the real caches.
Its exact write/retirement allowlist still depends on final source,
post-trailing metadata, parent review, and a live shared snapshot. The tool
requires the reviewed manifest SHA-256 and final SHA for every command, fixes
the production roots, verifies staged bytes, fsyncs journal intents before
atomic mutations, retains backups, and refuses blind partial-apply resumption.
It cannot lock unrelated library processes; the parent's external quiescent
window remains required. PROMOTION_COMMANDS.md describes the exact reviewed
write flow and rollback. No global clear, DVC CLI call, cache-bypass loop, or
manual restamping is present.

Timing
------

The earlier full cold corpus took 75.47 minutes at two country processes.
The current script-only proposal should require rebuilding Uganda rather
than all countries, but a complete ordinary 549-pair pass still records final
coverage. Final exact hash impact determines the actual rebuild scope. The
allocation ends at 22:41 UTC; final source, targeted CI, and the reviewed
promotion manifest must be ready early enough to complete and verify the
write window, otherwise retain the untouched shared cache and durable evidence.
