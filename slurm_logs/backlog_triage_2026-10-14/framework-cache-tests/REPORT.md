# Backlog triage `framework-cache-tests` -- 17 issues, read-only (2026-10-14, development @ a7cf04f2d)

Records: `records.json` (one object per issue, all 15 required fields plus `priority_why`). Logs: `802_pytest.log`, `cold_builds.log`, `issues/`. Nothing was committed, pushed, or written outside `$STAGE`; `git status --short` is empty.

## 1. Table

| # | tier | type | tractability | dedup | prio | root_cluster | approach (one line) |
|---|---|---|---|---|---|---|---|
| 968 | D | framework | blocked | partial (open PR #969 is the fix) | 3 | cache-mechanism-in-fingerprint | merge #969 (`_build_wave_script_target`, excluded from fingerprint, no-make fallback) |
| 932 | C | infra | human | open (#973 leaves data-tests gated to master) | 3 | ci-skip-is-the-primary-path | choose: data-tests on the development merge queue vs nightly vs document |
| 907 | C | test | auto | open (0 of 56 files marked) | 3 | ci-skip-is-the-primary-path | mark 56 files `requires_s3` (per-test where mixed); net warns on unmarked |
| 909 | C | test | auto | open | 4 | ambient-cache-state-in-tests | `_compute_panel_spine` reads `lsms_cache_waves`, falls to API, says coverage |
| 908 | C | framework | auto | open | 3 | cache-mechanism-in-fingerprint | extract read gate into `Country._read_l2_country_cache`, exclude; same window as #969 |
| 905 | C | perf | auto | open | 4 | groupby-python-callable | vectorise `.sum(min_count=1)` in build_transforms + 3 Panama scripts |
| 920 | C | perf | human | partial / rescope (09-18 Ghana triage; #809/#866 closed the only miss loop) | 3 | two-builders-one-table | rescope to warning text + read-path profiling; drop "never reads var/" |
| 915 | C | framework | auto | open | 3 | country-name-validation | raise with difflib suggestions now; decide on 11 unregistered dirs |
| 802 | D | test | auto | **resolved** (735aab33f; 11 passed today) | 5 | resolved-env | close |
| 798 | B | data-gap | diagnose-then-auto | partial (does not reproduce; snapshot 12c0dec2b stale; fix unpinned) | 3 | coverage-snapshot-stale | regrade `make matrix`; bisect only if still dropped |
| 792 | C | bug | auto | partial (nothing landed; 2 of 4 "real" items are not defects per CONTENTS.org) | 4 | sample-design-absent-weights | close the handle at `_build_registry.py:460`; adjudicate Panama 1997 / Serbia 2002 |
| 790 | C | framework | auto | partial (db2e63b18 landed un-ignore + guard; 5 writers remain) | 4 | cache-mechanism-in-fingerprint | delete 5 `to_csv('unitlabels.csv')` writes + 2 dead readers |
| 788 | B | framework | human | partial (laundering fixed by #809 + #866; two-builder divergence open) | 3 | two-builders-one-table | decide framework- vs script-authoritative; delete concatenate-only scripts |
| 778 | B | test | diagnose-then-auto | partial (#891 manifest records requested waves, not contributing ones; no skip) | 3 | ambient-cache-state-in-tests | 2-worker cold repro first; writer lock if real |
| 740 | C | test | auto | partial (wave parquet materialises cold; xfail XPASSes; fix unpinned) | 4 | ci-skip-is-the-primary-path | remove xfail, add the two checks |
| 734 | D | infra | auto | open | 4 | venv-per-clone-state-in-shared-location | idempotent `pytest_addoption` + neutral-cwd recipe in CLAUDE.md |
| 703 | C | test | auto | open | 4 | ambient-cache-state-in-tests | parametrise over expected set; skip-with-reason per missing cache |

## 2. Reconcile

**Close now (verified):**
- **#802** -- `735aab33f` (2026-09-10) gates the credential-only remote stanza on `<countries_root>/.dvc/config` existing, which is exactly the synthetic-root case these tests build. Diff read. Ran all 11 named tests on this Savio node today: **11 passed, 0 ConfigError** (`802_pytest.log`).

**Partial comment needed:**
- **#740** -- does not reproduce: cold private-root `Country('Pakistan').cluster_features()` writes `Pakistan/1991/_/cluster_features.parquet` via `run_make_target -> try_script` (`country.py:4127`), serves 300 rows on `(t, v)`, no `GrainCollapseWarning`; the live cache holds the same wave parquet (today 15:55). Fix commit unpinned. Close only after deleting the `xfail` at `tests/test_pakistan_cluster_features.py:82`.
- **#788** -- directions (3) done: `#809` `836289d47` (PR #918) grades hashless country parquets `stale` (`country.py:4493`); `#866` `88ac40464` (PR #924) evicts a stale country parquet before the make/script fallback. Open: the two-builder divergence (`Uganda/_/assets.py` writes `assets` on `(i,t,m)`, served table is `Value` on `(t,i,j)`), and the (1)/(2) decision.
- **#790** -- `db2e63b18` (2026-09-06) landed options 1 and 3 (`.gitignore:99-106` by-name un-ignore; `api_surface_audit.check_untracked_inputs` + test). Open: five side-effect writers (GhanaSPS:124, GhanaLSS:282, Ethiopia:241, Togo:127, Tanzania:527), not three.
- **#798** -- both halves no longer reproduce: Mali `interview_date` cold-builds all four waves (3,804/8,390/6,602/6,143); live Tanzania parquet holds all six `t` values and its hash equals today's expected hash; Guatemala/Panama `nutrition` are `undeclared/absent` in the 09-18 snapshot. The committed `latest.csv` (`12c0dec2b`) still says `dropped`; fix unpinned (between 09-09 and 09-13: `ef50e5dd7`, `cfcb9b18b`). Needs a regrade this agent may not run.
- **#778** -- `#891` (`ef50e5dd7`) added `lsms_cache_waves`, but it records the requested/union set by design, so step (1) is only nominal and step (2) conflicts with that decision; no skip (3). The failing assertion (`nan == 5570`) shows the household present with a null value, which does not match the "missing waves" hypothesis.
- **#968** -- fix is in open PR #969 with `tests/test_gh968_no_make_fallback.py`; blocked on that PR.
- **#920** -- already rescoped by the 2026-09-18 Ghana triage (`BACKLOG_TRIAGE.org:35`); add: the on-disk index without `v` is the documented pre-finalize shape, today's live GhanaLSS `food_acquired` hash equals the expected hash from this clone, and v0.13.0 (09-13) predates #809/#866 (09-14).
- **#792** -- GhanaSPS 2013-14/2017-18 and Tajikistan 1999 are producer-side absences (CONTENTS.org), not the #713 class; handle leak still live at `_build_registry.py:460`.

## 3. Dispatchable now

Tier A/B, auto, no decision: **none** (the two B-tier issues, #788 and #778, need a decision and a diagnosis respectively).

**Sequencing rule first.** #969 forces one corpus cold re-warm (`grab_data` edit). The 10-14 handoff already bundles #810/#868/#970 into one window. Put #908's extraction, #790's five `{country}.py` edits, and any #778 writer lock in that same window; each of them alone costs a second corpus or multi-country re-warm.

**C-tier mechanical, auto, no decision (ordered by leverage):**
1. **#907** mark the 56 files -- cache cost 0; biggest CI win; also makes #932's chosen option cheaper.
2. **#908** extract the read gate + exclude -- cost: one corpus re-warm, free only inside the #969 window.
3. **#792 part 1** `read_text` at `_build_registry.py:460` -- cost 0 (assert with a hash probe).
4. **#905** vectorise the reducer -- cost: `food_acquired` re-warm for Nigeria/Tanzania/Ethiopia/Malawi/Uganda/Panama.
5. **#703**, **#909**, **#740 un-xfail**, **#734 (1)+(4)**, **#915 half 1** (always-raise with suggestions) -- cost 0 each.
6. **#790** delete the five writers -- cost: full re-warm of five countries; window only.
7. **#798** regrade (human runs `make matrix`); bisect only if Mali/Tanzania still grade `dropped`.
8. **#778** diagnosis agent: two-process cold GhanaLSS `food_acquired` into one private root, diff vs serial.

## 4. Escalations

- **#932**: A) gate the development merge queue on `data-tests`, or B) nightly only? (#973 makes the queue the gate.)
- **#788**: A) framework authoritative -- delete concatenate-only country scripts as #808 did, or B) script authoritative when `materialize: make` is declared?
- **#915**: also raise for an existing directory with no `_/data_scheme.yml` (11 dirs incl. `.dvc`, Afghanistan #702)? yes/no.
- **#908 + #969**: land both in the next re-warm window? yes/no.
- **#920**: rescope to warning text + profiling and drop the "cache never read" claim? yes/no.

## 5. Contradictions with CONTENTS.org / CLAUDE.md

- **#792 vs GhanaSPS/Tajikistan CONTENTS.org.** The issue files GhanaSPS 2013-14/2017-18 and Tajikistan 1999 null weights as "the same class GH #713 fixed". `GhanaSPS/_/CONTENTS.org:449`: *"No panel weight exists in any wave, and waves 2 and 3 have no sampling weight at all -- all 189 of their .dta files were swept ... This is positive evidence"*. `Tajikistan/_/CONTENTS.org:132-136` quotes the 1999 documentation: *"Since the sample is nationally representative there are no weighting variables."* Those two are not-asked, not wiring gaps.
- **#798 vs `Mali/_/data_scheme.yml:70-73` vs CLAUDE.md.** The YAML comment says the per-visit rows are collapsed "via `groupby.first()`, keeping the first visit's date (GH #325)". CLAUDE.md (GH #871) says core now serves `most_complete_row`, never `first()`. The loud `GrainCollapseWarning` on Mali 2014-15/2017-18 (3,746 of 7,608 and 8,390 of 16,780 rows) is that deliberate two-visit collapse, not a new defect; the comment text is stale.
- **#920 vs CLAUDE.md.** The issue's "index discrepancy" lead (on-disk index lacks `v`) is the documented pre-finalize shape; `v` is joined from `sample()` in `_finalize_result`. Not a miss cause.
- **#909 vs its own code.** `diagnostics._compute_panel_spine` docstring lists the API as preference 1; the code (`diagnostics.py:1258`) reads the cached parquet first.
- **#734 vs this environment.** The mounted squashfs venv used by this job has no `lsms_library.pth`, so the double-registration cannot bite here; it bites the main `.venv` only. CLAUDE.md addendum 3 still lacks the 08-25 "neutral cwd" recipe.
- **#788 vs CLAUDE.md.** CLAUDE.md names a *different* still-open laundering route (`run_make_target` appending the country target to a wave's `unique_candidates`, `country.py ~3876/3933`); that is not this issue and stays open on its own.

## Method and limits

Static reads against `development @ a7cf04f2d` with `path:line` cites; live-cache parquets read with pyarrow only (footer metadata, null fractions); expected hashes computed from this clone with `Country._table_cache_hash` (no writes; private `LSMS_DATA_DIR` held only Mali and Pakistan afterwards). Two cold builds (Mali `interview_date` 5.9 s, Pakistan `cluster_features` 8.0 s) and one 77 s pytest run. Not done: a Tanzania cold build (relied on the live parquet's hash match), the coverage regrade (forbidden), and pinning the commits that made #740 and #798 stop reproducing.
