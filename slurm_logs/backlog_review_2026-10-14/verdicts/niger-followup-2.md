VERDICT: pass-with-fixes -- every number in the two follow-up commits (4998ef4d2, b6feef578) reproduces exactly on a cold rebuild in my private root and on the raw files, the narrowed universe test is exact (fails on the old form at hid 27010 only), `make panel-ids` is a no-op, and the full suite is green apart from a failure already fixed on development; the fixes are process: the report drafts no residuals for the three still-OPEN multi-country issues the branch partly resolves (#824, #833), and the "before" figures should say which code built them.

Reviewer: red-team element `niger-followup-2`, Slurm job 39922659, 2026-10-14. Branch `fix/niger-followup-2` @ b6feef578, merge-base with development 340e30f1e (development has since moved to c7377eca4). Private `LSMS_DATA_DIR` seeded pre-fix (sample 2014-15 = 3,558). Import identity asserted (`/local/job39922659/clone/lsms_library/__init__.py`). GitNexus MCP was unavailable; I substituted `git diff`/`rg` and the suite, as CLAUDE.md permits.

## 1. Refutations (every claim run, not read)

Scope note: the task was the four follow-ups (a-d). The whole branch vs development is 23 files (three-dot diff), all `Niger/**` config/scripts/docs + `tests/test_niger_*.py`; the two follow-up commits touch exactly the four named files.

**(a) 280 / 367 are all-raw-row figures; 17 / 50 on kept rows; served plot_inputs 2014-15 collapse 35/29 (was 88/82).** HELD. `census.py` on `ECVMA2_AS4AP2.dta` and `ECVMA2_AS02CP1.dta` via `get_dataframe(convert_categoricals=False)`: livestock raw 41,200 rows -> 280 (GRAPPE,MENAGE,AS4AQ04) dup groups, 0 with EXTENSION; owned 6,758 -> 17 / 0. plot_inputs raw 36,890 -> 367 / 0; applied 8,179 -> 50 / 0. Pre-fix `var/plot_inputs.parquet` embedded `lsms_grain_audit` 2014-15: destroyed 88, groups 82; rebuilt: destroyed 35, groups 29 (and the `GrainCollapseWarning` fired with the same numbers during the cold build).

**(a) s16d multiplicity == s16c harvest lines for 100% of 8,691 pairs; == distinct (field,parcel) plots for 1,729 / 65.7% of 2,633 multi pairs; 2-8 rows; 553 sale-bearing; 3,329 dropped; 5,786 of 6,174 exact; all 1,320 sale rows exact.** HELD, every figure: s16d 12,232 x 47; pairs 8,691, 0 unmatched, mult==hlines 8,691/8,691; multi 2,633 (rows 6,174, max 8, hist {2:1954,3:514,4:133,5:14,6:5,7:12,8:1}); mult==plots 1,729 (65.7%); sale-bearing multi pairs 553; `duplicated(keep='first')` 3,329; exact-dup rows among multi-pair rows 5,786; 1,320 sale-bearing multi-pair rows all duplicates.

**(b) KNOWN_STRAY names 27010; narrowed, not weakened; test was red before.** HELD. Copied the add102cbb version of the test into `tests/` and ran it against the rebuilt root: `AssertionError: crop_production 2011-12: 1 household ids not in sample(), e.g. ['27010']`. Branch version: 139 passed on `tests/test_niger_*.py` (recipe, -n 4). Raw: hid 27010 has 2 lines in `ecvmaas2e_p2.dta`, 0 in `ecvmamen_p1_en.dta`, 0 in `ecvmaind_p1p2_en.dta`; it is the only harvest hid absent from the individual file. `null_index_reports()` on the rebuilt root: crop_production `v` 2 NaN rows, 2011-12 -- unchanged from pre-fix. The assertion is set-equality, so a vanished 27010 fails too (by construction).  Mandate §6, stated plainly: this universe test would ALSO pass on origin/development -- there crop_production and sample both key 2014-15 on the two-column id, so the 2014-15 stray is empty either way and 2011-12's stray is {27010} under either key.  It is an exact pin, not a #679 discriminator; the discriminators in that file are HOUSEHOLDS=3617, LINKED_2014_15=3529 and the person-index uniqueness checks (not re-run on development: development code against my branch-built root would rebuild over it).

**(c) Five of 3,251 links dropped, named; 3,246 carry over identical, 0 change target; other waves byte-identical.** HELD. `xwalk2.py` on `git show origin/development:…/panel_ids.json` vs HEAD, matching old key k to new k+'0'/k+'1': old 3,251, new 3,529 (3,452 ext-0 + 77 ext-1), carried 3,246, changed 0, lost 5 = {89005->8905, 95001->9501, 128011->12811, 101010->10110, 201010->20110}. 2021-22 map 4,589 = 4,589, identical. Cover sheet `ECVMA2_MS00P1.dta`: pairs (89,5),(95,1),(128,11) have EXTENSION set {2}; (101,10),(201,10) have {0}; (1,11) and (2,11) exist with {0}, so their ids are 10110/20110; hid11 contains 8905, 9501, 12811, 10110, 20110, 10010, 20010, 20012 (all True) and not 27010. Households 3,617 on 3,558 pairs; ext hist {0:3478, 1:77, 2:62}; pair-set hist {0}:3421, {1}:75, {0,2}:57, {2}:3, {1,2}:2 -- all as CONTENTS states. `make -C lsms_library/countries/Niger/_ panel-ids` (needs the venv on PATH; the Makefile calls bare `python`) -> 3529 / 4589 linked, `git diff --quiet` exit 0 on both JSONs: NO DIFF.

**(d) household_characteristics 2014-15 3,558 -> 3,616; 2021-22 food_expenditures 98,249 -> 98,231, food_prices 98,247 -> 98,244, food_quantities 131,549 -> 131,522, food_acquired 131,549 -> 131,544 (239,882 -> 239,877).** HELD, all seven; see table. The claim "every other 2014-15 cell serves the same row count" (b6feef578) HELD: cluster_features 270 = 270 and community_prices 6,500 = 6,500 are the only 2014-15 cells not in the list and both are unchanged; every listed 2014-15 move reproduces (sample 3,558->3,617, roster 22,506->22,671, people 26,369->26,579, assets 17,070->17,267, plot_inputs 8,071->8,134, livestock 4,998->5,018, indiv_ed 2,821->2,834, crop 8,753->8,799, plot_labor 15,803->15,935, shocks 3,771->3,781, housing 3,558->3,617, food_security 3,557->3,616).

**#865 served moves (b6feef578): crop_production 2011-12 11,948 -> 11,937; plot_inputs 2021-22 12,433 -> 12,432; no other wave moves.** HELD; sentinel counts pre-fix Quantity {999999:11, 9999:23}, Quantity_sold {9999:5, 999999:3}, Value_sold {9999999:7}; rebuilt: none. `Unknown` 4,920 -> 4,909 and 75 -> 74 (test_niger_u_sentinel pins) HELD.

**"Before measured on the pre-fix warm root" is development's number, not a stale artefact.** HELD, checked harder than claimed: a cold build with origin/development code (worktree, fresh scratch root) gives sample 3,558, household_roster 2014-15 22,506, crop_production 11,948/8,753, plot_inputs 8,071/12,433, household_characteristics 3,558 -- identical to the seeded snapshot.

**Cache cost: 22 of 22 Niger table hashes move.** HELD: `_table_cache_hash` under `LSMS_COUNTRIES_ROOT=<add102cbb worktree>` vs HEAD, 22 probed, 22 moved (the panel_ids.py docstring is a `_/*.py` build input). One Niger re-warm; not paid on the live root by me.

**Regeneration/rebuild claims.** Cold rebuild of all 22 buildable entries after `cache clear --country Niger` on MY root: BUILT 22, FAILED 0, ~3 min. No new warning class vs the pre-fix audit (GrainCollapse on plot_inputs x4 waves, crop_production 2011-12/2021-22, cluster_features 2014-15; NullRead on sample.strata etc.; NullIndexKey crop_production v=2). `find lsms_library/countries -name '*.parquet'` = 0; `git status --short` empty after `make panel-ids`.

**CONTENTS.org / idiosyncrasy (mandate §7).** The REPORT names four: EXTENSION as the third element of the 2014-15 household key (3,617 households on 3,558 pairs, ext 2 = split-off not linked); the plot-less crop-level sale block (2014-15 AS2E2, 2021-22 s16d) with s16d's per-harvest-line replication; hid 27010 as a harvest-only 2011-12 household; and the two coexisting 2011-12 sentinels (u='Unknown' vs the all-nines value codes).  All four check out against the raw files above; nothing the branch wrote into CONTENTS.org contradicts the data I read.

## 2. Claimed vs measured

| claim (commit / CONTENTS) | claimed | measured |
|---|---|---|
| livestock raw dup groups / owned | 280 / 17 (0 / 0 w/ EXTENSION) | 280 / 17 (0 / 0) |
| plot_inputs raw / applied | 367 / 50 (0 / 0) | 367 / 50 (0 / 0) |
| plot_inputs 2014-15 collapse before -> after | 88/82 -> 35/29 | 88/82 -> 35/29 |
| s16d pairs / multi / max / mult==hlines | 8,691 / 2,633 / 8 / 100% | 8,691 / 2,633 / 8 / 8,691 of 8,691 |
| mult==plots | 1,729 (65.7%) | 1,729 (65.7%) |
| sale-bearing multi pairs / exact dups dropped | 553 / 3,329 | 553 / 3,329 |
| crosswalk old / new / carried / lost | 3,251 / 3,529 / 3,246 / 5 named | same, same five keys and targets |
| ext sets of (89,5),(95,1),(128,11) / (101,10),(201,10) | {2} / {0} | {2} / {0} |
| make panel-ids diff | none | none |
| hh_characteristics 2014-15 | 3,558 -> 3,616 | 3,558 -> 3,616 |
| food_expenditures / prices / quantities 2021-22 | 98,249->98,231 / 98,247->98,244 / 131,549->131,522 | identical |
| food_acquired 2021-22 / all | 131,549->131,544 / 239,882->239,877 | identical |
| crop_production 2011-12 / plot_inputs 2021-22 (#865) | 11,948->11,937 / 12,433->12,432 | identical |
| label folds 2021-22: Chou / Sucre / Aubergine / 166 | 973 / 5,537 / 452 / 468 | 973 / 5,537 / 452 / 468 (Choux, Sucre poudre/morceaux, 'Aubergine,' 0) |
| Region labels | 16 -> 8 | 16 -> 8 |
| hashes moved | 22/22 | 22/22 |
| tests/test_niger_*.py | 139 passed | 139 passed |
| full suite | 1 failed, 4618 passed, 8 errors (agent) | 1 failed, 5645 passed, 0 errors (see §3) |

## 3. Regression

Full suite, verbatim recipe (`OPENBLAS/OMP/MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1`, `-n 16 --dist=loadfile`, private root, log at `/local/job39922659/pytest.log`):

```
FAILED tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root
1 failed, 5645 passed, 167 skipped, 3 xfailed, 1 xpassed, 880 warnings in 957.01s (0:15:57)
```

The failure is `.github/workflows/ci.yml:96` lacking `--no-root`; the file is untouched by the branch and development fixed it AFTER the branch's merge base (`24cd065b0 ci: windows-smoke poetry install --no-root`); the same test passes on the development worktree. It disappears on merge. The agent's 8 `test_uganda_crop_condition.py` errors did not recur here (0 errors).

Niger: `139 passed, 51 warnings in 51.79s`. The pre-follow-up universe assertion: 1 failed at 27010 (expected red).

## 4. Scope findings

- Three-dot diff vs development: 23 files, exactly the task diffstat; follow-up commits touch exactly the four named files. No baseline/golden/blessed/lockfile/dependency edit. No xfail/skip added (the only `skip`s are the pre-existing cache-gated fixtures). The moved pin in `test_niger_u_sentinel.py` carries before/after numbers that match my measurement.
- No write outside the private root: `find lsms_library/countries -name '*.parquet'` empty; `git status --short` empty (after reverting the no-op `make panel-ids` rewrite, which is byte-identical anyway); dvc-cache symlink intact.
- **Residual drafting (rule of 2026-10-14) NOT done.** #679, #824, #833, #865 are all OPEN; the branch fully covers #679 and #865 for Niger but #824 (Nigeria + Niger) and #833 (five countries) only in their Niger part, and the REPORT drafts no residual text for either. #811 is CLOSED. This is the main fix.
- Not a branch defect, but a human should know: the LIVE root (`~/.local/share/lsms_library/Niger/var`) was re-warmed today 15:53-16:00 (before this element's fix job at 16:17 and before this review) by some other process; it is still pre-#679 (sample 2014-15 = 3,558) but its `household_roster` 2014-15 has 26,369 rows and crop_production 2011-12 11,953, which neither development (22,506 / 11,948) nor this branch (22,671 / 11,937) produces. Some other branch's agent is writing to the live root.
- `make -C Niger/_ panel-ids` invokes bare `python`; it fails with "Command not found" unless the venv is on PATH. Pre-existing, not this branch.

## 5. Residual risks / human decisions

1. **The three ext-2-only delinks (89005, 95001, 128011)** are a design choice, correctly named as such now; whether a split-off with no re-interviewed parent should carry its 2011 hid is still the human's call (unchanged from the previous verdict §5.1).
2. **hid 27010** stays served with NaN `v` in crop_production 2011-12 (2 rows) -- recorded, pinned by name, not adjudicated (mis-keyed 2701 vs genuine orphan).
3. **plot_inputs 2014-15 still destroys 35 rows / 29 groups** (seed-slot harmonisation collision) -- documented as not an EXTENSION defect; no issue number cited for it; it is a residual of #679's "plot_inputs 367" claim and deserves its own line in the #679 residual.
4. `assets` served index is NOT unique on either development or the branch (pre-existing; not touched; worth an issue).
5. Merge will cost one Niger re-warm for every user (22 hashes).

## 6. Recommended follow-up commits (pass-with-fixes)

1. **REPORT / PR body: draft the residuals.** For #824: "Niger done (Unit_sold carried, crop-level sale keyed on it, s16d de-dup); remaining: Nigeria sa3q11b (95-97% agree) + any other country whose sale unit column is dropped." For #833: "Niger folds landed (Choux, Sucre 134/135, 'Aubergine,'); remaining: Malawi groundnut 5.0x, Uganda maize 3.33x milling folds, the other three countries, and the seven-item Niger split list recorded in CONTENTS, not folded." For #679: "closed by this branch except the 35/29 seed-slot collision in plot_inputs 2014-15 (input axis, not id) and hid 27010 (2011-12 harvest-only household)." Paths: the PR description; optionally `lsms_library/countries/Niger/_/CONTENTS.org` #679 section's last paragraph.
2. `lsms_library/countries/Niger/_/CONTENTS.org` (#679 "served rows" paragraph): add one clause "before = origin/development @ 340e30f1e cold build (verified identical to the 2026-09-24 snapshot)" so the next reviewer does not have to rebuild development to trust the befores.
3. Optional: `lsms_library/countries/Niger/_/Makefile:26` -> `$(PYTHON) panel_ids.py` with `PYTHON ?= python`, so the documented `make panel-ids` recipe works without an activated venv.
