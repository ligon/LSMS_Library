**Verdict: CHANGES.** Candidate `3336ffc99f9d435dee2fe75ef382432cf781a2ff`, base `f138a3d61c203f892148592675a7ef72e160a322`.

The core holds up. I rebuilt everything cold, base against candidate, in private roots, and checked the coping rules with my own code (not `coping.py`). Every headline number reproduces, and I found no data defect in any `shocks` table. What needs changing: one test that breaks when the Nigeria stream merges, one Nigeria request whose answer depends on cache state, two questions the worker did not put to the user, one inaccurate GNB evidence sentence, and four integration items.

## What I re-ran (all cold, separate private roots; outputs in `reports/coping-redteam/out/`)

**Hash census (job 41278083)** matches the worker exactly:
- `build_transforms_fingerprint` 0/42, `framework_imports_fingerprint` 0/38, tables 218/555, wave-tables 538/2,321.
- Movement is confined to Benin, Burkina, CIV, GNB, Liberia, Mali, Niger, Nigeria, Senegal and Togo.

Three no-op edits ("probes") confirm the read/build boundary:

| probe | tables / wave-tables moved |
|---|---|
| A: no-op in `normalise_coping` plus an edit to Uganda `coping.yml` | 0 / 0 |
| B: no-op in `ehcvm.coping_set_from_grid` | 184 / 184, exactly the eight 2018-19 waves |
| C: no-op in `derive_coping_autre_rank` | 47 / 47, Benin and Niger 2021-22 |

**`shocks` in all 17 countries (jobs 41278051 and 41278052)**:
- Rows are equal except Nigeria.
- Every non-coping column is identical in value and dtype on common rows in all 17. The worker's claim holds.
- Positions equal base positions with the registry fillers set to NA: 0 mismatches.
- The served set equals my independent version of the rules on every row.
- At the parquet level, the built EHCVM 2018-19 set equals the set of base positions on every row in all 8 countries (GNB after the relabel).
- Nigeria W4's built set contains base's set and differs on exactly 31 rows, the `s15aq7_4` slot.

**Raw files (jobs 41278235 and 41278340)**:
- EHCVM 2018-19 grids hold only {0,1,3}, plus 2 on code 26 alone (Benin 1, Mali 253, Senegal 26). No grid has value labels.
- No row mixes NA with values. At most 3 positives per row. GNB has (1,1) on 47 rows and (1,1,3) on 5.
- 2021-22 option 25 chosen beside a ranked strategy: Benin 13, Niger 17 (2 of them beside a lone Deuxième), GNB 3. CIV, Mali, Senegal, Togo and Burkina: 0.
- Derivation inputs join the stamped rows: Benin 13/13, Niger 17/17, unique index. `ll.derivations()` lists both keys.

**Nigeria (job 41278188)**:
- Base's post-planting and post-harvest copies are identical row for row, for both `shocks` and `assets`.
- Candidate `assets` equals the base post-planting rows on all columns (865,318 → 432,659 rows).
- Candidate `shocks` is base post-harvest plus 2 rows (households 160068 and 180001), which match raw `only_5d=2`.
- The make backend equals the default backend for both tables.
- The coverage grader gives 8 new `dropped` cells.

**Read routes (jobs 41278576 and 41279618)**:
- Warm read equals cold in 17/17 countries. `Country(C, assume_cache_fresh=True)` equals cold in 17/17.
- Make backend equals cold in 15/17. The two exceptions are not caused by the candidate:
  - Uganda serves `Shock` as a column, on base too; once aligned, the frames are equal including `HowCopedSet`.
  - Tanzania raises "prepared wave input changed during country Make build", on base too (job 41279188).
- `normalise_coping` is idempotent on all 17 served frames and costs 0.06–0.52 s.

**`Feature('shocks')`, one all-country call (jobs 41278251 and 41278701)**:
- 359,861 → 348,588 rows, with a unique index.
- `HowCopedSet` is present for the 15 coping countries.
- Each country's slice equals its `Country` frame on every coping column, and the warm call equals the cold one.

**Tests**:
- `-k "shock or Shock or coping or HowCoped or assets or Liberia"` over `tests/`, S3 open, cold roots (job 41278248): candidate 667 passed, 0 failed; base 556 passed, 0 failed.
- Warm-cache run of `test_table_structure`, the schema, coping, gh1562, shocks and derivation tests (job 41279049): candidate 427 passed, 0 failed; base 316 passed, 0 failed.

## Findings

**F1 (should-fix; blocks integration order). The Nigeria-stream merge breaks two new tests.**
- `tests/test_gh1562_coping_build.py:216` asserts `_pinned(info['individual_education'])[-1] == ph`.
- `3d34550d5` appends the post-planting `sect2` file after the post-harvest one in W1 and W2.
- On the synthetic merge (job 41278235): 2 failed, 560 passed — `test_nigeria_shocks_is_served_once_under_the_post_harvest_quarter[2010-11]` and `[2012-13]`.
- Fix: assert `ph in _pinned(info['individual_education'])`.

**F2 (should-fix; needs a ruling). Nigeria W1 `assets` gives a different answer cold and warm.**
- Cold `assets(waves=['2011Q1'])` raises `builtins.RuntimeError` (the GH #1292 empty-answer check: `Age` and `Value` missing).
- Warm, after an all-waves build, the same request returns 0 rows and no warning. Base returns 38,947 rows both ways.
- The worker measured only the cold case. CLAUDE.md treats a cache-state-dependent answer as a defect.
- Root cause is older than this change: W1's block never wired `Age` or `Value`, and base silently served a `Quantity`-only frame for W1.
- Neither the coverage grader (which builds all waves and grades the cell `dropped`) nor my test runs hit it.
- Preferred fix is the worker's option (a): serve `Age` and `Value` as NA for W1 only, without `optional:`.
- Scope: the user's ruling says "check assets"; only Sue's brief adds "fix it if it is the same defect". The user should confirm the `assets` change.

**F3 (should-fix; a question the worker did not raise). Uganda 2013-14 and 2015-16 "Nothing else" as the first answer.**
- The printed code list (`unps_2013_14_household_qx.pdf` p32) has 1–15 and 96, with no "did nothing" option.
- Code 40 "Nothing else" is `HowCoped0` on 58 / 44 rows and the only answer on 55 / 41 rows.
- As a filler these rows are served all-NA, the same as "not asked", while 2018-19 serves {Did nothing}.
- The worker followed the ruling's wording, which names "Nothing else" as a filler. But the companion bullet says a lone "did nothing" must be distinct from NA. Put it to the user. Honouring it would need a slot-aware registry class.

**F4 (should-fix; documentation). The GNB evidence sentence overstates the data.**
- The relabel itself is right. I confirmed the Portuguese form: S14_Chocs rows r30–r34 are codes 18–22, r35 is blank, r36 is 25 "Nenhuma estratégia".
- Within-country evidence the worker did not use, from GNB 2021-22's full 26-option list (per mille of chosen cells, job 41279618):

| strategy (2021-22 code) | 2021-22 | 2018-19 code under the shift | 2018-19 |
|---|---|---|---|
| bétail (21) | 24.8 | 20 | 27.9 |
| spirituelles (23) | 2.2 | 22 | 1.9 |
| Aucune (26) | 177.8 | 25 | 137.9 |

  - Under the old alignment, 20, 21 and 25 miss badly.
- The audit's cross-country comparison is mixed: at codes 18 and 22 it favours the old labels.
- CONTENTS should cite the within-country comparison and say 23 and 24 are inferred.

**F5 (integration; GNB `a51a616e2` / `90bdd6742`).**
- (i) The GNB 2021-22 hook infers the option-25 slot without the stamp. The ruling's "GNB 34" counts every row choosing option 25; only 3 sit beside a ranked strategy, so the stamp covers 3 rows.
- (ii) Once 2021-22 lands, `optional: true` on GNB `HowCoped0..2` silences the 2018-19 empty-positions warning (the Site B null-read report) for one wave, against the CLAUDE.md rule. `test_gnb_positions_are_optional_and_the_set_declared` pins it.
- The merged tree otherwise builds cleanly: 13,904 rows, 2021-22 positions filled, the set on every row, 562 static tests pass.

**F6 (integration). The Windows build pin goes stale again.**
- The base Uganda `shocks` digest matches #1716's new pin (`38a3b0865`, `aac42079f006`). The candidate's differs, because of the new `HowCopedSet` column and the filler NAs (job 41279643).
- Re-measure `EXPECT` after both land. The value is in `out/windigest/digest.log`.

**F7 (must-state). Strict mode.**
- Under `LSMS_READ_STRICT=1`, `shocks` for Benin, CIV, Mali, Senegal, Togo and Nigeria now raises `NullReadError`; on base it returned rows (job 41279618).
- Burkina and Niger already raised on base. Uganda raises on both (a NaN index key).
- That is 19 new per-wave warnings in all.

**F8 (must-state). Coverage.** 8 new `dropped` cells:
- Nigeria `shocks` 2010Q3, 2012Q3, 2015Q3, 2018Q3;
- Nigeria `assets` 2011Q1, 2013Q1, 2016Q1, 2019Q1.

  EHCVM 2018-19 cells and Nigeria 2019Q1 stay `sane`.

**F9 (note). Wording errors.**
- Ethiopia `coping.yml`: W1 has 13 unlabelled cells ('0.0' ×10, then '24.0', '25.0', '60.0'); '21.0' occurs only in W2.
- CIV CONTENTS: "three positives per option" should read "per row".
- `tests/test_assets_additive.py:8` docstring still says "in EACH of t=2012Q3 and t=2013Q1".

**F10 (note).** The Benin panel branch `d5fdf326c` walks 2021-22 household ids. After integration, re-check the option-25 inputs join through `derivation_inputs`, not the raw callable.

**F11 (note). Two things the PR body should state.**
- The wave-level API serves the build's set before the read-time rules. Mali 2018-19 keeps "Aucune stratégie" beside a strategy on 444 rows, and other waves have no set. This matches kinship and is acceptable.
- In Burkina 2014, Mali 2014-15 and 2017-18, and Niger 2011-12 and 2014-15, positions still carry "Aucune"/"Nothing" used as a filler after a strategy, as ruled. Their `HowCoped1`/`HowCoped2` fill rates are not strategy counts; use `HowCopedSet`.

**F12 (disclosure).** I ran `python3 -` once on n0035 with empty stdin (no import). Nothing else ran there.

## Collisions
All textual conflicts are appended `CONTENTS.org` sections; resolve by keeping both:
- GNB;
- Nigeria;
- CIV, Senegal and Togo (manquant);
- Burkina (burkina and strata);
- Mali and Niger (cropkg, strata);
- Benin.

`test_gh1500` and `_build_registry.py` auto-merge. The only semantic conflicts are F1, F5, F6 and F10. Merged-tree tests ran at the brief's SHAs; GNB's and Burkina's newer heads change only CONTENTS and crop code.

## Must-state for the PR body
- The F2 cold/warm divergence, and that the `assets` change rests on the brief's wording, not the user's.
- F7 and F8.
- GNB: 2,199 rows relabelled, with 23 and 24 inferred.
- Uganda's 55 / 41 lone "Nothing else" rows are served NA (F3).
- The integration tasks in F5 and F6.
- The F11 caveats.
- Cost: a cold re-warm of 10 countries, with CIV's 126 wave-tables one-time.

## The worker's questions
1. **Nigeria W1 assets.** The framing is fair but incomplete: the request fails only cold (warm gives 0 rows). Recommend option (a).
2. **GNB codes 23–24.** The evidence is stronger than stated (F4); 23 and 24 remain inferences.
3. **Chosen = positive.** This is base's own `>=1` reading, so set membership is unchanged; only the code-order positions go.
4. **Unlabelled numeric codes.** The framing is fair; correct the counts (F9). '0.0' ×10 may be a "none" code.
5. **Malawi "Dont know".** All 185 cells are in the first slot, and 125 rows end up with no set. Treating it as a missing code is reasonable.
6. **Hollow rows.** Keeping them is consistent cold and warm.
7. **Liberia column names.** No evidence issue.
8. **Read-time recode not registered.** The framing is fair.
9. **Option-25 stamp in CIV, Mali, Senegal, Togo.** The raw files confirm 0 rows today. A guard that raises instead of inferring costs the same hashes.
10. **GNB `optional:` positions.** Yes, revisit at integration (F5).

## Not verified
- The ~200 other tables whose hashes moved: not rebuilt.
- The full static suite and the full S3 suite on the candidate alone: not run.
- `CONTENTS.org` for the touched countries: I read the diffs, not the whole files.
- Merged-tree tests: run at the brief's SHAs only.
- The synthetic merges used `commit-tree` in job-local throwaway clones; no refs were written.
