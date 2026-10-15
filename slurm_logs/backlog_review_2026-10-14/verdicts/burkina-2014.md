VERDICT: pass -- the branch does what it says, every claimed number reproduces on a cold private-root rebuild, the 14 new tests all fail on origin/development and pass on the branch, the other 16 Burkina tables are byte-identical after the re-warm, and nothing outside Burkina_Faso moves.

Reviewer: red-team clone of `fix/burkina-2014` @ 366788665, private `LSMS_DATA_DIR`, base worktree at `origin/development` (61217ec98), merge-base worktree at 2ed85b49b.  Package identity asserted (`lsms_library.__file__` under the clone; `/base/` for the base runs).

## 1. Refutations (each claim: what I ran, what I got)

1. **"405,127 passage-2/3/4 purchased rows and 44,125 passage-1 rows were served with Quantity == 0"** -- HELD.  Base code + pre-fix root: visit 1 zeros 44,125 / nulls 0; visits 2/3/4 zeros 139,798 / 136,913 / 128,416, nulls 0 (`measure.py`, base).  Nit: the commit message says "43,906 passage-1 rows" were served 0; it was 44,125, of which 43,906 become NaN and 219 stay 0 (the report's own table has it right).
2. **"qachat is never 0 where achat > 0 (0 of 140,908); NaN on 43,906"** -- HELD, re-measured on the raw `emc2014_p1_conso7jours_16032015.dta` via `get_dataframe`: achat>0 140,908; qachat NaN 43,906; qachat==0 0.  Item concentration held exactly (Sel 10,244/10,259, Oignons 7,668/7,672, Soumbala 7,138/7,190, Feuilles 4,821/4,821, Gombo 4,682/4,683, Pâte d'arachide 3,233/3,236, Haricot 2,540/2,542, Beurre de Karité 1,774/1,774, Fruits 1,099/1,099; 45 of 60 items with zero nulls).  p2/p3/p4 files: no `qachat`/`uachat` column; `autocons` present with 23,528 / 24,934 / 37,029 rows > 0 (the unread own-consumption VALUE, see §5).
3. **"The 219 remaining p1 zeros are Produced >= Quantity clips"** -- HELD: raw p1 rows with achat>0 & qachat notna & qautocons >= qachat = **219** exactly (39 strictly greater).  So `_restore_missing_quantity`'s key-based mask NaN'd no clipped row and no reported row.
4. **"Row counts, Expenditure and Quantity totals unchanged; no row added or removed"** -- HELD at row level, not just totals: after mapping the four label merges, an outer merge of the before and after `var/food_acquired.parquet` on `(t,v,i,j,u,s,visit)` gives both=765,816, left_only=0, right_only=0; Expenditure equal on every row; exactly 449,033 rows (= 43,906 + 405,127) changed Quantity, all 0 -> NaN.  Index unique before and after; `grain_reports()` empty both; `null_index_reports()` identical both (food_prices v NaN 3,786/3,787, food_quantities 3,987 -- pre-existing, the EMC 2014 cluster gap); `null_read_reports()` identical (sample/strata, pre-existing).
5. **"47,137 -> 0 mojibake rows; beef one label; Cube 38,982 -> Cube alimentaire; Feuilles 16,610"** -- HELD.  Before: j regex hits 47,137 (Cube 38,982 + `Viandes de b\x9cufs` 6,756 + 2018-19 `Viande de bÅ\x93uf` 1,399), Feuilles (truncated) 16,610; after: 0, `Viande de boeuf` 6,756 / 1,399 / 665, `Cube alimentaire` 2014 38,982, `Feuilles` 16,610.  Per-wave j vocabularies differ ONLY by those labels (set diff: 2014 -3/+3, 2018-19 -1/+1, 2021-22 none); u vocabularies identical.
6. **"The crosswalk rows were never LOADED: 291 parsed rows, now 294"** -- HELD.  `df_from_orgfile(..., 'harmonize_food')` on base = 291 rows, last key `Chocolat Ã croquer...`, all four damage keys -> None; on branch = 294, all four keys resolve, no `')'` / `'Cube (Maggi, Jumbo,'` fragment keys.  The committed blob carries the literal NEL byte (0xC2 0x85) on one line; git has no attrs/autocrlf on the file.
7. **"food_prices(kgvalue): 449,431 of 683,713 (65.7%) -> 44,304 of 278,586 (15.9%), money_only 405,127; unitvalue 449,252 -> 44,125"** -- HELD to the row (table below).  Also holds on the `Feature('food_prices')(['Burkina_Faso'])` path (same tally, same warning text), so the classifier is not defeated by Feature assembly.
8. **"food_quantities 2014 kg 310,338 -> 306,309; 2018-19 206,669 -> 206,719; 2021-22 98,690 -> 98,736; rows 316,441 unchanged"** -- HELD (all-rows totals).  Source partition moved item_unit 123,139 -> 132,559 and unit 132,629 -> 123,209, consistent with the stated cause (label merge changes which rows pool in the per-(j,u) inference); no Quantity value changed.
9. **"Cache cost: all Burkina_Faso tables re-warm; nothing else; transformations.py reaches no fingerprint"** -- HELD.  `Country._table_cache_hash` for food_acquired/household_roster/sample: Burkina moved vs merge-base; Togo, Niger, Senegal unchanged vs merge-base; `build_transforms_fingerprint` identical for food_acquired and household_roster.  (Senegal's hash differs between development and the merge-base -- development drift from another PR, not this branch.)
10. **"Other Burkina tables unaffected"** (implicit) -- HELD: built all 16 other tables on the branch in the cleared private root and compared each `var/*.parquet` to the pre-fix copy: cluster_features, household_roster, individual_education, subjective_well_being, assets, housing, shocks, plot_features, food_security, food_coping, months_food_inadequate, interview_date, plot_inputs, livestock, plot_labor, people_last7days, crop_production, sample -- all `equals` True, same shapes.
11. **"money_only cannot census other countries" (report §5)** -- I did it read-only on the shared cache: over 20 warm `var/food_acquired.parquet` (Benin ... Uganda, incl. GhanaLSS which also carries `visit`), **0** `(t,[visit],s)` groups with Quantity 100% null, >= 30 rows and any Expenditure > 0.  The bucket fires today on Burkina 2014 p2-4 and nowhere else.
12. **"Decoders change 0 rows today"** -- consistent: after-labels equal the crosswalk's Preferred Labels exactly and no residual C1/mojibake remains; `_repair_label` unit test passes on clean French, `Kg`, `Unknown`, float.

## 2. Claimed vs measured

| figure | claimed | measured (base / branch) |
|---|---|---|
| food_acquired rows | 765,816 / 765,816 | 765,816 / 765,816 |
| p1 purchased null / zero | 0 / 44,125 -> 43,906 / 219 | 0 / 44,125 -> 43,906 / 219 |
| p2, p3, p4 purchased null (zero) | 139,798 / 136,913 / 128,416 (0) | same |
| 2014 Expenditure / Quantity total | 342,110,515 / 4,208,737.6 unchanged | same, unchanged |
| mojibake j rows | 47,137 -> 0 | 47,137 -> 0 |
| Feuilles leak | 16,610 -> `Feuilles` | 16,610 -> `Feuilles` 16,610 |
| beef labels | 3 -> 1 (6,756 / 1,399 / 665) | same |
| harmonize_food parsed rows | 291 -> 294 | 291 -> 294 |
| food_prices(kgvalue) dropped / expected | 449,431 / 683,713 (65.7%) -> 44,304 / 278,586 (15.9%) | same; inf 255 -> 219, nan 449,176 -> 44,085 |
| money_only | 405,127 | 405,127 (= 139,798 + 136,913 + 128,416) |
| food_prices(unitvalue) dropped | 449,252 all inf -> 44,125 (inf 219, nan 43,906) | same |
| food_prices rows kgvalue / unitvalue | 234,282 / 234,461 unchanged | same |
| food_expenditures rows / 2014 CFA | 377,563 / 342,110,515 | same |
| food_quantities rows; kg 2014 / 2018-19 / 2021-22 | 316,441; 310,338 -> 306,309 / 206,669 -> 206,719 / 98,690 -> 98,736 | 316,441; 310,338 -> 306,309 / 206,669 -> 206,719 / 98,690 -> 98,736 |
| raw p1: achat>0, qachat NaN, qachat==0 | 140,908 / 43,906 / 0 | 140,908 / 43,906 / 0 |
| p1 clipped rows (qautocons >= qachat) | 219 | 219 |
| cold rebuild time | 1m25s | 1m23s |
| money_only groups elsewhere in corpus | "could not census" | 0 of 20 countries |

## 3. Regression

- New/changed tests on **origin/development** (base worktree, base data): `14 failed, 20 passed in 24.05s` -- all 14 new tests fail there (4 org-parse/hook tests, 5 `TestMoneyOnlyRecallOccasion`, 2 served-label, 3 passage/NaN/tally); the 20 pre-existing pass.
- Branch selection (every test file naming food_prices / _drop_unpriceable / price_rows_dropped / Burkina, plus test_schema_consistency, test_feature_canonical_index, test_build_transform_hash, test_categorical_mapping_resolution, test_ehcvm_7day; `-n 12 --dist=loadfile`, thread pins, `LSMS_BUILD_WORKERS=1`): `1083 passed, 29 skipped, 161 warnings in 706.42s (0:11:46)`.
- Full suite (`tests`, `-n 24 --dist=loadfile`, same pins, private root cold for every non-Burkina country): `1 failed, 4759 passed, 185 skipped, 3 xfailed, 1 xpassed, 885 warnings in 987.16s (0:16:27)`.  The one failure, `tests/test_sample.py::TestSample::test_covers_all_waves[Albania]` ("sample missing waves: ['2005']"), is a transient of the concurrent cold build in a root that held no Albania parquet: rerun alone it passes on the branch (`1 passed in 7.32s`) AND on development code against the same root (`1 passed in 7.31s`).  Unrelated to Burkina_Faso or to any file this branch touches.
- `bench/feature_audit/scan.py --features food_prices --countries Burkina_Faso Togo Niger`: `features: 1  records: 142  skips: 0 / by status: {'warn': 138, 'fail': 3, 'pass': 1}`.  The 3 `fail` are `assembly_nonempty` under `units='kgprice'`, `units='unitprice'` and `labels='Aggregate'` -- none of the three countries records a unit price or curates an Aggregate column (documented no-fallback behaviour), unrelated to the branch.  The 138 warns are unclosed-file noise, pre-existing Site B/I/Q warnings (strata null, v NaN, Quantity outliers), the population-pooling notice, and the branch's own 15.9% price-loss warning carrying the MONEY-ONLY note.
- `find lsms_library/countries -name '*.parquet'`: empty, before and after.  No writes outside the private root (all parquets under `$LSMS_DATA_DIR`).

## 4. Scope findings

- `git diff --stat HEAD~1..HEAD` (= merge-base..HEAD) touches exactly the 7 files in the brief.  The larger `origin/development..HEAD` diffstat shows the Burkina `* Region` table and three test files (`test_crop_kg_factor`, `test_gh842_uganda_harvest_unit`, `test_guinea_bissau_cluster_features`) as *removed* -- that is **branch drift**, not the agent: PRs #971/#972 landed on development after the branch base.  `git merge-tree --write-tree origin/development HEAD` merges **clean** (tree 4a3e29b0), and the merged `categorical_mapping.org` keeps `#+name: Region` and the one-line Cube key.  Merge or rebase before review of the diff on GitHub so the PR diff does not show those deletions.
- No baseline/golden/blessed/lockfile/dependency edit.  No assertion weakened, skipped or xfailed; the only changed pre-existing test file (`test_burkina_faso_2014_passages.py`) is append-only.
- `categorical_mapping.org`: the dead `Feuilles (oseilles, baobab, boulvaka` row was deleted (it was unreachable: the raw label ends in U+0085 and the org reader strips it) and replaced by the whole-value `_J_FIX` in the hook; the four damage keys are retained (strategy B unchanged).
- `transformations.py` is the one framework file touched; it is read-path only (fingerprints unchanged, measured), and the `*price` modes are explicitly excluded from the new bucket (pinned by a test).
- The three dead `encoding=` wave scripts were deliberately not deleted (report says so; the residual draft names them).  Fine.
- CONTENTS.org: the idiosyncrasy is named correctly and the correction to :465 is true (the source is NaN, the served table was 0).  Its new tables reproduce.  One claim I could not verify and the agent flags as unchecked: whether the questionnaire skips quantity for the nine p1 items (C4).

## 5. Residual risks and what a human must decide

1. **Serving p2-4 purchased `Quantity` as NaN rather than 0 is a data change for every Burkina 2014 consumer** (the `decision_for_human` in the triage record).  Totals are invariant, but any downstream `Quantity.sum()` with `min_count` or any `Quantity == 0` filter now sees NaN.  I think it is right (0 means "bought none"); it still needs the yes.
2. **The `money_only` bucket is a warning-only concern but it is a laundering vector in principle**: a Quantity column mis-bound under `missing_ok` that arrives 100% null for a whole `(t, s)` purchased group (>= 30 rows) is now excused from the price-loss warning.  Site B only fires if the whole wave's Quantity is null (produced rows would normally keep it non-null), so that case would be silent at `food_prices`.  Today 0 groups in the corpus qualify (my census), rows are dropped either way, and `food_acquired` itself still shows the null column.  Acceptable; worth a line in the `_money_only_rows` docstring.
3. **p1's residual 15.9% is reported, not explained**: nine items at >= 99% null quantity is a module-coverage fact at item grain; C4 (questionnaire) unchecked.  The drafted #864 residual covers this.
4. **p2-p4 `autocons` (own-consumption VALUE) is still unread**: 23,528 / 24,934 / 37,029 raw rows > 0.  The branch neither reads it nor adds rows; the residual draft names it.  Decide whether it should be served (it would add produced rows with Expenditure-style value and change totals).
5. **`qachat` may be a purchased quantity, not a total** ("quantité achetée"); the reshape subtracts `qautocons` from it and clips 219 rows to 0.  Flagged in CONTENTS.org, not adjudicated -- a questionnaire question.
6. **The org file now depends on a literal NEL byte surviving editors and tooling.**  The test pins it, which is the right guard; a future `sed`/editor normalisation goes red rather than brown.  A `df_from_orgfile` warning on a dangling row fragment (named in the residual) would make this class self-reporting.
7. **#794 reconciliation**: the issue was already narrowed on 2026-10-14 (by Sue) to Mali + the strategy-B migration decision, which matches the agent's drafted residual.  #864 is still OPEN with its original body; the agent drafted the close-with-residual text.  Both residual drafts are present in the report (rule satisfied).

## 6. Recommended follow-up commits

None required for merge.  Optional, on the same branch or after:
- `lsms_library/transformations.py::_money_only_rows` docstring: one sentence naming the mis-bound-column exposure and that Site B is the independent guard.
- Commit-message nit (cannot be fixed without rewriting history; note it in the PR body): "43,906 passage-1 rows were served 0" should read "44,125 (43,906 filled NaN + 219 clips)".
- Rebase/merge development before opening the PR so the diff does not show the Region table and three test files as deletions.
