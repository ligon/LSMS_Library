# Served-wave corrections of 2026-11-12: Mali rice block, Senegal salt and near-zero, Niger flag, Uganda 2009-10 ea

Fix agent **Claude Opus 5.5** (`claude-opus-5-5`), for Sue (scrum master, Slurm job 41201840).  Written 2026-11-12.
No sub-agents were used.  No GitHub writes were made.  This report and every file beside it hold aggregates only:
no microdata rows and no household ids.  Row-naming warning texts, assertion details and pytest warning summaries
were removed from the copies kept here.  A `grep` of the directory for the `i=` and index-dump forms the library
prints finds only source code in `scripts/smoke1.patch`.  That check covers those two forms only.

## Verdict

All five rulings are implemented and verified: the four in the brief, plus GH #1519 from the coordinator's addition.
Each is a separate commit.  Four more commits carry the records, the tests, a follow-up to the hooks and the
re-pinned test digests.

- **Hash footprint.**  Mali, Senegal, Niger and Uganda only.  0 of 41 build fingerprints moved.
- **Frame equality.**  Every other row and wave is frame-equal, base against candidate, in cold private roots.
- **derivation_inputs.**  It joins for every new key.

Three findings contradict the brief, and the implementation follows the data (§ Discrepancies):

- **Mali.**  The block is the 3,357 records with no `vague`.  1.4 x hhsize holds on only 3,348 of them.  One block
  row *was* summed into a real row.
- **Senegal salt.**  The constant is 0.0199997 kg x hhsize, exactly.  No row equals 0.02 x hhsize.
- **Niger.**  The 14.2% are rows where the CAPI *enables* `s07bq07a`.  The disabled-but-filled rows are a different
  set of 21,063.

**Branch** `fix/isa-served-wave-corrections-20261112`.  **Head `3ae15b56ceb8093acaf75c8e24369dcd4d012aea`**, on development
`e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

| # | commit | change | count |
|---|---|---|---|
| 1 | `c48fd23a1320ea5e72f023b197e407cc3eac3017` | Mali 2018-19 rice-ration block withheld (#1707) | 3,357 records; 3,356 served rows gone, 1 corrected |
| 2 | `89f16da95afcec27c7c526ec429ea724b848ef62` | Senegal 2021-22 salt at a fixed amount per person withheld | 100 records = 100 rows |
| 3 | `5f4f0acfcde99ca9b5ac2b4bd87d58b3a592e179` | Niger 2018-19 computed `s07bq07a` flagged (#1708) | 15,264 records; 15,014 rows keyed |
| 4 | `4ad000b8d6fb5fedf1dcf6dbf227889cd1beda0f` | Uganda 2009-10 `ea` filled from the same household (#1536) | 514 of 569 filled |
| 6 | `8f0b7d91a0589eeae9befe29d39dab83fa65d0e4` | Senegal 2018-19 near-zero quantities NA (#1519) | 3 rows |
| 5 | `f435ac841cb78c4a49463fd41c3499e245ccb9c6` | CONTENTS.org records, the four countries | docs only |
| 7 | `73355b28dc8d21c8d08475ef70b06f3a60600c0e` | tests: new `test_gh1707_served_corrections.py`; Uganda re-pins | tests only |
| 8 | `e1c23e848810902f3b00365876d629a2470a8efd` | follow-up: hook wiring kept readable; #876 / Niger registry text; re-pins in `test_ehcvm_7day`, `test_ehcvm_unit_weights`, `test_gh1493` | no served value moves |
| 9 | `a495b16d11547096510fad33f85e5b4f082c81cb` | tests: GH #1500 source digests re-pinned | tests only |
| 10 | `52386ee648ccc0139863179ac743825ee5cf0d3b` | near-zero `contents:` pointer; Uganda function tested on real inputs | registry text and tests |
| 11 | `3ae15b56ceb8093acaf75c8e24369dcd4d012aea` | `test_sampling`'s Uganda historical view lists the new `ea` key | tests only |

Registry keys (all in `countries/{C}/_/derivations.yml`):

- `Mali::food_acquired::rice-ration-block-withheld` (`rows: 0`)
- `Senegal::food_acquired::salt-per-capita-withheld` (`rows: 0`)
- `Senegal::food_acquired::near-zero-quantity-withheld` (`rows: 0`)
- `Niger::food_acquired::computed-last-purchase-quantity` (per row, `{s: purchased}`)
- `Uganda::sample::ea-same-household` (per row)

## Idiosyncrasies read first (CONTENTS.org), and where each bit

- **All four EHCVM waves.**
  - `food_acquired` goes through `ehcvm.food_acquired_with_unit_weights` (#1452).
  - Its NSU join reads the source **by position** and refuses a frame that is not the file row for row
    (`_nsu_factor`).  So no row can be dropped, and no `Quantity` NA'd, before the shared hook.
  - Withheld rows are therefore marked in the wide frame (their `j`) and dropped after the hook.  Flags and NA are
    applied after it, by the served keys the selected records become (reshaped on their own).
- **Mali.**  The 2018-19 labels are padded (#1513), and #1594 is the exact-value precedent.
  - The code is wave-level (`2018-19/_/mapping.py`) and reads `s07b` in its codes.
  - Nothing goes into `ehcvm.py`: an edit there would move all eight EHCVM countries.
- **Senegal.**
  - The `u` / `j` crosswalk is keyed on mojibake (left alone).
  - 2021-22 `i` is grappe + '0' + menage.
  - #1500: the missing-`u` `Unknown` fill is in 2018-19's hook, and the #1519 NA runs before it.
- **Niger.**  2018-19's index is swept for mojibake after the reshape (`_decode_mojibake_in_df`), then `fill_missing_u`.
  The flag's key lookup reproduces both.
- **Uganda.**
  - `ea` is a label of origin, carried when a household moves (#689).  Early waves fill it through `comm_ea` (#1459).
    `_mark_comm_ea` stamps every early-wave row with an `ea`, so the fill runs after it.
  - `HHID` is unchanged 2005-06..2011-12, and no `updated_ids` entry covers those waves.  2013-14 links by `HHID_old`.
    The fill reads raw ids, never the walked `i`.
  - `dfs:` merges are outer, so the new sub-dfs keep only 2009-10 cover households.  No phantom rows result:
    2,979 = 2,979.
  - 2009-10 movers and split-offs carry a blank `comm`.

## The changes

### 1. Mali 2018-19 rice block (#1707): "Remove, registered"

- **Selector.**  A record of `s07b_me_mli2018.dta` with no `vague` (3,357; every other record has one).
- **Fingerprint.**  The build checks it on every such record and raises `RiceRationBlockError` if one lacks it:
  - product 1 (`Riz local Gambiaka`), unit 100 (kg), size 0;
  - `s07bq04`/`s07bq05`/`s07bq07a`/`s07bq08` empty;
  - `s07bq03a` a whole multiple of 1.4.

  All 3,357 also answer `Plus de 30 jours`, one per household.
- **Served before and after.**  2018-19 `food_acquired`:

  | | before | after |
  |---|---|---|
  | rows | 161,212 | 157,856 |
  | purchased rows | 129,085 | 125,729 |
  | `Riz local Gambiaka`/Kg purchased rows | 5,632 | 2,276 |
  | their kg | 55,895.1 | 23,581.7 |

  - Expenditure is unchanged at 89,614,678.64 FCFA; the block had none.
  - Purchased rows with Expenditure NA: 25,572 -> 22,216.
- **Rows removed.**  3,356 rows go.
  - **1 real rice row changes.**  That household's block row shared its served key with its own rice-kg purchase,
    and core summed the two.  The row now serves the household's answer alone, 5.6 kg less.
  - Every other row and column is equal, in all three waves.
- **Derived tables.**  `food_expenditures` and `food_prices` (kgvalue, unitvalue) are frame-equal.  `food_quantities`
  moves only the same 3,356 + 1.
- **Inputs.**  `derivation_inputs` returns the 3,357 records at served `(t, i, j)`; all are on base's served keys.
- **Registry.**  The #876 entry's served-row count is updated (129,085 -> 125,729; NA 25,572 -> 22,216).
  Senegal's #876 entry carries a dated note of what moved there (2021-22 -95 purchased rows, -93 NA; 2018-19 +2 NA).

### 2. Senegal 2021-22 salt: "Remove, registered"

- **Selector.**  Product 139 (`Sel`), unit 100 (kg), size 0, with
  `|s07bq03a - 0.0199997 x hhsize| < 1e-9`.
  - hhsize is `ehcvm_welfare_sen2021.dta`, now declared in the food_acquired block (`household_size:`) so its
    sidecar is hashed.
  - Its (grappe, menage) key is unique and covers every salt record.
- **Count.**  **100 records**, each within 1e-12.  No other product or unit meets the constant.
- **Rows removed.**  100 served rows go (95 purchased, 5 in kind).  They carried 17.04 kg, plus 17.78 FCFA on the 2
  rows that had an Expenditure: 2021-22 Expenditure 182,628,314.17 -> 182,628,296.39.
- **Everything else equal.**  Every other row is equal.  `food_expenditures`/`food_prices` lose the 2 valued rows and
  `food_quantities` the 100; nothing else moves.

### 3. Niger 2018-19 `s07bq07a` (#1708): "Flag now, diagnose"

- **Class.**  `s07bq07a` positive and off the quarter grid: **15,264 of 107,263 (14.23%)**.
  - 14,908 carry 13-18 decimals; the other 356 are kept, because `q8/P` can be short (25/200 = 0.125).
  - 2021-22 has 15 off-grid values in 112,923.
- **Keyed.**  The 15,014 purchased rows these records become.  12,227 carry an Expenditure, which is 26.04% of
  2018-19 served Expenditure.
- **Everything else equal.**  Every column but `Derivation` is equal, and `Derivation` changes only by this key.  The
  derived tables are frame-equal.  All 15,014 keyed rows join `derivation_inputs` (15,264 records).
- **Not the disabled set.**  15,263 of the 15,264 have `s07bq06` in (1, 2, 3).
  - The CAPI-disabled-but-filled set is 21,063 records: all on grid, `s07bq08` empty on every one, 8,050 equal to
    `s07bq03a`.
  - That set feeds no served value.  It is recorded in CONTENTS and not keyed.

**Diagnosis** (read-only, raw file; burst jobs 41248432 / 41248724 / 41249061):

- **A reference unit value per cell.**  `s07bq08/s07bq07a` takes one value per (product, unit, size, region) on
  13,735 (90.0%) of the flagged records: 926 cells, with the value repeating across different `s07bq08`.
- **Quantity backed out of value.**  On 12,250 records `s07bq08` is an integer, so `s07bq07a = s07bq08 / P`.
- **Both rebuilt.**  On 3,013 records `s07bq08` is non-integer too, in a CAPI integer field, so
  `s07bq08 = s07bq07a x P`.  This covers all 2,186 records of product 119 at P = 200 per `Sachet (Moyen)`.
- **P is not reproducible from the distributed data.**  It equals:
  - the cell's on-grid median unit value on 3,184 records, and the on-grid mean on 2,168;
  - the national on-grid median in 76 of 926 cells;
  - the s05 community price mean or median in 8-24 of 282-539 cells.
- **Not an NSU conversion.**  `q7a x weight` lands on a round 0.1 kg for 0.95% of flagged records, against 9.4% of
  on-grid ones.
- **Systematic, not outliers.**
  - 14.2% of each vague.
  - 10-18% of a grappe's records.
  - 5,509 of 5,987 households have at least one; none has all.
  - The on-grid unit values run from 0.46 to 1.36 x P (p5-p95), so the rule is not a band trim.

How P was chosen, and which rows were picked, is not established.  The original answers are not recoverable.  No
document explains it.

### 4. Uganda 2009-10 `ea` (#1536): "Fill, registered"

- **Rule.**  A 2009-10 GSEC1 household whose own `comm` gives no `ea` takes, in order:
  1. its `comm` in 2005-06, 2010-11 or 2011-12 by `HHID`, through `comm_ea`;
  2. else 2013-14 `GSEC1.ea` by `HHID_old`.

  The labels must agree, or `OriginEaError` is raised.
- **Which wave, and why.**  The comm waves come first.  They name the frame EA of the same `HHID` directly, and
  2005-06 is the wave of sampling.  2013-14 is reached only for 22 households no comm wave records.  On all 514, every
  label held in all **seven** other waves agrees (measured on the served frame by walked `i`), so the order decides
  nothing.
- **Filled.**  **514 of 569**: 192 movers, 317 split-offs and 5 originals.
- **Still NA.**  55 rows (51 split-offs no other wave records, 4 AGSEC3B/GSEC12-only).  **Weight share still NA:
  29.32% -> 3.53%.**  Distinct labels 317 -> 321, all already served in other waves.
- **No non-missing `ea` changes, in any wave.**  `Derivation` changes on exactly the 514.
  `cluster_features` and `household_characteristics` are frame-equal.
- **Inputs.**  `derivation_inputs` gives 1,303 rows for the 514: 2005-06 197, 2010-11 440, 2011-12 420, 2013-14 246.
  All join.
- **Sources.**  Two new `dfs:` sub-dfs read the other covers, so a lost cover costs the fill, not `v`.

### 6. Senegal 2018-19 near-zero (#1519): "Exact-value NA rule"

- **Rule.**  `s07bq03a` and `s07bq07a` both in (0, 1e-3).
- **Catches exactly the 3.**  All product 132, each with `s07bq03a` = 40 x `s07bq07a`.  The rule catches nothing in
  the other 12 EHCVM `s07b` files checked: Senegal 2021-22, Mali, Niger, Burkina and Benin in both waves, Togo 2018,
  Guinea-Bissau 2018-19 and Cote d'Ivoire 2018-19.
- **What changes.**  Quantity NA, so Expenditure NA too.
  - The registered #876 rule multiplies the served quantity.  It was 2,000 and 4,000 FCFA on two rows; the third was
    already NA.
  - Quantity_kg was already NA on all three.
- **What stays.**  The rows stay with their #876 key.
- **Prices.**  The 2018-19 `food_prices` (kgvalue) maximum goes from 1.79e22 to 479,452 FCFA/kg, and the mean from
  2.17e17 to 1,698.5.  For `unitvalue` the maximum goes from 4.79e21 to 70,000.  **No price is above 1e10.**
- **Precedent.**  The #1594 precedent is a mapper mask with **no** `derivations.yml` entry.  This rule is registered
  in the shape of Burkina's withheld GPS (`rows: 0`); CONTENTS says so.
- **CONTENTS.**  `Senegal/_/CONTENTS.org` 836-858 is corrected: it is marked superseded.  The stale "Site Q does not
  screen food_prices" is fixed; Site Q has screened `food_prices.Price` since PR #1544 / GH #1493.
- **Site Q now names less.**  The lower-tail report of the 3 quantities is gone, and the `food_prices` upper tail
  names 1 row instead of 3.

## Discrepancies with the brief, for the coordinator

1. **The Mali selector is not "1.4 x hhsize AND no vague".**  That rule gives 3,348 and would leave 9 block records
   served as purchases.  Those 9 are multiples of 1.4 of another integer, and 3 are on no cover.  The missing `vague`
   selects all 3,357, the ruling's count, and the other features are checked and raise if absent.  On rows WITH a
   vague, 1.4 x hhsize matches 284 genuine rice-kg answers, so hhsize alone cannot select.  The welfare file is not
   read.
2. **Mali "1 household has both" was summed in base.**  The live report said no block row was summed into an
   ordinary one; one was.
3. **Senegal "0.02 kg x hhsize" is 0.0199997 kg x hhsize.**  An exact 0.02 test finds 0 rows (7 within 1e-6).  The
   rule uses the measured constant, exactly; it is not a rounding.  The constant was not tested on 2018-19.
4. **Niger "where the CAPI questionnaire disables s07bq07a".**  The 14.2% are enabled rows.  I keyed the 14.2% (the
   ruling's count and #1708's 26%), not the 21,063 disabled rows.
5. **#1519 Expenditure.**  The ruling names the quantity.  Expenditure goes NA with it (2 rows, 6,000 FCFA), because
   the registered #876 rule is Quantity x unit value.  Confirm that this is the intent.

## Verification

- **Cold builds.**  Base `e5fdd2fa8` against the candidate, each (arm, country) in its own private `LSMS_DATA_DIR`
  on `/local`, three times:
  - Smoke run on a patch: job 41249476.
  - Run on committed `8f0b7d91a`: job 41249611.
  - Run on the **final head** `a495b16d1`: job 41250713.

  All three agree in every figure in this report; the comparisons are identical.
- **Tables built.**
  - Mali, Senegal and Niger: `food_acquired`, `food_expenditures`, `food_prices` (kgvalue, plus unitvalue for Mali
    and Senegal) and `food_quantities`.
  - Uganda: `sample`, `cluster_features` and `household_characteristics`.
- **Warnings.**  Identical, except Senegal's Site Q reports above.  Also, the `StatedVsInferredKgWarning`
  diagnostic for `bouteille de 25 cl` moved from inferred 0.8 to 0.784.  The stated factor is served, so no value
  moves.
- **Hash footprint** (all 38 countries probed):

  | | moved | where |
  |---|---|---|
  | `build_transforms_fingerprint` | 0 of 41 | |
  | `_table_cache_hash` | 96 of 544 | Mali 23/23, Niger 24/24, Senegal 22/22, Uganda 27/27 |
  | `Wave._input_hash` | 307 of 2,258 | Mali 23/92, Niger 24/96, Senegal 44/44, Uganda 216/216 |

  Nothing moved outside those four countries.  Measured at `8f0b7d91a` and again at the final head: the two
  probes are identical, 0 of 2,843 hashes different.  Cost: one re-warm each of Mali (2018-19 wave), Senegal,
  Niger (2018-19 wave) and Uganda.
- **Uganda fixture.**  `tests/fixtures/uganda_baseline.json` `var/sample`: the base build matches the old pin.  The
  candidate's content hash is `0803aacc…`, re-pinned; shape, columns and dtypes are unchanged.  The final-head build
  matches the new pin.
- **Tests.**  Targeted pytest, `-n 8 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`, cold private root.  The
  file set covers every Mali, Niger, Senegal, Uganda and EHCVM test, plus:
  - `test_derivations.py`, `test_build_transform_hash.py`, `test_no_build_input_in_contents_org.py`,
    `test_failure_is_not_absence.py`;
  - `test_gh812_ehcvm_strata.py`, `test_gh1578_derivation_duplicates.py`, `test_schema_consistency.py`,
    `test_categorical_mapping_resolution.py`;
  - `test_gh689`, `test_gh1459`, and the new `test_gh1707_served_corrections.py`.

  | run | result |
  |---|---|
  | base (job 41249612) | 4802 passed, 69 skipped, 0 failed |
  | base, `test_gh1493_near_zero_screen.py` (job 41250718) | 27 passed; it pins the 1e22 prices |
  | first candidate run, `73355b28d` (job 41249778) | 56 failed |
  | **final head `a495b16d1`** (job 41250714) | **4863 passed, 69 skipped, 0 failed** (adds `test_gh1493` and the new file) |

  Skips are data prerequisites, the replication directory, and Uganda parquets the run did not build.

  The 56 first-run failures were pins and harnesses the rules move, and were fixed in commits 8-9:
  - two regex wiring checks;
  - the GH #1500 harness, which stubs the shared hook name and forbids source reads, and its byte digests;
  - Senegal's 7-day totals, by 6,000 FCFA and 5 gift rows;
  - Niger's allowed-key set;
  - the unit-weight row oracles on the withheld records.

  The base `test_gh1493` Senegal test pinned the 1e22 prices; it is re-pinned to their absence.

  **Sweep outside the filename filter** (jobs 41250937 base / 41250938 at `52386ee64`):
  - 85 more test files that name Mali, Senegal, Niger or Uganda, run under `-k <country>`: base 554 passed;
    head 552 passed and 2 failed.  The 2 failures were `test_sampling`'s Uganda historical view, which refused the
    new key.  Fixed in commit 11 and re-run at `3ae15b56c` (job 41251095: `test_sampling.py` and `test_gh1707` whole, 319 passed).
  - Six kg-factor and food files, run whole: 324 passed at both.
  - `test_gh1707` + `test_derivations` at `52386ee64`: 79 passed.  This includes the registered Uganda function
    run on the real inputs, which reproduces the served `ea` on all 514.
  - Hash probe at `52386ee64`: identical to `a495b16d1`.
  `test_gh1459` and `test_gh689` are re-pinned for the Uganda fill.
- **Departures from the hard rules, disclosed.**
  - **Private roots.**  In the explore, validate, test and digest jobs, `bin/savio_venv.sh mount` ran its own
    import check before the private roots were exported.  `LSMS_DATA_DIR` was unset then, so a default data root
    was not overridden for that check.  It imports and reads no data.
  - **Digests job.**  It ran `python -I digests.py` with no private roots; that script reads only source files.
    The sweep job exports the roots first.
  - **n0035.**  System `python3` ran on n0035, only for text edits to tracked files and for reading result JSON.
- **New failure mode.**  Senegal 2021-22 `food_acquired` now raises if `ehcvm_welfare_sen2021.dta` cannot be fetched.
  The file is declared, so its sidecar is hashed.  It is the same kind of new dependency as the NSU tables of #1452.
- **Hygiene.**
  - No in-tree parquet in any clone.
  - The issues logs were private.
  - No `dvc` CLI was run, and no lock was removed.
  - Every source was read through `get_dataframe` into the private L1.
  - `~/.config/lsms_library/config.yml` was untouched, and `LSMS_SKIP_AUTH` was unset.

## Residuals

- **#1708 drop decision.**  This is the maintainer's: drop, NA the Expenditure on the 15,014 rows, or keep.
- **Uganda.**
  - A lost 2005-06/2010-11/2011-12/2013-14 cover silently drops the fill: the sub-df's columns are not declared, so
    #675 does not grade it degraded.  `v` survives.
  - The 55 still NA need data the library does not hold.
- **Senegal.**  The salt constant is scoped to 2021-22, and 2018-19 was not tested for it.
- **CLAUDE.md is stale and was not edited.**  Its Site Q lines name "Senegal 2018-19's float residues near 1e-19"
  (lower tail) and "Senegal's two prices near 1e22" (`food_prices`).  Neither is served now.
- **Rationale.**  `ehcvm.py` and the shared docstrings are unchanged; the rules are wave-scoped by design.

## Files

- `scripts/` (all scripts and sbatch files):
  - `explore.py`, `explore2.py`, `explore3.py`: the read-only measurements.
  - `build.py`, `probe.py`, `compare.py`, `hashprobe.py`, `hashcompare.py`, `uganda_fp.py`
  - `job_explore*.sh`, `job_validate.sh`, `job_tests.sh`
  - `smoke1.patch`: the pre-commit patch.
- `scripts/`, also: `digests.py` and `job_digests.sh`, which recompute the GH #1500 test digests.
- `results/`:
  - `explore*.json`
  - `smoke1/`, `final/` and `final2/` (the final head): build logs, compare, hash and fingerprint JSON.
  - `digests.json`
  - `tests/`: the pytest output and junit for every run.
- `logs/`: Slurm and per-step logs.
- `PR_BODY.md`: draft PR body with close/narrow drafts.
