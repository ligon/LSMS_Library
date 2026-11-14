# Red team: served-wave corrections (Mali #1707, Senegal salt, Niger #1708, Uganda #1536, Senegal #1519)

Reviewer: Claude agent, model **Claude Opus 5.5** (`claude-opus-5-5`), independent of the author, for Sue (Slurm job 41201840). 2026-11-12.
Candidate: `fix/isa-served-wave-corrections-20261112` @ `3ae15b56ceb8093acaf75c8e24369dcd4d012aea`. Base: development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
I did not open the author's report (`reports/served-corrections/`). I judged disclosure only from the branch: CONTENTS.org, `derivations.yml`, YAML comments and commit messages.

## Verdict: **CHANGES** (small, none blocking on data)

On the data, every served change is the one the rulings ask for, and nothing else moves. I checked this with cold base and candidate builds in separate private roots, an outer-merge diff, per-wave `assert_frame_equal`, and raw re-derivation through `get_dataframe`. The hash footprint is exactly the four countries, and no build fingerprint moves. Two changes are requested and two findings should go to the maintainer:

1. **Senegal 2021-22 no longer builds without the welfare file, and it does not need that file.** A selector with no `hhsize` picks exactly the same 100 records: salt, in kg, size-free, with `s07bq03a` a whole multiple of 0.0199997. When I simulated the file being unavailable, `food_acquired()` and `food_expenditures()` raised `FileNotFoundError`; `food_acquired(waves=['2018-19'])` still served. That is loud, not silent, but it is a new and avoidable dependency, and the branch does not state it. Either drop the dependency (read `hhsize` only in the `inputs` callable), or record the failure mode in CONTENTS.
2. **No test pins the served Mali withholding or the served Niger flag.** `TestHeld` pins only the raw selectors. `test_ehcvm_7day`'s Niger change makes the flag *allowed* (`<=`), not required. The Mali AFTER tuple (Expenditure, in-kind rows) cannot see a block with no Expenditure. The evidence is direct: running the base tests on candidate code (Q8) produced **zero Mali failures**. Removing 3,356 served Mali rows was invisible to the existing suite, and the branch added only raw-selector tests. If the hook were unwired, the suite would still pass. Add one `requires_s3` served pin for each: Mali 2018-19 157,856 rows and 2,276 `Riz local Gambiaka`/`Kg` purchased rows; Niger 15,014 stamped rows. A smaller gap of the same kind: Uganda's per-household disagreement guard in `_ea_from_another_wave` (two labels across the comm covers -> `OriginEaError`) has no test; only `_choose_ea`'s cross-group raise does.
3. *Finding for the maintainer (no code change):* the Niger ruling text rests on a reversed premise (Q3).
4. *Finding for the maintainer:* the #1519 Expenditure NA goes beyond the literal ruling (Q5).

**Gating.** Neither change touches served data. Change 1 is a robustness and disclosure fix and change 2 is test coverage, so both can land as a follow-up commit on this branch before merge, or as an immediate follow-up PR, at the coordinator's discretion. Nothing found requires a re-warm beyond the one the branch already costs.

Tests: 0 candidate failures across 9,920 items in 258 + 1 files; the base tests run on candidate code fail on 14 nodes, all in re-pinned files and each explained by its pin (Q8).

## What was run

- **Five burst jobs**, all `savio4_htc`, 8 cores, 64 GB, `--exclude=n0035.savio4`, all COMPLETED, on n0194, n0219 and n0052; none on n0035:
  - 41251812: base build.
  - 41251813: candidate raw checks, candidate build, two outage simulations.
  - 41251821: diff and hash probe.
  - 41251814: candidate tests.
  - 41251822: base tests on candidate code.
- **Clones:** each job made a fresh `git clone --shared` of the mirror, checked out at the full SHA (asserted).
- **Venv:** the mounted image (`bin/savio_venv.sh mount`, generation `20261019T221223Z`), with `PYTHONPATH` and `LSMS_COUNTRIES_ROOT` set to the clone. The import identity was asserted by realpath.
- **Environment:** BLAS pinned to 1 thread, `LSMS_BUILD_WORKERS=1`, pytest `-n 8 --dist=loadfile`.
- **Private roots, exported before the mount:** `LSMS_DATA_DIR` was a separate mode-700 dir per run, under `private/served-corrections-redteam/root-{base,cand,tests,compare,unavail-sen,unavail-uga}`. `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` were under `/local/job<ID>/`. Each job asserted that `data_root()` was private.
- **Hygiene:** in-tree parquet was 0 in all five jobs, checked by `grep -H` over each log. The private issues log had 0 lines in the base build, 12 in the candidate build job and 240 in each test job. The candidate job also ran the two outage simulations, but those logs were under `/local/job<ID>/` and are reaped, so I cannot say which lines came from where.
- **Sources:** read only through `get_dataframe` and the library's own `derivation_inputs`. The scripts are in `scripts/` and the aggregates in `results/` (`cand_checks.json`, `compare.json`, `hash_diff.json`, `unavail*.json`, `pytest_*_summary.txt`). The record-level frames, and the full pytest logs (whose warning summaries and tracebacks can name households), are in the private dir only. No microdata rows appear here or in `results/`.
- **Disclosure, rule slip.** On n0035 I ran system `python3` twice, for trivial work: a no-op `python3 -c 1` in a pipe, and a key count of the committed `Uganda/_/updated_ids.json`. I also ran `jq` there once, on my own results JSON. None of these imported `lsms_library` or read microdata. Everything else ran in the burst jobs.

## Idiosyncrasies read (CONTENTS.org), and how they bear

- **EHCVM keys, all three food countries.**
  - Keys: `v: grappe`, `i: [grappe, menage]`, `visit: vague`.
  - Senegal's `CONTENTS.org` (sub-sample section, quoting the DDI) says each EA was split in half across the two `vague`s. That appears to sit uneasily with CLAUDE.md's "each `grappe` visited in one `vague`"; I read only that section, so I note it rather than assert it. Not material here.
  - The Mali block is exactly the 3,357 records with no `vague`. They reach `food_acquired` with `visit` absent from the canonical key, which is why one of them summed into a real row.
- **Mali.**
  - 2018-19 `s07bq01` labels are padded and fold 138 -> 125 (GH #1513).
  - Kilogram factors are pooled across waves (GH #850/#1487), so a withheld row could in principle move other rows' factors. Measured: it does not; the block rows carry no Expenditure.
  - Household ids are namespaced (GH #1494). `derivation_inputs` walks them.
- **Senegal.**
  - 2018-19 `food_acquired.py` is dead code; the build is the `mapping.py` hook.
  - GH #1500 `Unknown` `u` fill.
  - The 2018-19 near-zero rows were recorded under GH #1489 as "reported, not masked". The branch supersedes that entry in place, with the reason.
- **Niger.**
  - Mojibake repair runs before the `Unknown` `u` fill (GH #842/#847).
  - The label drift between the two EHCVM waves (GH #833) is why the flag key must be stamped after `_decode_mojibake_in_df`/`_fill_missing_u`, which the hook does.
- **Uganda.**
  - **Hybrid `v` in 2009-10:** `comm` is blank for 192 movers, 368 split-offs and 5 originals. That is why `ea` was NA there.
  - **`ea` is a label of origin**, carried with the household.
  - **Two renderings of `comm`** (GH #1133).
  - **Raw-id keys.** `updated_ids` covers only 2013-14, 2015-16, 2018-19 and 2019-20 (checked), so 2009-10's served `i` is the raw `HHID`. The 2013-14 link is `HHID_old`, the id that `updated_ids['2013-14']` walks to.
  - **Single-pass `id_walk`:** the fill runs in the wave hook, before the stage-2 walk, on raw ids. That is correct.

## Q1 Mali: exact, and exactly the block

Raw file `s07b_me_mli2018.dta`, 159,968 records:

- **The selector is exact.** `vague` is NaN on 3,357 records. Every one of them carries the fingerprint:
  - product 1 (`Riz local Gambiaka`, code label checked), unit 100, size 0;
  - `s07bq06` = 4 (`Plus de 30 jours`, from the file's own labels);
  - `s07bq04`, `s07bq05`, `s07bq07a` and `s07bq08` all 0 or empty;
  - `s07bq03a` a whole multiple of 1.4 kg.
- **Household size.** 1.4 x welfare `hhsize` holds on 3,348 of the block. 3 records have no welfare row.
- **Why `hhsize` cannot be the selector.** Among the 2,617 genuine rice-kg records (with a `vague`), 284 equal 1.4 x `hhsize` and 761 are whole multiples of 1.4. Both author figures reproduce.
- **One block record per household:** 3,357 households.

**Served, base -> candidate** (outer merge on stringified keys, NA kept as a value):

- **2018-19 changes:** 3,356 rows go, all `Riz local Gambiaka`/`Kg`/`purchased`, Expenditure NA on every one, 32,307.8 kg. Exactly 1 row changes, and only in `Quantity`.
- **Other waves:** 2014-15 and 2021-22 are `assert_frame_equal` exact.
- **The one household with both a block record and a genuine rice-kg record.** On its served row:
  - base minus candidate equals the block's `s07bq03a`;
  - the candidate value equals the genuine record's purchased quantity;
  - Expenditure, `Quantity_kg` and `Derivation` are unchanged.
- **The real row is intact.**
- **Totals:** 32,307.8 + 5.6 = 32,313.4 kg. The registry's figure is exact.
- **The `inputs` callable** returns 3,357 rows at `(t, i, j)`, all with `vague` NA, summing to 32,313.4 kg. Its key set equals the block records, and equals the 3,356 removed rows' keys plus the changed row's key.
- **Derived tables:** `food_expenditures` and `food_prices` are frame-equal. `food_quantities` (`kgs` and `units`) loses the same 3,356 rows and changes the same 1.
- **The branch's numbers reproduce:** 161,212 -> 157,856 rows; 129,085 -> 125,729 purchased; 25,572 -> 22,216 Expenditure-NA; 5,632 -> 2,276 rice-kg rows; 55,895.1 -> 23,581.7 kg; Expenditure 89,614,678.64 unchanged.

## Q2 Senegal salt: exact, 100 rows, but the welfare dependency is avoidable

- **The constant and its tolerance.** Among the 294 positive salt (code 139, label `Sel`) kg (unit 100, `100. kg`) size-free records, exactly 100 sit within the code's 1e-9 tolerance of 0.0199997 x `hhsize`. The largest deviation is 5.6e-17. The count is still 100 at every tolerance from 1e-12 to 1e-3.
- **No other product matches.** Over all products, 100 records match at 1e-9, so no non-salt record meets the constant. At 1e-6 the count is 102, because of two non-salt records.
- **It is not 0.02.** No record equals 0.02 x `hhsize`; 7 are within 1e-6 of it.
- **The welfare join.** `(grappe, menage)` is unique in the welfare file and covers all 294 salt records.
- **What the 100 were served as.**
  - 95 purchased rows and 5 in-kind rows (5 records are wholly gifts).
  - 17.04 kg.
  - 17.78 FCFA of Expenditure, on the only 2 rows with a same-unit last purchase. 28 of the 100 have an `s07bq08`.
- **2018-19 does not have the pattern.** It has 0 records at 0.0199997 x `hhsize` and 1 at 0.02 x `hhsize` (code 118 is `Sel` there). Scoping the rule to 2021-22 is right.
- **Served, base -> candidate:**
  - 2021-22 loses exactly the 100 rows. Their `(t, i, j)` keys are the selected records', all `Sel`/`Kg`.
  - The other 194 salt-kg rows are all present and unchanged. No other 2021-22 row or column changes.
  - `food_expenditures` and `food_prices` lose 2 rows each, and `food_quantities` 100. No other row's kg factor moves (0 changed rows).
  - Expenditure 182,628,314.17 -> 182,628,296.39 and in-kind 10,917 -> 10,912, as pinned.
- **The registry.** `inputs` returns 100 records, the selected set.
- **The dependency is avoidable.** "Salt-kg and `s07bq03a` a whole multiple of 0.0199997" selects the identical 100 records with no `hhsize`, at tolerances from 1e-5 to 1e-9, and its multiple equals `hhsize` on all 100. Mali reads no `hhsize`; Senegal could follow it.
- **What an outage does now.** I simulated `FileNotFoundError` for `ehcvm_welfare_sen2021.dta` only, in a fresh root.
  - `food_acquired()` raises `FileNotFoundError`.
  - `food_expenditures()` raises `FileNotFoundError`; there is no legacy route.
  - `food_acquired(waves=['2018-19'])` serves its 215,660 rows.
- **So the failure is loud, not read as absence**, which is acceptable under GH #1546. But it is a new failure mode for a whole table. The branch says only that the file is "declared so the sidecar is in this table's hash".
- **The hash side.** `_collect_file_paths_from_block` does pick up `household_size.file`.
- **Untested:** a DVC or network exception class other than `FileNotFoundError`.

## Q3 Niger: the author is right and the ruling's premise is reversed

- **The labels.** `s07bq06` codes from the file's own labels: 1 `Hier`, 2 `7 dernier jours`, 3 `30 derniers jours`, 4 `Plus de 30 jours`, 5 `Jamais`.
- **The flagged rows are where the field is enabled.** 15,264 of the 107,263 positive `s07bq07a` are off the quarter grid (14.23%). By `s07bq06`:
  - 3,543 are `Hier`, 10,774 `7 jours` and 946 `30 jours`, all codes where the CAPI enables the field;
  - 1 is `Plus de 30 jours`.
- **The disabled-but-filled records are a different set.** `s07bq07a` is filled on 21,063 records where it is disabled: 17,924 code 4, 3,086 code 5 and 53 missing. On those:
  - only 1 is off-grid, the same record as above;
  - 8,050 equal `s07bq03a`;
  - `s07bq08` is present on 0, so none feeds a served value.
- **So the ruling text "CAPI disables it there" describes the other set.** The doc search's §5c in fact lists the two separately. The branch's CONTENTS states the correction; relay it, since the ruling was made on the other premise.
- **The flag key is on exactly the intended rows.**
  - 15,014 rows change, only `Derivation`, all `purchased`, all 2018-19.
  - Each gains exactly this key and loses nothing.
  - Index, values and 2021-22 are exact.
  - `food_expenditures`, `food_prices` and `food_quantities` are frame-equal.
  - The stamped `(t, i, j)` set equals the set of flagged records with a positive purchased quantity: 15,014 = 15,014, 0 either side. No unstamped purchased row shares a `(t, i, j)` with a flagged record.
  - 12,227 stamped rows carry Expenditure, 26.04% of 2018-19's.
  - `inputs` returns the 15,264 records, with `s07bq06`.
- **The diagnosis holds as stated.**
  - Grid and decimals: 14,908 flagged values have 13 or more decimals. 2021-22 has 15 off-grid values of 112,923.
  - **The cell constant.** On 13,734 flagged records (author 13,735; my 6-significant-figure rounding), the unit value `s07bq08`/`s07bq07a` equals its (product, unit, size, region) cell's modal value P. That is over 926 cells, 431 of them with an integer P.
  - **Integer and non-integer values.** 12,250 flagged records have an integer `s07bq08`; 3,013 do not, and 3,008 of those sit at P. On-grid unit values in the same cells run 0.457 to 1.365 x P (p5 to p95).
  - **The reading.** The quantity was backed out of a reported value at a reference P, or both were rebuilt. This is consistent with the evidence, and the branch rightly calls the mechanism "not established".
  - **P is also a common real price.** 3,609 of 19,936 on-grid records in those cells sit exactly at P. Matching P alone proves nothing; the decimals do the work.
- **Residual, not flagged.** 162 on-grid records with an `s07bq08` also carry a non-integer `s07bq08`. That is the same "cannot have been entered" signal the branch uses on the flagged class, but on rows the flag cannot see. Worth a line in CONTENTS for the drop decision.

## Q4 Uganda: sound and consistent

- **Base -> candidate `sample`:**
  - same index;
  - only `ea` and `Derivation` change, on exactly 514 rows, all in 2009-10, all NA -> label;
  - 0 already-served `ea` change, in any wave;
  - `v` changes on 0 rows;
  - the stamp is on exactly the 514. 20 of them already carried `unlocated-identity` and now carry both keys.
- **Composition:**
  - **Filled:** `hh_status` 3 (split-off) 317, 2 (mover) 192 and 1 (original) 5.
  - **Left:** 51 split-offs, plus 4 households not on GSEC1.
- **The weight share NA:** 29.32% -> **3.53%** of the 2009-10 weight. 4 of the 55 carry no weight.
- **Distinct `ea`:** 2009-10 317 -> 321. All waves 427 -> 427; every filled label is one another wave already serves.
- **Precedence and agreement.**
  - **Sources:** 197 from 2005-06, 440 from 2010-11, 420 from 2011-12 and 246 from 2013-14. These are `inputs` rows, 1,303 in all.
  - **Agreement across waves:** 0 of 514 filled households disagree with their own label in any other served wave.
  - **Non-source waves agree too:** 2015-16 (229 households), 2018-19 (207) and 2019-20 (197), which are not sources, so the check is not circular there.
  - **The `OriginEaError` guard** never fired, so "the order decides nothing today" holds.
- **Positive control on the `HHID` key.** The 2,410 households with their own `ea` agree with the same `i` in every other wave: 2005-06 2,410/2,410, 2010-11 2,207, 2011-12 2,167 and 2013-14 1,442, all with 0 disagreements. 2015-16, 2018-19 and 2019-20 also show 0.
- **Downstream tables.** `cluster_features` (which carries no `ea`) and `household_characteristics` are frame-equal in every wave. The `v`-join is untouched.
- **The registered function** reproduces the served value on all 514 households from `derivation_inputs`.
- **Outage.** I simulated `FileNotFoundError` for `2013-14/Data/GSEC1.dta` and built `sample(waves=['2009-10'])` cold.
  - That build filled 492 rows and left 77 NA (5.11% of weight). It emitted the generic `UserWarning` "could not load sub-df 'ea_2013_14' … proceeding without it", and the drop was stamped in `lsms_build_drops`.
  - In this run, the next fresh process (source available again) emitted a `CacheRebuildWarning`, rebuilt, and served 514 and 55. I did not capture the warning text, so the cause of that rebuild is unknown. The GH #675 degraded grade, as CLAUDE.md states it, needs a lost declared column, and `_ea_2013_14` is a temporary column. So "a lost cover costs the fill and says so" is established; "it heals" is observed once, without a known mechanism.
  - Not tested: a persistent outage, and whether a warm read during one warns or silently serves 77 NA.

## Q5 Senegal #1519: consistent mechanically, but beyond the literal ruling

- **The rule hits exactly 3 records** (0 in 2021-22). All are product 132, with `s07bq03a` = 40 x `s07bq07a` exactly, no own production or gift, and 3 households.
- **Served change.** Only those 3 rows change: Quantity becomes NA on all 3, and Expenditure on the 2 that had one. `Derivation` and every other row are unchanged.
- **Prices.** `food_prices` 2018-19 maximum 1.79e22 -> 479,452, mean 2.17e17 -> 1,698.51. `unitvalue` maximum 4.79e21 -> 70,000. No price above 1e10 is left.
- **What NA removes.** The served Expenditure was (q3/q7) x q8 = 40 x q8: 6,000 FCFA in all, on the 2 same-unit rows. That value is finite and invariant to whatever common scale corrupted q3 and q7.
- **Is it consistent?** Setting it NA follows mechanically from the registered #876 rule: NA quantity x unit value is NA. The branch discloses it (`columns: [Quantity, Expenditure, Quantity_kg]`, CONTENTS).
- **But the ruling said "set the quantity to NA"**, and dropping 6,000 FCFA of a value that does not depend on the bad scale is a choice. Put it to the maintainer as such. I would not block on it.

## Q6 Hash footprint: exactly the four countries, no build fingerprint

I probed every country x table `_table_cache_hash`, every wave x table `Wave._input_hash`, and `build_transforms_fingerprint` for all 40 tables plus `None`, base clone against candidate clone. There were no errors.

| | Mali | Niger | Senegal | Uganda | elsewhere |
|---|---|---|---|---|---|
| `_table_cache_hash` | 23 / 23 | 24 / 24 | 22 / 22 | 27 / 27 | 0 of 448 |
| `Wave._input_hash` moved / probed | 23 / 92 (2018-19 only) | 24 / 96 (2018-19 only) | 44 / 44 | 216 / 216 | 0 of 1,810 |

`btf`: 0 of 41 moved.

## Q7 Other rows

Every touched table was compared base -> candidate by outer merge on stringified index keys, NA as a value, with a duplicate ordinal: 0 NaN-key or duplicate issues. Per-wave `assert_frame_equal` was exact on the untouched waves.

Across the 17 calls, the only differences are the ones listed in Q1-Q5:

- Mali (4 tables + `food_quantities(units='units')`);
- Senegal (4 + `food_prices(units='unitvalue')`);
- Niger (4);
- Uganda `sample`, `cluster_features` and `household_characteristics`.

Nothing appears only in the candidate. Warning classes and counts differ only as expected: Senegal has one fewer `QuantityImplausibleWarning`, because the near-zero residue is gone.

## Q8 Tests: pins justified, nothing unpinned, two mild weakenings

**Selection.** Every test file that names Mali, Senegal, Niger or Uganda (word-bounded) or `ehcvm`: 258 files, plus the new `test_gh1707_served_corrections.py`. These were run in a private, initially cold root. **293 of the 551 test files were not run** (other countries, framework-only); the 14-node base-on-candidate check covers only the 258.

- **Candidate code, candidate tests:** 9,723 passed, 189 skipped, 8 xfailed, **0 failed**, in 951 s (`results/pytest_cand_summary.txt`; the full log is private). Every candidate counterpart of the 14 nodes below PASSED, none SKIPPED: the renamed `test_gh1493...serves_its_residues_na_since_gh1519` and the re-parametrised `test_gh689...[2009-10-2410-514]` included. So did all 5 `TestHeld` tests. `test_uganda_invariance[var/sample.parquet]` passes against the re-pinned hash. Note that this run compared only 12 of that fixture's 53 entries; the other 41 were SKIPPED as not built in this root.
- **Candidate code, BASE tests** (`git checkout e5fdd2fa8 -- tests` wholesale, same warm root): 9,704 passed, **14 failed** (`results/pytest_basetests_summary.txt`).
  - Every one of the 14 is in a file the author re-pinned, and every one maps to its pin.
  - No file the author left alone fails. So there is no unpinned failure in this selection.
  - The author's "56" was measured at the intermediate head `73355b28d`. Commit `e1c23e848` says those 56 included the GH #1500 harness, which it then avoided by restructuring the Senegal 2018-19 hook; at the final head the count is 14.

| base test failing on candidate code | pin | reason sound? |
|---|---|---|
| `test_ehcvm_7day` key test [Niger-2018-19] | allow the #1708 key | yes, but **permissive**: allows the key, never requires it |
| `test_ehcvm_7day` served total [Senegal-2018-19] | -6,000 FCFA | yes (Q5) |
| `test_ehcvm_7day` gifts [Senegal-2021-22] | in-kind 10,917 -> 10,912, -17.78 FCFA | yes (Q2) |
| `test_ehcvm_unit_weights` older-row and inputs-weight [Senegal / Senegal-2018-19] | `_withheld()` drops `rows: 0` records from the oracle | yes: the 3 #1519 rows are now NA. See note 2 |
| `test_gh1500` digests and AST (2) | re-hashed | yes, mechanical trip-wires. The AST pin still guards unrelated edits |
| `test_gh1459` counts and frame-code [2009-10] (2) | NA 569 -> 55; a stamped fill may sit on a non-frame `v` | yes, and stricter: now asserts 514 stamped and none on a frame code |
| `test_gh689` early waves [2009-10] | 2,410 unstamped + 514 stamped | yes, stricter |
| `test_sampling` reproduction (2) | allow the key in the historical view, scoped to 2009-10 | yes: `v` untouched (Q4) |
| `test_uganda_invariance[var/sample.parquet]` | re-pinned hash | yes: shape, columns and dtypes unchanged |
| `test_gh1493` Senegal residues | rewritten to "none served" | yes. See note 1 |

**Weakenings, mild:**

1. **`test_gh1493`.** The Senegal real-data positive control for the lower-tail and `food_prices` screens is replaced by an absence assertion. The synthetic tests still cover the mechanics.
2. **`_withheld()` in `test_ehcvm_unit_weights`.** It takes its exclusions from the library's own `derivation_inputs`, keyed `(i, j)`, so it trusts what it is testing. Measured, it drops:
   - 1 genuine Mali record (the household with both);
   - 0 extra Senegal salt records;
   - the 3 near-zero records themselves (I did not count other records sharing their `(i, j)`).

   Negligible today. It would silently widen if an `inputs` callable over-returned.

**Not weakened, but a gap (requested change 2):** no test pins the served Mali withholding or the served Niger stamp.

**Nit.** Senegal 2018-19 defines a module-level `_food_acquired_with_unit_weights` wrapper. It shadows the shared function's import name, which every other EHCVM wave uses for the shared hook. So `test_ehcvm_7day::test_the_hook_calls_the_shared_derivation`'s regex now matches the wrapper. Behaviour is correct (the wrapper calls the shared function), and the commit says why. Readers should know the name means something different in that one file.

## Summary of requested changes

1. **Senegal 2021-22.** Drop the welfare-file dependency from the build: "salt-kg x whole multiple of 0.0199997" is identical on all 100 records; read `hhsize` only in `inputs_salt_per_capita`. Or state in `Senegal/_/CONTENTS.org` that `food_acquired` and the derived food tables now raise if `ehcvm_welfare_sen2021.dta` cannot be fetched.
2. **Served pins.** Add a `requires_s3` served-table pin for Mali 2018-19 (rows, rice-kg purchased rows and kg) and for Niger 2018-19 (15,014 rows carry the #1708 key; no row of another wave or source carries it).

**For the maintainer, not blocking:**

- the Niger ruling's premise is reversed (Q3);
- 6,000 FCFA of scale-invariant Expenditure is dropped beyond the literal #1519 ruling (Q5);
- 162 on-grid Niger 2018-19 records carry a non-integer `s07bq08` and are not flagged (Q3).

## Retained private data (for deletion)

`/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam/` (mode 700). It holds:

- the six data roots (DVC blobs plus caches): `root-base`, `root-cand`, `root-tests`, `root-compare`, `root-unavail-sen`, `root-unavail-uga`;
- `frames-base` / `frames-cand` (pickled built frames and record-key frames);
- the full pytest logs.

Delete it when the review is closed. Nothing there is needed to read this verdict.
