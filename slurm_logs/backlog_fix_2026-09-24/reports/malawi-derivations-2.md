## SCOPE DEVIATIONS

None of the stop-list kind; no `!!! SCOPE-VIOLATION-CANDIDATE !!!` entries. One file touched that the task did not name as an edit target (it names it only in the pytest recipe): `tests/test_malawi_shipped_factors.py`, where `test_not_applicable_against_shelled_attaches_as_the_basis` (a no-reference fixture) now asserts the key `sale-basis-no-reference` instead of `sale-basis-not-applicable`, moved by ruling 2, plus one added assertion that the measured key's count is 0. Nothing weakened, skipped or xfailed. `slurm_logs/backlog_fix_2026-09-24/issues_to_file.org` did not exist on `development` or on any branch; I created it, as the task asked. `git status --short` is empty. No force-push, no amend after push, no `gh` write, no DVC CLI.

## 1. Commit and branch

- `a2051b6ac` on `fix/malawi-derivations-2`, pushed to `origin` (the mirror; `refs/heads/fix/malawi-derivations-2` = `a2051b6acbe27bb7024d003b561edeaf53b1ea95`). One commit. Its message says "cold API build to follow in the next commit if any pin moves"; the cold build then reproduced every pin exactly, so there is no second commit.
- Files: `Malawi/_/malawi.py` (two rungs, the per-ground partition of the deletion, the reworded warning), `Malawi/_/derivations.yml` (eight entries: two new, four edited), `Malawi/_/CONTENTS.org` (LADDER sections rewritten for eight keys, the inline reversal marker, the collapse attribution, the cache-cost sentence), `tests/test_malawi_sale_basis.py` (re-baselined pins, new static and delivered tests), `tests/test_malawi_shipped_factors.py` (above), `slurm_logs/backlog_fix_2026-09-24/issues_to_file.org` (new).

## 2. Verification (private `LSMS_DATA_DIR`, all foreground with timeouts; scripts and logs in `$STAGE/verify/`)

| command | result |
|---|---|
| source replay of `_attach_sales` per wave (`replay.py`, before the cold build) | overrule 0/0/2/2 = 4 / 157,000 MWK; not-applicable (measured) 5/0/42/67 = 114 / 25,416,200; no-reference 117/11/37/131 = 296 / 13,270,507; unresolved 94/21/88/138 = 341 / 13,648,180; tier B 1:1 ambiguity 0 in every wave |
| `python -m lsms_library.cli cache clear --country Malawi` (private root) | exit 0; 0 parquets left under `Malawi/` |
| cold API build of `crop_production` (`build_cp.py`) | exit 0; 45 s; 135,078 rows; index unique; total `Value_sold` 1,162,507,323 (was 1,162,350,323 on the 2026-09-24 ladder; +157,000 = the overrule rung exactly) |
| monotonicity vs the reviewer's 2026-09-24 cold build (`compare.py`) | index identical; 24,066 rows attached both times, `Quantity_sold`/`Value_sold` identical; 4 newly attached rows = 157,000 MWK, all `from-price-overrule`; 296 rows re-keyed `not-applicable` -> `no-reference`, values unchanged; all harvest columns identical (three compared unequal as strings; NA pattern identical and 0 differing non-null values) |
| `grain_reports()` after | `[]`, unchanged |
| `null_index_reports()` after | `plot_id` 41 (2016-17), `u` 8,902 (1,279 / 594 / 3,408 / 3,621), identical to before |
| `null_read_reports()` after | `[]` |
| `ll.Country('Malawi').derivations('crop_production')` and `ll.derivations()` | all eight keys listed; `attrs['derivations']['Malawi']` rows equal the column counts (wildcard 2,778, no-reference 296, c-sale 217, not-applicable 114, c-harvest 102, from-price 38, overrule 4, unresolved 0) |
| `food_expenditures()` carries `Derivation` | False (681,980 rows) |
| `find lsms_library/countries -name '*.parquet'` | empty |
| pytest, verbatim recipe (`-n 8 --dist=loadfile`), `tests/test_malawi_sale_basis.py tests/test_malawi_shipped_factors.py tests/test_derivations.py tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_feature_canonical_index.py` | `498 passed, 39 warnings in 439.43s (0:07:19)`; 0 skipped (the `TestDelivered` tier ran against the cold-built private root) |

Per wave and per key, delivered ROWS [MWK] (cold build; rows == attached sales on every rung but the wildcard):

| wave | wildcard | from-price | from-price-overrule | not-applicable (measured) | no-reference | contra: sale | contra: harvest | unresolved before -> after |
|---|---|---|---|---|---|---|---|---|
| 2010-11 | 947 [5,664,488] | 1 [800] | 0 | 5 [68,400] | 117 [3,087,827] | 67 [599,290] | 19 [77,740] | 94 -> 94 / 1,088,250 |
| 2013-14 | 341 [2,974,002] | 0 | 0 | 0 | 11 [307,000] | 18 [292,200] | 9 [54,700] | 21 -> 21 / 315,300 |
| 2016-17 | 449 [12,372,397] | 8 [348,000] | 2 [37,000] | 42 [7,535,300] | 37 [2,391,230] | 47 [2,266,100] | 23 [1,686,540] | 90 -> 88 / 5,972,600 |
| 2019-20 | 1,041 [32,298,952] | 29 [638,500] | 2 [120,000] | 67 [17,812,500] | 131 [7,484,450] | 85 [3,415,900] | 51 [2,238,500] | 140 -> 138 / 6,272,030 |
| total | | 38 [987,300] | 4 [157,000] | 114 [25,416,200] | 296 [13,270,507] | 217 [6,573,490] | 102 [4,057,480] | 345 / 13,805,180 -> 341 / 13,648,180 |

- Before the ladder (the GH #833 bundle) the unresolved tally was 303 / 59 / 247 / 503 = 1,112 sales / 64,110,157 MWK. Before this branch, `not-applicable` carried N + NR (122 / 11 / 79 / 198 = 410 / 38,686,707).
- Unresolved by ground (new `attrs['sale_basis_ladder'][unresolved]['grounds']`, a partition of the sales): contradiction in a non-separating cell 271; contradiction with a price in the middle half 57; NA sale, price undecided 6 (267,800 MWK); S/U sale vs NA row, margin failed 7 (550,400 MWK); claimed by tier A 0.
- Margin prose (ruling 3): 77 of the 357 sales admitted on the three price rungs (9/1/9/58) have a price strictly between the medians but in the outer quarter; 80 of 361 with the overrule rung. The gate is unchanged.
- The four overruled sales are all Groundnut 50 kg Bag on the fine rung: 2016-17 one recorded unshelled priced as shelled (logp 9.10 vs m_S 9.21 / m_U 8.70) and one recorded shelled priced as unshelled (8.29); 2019-20 both recorded unshelled and priced as shelled (9.21 vs 9.30 / 8.52).

## 3. Cache cost

`malawi.py` is in every Malawi table's input hash, so all 21 registered Malawi tables re-warm on the next read of a shared root (the review's measured count; the 2026-09-24 "eleven" was the count of warm parquets in that agent's root and is corrected in CONTENTS). Paid only in my private root. `derivations.yml`, `CONTENTS.org`, the tests and `issues_to_file.org` move no hash. Nothing outside Malawi moves.

## 4. Surprises, and anything contradicting CONTENTS.org

1. The review's verdict and the previous report the task names by repo path are not in the clone or on the fix branches; they live on `origin/development` and in scratch (`backlog_review_39205842/malawi-derivations/VERDICT.md`, `backlog_fix_39204616/malawi-derivations/REPORT.md`). Read from scratch.
2. `make: *** No rule to make target 'panel_ids.json'` appears in the build log. That is Malawi's Design-B pending state (CLAUDE.md, `DESIGN_B_PENDING`), pre-existing and unrelated.
3. The wave-script defensive collapse's 27 dropped sales / 373,600 MWK (6/3/1/17 per wave; Citrus 1005/1010/1011, Groundnut 11/12/16, Maize 1/3) is NOT fixed, as instructed; its accounting is corrected in CONTENTS and the test comment, and the issue text is in `slurm_logs/backlog_fix_2026-09-24/issues_to_file.org`.
4. CONTENTS "The rule" still records the 2026-09-24 morning decision that NA against S/U is a contradiction; it now carries an inline `[REVERSED the same day ...]` marker rather than being deleted, so the record of the decision survives beside its reversal.
5. Tobacco is the one crop split across the two rung-3 keys (72 measured, 37 no-reference); Cotton is entirely no-reference (57): no wave holds 10 shelled and 10 unshelled Cotton sales, so its admission rests on the instrument's wording and the probe's pooled cell only. Both stated in CONTENTS and in the entries.

## 5. Decisions, named

1. The unresolved `grounds` partition is per SALE (each unresolved sale classified by its first refused tier-B pair), not per pair: a per-pair count double-counted a sale admitted through one row and refused through another (fixture `x`). The invariant `sum(grounds) == unresolved sales` is tested on synthetic frames and on 2013-14 from sources.
2. Ruling 1 is applied on the harvest-NA side only. An NA SALE whose unambiguous price names a basis the single unclaimed row does not carry stays unresolved: there is no row to attach it to, and re-conditioning a harvest row is not attachment. The served row under the overrule keeps its recorded `condition` (NA); the price-chosen basis lives on the decision frame (`basis`), as on every price rung.
3. `no-reference` includes the sub-case where a `(crop)` cell exists but the sale's own `(crop, u)` cell holds fewer than 3 reference sales for the offset (the `mid` rung's own gate). Stated in the entry's rule.
4. The overrule entry's `validated_against` says its accuracy cannot be measured directly (every sale on it has a recorded answer the price disagrees with and there is no third source), and gives the sale field's 61-77% agreement with price on the contradictions as the expectation.
5. The reworded unresolved warning keeps the word "CONTRADICTS" (it is the first of the three grounds), so the three existing `match='CONTRADICTS'` assertions stay valid without weakening them.
6. Cosmetic, not fixed because the branch is pushed: `test_the_margin_prose_matches_the_gate`'s second assertion contains a no-op `.replace()`; the first assertion carries the check.
7. The probe's NULL-plot perennial sale (1 sale, 2016-17 Mango, 3,600 MWK) remains unfixed and counted in neither tally, as before.

The same report is at `$STAGE/REPORT.md`.