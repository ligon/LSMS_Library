**Delta verdict: APPROVE.** Candidate `d4d063fa11dfe582409e830c7b488f34fdae468d` against base `f138a3d61c203f892148592675a7ef72e160a322`, reviewed as the delta from `2e5b9dcd7`.

No served value is wrong, both rulings are implemented as written, and every new construction is registered with inputs that join exactly to the rows that carry its key. Four should-fix items remain: three are text-only and move no cache hash, and one is test tightening. None of them gates the merge.

## The six points

**1. Harvest from dispositions: faithful to the shared rule, and the inputs join.**
- It runs through the same `ehcvm.derive_hh_crop_harvest_total` as Benin and Côte d'Ivoire.
- An independent oracle on the raw unit codes reproduces it exactly:
  - 6,397 finished crops with no recorded quantity;
  - 3,958 qualify: 3,421 on a single line, 537 on a Farm-level row;
  - 2,272 span units and stay NA, and 167 record no disposition;
  - 739 crops with an unfinished line are held out;
  - the sum is 901,407.87, equal to the served total.
- Inputs join 3,958 of 3,958 rows, and `Quantity` equals the sum of the input dispositions on all 3,958.
- The 2018-19 in-row check reproduces the worker's 12,490 rows: 87.9% exact, 90.5% within 10%, median ratio 1.000.

**2. `production_obs`: verified.**
- The index is unique. All 8,476 lines carry a unique `row:dvc:` token, and all 642 Farm-level rows carry `unresolved_observation`.
- The 4 rows that repeat the old key are two code-65 pairs. None carries a key or a quantity.
- There is no grain report, and `LSMS_GRAIN_STRICT=1` now passes on all waves (24,705 rows) and on 2018-19 alone.
- The strengthened guard asserts what the worker says it does.
- 2018-19 rows are equal on every base column.
- **Feature equality holds on values, not on dtype.** The Burkina 2018-19, Mali and Niger slices are value-equal in every Feature mix I tried. But `production_obs` changes dtype from `str` to `string` in any Feature call that leaves out Niger; with Niger included, base is already `string`.
- `data_info.yml` and `feature.py` are untouched, and the diff touches nothing outside Burkina's directory and tests.

**3. The crop labels.**
- With my own (broader) pattern, the roster text names cashew on 58 of 59 code-55 lines; the worker's narrower pattern gives 54, so that is not an error. It names banana on 5 of 5 code-58 lines and soja on 88 of 92 code-64 lines.
- The four code-64 lines whose text names maize are served as Soja, as the code says.
- No source-qualified crop label remains.
- **Code 65:** the evidence supports serving s16d's 'Autre (à préciser)'. The Précisez field is filled on 38 of 38 code-65 lines and 0 of 59 code-55 lines. I do not count it as an unregistered construction: it is a source value label carried across by the code link, which is already registered as `derived-from-data`. But the registry text is now false for 65 (S1 below).

**4. Unit canonicalisation does not touch shared code.** `ehcvm.py` is not modified anywhere on the branch. `served_unit_labels` is only called, and Burkina's 2018-19 `mapping.py` already called it.
- No `Kilogramme` is served; `u` and `Unit_sold` share 'Kg' and 'Plat Yoruba'.
- Other countries' values are unchanged (Mali and Niger slices value-equal).

**5. Hash census, re-run at `d4d063fa1`:**

| kind | moved / total | where |
|---|---|---|
| `build_transforms_fingerprint` | 0 / 42 | – |
| framework imports | 0 / 38 | – |
| `Country._table_cache_hash` | 25 / 555 | all Burkina |
| `Wave._input_hash` | 75 / 2,321 | all Burkina |

There were 0 errors.

**6. Must-states.** The PR body carries most of them; the four it lacks are listed at the end.

Also re-verified:
- `Condition_sold` is on all 1,548 sales. The source labels match the 2018-19 fold exactly: shelled is Grain 764 + Décortiqué 94 = 858; unshelled is Epis 13 + Non-décortiqué 344 = 357; shell_not_applicable is 333.
- `Value_sold` is 270,464,270 FCFA, which is also the 2021-22 `gross_crop_revenue` total.
- All four keys' inputs join 1:1: 1,331 / 179 / 3,958 / 159.
- The 2018-19 slices of `harvest_kg`, `yield_kg`, `gross_crop_revenue`, `crop_diversity` and `median_price_valuation` equal base.
- The food tables are as at the previous head: no existing price moves, 17 kg factors move, 12,843 existing produced/inkind rows gain `Expenditure`.
- `interview_date` and `plot_inputs` are frame-equal to base.
- Tests: 9 files, 612 passed, 3 skipped (tests needing three warm caches), 0 failed (job 41276882).

## Should-fix

- **S1. The registry text is false for code 65.** The `crop-code-from-roster-text` rule says "every other code keeps the harvest file's own label". Code 65's label comes from s16d, and its 41 rows carry no key. Add a sentence naming 65 and its evidence (the Précisez count and the code link). `derivations.yml` is not a build input, so this moves no hash.
- **S2. The 12,490 wording.** 12,573 finished rows have every positive disposition in the harvest unit (87.4% exact). 12,490 is that set restricted to rows with at least one disposition (87.9%). Fix the prose in CONTENTS.org, `derivations.yml` and REPORT.
- **S3. Two tests were loosened.**
  - The condition-vocabulary assertion was dropped.
  - The text-key inputs test now accepts a superset, although the join is exact today (159 of 159).
  - Restore both.
- **S4. Two dispositions disclosures are missing.**
  - The number of totals summed across different product states is not given (Mali's entry gives its count).
  - "One UML list" for the bare unit codes is asserted, not shown. It affects 32 derived totals.

## Must-states for the PR body

- At the Country level, 2018-19's index gains a level: `production_obs`, `unresolved_observation` on every row.
- The 2021-22 `harvest_kg` (586 rows) and `yield_kg` (506) are mostly disposition totals. The 179 `harvest-to-date` rows among them are partial harvests, and 57 of the `harvest_kg` rows are Farm-level rows, not plots.
- `production_obs` dtype in Feature: `str` becomes `string` in any call without Niger; values are unchanged.
- Code 65 is served as s16d's 'Autre (à préciser)' (the worker's open question 2), and 4 code-64 lines whose text names maize are served as Soja.

## Not re-run at the new head

Warm-equals-cold equality, the cold single-wave requests, the seed match, Site Q details, the whole suite, and the survey unit-weight comparison for the 17 moved kg factors.

The `inrow_2018` block in `out/delta-cmp.json` is zeros from a bug in my script; `out/fu-cand.json` (job 41277132) supersedes it.

All outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/burkina-redteam/out/`.
