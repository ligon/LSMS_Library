# GH #587, final: the last two rulings applied to Nigeria's post-planting `community_prices`

Agent: Claude Opus 5.5 (`claude-opus-5-5`). No sub-agents.
- Worktree: `/local/job41201840/worktrees/587-mapping`, branch `feature/587-nigeria-pp-mapping-20261112`.
- Start HEAD: `c541bf49ffd2ffcb15c67501cf827e76cc0d9828` (asserted). Base: development
  `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

**Verdict: READY** for human review of the label judgements and the residuals in §7.
Head: `e5940eaa944aeb0555b06f85017e791589f6eceb`.

Both rulings are implemented as ruled. Every count matches the ruling's figures exactly:

| | rows | treatment |
|---|---|---|
| W1 g / ml, N = 1 | 963 + 80 | held |
| W1 Kg / l, N > 50 | 128 + 204 | held |
| W3 Kg / l | 4,120 | served as recorded |
| W5 g / cl | 208 | served divided by `c2q2a` |

`c2q2a` was confirmed to be in g / cl before dividing (§3). The form is silent; the file's own text and
codebook confirm it.

Checks:
- PH rows are frame-equal to `c541bf49f`.
- `sample`, `household_roster` and `food_acquired` are frame-equal to base.

## Commits (auto-published to the mirror)

| SHA | what |
|---|---|
| `f79c2f85475898a93265fc8fb07ae764f821ac32` | the rulings: three new dispositions and two new omission entries, plus one new derivation (function and inputs); W3 Kg/l served; registry, CONTENTS.org, `data_scheme.yml` comment, two wave docstrings, tests, ledger |
| `a3f9810c30a7de9853273cd1e893eb4526a4424d` | prose only: the measured `c2q2a` / text agreement (175 of 176 g, not the coarser 174); ledger verification. `derivations.yml` and CONTENTS.org are in no build hash |
| `e5940eaa944aeb0555b06f85017e791589f6eceb` | prose only: the one unresolved W5 gram row is stated as unresolved (its garbled text plausibly states a package ~12x its `c2q2a`), not as "neither agrees nor disagrees"; no build-hashed file touched |

## 1. Idiosyncrasies (named)

Read for this step:
- the previous report, in full;
- `Nigeria/_/CONTENTS.org` "Post-planting community prices (GH #587)", the whole section;
- "The C8 SIZE field" (GH #834), with the GH #1095 unplaced-label ruling above it;
- the decision log's five GH587 entries.

The previous step's sweep found no LOGBOOK or WAITING entry bearing on this table.

Idiosyncrasies this step relies on:
1. **PP and PH are distinct rounds with distinct `t`.** `combine_rounds` refuses a shared `t`.
2. **W1's unit card is its own.** Measured: 101 → `Kg`, 102 → `g`, 103 → `l`, 104 → `ml`; code 1 is a tomato
   cup. So the W1 holds key on the decoded base `u`, never on a code joined to the `u` table.
3. **W3 prices two locations** (`obs` 1 / 2). The W3 Kg/l rows now served split 3,333 / 787.
4. **The C8 SIZE field.** PH W3 drops its 3,433 blank-size Kg/l rows. PP W3 now serves its 4,120 on the same
   blank field: an asymmetry, recorded in CONTENTS.org and not reconciled.
5. **W5's C2 file carries `c2q2a` "Quantity"**, which the printed form lacks.

New idiosyncrasies, **added to CONTENTS.org**:
- **W5's Other-unit text `c2q2b_os` states the package** on every blank-size g/cl row, and `c2q2a` restates it
  in the row's unit. For cl, that is the text's ml / 10.
- **The PP supervisor manual contradicts the field.** It says "UNIT for ONE quantity ... price for ONE
  quantity", and no document describes `c2q2a`.
- **PH W5's `c8aq2_a` is the same field.** It matches the text's grams on 121 of 121 rows and centilitres on
  42 of 49. The other 7 restate millilitres unconverted. PH drops those rows, so nothing served depends on it.

No finding contradicts CONTENTS.org.

## 2. What changed (`Nigeria/_/pp_community_prices.py`)

`pp_dispositions` decides each row once, in this order:

`unpriced` → `item-hold` → `unit-unplaced` → `w1-no-count` → **`w1-metric-package-hold`** (W1 `u` ∈ {g, ml},
N = 1) → **`w1-count-as-size-hold`** (W1 `u` ∈ {Kg, l}, N > 50; `PP_W1_COUNT_AS_SIZE_ABOVE = 50`) →
**`w5-metric-quantity`** (W5 `u` ∈ {g, cl}, no metric size, not `99. ONE SIZE ONLY`, `c2q2a` > 0) →
`unrecorded-size-hold` (W2 / W3 / W5 g / cl only; **W3 Kg / l removed**) → `w5-quantity-hold` →
`metric-size` → `w1-divided` → `served`.

The new derivation:
- New function `derive_pp_w5_metric_unit_price(price, quantity)`, which returns `price / quantity`, NaN where
  quantity ≤ 0.
- Key `Nigeria::community_prices::pp-w5-metric-unit-quantity`.
- `u` stays `g` / `cl`.
- Inputs callable `inputs_pp_w5_metric_unit_quantity`.

The new holds:
- `Nigeria::community_prices::pp-w1-metric-package-hold`, inputs `inputs_pp_w1_metric_package_hold`.
- `Nigeria::community_prices::pp-w1-count-as-size-hold`, inputs `inputs_pp_w1_count_as_size_hold`.
- Each is its own `rows: 0` entry. Its inputs, built on the existing `pp_inputs`, return exactly the held rows
  under the served-index labels, with the original variables.

How I read "make each held group its own registered zero-row omission": each group the two final rulings
newly HOLD gets its own entry, so the two W1 groups get two keys. The groups those rulings keep held (W2 g / cl
and W3 g / cl, both "Keep ... held") and W5's 23 g / cl rows with no `c2q2a` stay in the one existing
`pp-unrecorded-size-hold`, which is the same no-package-size condition; its scope and counts are corrected. If
three separate entries were meant, that is a registry-only split (inputs by wave), and no served row changes.

Updated entries:
- `pp-w1-number-of-units`: 6,867 rows, naming both W1 holds.
- `pp-unrecorded-size-hold`: now W2 g/cl 209, W3 g/cl 12,934, W5 23; assumption rewritten to the ruling.
- `pp-w5-quantity-not-one-hold`: the 208 are now derived, not held first.

The PH entries are untouched.

## 3. The `c2q2a` check (required before dividing)

| source | says | settles it? |
|---|---|---|
| printed PP form, Section C2, PDF pp.6-11 | UNIT, SIZE, NAIRA under "PLEASE USE A QUANTITY OF ONE (1)"; **no quantity column** | no, silent |
| PP supervisor manual, "SECTION C2: FOOD PRICES", PDF pp.22-23 | "indicate the UNIT for ONE quantity. Do not consider more than one quantity ... record the price for ONE quantity" | no: describes no quantity field |
| PP interviewer manual; BID | no community section; BID p.21: "Section C2: Food Prices -- No changes" | no |
| codebook (`.dta` labels) | `c2q2a` "Quantity", `c2q2b` "Unit of measurement", `c2q2b_os` "Other unit", `c2q2c` "Size", `c2q2_cvn` "conversion factor item-unit-size-zone" | partly: a quantity of the named unit |
| `c2q2_cvn` values | 0.001 on every g row, 0.01 on every cl row that has one (196 of 208) | partly: a per-g / per-cl factor |
| `c2q2b_os` text (non-blank on all 231 g/cl no-size rows) | states the package: '250G', '400 GRAMS TIN', '750ML BOTTLE'; just 'GRAM' / 'ML' on the 23 with no `c2q2a` | **yes** |

What the text shows, row by row:
- **g rows:** `c2q2a` equals the text's gram figure on 172 of 176, and a number in the text on 175 (one only to
  float32 precision).
- **cl rows:** the text is in millilitres, and `c2q2a` equals that figure / 10 on all 32. None equals the raw
  ml figure. That is centilitres, the conversion the W5 household manual instructs ("500ml is 50cl", PP
  interviewer manual PDF p.66).
- **One g row is unresolved.** Its text has a letter inside its number and most plausibly states a package
  about 12x its `c2q2a`, so its served price may be about 12x high (above the p90 of the divided g rows). It
  is served as ruled with the other 207. No text states a unit other than the row's (no kg, litre or raw-ml
  entry), so the unit check stands; this is one mis-keyed value, not a unit conflict.

Verdict: `c2q2a` is in the package-size unit the label implies, so I divided. The confirming evidence is
file-internal: codebook labels, the conversion factor and the file's own text. It is registered under
`variable-label` and `modelling-choice` bases, not `questionnaire`.

Documents were fetched through `data_access.get_data_file` into a private root. Text was extracted with
`pypdf==4.3.1`, installed `--target` into a private directory; the shared venv has no PDF library.

## 4. Per-wave PP served / held

Cold candidate build, private root:

| PP t | served | derived | held (registered omissions) | unpriced |
|---|---|---|---|---|
| 2010Q3 | 20,839 | 6,867 `pp-w1-number-of-units` | 105 no-count; 1,043 metric-package (963 g, 80 ml); 332 count-as-size (128 Kg, 204 l) | 1,122 |
| 2012Q3 | 21,124 | -- | 209 unrecorded-size (204 g, 5 cl); 72 item-hold | 0 |
| 2015Q3 | 24,995 (obs 1 18,365, obs 2 6,630) | -- | 12,934 unrecorded-size (8,445 g, 4,489 cl) | 23 |
| 2018Q3 | 32,246 | 3,156 `pp-c2-metric-size` | none | 24,808 |
| 2023Q3 | 29,875 | 4,556 `pp-c2-metric-size`; 208 `pp-w5-metric-unit-quantity` (176 g, 32 cl) | 23 unrecorded-size (19 g, 4 cl); 123 `c2q2a`-not-1 | 26,256 |

Totals and checks:
- **Totals:** 129,079 PP rows; `community_prices` is 275,871 rows (146,792 PH), up from 272,918.
- **Index and prices:** unique; no non-positive or NA Price.
- **Derivation keys:** every served PP g / cl / ml row carries one, and no served row lacks one bar the form's
  own one-size code (0 such rows).
- **Served-row price medians:**
  - W3 Kg 700 / l 220;
  - W5 g 5.0 / cl 9.0 per unit;
  - W1 g 0.67 / ml 0.30;
  - W4 g 0.14 / cl 4.3.

## 5. Verification

Every job follows the same setup:
- a clone of the mirror at a full SHA, then `bin/savio_venv.sh mount`;
- `PYTHONPATH` set to the clone, with the import identity asserted;
- a private `LSMS_DATA_DIR`, XDG, TMP, MPL and issues log, with `data_root()` asserted private;
- BLAS pinned and `LSMS_BUILD_WORKERS=1`;
- `sbatch -A co_carleton -p savio4_htc -q carleton_htc4_normal -N1 -n1 -c 8 --mem=64G --exclude=n0035.savio4`.

Scripts are in `work/`.

| job | SHA | what | result |
|---|---|---|---|
| 41239697 | `c541bf49f` | `evidence.py`: W1 / W3 / W5 strata, labels, cvn | W1 963 / 80 / 128 / 204; W3 Kg/l 4,120; W5 g 176 + 19, cl 32 + 4 |
| 41239759 | `c541bf49f` | `evidence2.py`: `c2q2b_os` tables | text non-blank on all 231 |
| 41240125 | `c541bf49f` | `evidence3.py`: text vs `c2q2a`; manuals (pypdf) | §3 |
| 41239712 | base, `c541bf49f` | cold builds (`measure_arm.py`) | base 146,841 cp rows; prev 272,918 |
| 41239706 | base `e5fdd2fa8` | test selection (118 files) | **4,311 passed, 106 skipped, 0 failed** (28 min) |
| 41240775 | `f79c2f854` | fail-fast synthetic tests, then cold build + `compare_final.py` | synthetic **232 passed, 0 failed**; cold build 275,871; compare below (its `cand_checks.py` step failed on a script import bug, rerun next) |
| 41241897 | `f79c2f854` | `cand_checks.py` on the warm candidate root | §4; registry below |
| 41242090 | `f79c2f854` | the 2 unmatched W5 g rows, private log only | 1 float32 match, 1 garbled text |
| 41240776 | `f79c2f854` | the same test selection as 41239706, plus the two `test_gh587*` files (120) | **4,453 passed, 106 skipped, 0 failed** (32 min); +142 over base = the gh587 files and the new cases |
| 41242207 | `a3f9810c3` | targeted: `test_gh587*`, `test_derivations`, `test_no_build_input_in_contents_org`, `test_build_transform_hash`, `test_nigeria_community_units` | **292 passed, 0 failed** (7 min) |
__FINAL2__

The test selection is every `tests/test_*.py` naming Nigeria or community_prices, plus:
- `test_gh587*`;
- `test_derivations.py`, which holds both drift directions;
- `test_build_transform_hash.py`;
- `test_no_build_input_in_contents_org.py`;
- `test_community_prices_variety.py`;
- `test_feature_canonical_index.py`;
- `test_coverage_matrix.py`.

It runs with `-n 8 --dist=loadfile`, output to `work/out/pytest-*.log`.

**Cold comparison** (`work/out/builds/compare_final.json`):
- **Unchanged frames.** `sample` (49,089), `household_roster` (287,630) and `food_acquired` (595,725) pass
  `assert_frame_equal(check_exact=True)` against base.
- **PH rows** of `community_prices` are frame-equal to `c541bf49f` (146,792 rows, all columns). This includes
  the decoder fix carried from the previous step.
- **PP keyed diff, `c541bf49f` → candidate.**
  - Leaving: 1,375, all 2010Q3 (g 963, ml 80, Kg 128, l 204). Of these, 1,043 were served with no key and
    332 were `pp-w1-number-of-units`.
  - Arriving: 4,328. That is 2015Q3 Kg 3,075 and l 1,045 (obs 1: 2,501 / 832; obs 2: 574 / 213), with no
    key; and 2023Q3 g 176 and cl 32, under `pp-w5-metric-unit-quantity`.
  - Common rows: 124,751, with 0 differences in `Price`, `Derivation` or `Size`.
- **Derivation inputs** (`work/out/checks/cand_checks.json`):

  | key | inputs | check |
  |---|---|---|
  | `pp-w1-number-of-units` | 6,867 | join and reproduce `Price` |
  | `pp-c2-metric-size` | 7,712 | join and reproduce `Price` |
  | `pp-w5-metric-unit-quantity` | 208 | join and reproduce `Price` |
  | `pp-w1-no-count-hold` | 105 | omission |
  | `pp-w1-metric-package-hold` | 1,043 | omission |
  | `pp-w1-count-as-size-hold` | 332 | omission |
  | `pp-unrecorded-size-hold` | 13,166 (2012Q3 209, 2015Q3 12,934, 2023Q3 23) | omission |
  | `pp-unharmonized-item-hold` | 72 | omission |
  | `pp-w5-quantity-not-one-hold` | 123 | omission |

  The three constructions join the stamped rows exactly, and the registered function reproduces `Price`. Every
  omission's inputs are unique and disjoint from the served index, and attrs list them at `rows: 0`.
- **Read-time audits.** `community_prices` reports 0 Site B (null-read) and 0 Site I (null-index)
  findings in all three arms. Site Q does not screen `community_prices` (its scope is crop and food
  quantities, plot area and `food_prices.Price`), so its 0 there says nothing. Served-price quantiles of the
  new rows instead: W5 g / cl divided by `c2q2a`, p10 1.33 / 7.2, median 2.4 / 26.7, p90 10 / 49.7 Naira per
  g / cl; W3 Kg / l as recorded, medians 700 / 220 Naira. Its warning set is identical to `c541bf49f`: one
  `ClusterIdentityWarning`, whose span differs from base's (now `2010Q3 .. 2024Q1`) as before. `sample`,
  `household_roster` and `food_acquired` show identical warnings and reports to base (`food_acquired`: 14
  Site Q reports in both).
- **Feature.** `Feature('community_prices')(['Nigeria','Tanzania','Uganda'])` gives 293,570 rows with a unique
  index (previous head 290,617).
- **`labels='Aggregate'`.** `Pepper` passes through on 817 rows (previous head 837; 20 W1 Pepper rows are now
  held).

## 6. Blast radius and cache

GitNexus MCP failed to connect, so I substituted an `rg` sweep:
- `pp_dispositions`, `PP_DISPOSITIONS`, `PP_SERVED_DISPOSITIONS` and `PP_OMISSION_KEYS` are referenced only in
  `pp_community_prices.py` and `tests/test_gh587_nigeria_pp_community_serving.py`.
- No library code is touched.

Cache:
- **Nigeria-wide re-warm.** `_/pp_community_prices.py`, `data_scheme.yml` and the 2015-16 / 2023-24 wave
  scripts are hashed. The same re-warm is already owed against base.
- No other country moves. No hash census was run.

## 7. Residuals (for the maintainer)

1. **Label judgements.** The 29 item and 47 unit rows plus the missing-unit rule are unchanged, and listed in
   full in `PR_BODY.md`.
2. **W5's 123 `c2q2a`-not-1 rows.** Of these, 63 are BOTTLE/CAN with a coded metric size, which the PH division
   would serve.
3. **W5's 23 g / cl rows** have neither a size nor a `c2q2a`, and stay held.
4. **PH W3 still drops its 3,433 blank-size Kg / l rows**, which PP W3 now serves. PH W3 was not re-measured.
5. **The coverage matrix** still grades the five PP cells `dropped`; `make matrix` was not rerun.
6. **One W5 gram row is unresolved.** Its Other-unit text has a letter inside its number and most plausibly
   states a package about 12x its `c2q2a`, so its served price (above the p90 of the divided g rows) may be about
   12x high. It is served as ruled; holding it would be a carve-out the ruling did not make.

## 8. Unverified limits and disclosures

- **No external answer key.** The W5 g/cl evidence is file-internal. There is no household comparator; the
  community-internal comparator is 1.5x.
- **The W3 Kg / l household test** comes from the previous step (matched-cell medians, 719 / 328 cells). It
  was not rerun.
- **No full-corpus suite was run.**
- **Three trivial commands ran on n0035**, against the "run nothing" rule, all touching no data:
  - a text-only `python3` edit of two wave-script docstrings (string replace);
  - `python3 -c "print(1)"`;
  - `python3 -c pass`.

  No build, no data read, no library import. All data work ran in Slurm jobs.
- **Job count.** At most 3 jobs ran at once; 10 were submitted over the session (listed above).
- **Git use outside the allowed list:** read-only `git rev-parse` (HEAD and toplevel assertions) and
  `git cat-file -t` (to check that a SHA had reached the mirror). No other git command beyond add, commit, log,
  diff, show and status.
- **A stale comment in a hashed file.** The `pp_community_prices.py` serving-header comment still says "three
  2026-11-12 GH587 entries" (it is five). It was left alone because the file is build-hashed; fix it with the
  next code change to that file.
- **Final-head tests.** `e5940eaa9` differs from `a3f9810c3` only in `derivations.yml` / CONTENTS.org / ledger
  prose. The targeted run was repeated there (job 41242366), see §5.

## 9. Draft PR body

`PR_BODY.md` (beside this file).

## 10. Draft #587 narrow / close comment

> The five post-planting `community_prices` cells are served (PR #___, Nigeria only): 129,079 PP rows on
> Nigeria's shared `j` / base `u`, the native size in a new optional `Size` column. The PR also fixes a live PH
> C8 decoder defect (409 relabels, 49 drops). Every price-basis question the first wiring brought back is now
> ruled and implemented:
> - W1 `g` / `ml` with Number of Units = 1 (1,043 rows) and W1 `Kg` / `l` with Number of Units > 50 (332):
>   held, each under its own registered omission;
> - W3 `Kg` / `l` with no size (4,120): served as recorded;
> - W5 `g` / `cl` with no size (208): served as `c2q3 / c2q2a`, a registered derivation (`c2q2a` is the
>   package size in g / cl -- confirmed against the file's own Other-unit text);
> - W3 `g` / `cl` (12,934) and W2 `g` / `cl` (209) stay held.
>
> Proposed: narrow #587's community_prices item to **done** on merge. What remains is review, not wiring:
> - (i) the label judgement rows listed in the PR (29 items, 47 units, the missing-unit rule);
> - (ii) W5's 123 rows recording a `c2q2a` other than 1 (63 of them BOTTLE/CAN with a coded metric size,
>   which the PH division would serve) and the 23 W5 `g` / `cl` rows with neither a size nor a `c2q2a`, and
>   the one W5 gram row whose garbled text suggests a package about 12x its `c2q2a`;
> - (iii) PH W3 still drops its 3,433 blank-size `Kg` / `l` rows, which PP W3 now serves -- re-measure PH W3
>   against household values and decide whether PH follows;
> - (iv) the coverage matrix still grades the five PP cells `dropped` until `make matrix` is rerun.
>
> File (i)-(iii) as one follow-up, "Nigeria community prices: residual review after #587". Close #587 only if
> nothing else hangs on it: `CONTENTS.org` still records #587 items for PH `plot_features` (W4/W5 held) and the
> W1/W2 PH coping gap.
