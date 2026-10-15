# Backlog triage `isa-east-gaps` -- 14 issues, read-only (2026-10-14, development @ a7cf04f2d)

No guardrail conflicts. No files written outside `$STAGE`. One cold read of a raw `.dta` via `get_dataframe` (Nigeria W5 cover page) and direct `pd.read_parquet` reads of the shared warm cache; no `Country()` call was made.

## 1. Table

| # | tier | type | tractability | dedup | prio | root_cluster | approach (one line) |
|---|---|---|---|---|---|---|---|
| 881 | C | design | human | partial | 2 | recall-window-disclosure | Decide A (identity derivation labelling W4 purchased rows) vs B (#876-style derived 7-day Expenditure = q5a x q10/q9a) |
| 860 | C | design | human | open | 3 | area-unit-factors | Decide: wire EPAR's BID zone x {heaps,ridges,stands} grid with the 4.9x disagreement recorded, or record "evaluated, not wired"; "plots" x0.0667 is separable |
| 858 | C | test | auto | open (answered here) | 4 | - | Close with the measured result: 0 of 7,924 plot_inputs 2020-21 rows lack a plot_features / crop_production plot |
| 856 | C | data-gap | diagnose-then-auto | partial | 3 | unwired-instrument-module | Drop is now counted (#869); measure harvest-side free-text residue per wave before wiring any recovery |
| 855 | C | data-gap | auto | open | 4 | unwired-instrument-module | Add `headcount_year_ago='ag_r07'` to `_livestock_block`, optional canonical column, four wave scripts |
| 851 | C | framework | human | partial | 3 | recall-window-disclosure | Phase I landed; Phase II = three charter decisions (coverage threshold, Contract B, food_prices refuses period=) |
| 846 | C | docs | human | open | 4 | coverage-grain-mismatch | Decide: extend `lacks`/verdict vocabulary to column grain, or record the C4 finding in CONTENTS.org only |
| 844 | C | data-gap | diagnose-then-auto | open | 3 | unwired-instrument-module | Check W3-W5 question numbers + run ligon's sa3q18-vs-ladder comparison, then wire `Value_if_sold` (optional) |
| 843 | B | data-gap | human | open | 2 | unwired-instrument-module | Decide grain: separate `crop_sales` (t,i,j,u) table for W3-W5 (recommended) vs sentinel-plot rows |
| 841 | B | data-gap | auto | open | 2 | missing-wave-config | Declare `cluster_features` in `Nigeria/2023-24/_/data_info.yml` from `secta_plantingw5.dta` (df_key + df_main, NO df_geo: W5 ships no geovars) |
| 830 | C | framework | auto | open | 3 | grader-blind-spot | Cross-wave median-ratio check on `monetary: true` columns in `coverage_matrix.grade_feature`, warn-only |
| 820 | C | data-gap | diagnose-then-auto | open (scoped W1-W3) | 2 | unwired-instrument-module | Second sale frame (sect12_ph_w1-3) into `ethiopia.crop_production_for_wave`; first confirm §12 crops have harvest rows |
| 737 | C | design | human | partial | 2 | livestock-convention-undeclared | Decide: relabel *lvs5 to t=1989 and build the roster (library tlu()), or keep IFPRI's scalar with a convention marker; add mule/camel factors |
| 771 | D | infra | diagnose-then-auto | partial | 2 | nonreproducible-build-input | Materialise Ethiopia's FCT to a tracked CSV, make the USDA fallback opt-in, move the key to config and rotate it |

## 2. Reconcile

**Nothing can be closed as resolved-in-development.** Verified by reading the diffs:

- **#858 -- close with a measurement, not a commit.** The check the issue asks for was run on the warm parquets: 2020-21 `(i, plot_id)` sets are plot_inputs 3,746 / plot_features 6,560 / crop_production 4,899; `plot_inputs - plot_features = 0`, `plot_inputs - crop_production = 0`. The reverse gaps (2,814 / 1,661) are plots with no inputs or no harvest, expected. Our tables are built independently (`Tanzania/2020-21/_/plot_inputs.py:18-24`, no merge), so EPAR's `keep(1 3)` drop has no analogue. Recommend a closing comment with these numbers and a LOGBOOK line in `Tanzania/_/CONTENTS.org`.

**Partial -- need a comment:**

- **#851** -- Phase I landed: `634db2697` (recall.py, `RecallRecord`, `ll.recall()`, 20 `recall.yml`), `48c97caaf`, `3989a7281` (#873: `Feature()` re-attaches `attrs['recall']`), `1446e4c1b` (Burkina 2014 passages). Phase II unstarted, gates at `.coder/charter-recall-windows.md:203-215`; follow-up GH #864.
- **#881** -- its option (1) dependency (#873) is fixed by `3989a7281`; its option (2) mechanism exists (derivations, v0.12.0) and the EHCVM twin #876 was CLOSED via `76199f590` by registering the derived 7-day Expenditure that `Nigeria/_/CONTENTS.org:60-63` rejects. The pairing itself is unchanged (`2018-19/_/food_acquired.py:45-69`). The decision is now live, not hypothetical.
- **#856** -- the "drop is not counted" half is fixed by `43d2900fb` (GH #869): `malawi.py:1844-1855` warns with codes and counts; `CONTENTS.org:714-717` records it. Harvest-side free-text recovery remains, unmeasured.
- **#737** -- the factor table is recovered and documented (`1999/_/data_info.yml:181-186`, `1990988e4` / #738 established the files are 1989 data); served `TLU` is unchanged and `data_scheme.yml:71` still equates LSU with TLU.
- **#771** -- Uganda's `fct_usda.csv` is now tracked (`8ec0bab4e`, `.gitignore:84-92` un-ignores it by name). Still open: the silent fallback (`Uganda/_/nutrition.py:34-39`), the literal key (`:21`), and **Ethiopia, which is worse than filed**: `Ethiopia/_/nutrition.py:33-34` calls the live USDA API unconditionally with no CSV at all.
- **#830** -- its instance #829 is closed (Uganda guard), the grader check does not exist (`grep -i scale|monetary` in `coverage_matrix.py`/`diagnostics.py` = 0).
- **#820** -- scoped by `Ethiopia/_/CONTENTS.org:1698-1706` to W1-W3 (ESS4/5 folded §12 into §11).

None of the in-flight PRs (#967 Uganda units, #969 Windows, #971 Region labels, #972 Uganda condition slot, #973 CI) touches these 14. `slurm_logs/backlog_fix_2026-09-24/REVIEW_SUMMARY.org` mentions none of them.

## 3. Dispatchable now (tier A/B, auto, no decision)

1. **#841** Nigeria W5 `cluster_features` (B, auto, prio 2). Edit only `Nigeria/2023-24/_/data_info.yml`: copy W4's `df_key` (`v: [state, lga, ea, hhid]`) and `df_main` (`Region: zone`, `District: state`, `Rural: sector` mapping) from `Post Planting Wave 5/Household/secta_plantingw5.dta` (verified columns: zone, state, lga, sector, ea; 5,067 rows). **No `df_geo`** -- W5 ships no geovariables file, so Latitude/Longitude will be NaN and Site B will warn for that wave (the known country-grain `optional:` mismatch; do not set `optional: true`). Remove the `:11` comment claiming the wave has no cluster_features. Verify: cold build Nigeria `cluster_features`, `community_prices` 2024Q1 `v` join 507/507 (`CONTENTS.org:814-829`), `tests/test_nigeria_cluster_identity.py`, Site 2 audit lossless on the composite key. **Cache cost**: the wave `data_info.yml` is in `Wave._input_hash` for every 2023-24 table, so every Nigeria L2-country table with a 2023-24 wave re-warms (sample, community_prices, household_roster, housing, plot_features, individual_education, crop_production ...); other waves' L2-wave parquets are untouched.

Everything else in tier B needs a decision (#843) or is tier C/D. Of the tier-C items that are mechanically auto once a one-word decision is taken: **#855** (a column name; full Malawi re-warm, `malawi.py` is in every Malawi table's hash), **#830** (a redenomination exception list; zero cache cost, read-path only), **#858** (a closing comment; zero cost).

Ordering within the `unwired-instrument-module` cluster, roots before symptoms: #843's grain decision should come before #844 (both add hh-crop-grain Nigeria sale/valuation data; `Value_if_sold` is plot-crop grain in W1/W2 but the W3-W5 twin lives in the same `secta3ii`/`secta3i` split) and before #820 (Ethiopia's §12 sales may hit the same "sale without a harvest row" question). One decision can settle all three.

## 4. Escalations (one line each)

- **#881**: follow the #876 precedent (derived 7-day Expenditure, registered) or label the reported 30-day pairing with an identity derivation? CONTENTS.org:60-63 and the #876 resolution currently pull opposite ways.
- **#843**: W3-W5 Nigeria sales as a separate `crop_sales` (t,i,j,u) table (recommended) or as `'Unknown'`-plot rows inside `crop_production`?
- **#860**: wire EPAR's BID grid (disagreement recorded) or record "evaluated, not wired"; separately, convert "plots" at x0.0667?
- **#851**: ship `period=` with coverage at 28 of 67 cells under Contract B?
- **#846**: extend `SeriesCapability.lacks` / `absent_verdicts.csv` to column grain, or record the not-asked finding in `Tanzania/_/CONTENTS.org` only?
- **#737**: relabel the `*lvs5` files to t=1989 and build the roster, or keep IFPRI's scalar under `TLU` with a convention marker? Plus the cattle 0.70 / 0.83 / 0.50 sourcing.
- **#771**: rotate the FDC key (a literal in two tracked files and in history)?
- **#830**: is a currency-redenomination table an acceptable exception list?
- **#855**: accept `HeadCountYearAgo` as the optional canonical column name?

## 5. Contradictions with CONTENTS.org / CLAUDE.md / the issues' own text

- **#846's premise vs `.coder/coverage/latest.csv`**: the issue says Tanzania's food-price cells "grade `absent` in the coverage matrix". They do not: `Tanzania,food_prices,{all six},sane`. The "absent" is in the price-source ledger (`slurm_logs/price_sources/DATA_PROBLEMS.csv:53`), and the mechanisms it proposes are feature-grain (`capability.py:120 lacks: tuple[str, ...]`; `absent_verdicts.csv` header `country,feature,wave`) while the absence is a column (`food_acquired.Price`). There is no matrix cell to close.
- **#771's claim about Ethiopia**: "has the same fallback and the same un-versioned `fct_usda.csv` pattern" -- `Ethiopia/_/nutrition.py:33-34` has no fallback and no CSV; it always calls the API.
- **#856's "not counted"**: no longer true, `malawi.py:1844-1855` (since `43d2900fb`).
- **#860 cites `Nigeria/_/crop_production.py` for the area table**: `Area`/`AreaUnit` are built in `nigeria.py:1250-1275` for `plot_features`, not in crop_production.
- **#841's "following the W4 declaration"**: W4's block has a `df_geo` sub-df (`2018-19/_/data_info.yml:79-85`); W5 has no geovariables file, so copying W4 verbatim would trip the #515 dropped-sub-df guard. Omit `df_geo`.
- **#881 vs CLAUDE.md's "a derived value is a first-class value"**: `Nigeria/_/CONTENTS.org:60-63` rejects imputing `q5a x price` as "fabricating a number no respondent reported"; CLAUDE.md (2026-09-14) and the #876 resolution hold that a *registered* derivation with recoverable inputs is not a fabrication. The CONTENTS.org argument predates the registry; whichever way #881 is decided, that paragraph needs rewriting.
- **#737 vs `EthiopiaRHS/_/data_scheme.yml:71`**: the comment `TLU: float # Livestock Standard Units (tropical livestock units)` still asserts the equation the issue refuted; `1999/_/data_info.yml:181-186` next to it states the recovered 0.83 table. Two statements in one config tree.
- **CLAUDE.md 'Countries Without Microdata' / coverage**: nothing contradicted. The CLAUDE.md cache-cost rule used throughout: country `_/*.py` and `.csv` are in every wave's `binp:` hash and the country module in every table; a wave `data_info.yml`/`mapping.py` moves every table of that wave; `.yml` siblings, `CONTENTS.org`, `transformations.py`, `diagnostics.py`, `capability.py` move nothing.
