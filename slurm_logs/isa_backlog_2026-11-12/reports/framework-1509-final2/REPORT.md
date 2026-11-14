# GH #1509 / #1068 / #1129, final round: ladder order and build-time Country-API dependencies

Agent: framework fix agent, Claude Opus 5.5 (`claude-opus-5-5`), for Sue (job 41201840, n0035).
Worktree `/local/job41201840/mirrors/LSMS_Library/.claude/worktrees/agent-ab25b116f4113864a`,
branch `fix/1509-1068-1129-food-unit-evidence-20261112`.
- Started at head `0966d62ef3dc3ee60ba9a17f6ab90dd26043c15c` (asserted). Base: development
  `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
- Commits: `30523f946` (code and tests), `8abd5fa3b` (docs, CONTENTS.org, data pins), `30f461b04`
  (docs: the #1068 drops by country, the stale cache claims, two test docstrings).
- **Final head: `30f461b04ed1f3c9e2a1c3d989506c3cdda3714e`.** Published to the mirror by the hook.
  No GitHub writes.

Inputs read: the red team's `framework-1509-redteam/VERDICT.md`; `framework-1509-unit-rung/REPORT.md`
and `PR_BODY.md`; every 2026-11-12 GH1509 entry of `docs/isa-sweep-20261103:.sucoder/isa-decisions.org`
(the newest, "GH1509 ladder order and build-time Country-API dependencies"); Uganda and Ethiopia
`CONTENTS.org` (nutrition entries: #1062 sibling writes, #967 fixture approval, the BLAS-variant
fingerprint, #1171 Make recipe, #771 offline FCT).

Work files: `scripts/` (job driver `job.sh`, `guard.py`, `measure.py`, `census.py`, `census_diff.py`,
`compare.py`, `warm.py`), `out/` (per-arm JSON and row parquets, `compare.json`, `tables.md`, censuses
and diffs, warm-root JSON, pytest logs), `logs/`.

## Verdict: READY, with one maintainer item

Both rulings are implemented and measured. One item needs the maintainer: the Uganda
`var/nutrition.parquet` entry of `tests/fixtures/uganda_baseline.json` no longer matches (it moved with
#1509 at `0966d62ef` already). Per the #967 precedent it is not regenerated; the proposed two-field
change is in the PR body.

## 1. "Item self-valued first"

**Code** (`transformations.py`, `food_kg_factors`): the `np_ok` and `unit_ok` branches swap in both the
`resolved` and the `source` chain, and `FOOD_KG_FACTOR_LAYERS` becomes
`('survey_kg', 'metric', 'item_unit', 'item_unit_tight', 'item_unit_nonpurchase', 'unit',
'unit_nonpurchase', 'none')`.
- One order expresses the ruling: the purchase item rungs need purchases in the cell (`cell_priced`),
  and both fallbacks need none (`eligible = ~cell_priced` blanks their candidates). So a purchase cell
  still sees `item_unit`, `item_unit_tight`, `unit`, `none`, and a no-purchase cell sees
  `item_unit_nonpurchase`, `unit`, `unit_nonpurchase`, `none`. Measured: `kg_item_unit` and
  `kg_item_unit_nonpurchase` are both set on 0 rows in all 11 countries.
- The item fallback's gate ledger gains `refused_spread_to_unit`: a row its dispersion gate refuses can
  now land on `unit`, and the gate cost it its own item's factor. Identity: `rows_refused_spread ==
  refused_spread_to_unit + refused_spread_to_unit_nonpurchase + refused_spread_to_none`.
  GhanaLSS 48,989 = 25,833 + 23,156 + 0 (head: 23,156 = 23,156 + 0). The rise is the ledger's scope
  widening, not new refusals: the 25,833 were refused under the old order too, where `unit` outranked
  the item fallback and so the gate cost them nothing.
- The purchase gate's identity (`rows_refused == refused_to_unit + refused_to_none`) is untouched: a
  purchase-refused row always has purchase evidence. GhanaLSS 446,696 = 446,696 + 0.

**Measured effect** (cold, private scratch roots `work/roots/{base,head,cand}`, all three arms in parallel
on n4257, job 41251759; `food_acquired` frame hash identical across arms in every country, so rows
compare positionally):

| GhanaLSS | base `e5fdd2fa8` | head `0966d62ef` | cand `30523f946` |
|---|---|---|---|
| kg, `food_quantities()` | 11,007,391.9 | 9,918,751.3 | **10,209,229.9** |
| vs base | | -9.89% | **-7.25%** |
| `item_unit` | 993,954 | 532,603 | 532,603 |
| `item_unit_tight` | 70,333 | 5,980 | 5,980 |
| `item_unit_nonpurchase` | — | 208,090 | **287,863** |
| `unit` | 605,938 | 894,613 | **814,840** |
| `unit_nonpurchase` | — | 28,939 | 28,939 |
| `metric` / `none` | 86,495 / 3,571,848 | same | same |
| `food_prices()` / `kgprice` / `unitvalue` / `units` | 560,023 / 187,096 / 1,676,514 / 1,695,046 | 560,023 / 187,096 / 568,130 / 1,694,111 | same as head |

- **Head -> cand moves exactly 79,773 rows, all `produced`, all `unit -> item_unit_nonpurchase`**, in
  147 cells. By wave: 1991-92 18,943; 1998-99 13,313; 2005-06 14,990; 2012-13 18,577; 2016-17 13,950.
  Every one is now at its base factor (share equal to 1e-9: 1.0); median 6.6x the pooled factor it had;
  75,797 move by more than 2x. Their kg: 46,889 (head) -> 337,368 (cand = base), **+290,479 kg**. Top
  labels: `Bundle` 25,969, `American Tin` 25,925, `Bowl` 17,360.
- 0 rows in a purchase cell change source or factor (`kg_cell_has_purchase`), and 0 fallback rows sit
  in a cell with any purchased row (independent recount over all waves).
- GhanaLSS kg by wave, head -> cand: 1991-92 235,220 -> 252,636; 1998-99 293,188 -> 345,343; 2005-06
  663,604 -> 730,108; 2012-13 857,377 -> 942,279; 2016-17 7,869,362 -> 7,938,864.
- Price **counts** do not move (a moved row had a factor either way); kgvalue and kgprice Price
  **values** on the moved rows do.
- What GhanaLSS still lacks against base, row by row: 238,616 rows in cells with purchase evidence that
  the item rungs refuse and `unit` serves (`item_unit -> unit` 215,839, -750,783 kg; `item_unit_tight
  -> unit` 22,777, -5,351 kg), plus -42,103 kg of purchase factors changed in place, plus +75 kg on 775
  `unit -> item_unit` rows. The rulings keep those cells off the self-valued rungs.
- **The other ten countries are frame-equal head -> cand** (kg, partition, prices in every mode, by
  price-frame hash): GhanaSPS, Uganda, Ethiopia, Cambodia, Serbia, Burkina Faso, Malawi, Niger,
  Panama, EthiopiaRHS. None has a row with both an item-fallback estimate and a purchase `unit` factor.
- Matches the red team's estimate exactly (79,773 rows, 290,479 kg).

**#1487** (`waves=[w]` frame-equal to the all-waves slice, `kgs` and `kgvalue`): GhanaLSS 7 waves,
Uganda 8, GhanaSPS 3: all equal, in all three arms.

## 2. "Make the dependency visible"

**Search.** `rg` over `lsms_library/countries/*/_/*.py` and `*/*/_/*.py` for `.food_quantities(`,
`.food_prices(`, `.food_expenditures(` and `Country(`:
- `Uganda/_/nutrition.py:43` `uga.food_quantities()` and `Ethiopia/_/nutrition.py:53`
  `eth.food_quantities()`: the only build-time readers of a derived food table through the API.
- `GhanaLSS/_/nutrition.py` already imports and calls `food_quantities_from_acquired`.
- Not callers: `Panama/_/nutrition.py` (unreachable: not in its `data_scheme`, imports a missing
  module, reads a retired parquet); Uganda `income.py` (`getattr` over `earnings` /
  `enterprise_income`); the many `Country(...)` scripts reading non-food tables; module functions named
  `food_quantities` / `food_expenditures` (definitions, not API calls).

**Scripts that gained the explicit import: Uganda and Ethiopia** (`nutrition.py`):
```python
from lsms_library.country import Country as _Country
from lsms_library.transformations import food_quantities_from_acquired
assert _Country._FOOD_DERIVED['food_quantities'] == food_quantities_from_acquired.__name__, ...
```
- The import is what `framework_imports_fingerprint` folds; the assertion uses it and fails the build if
  the API stops routing `food_quantities` through that transform.
- `lsms_library.country.Country`, not `ll.Country`, because the scripts' exec tests (GH #1062, #771)
  replace `ll.Country` with a fake; they pass unchanged.
- `from lsms_library.country import Country` folds nothing (a class has no `__code__`).

**Enforcement:** `tests/test_gh1509_build_time_food_api_dependency.py` (data-free, 4 tests): the static
sweep (any country/wave `_/*.py` calling those methods must import the matching transform; positive
control that it finds both scripts); the `_FOOD_DERIVED` map; the closure of
`food_quantities_from_acquired` contains `food_kg_factors`; and the no-op probe (`inspect.getsource`
patched to add a statement to `food_kg_factors`) moves the framework fingerprint of exactly
`['Ethiopia', 'GhanaLSS', 'Uganda']` and not `build_transforms_fingerprint(None)`.

**Hash footprint** (census of every country, table and wave, config reads only):

| | base -> cand | head -> cand | cand -> cand + probe | cand -> `8abd5fa3b` and -> final `30f461b04` |
|---|---|---|---|---|
| `_table_cache_hash` | 75 / 555 | 75 / 555 | 75 / 555 | 0 / 555 |
| `Wave._input_hash` | 503 / 2,321 (30 None both) | 503 | 503 | 0 |
| `framework_imports_fingerprint` | 3 / 38 | 3 / 38 | 3 / 38 | 0 |
| `build_transforms_fingerprint` | 0 / 42 | 0 / 42 | 0 / 42 | 0 |

- Every moved hash is Ethiopia (25 tables, 125 wave-tables), GhanaLSS (22, 154) or Uganda (28, 224):
  every table and wave of the three. base -> head was GhanaLSS only (22 / 154 / 1 / 0).
- The probe is one statement (`_gh1509_probe = 0`) inserted in `food_kg_factors` in a scratch clone at
  `30523f946` (`logs/M_probe_diff.txt`): exactly the same 75 / 503 / 3 / 0. So a future kg-factor logic
  edit re-warms Ethiopia, GhanaLSS and Uganda cold, and nothing else.
- **Warm root** (`out/warm-{head,cand}.json`): two copies of the base root, whose Uganda
  `var/nutrition.parquet` base built and stamped (`22ec9384...`).
  - Read by `0966d62ef`: served as fresh, 0.9 s, no rebuild, Energy 2,087,321,650 = base's; that code's
    cold build gives 2,065,011,048. The stale case the red team inferred (F3, limit 4) is reproduced.
  - Read by `30523f946`: the stamped hash no longer matches (`837ce5b3...` expected), rebuilt in 70 s,
    Energy 2,065,011,048, invariance hash equal to the cold build's.

## 3. Disclosures (red team F1, F3, F4, F5-F8)

Stated in AGENTS.md (= CLAUDE.md), the PR body and, where country-specific, CONTENTS.org:
- **GhanaSPS** (F1): kg 1,384,290 -> 1,438,672 (+3.93%; 2009-10 +4.05%, 2013-14 +4.74%, 2017-18
  +3.43%); 250,258 of 407,929 rows change factor (17,961 >2x); 18,741 change rung (9,403 `item_unit ->
  unit`, 9,338 `unit -> item_unit`); `food_prices()` 387,440 rows, Price moves; no fallback rows; no hash.
- **Uganda** (F4): -1.36% kg from 141,884 rows changing factor (4,850 >2x), 4,042 changing rung
  (`item_unit -> unit` 3,992; `unit -> item_unit` 23; `unit -> unit_nonpurchase` 15; `item_unit ->
  item_unit_tight` 9; `unit -> none` 3).
- **`nutrition`** (F3), cold, base -> cand:

  | | households | Energy | other |
  |---|---|---|---|
  | GhanaLSS | 34,798 | 18.257e9 -> 15.504e9, **-15.08%** (head -18.67%) | Protein -5.80%, Carbohydrate -17.58%, Fiber -11.63% |
  | Uganda | 23,777 | 2.0873e9 -> 2.0650e9, **-1.07%** (head the same) | Protein +0.40% |
  | Ethiopia | 25,644 | frame-equal | |

- **"None of this invalidates a cache"** is replaced by what a kg edit costs (0 `btf`; every hash of
  the three countries), with the census, probe and warm-root numbers. The docstring of
  `test_no_transformations_symbol_reaches_any_fingerprint` now says it covers `btf` only.
- **The ladder order is recorded as the maintainer's, 2026-11-12**; the earlier paragraphs say the
  unit-first order was this branch's choice (F2).
- **F7b:** the #850 gate and wave-spread paragraphs now say their figures are pre-#1509 (Uganda
  refused 13,571 -> 2,834; GhanaLSS 428,168 -> 446,696; GhanaLSS item-rung rows all single-wave).
- **F5, F6, F7** are put to the maintainer in the PR body (cell- vs label-level eligibility;
  `quantity_rows_dropped`; the `currency` bucket's semantics). F7 and the #1068 / #1129 drops by
  country (must-state 8; Nigeria and Senegal cited from the red team, transform level) are also in
  AGENTS.md (`30f461b04`), with GhanaSPS's 13,481 / 13,728 NaN-`u` rows, which base's groupby deleted
  silently and which the tally now counts (confirmed in `out/cand/GhanaSPS.json`). **F8:** `feature.py`
  docstring lists `kg_factor_evidence`; the GhanaLSS test is renamed.
- **Stale cache claims above the corrected paragraph:** the three earlier #1509 paragraphs'
  "GhanaLSS only" footprints now say they describe their own commit, and the docstrings of
  `test_metric_decimal_comma.py` and `test_gh1454_code_prefixed_units.py` name Uganda and Ethiopia too.
- **New data pins** (`tests/test_gh1509_data_pins.py`, requires_s3): GhanaSPS kg 1,438,671.82 and
  rungs; GhanaLSS kg 10,209,229.94 and rungs (`item_unit_nonpurchase` 287,863, `unit` 814,840);
  GhanaLSS `nutrition` Energy 15,503,685,126.7; Uganda 2,065,011,048.5 (rel 1e-6).

## 4. Tests

| run | job | result |
|---|---|---|
| synthetic subset, cand `30523f946` (17 files, `-m "not requires_s3"`) | 41251758 | 619 passed, 0 failed |
| full non-S3 suite, base `e5fdd2fa8` | 41251760 | 15,109 passed, 35 skipped, 4 xfailed, 0 failed |
| full non-S3 suite, cand `30523f946` | 41251760 | 15,182 passed, 35 skipped, 4 xfailed, 0 failed |
| data-gated, base, its warm root (19 files: base lacks the four #1509 files) | 41251759 | 773 passed, 42 skipped, 0 failed |
| data-gated (23 files), cand, its warm root | 41251759 | 845 passed, 42 skipped, **1 failed** |
| data-gated (24 files incl. data pins), `8abd5fa3b`, cand root | 41253568 | 849 passed, 42 skipped, **1 failed** (the same); the 4 data pins pass |
| data-gated (25 files, + `test_gh1454_code_prefixed_units`), **final `30f461b04`**, cand root | 41253845 | 854 passed, 42 skipped, 4 xfailed, **1 failed** (the same) |
| full non-S3 suite, **final `30f461b04`** | 41253845 | 15,182 passed, 35 skipped, 4 xfailed, 0 failed |

- The +73 passed in the full suite are the #1509 test files (base lacks them).
- **The one failure** is `test_uganda_invariance.py::test_parquet_matches_baseline[var/nutrition.parquet]`:
  base matches the #967 fixture; this branch serves `content_hash e9fb4c67b246...` (shape 23,777 x 15
  unchanged). Uganda `nutrition` is frame-equal between `0966d62ef` and cand, so the entry was already
  broken at the previous head. The file is `requires_s3`, so every non-S3 suite deselects it, and
  neither earlier data-gated run listed it (no earlier log names it). Not regenerated, by the #967
  precedent; proposal in the PR body.
- The 42 skips on both arms are Uganda invariance entries whose parquet the measurement did not build.
- The full suite ran with credentials and network, `-m "not requires_s3"`, not under `unshare -rn`
  (the previous report's 113 data-free failures were that harness's).

Files in the data-gated set: `test_currency_denominated_units`, `test_962_unit_sentinel`,
`test_ghanalss_nutrition`, `test_gh1042_missing_unit_spellings`, `test_food_kg_inference`,
`test_food_kg_size_pool`, `test_gh1487_kg_subset_is_a_slice`, `test_feature_attrs_carriers`,
`test_food_prices_units_kwarg`, `test_burkina_faso_2014_passages`, the four #1509 files,
`test_unpriceable_price_drop`, `test_build_transform_hash`, `test_uganda_invariance`,
`test_gh1171_ethiopia_nutrition_make`, `test_gh1062_uganda_sibling_writes`,
`test_gh771_offline_fct_refresh`, `test_gh1454_served_unit_fingerprint`, `test_metric_decimal_comma`,
and (final only) `test_gh1509_data_pins`.

## 5. Cost

Same node, the three arms in parallel (`out/compare.json` `timing`):

| GhanaLSS | base | head | cand |
|---|---|---|---|
| `food_kg_factors` | 18.8 s | 38.9 s | 39.0 s |
| `food_quantities(units='kgs')` | 43.2 s | 63.4 s | 63.0 s |
| `food_prices()` | 37.4 s | 57.5 s | 58.0 s |
| `nutrition()` cold | 36.7 s | 45.7 s | 46.9 s |

The reorder costs nothing; the second (non-purchase) estimate is what costs. Uganda `food_kg_factors`
4.6 s (cand). A kg-factor edit now also costs a cold re-warm of Uganda and Ethiopia (§2).

## 6. Limits and how the rules were kept

1. **No accuracy claim.** The moved GhanaLSS rows return to their pre-#1509 factors; no external unit
   weight was scored.
2. **Uganda fixture**: see §4; the Haswell-kernel variant of the new fingerprint was not measured.
3. **Countries measured end-to-end**: the 11 above. Not measured: Tanzania, Nigeria, Mali, Senegal,
   CotedIvoire, Guatemala, Togo, Benin, Guinea-Bissau, Peru, GhanaAHIES, Nepal. The red team measured
   12 of them at transform level against base with 0 kg-factor rows changed; the reorder can move a row
   only where a no-purchase cell has both an item-fallback estimate and a purchase `unit` factor.
4. **Private roots and data access.** Each job exported private `TMPDIR`, `XDG_CACHE_HOME`,
   `MPLCONFIGDIR`, `LSMS_ISSUES_LOG` and `LSMS_DATA_DIR` before `bin/savio_venv.sh mount`; every
   measuring process asserted import identity, the full SHA and a private `data_root()` (`guard.py`).
   Roots under `framework-1509-final2/work/roots/`. No `dvc` CLI, no lock removal, no `LSMS_SKIP_AUTH`,
   no writes into `lsms_library/countries/` at runtime; BLAS threads 1, `LSMS_BUILD_WORKERS=1`, pytest
   `--dist=loadfile` to files. Slurm's own prolog printed "Unable to create TMPDIR
   [/local/job41201840/tmp] ... Setting TMPDIR to /tmp" (inherited from my session) before the job
   script set its own `TMPDIR` as its first action.
5. **n0035.** Only git, file edits, shell heredocs for scripts and these reports, `jq`/`grep`/`sed`/
   `ls`, and `sbatch`/`squeue` ran there, **with one slip**: early on I ran `python3 -` with an empty
   heredoc (system python, no program, no import, no data), which the rule forbids. No other python ran
   on n0035. At most 3 of my jobs ran at once (41251758 syn, 41251759 measure, 41251760 full; then
   41253568 final, 41253845 final2).
