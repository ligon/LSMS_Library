# Prior-Art Ledger — GH #323 (reopened), Burkina Faso

**Search tier used:** ripgrep + git floor (gitnexus MCP tools not reachable in this worktree; read-only greps + direct source reads used instead).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py`) collapses a non-unique
DECLARED index with `groupby().first()`. For Burkina Faso this is not one bug but
four, in three different layers:

1. **2014 `food_acquired`** — the wave's `data_info.yml` listed the four
   `emc2014_p{1..4}_conso7jours` files under a single `file:` key. Those are the
   four quarterly PASSAGES of the EMC 2014 continuous survey over the same 10,800
   households, each with its own independent 7-day recall. With no index level to
   tell them apart and `t='2014'` for all four, they collide on one
   `(t,v,i,j,u,s)` tuple and — because `food_acquired` is in
   `_ADDITIVE_MEASURE_COLUMNS` — get SUMMED into a single bogus "7-day" figure.
   Riding on that, the NaN-`u` rows (p2/p3/p4 have no unit column at all) were
   DELETED outright by pandas' `groupby(dropna=True)` default.
2. **2018-19 `plot_inputs`** — `harmonize_seed_crop` mapped four distinct seed
   types onto one `crop='Autre crop'` cell, MANUFACTURING 183 duplicate rows that
   `first()` then threw away (2,778.98 kg of seed).
3. **`cluster_features` (all 3 waves)** — declared at `(t, v)` but extracted from
   a household-level cover page; the collapse is intended but was silent, and
   2014's `District` is not constant within `zd` for 94 of 900 clusters.
4. **2014 `shocks`** — two households have their whole 19-row roster entered
   twice, making the index non-unique.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; collapses dups via additive-sum or `first()` | yes (schema tests) | **extend** — `dropna=False` + NaN-key warning |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/feature.py:101` | tables whose measures are summed on collapse (GH #501) | yes | **extend** — add `plot_inputs` |
| `food_acquired_to_canonical` | `lsms_library/build_transforms.py:28` | wide→long reshape on `s`; `drop_columns=('visit',)` | yes | **reuse** (EHCVM waves) |
| GhanaLSS `visit` level | `GhanaLSS/_/data_scheme.yml:17,54` | `food_acquired: (t,v,i,j,u,s,visit)` — a 7th recall level | in use | **reuse the pattern** |
| GhanaLSS `u='Value'` | `GhanaLSS/2012-13/_/food_acquired.py:66` | LCU convention: value-only rows get `u='Value'`, `Quantity=Expenditure` | in use | **reuse the convention** |
| `food_prices_from_acquired` | `lsms_library/transformations.py:815` | collapses an extra recall level (median for price, sum for qty/exp) | yes | **reuse** — already handles `visit` |
| `Wave.grab_data` script path | `lsms_library/country.py:1056` | when a wave has NO `data_info.yml` entry, builds via Makefile | yes | **reuse** — remove YAML entry → script path |
| `reduce_to_agreed` | `lsms_library/build_transforms.py` | **new** — agree-or-NA reducer | new tests | **new** |
| `add_visit_level` | `lsms_library/build_transforms.py` | **new** — constant `visit` for single-recall waves | new tests | **new** |

## §3 Definitions & conventions in force

- Canonical `food_acquired` index: `(t, v, i, j, u, s)` — `lsms_library/data_info.yml:20`.
  `Feature()` canonicalizes to this and collapses any extra level (GhanaLSS
  precedent, GH #517), so a country-local 7th level is legal and already handled.
- `s` ∈ {purchased, produced, inkind, other} — `lsms_library/data_info.yml:11`.
- `u='Value'` (LCU convention): a monetary amount with no physical quantity;
  `Quantity = Expenditure`, `Price = NaN` — `GhanaLSS/2005-06/_/food_acquired.py:11`.
- Script path vs YAML path: "multi-wave source files with a round column → script
  with `materialize: make`" — `CLAUDE.md`, "Two Build Paths".
- EHCVM `vague` is a SAMPLE SPLIT, not a repeated measure — `CLAUDE.md`, EHCVM gotcha;
  dropped by `food_acquired_to_canonical`.

## §4 Invariants & assumptions

- `grab_data` filters every wave frame to `t == self.year`
  (`country.py:1142`, mirrored at `country.py:2699`). **Therefore a wave folder
  CANNOT emit more than one `t`** without `wave_folder_map`. This is why the
  "distinct `t` per round" option was rejected — see §5.
- `aggregation:` in `data_scheme.yml` is **NEVER READ AS POLICY**. It appears only
  in two skip-sets (`country.py:2387`, `diagnostics.py:174`). Burkina's existing
  `interview_date: aggregation: {visit: first}` is decorative. Writing an
  `aggregation:` block would have been prose, not enforcement — so the
  `cluster_features` fix was done at the source grain instead.
- `mapping.py` is loaded via `spec_from_file_location` + `exec_module`
  (`local_tools.py:2231`) with the ambient `sys.path`, so it CANNOT import the
  country module (`from burkina_faso import ...` raises). Shared helpers must
  live in `lsms_library.transformations` / `build_transforms`.
- `GroupBy.first()` is per-column FIRST-NON-NULL, not first-row — it can
  synthesize a record present in no source row. (It does **not** do so in Burkina
  shocks; see §7.)
- 2014 `i` = `format_id(zd) + format_id(menage, zeropadding=3)` (`burkina_faso.i`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| passage/recall level | **reuse** `visit` (GhanaLSS shape) | canonical `Feature()` already collapses a 7th `visit` level (GH #517); inventing `passage` would need new framework support |
| distinct `t` per passage | **REJECTED** | `grab_data` pins `t == self.year`; the only route is `wave_folder_map`, which would rebuild the p1-sourced roster/housing/assets **4× with identical rows** — fabrication. Documented here because the brief preferred this route. |
| `aggregation: sum` over passages | **REJECTED** | summing four independent 7-day recalls into one 7-day figure is a category error — it is exactly what the code silently did |
| value-only rows | **reuse** `u='Value'` (GhanaLSS LCU convention) | already understood by `food_prices(units='unitvalue')` |
| `plot_inputs` collapse | **extend** `_ADDITIVE_MEASURE_COLUMNS` + de-collide at source | Quantity is additive; but better to remove the manufactured collision entirely |
| `cluster_features` collapse | **new** `reduce_to_agreed` | `aggregation:` is dead config (§4); source-grain reduction is real enforcement |

## §6 Numbers (before → after)

Instrument validated on the known positives before use: Mali/2014-15/household_roster
→ 32,026 dups; Guyana/1992/housing → 311. Scans read the **L2-WAVE** parquet
(`{wave}/_/`), never `var/` (which is written POST-collapse).

| cell | BEFORE | AFTER | note |
|------|--------|-------|------|
| `food_acquired` 2014 | 97,361 rows | **672,656** | 4 passages × 3 sources |
| `food_acquired` purchased Expenditure 2014 | 80,286,096 CFA | **342,110,515 CFA** | == Σ source `achat` (93,383,167 + 82,473,394 + 93,328,730 + 72,925,224) |
| `food_acquired` 2018-19 / 2021-22 | 137,934 / 69,208 | 137,934 / 69,208 | unchanged (`visit=1` added, no rows moved) |
| `plot_inputs` 2018-19 | 22,694 rows | **22,877** | +183; Quantity +2,778.98 kg |
| `shocks` 2014 (var) | 194,199 rows, 0 NaN-key kept | **194,208**, 9 NaN-key kept | 194,246 − 38 dups − 9 annihilated = 194,199 |
| `cluster_features` 2014 District NA | 0 | **94** | of 900 clusters (the genuinely ambiguous ones) |
| duplicate declared-index tuples, all Burkina tables | >0 | **0** | |

## §7 Corrections to the incoming diagnosis (verified, not assumed)

- **The "shocks Frankenstein" does not exist in the output.** The diagnosis
  reasoned from the raw `CS1` flag, which is NOT part of the `shocks` schema. In
  the extracted frame the conflicting pair is one ALL-NULL row plus one populated
  row, so `first()` (per-column first-non-null) already returned exactly the
  populated row. Verified directly on the pre-fix L2-wave parquet. The dedup is
  still correct (it removes the duplicate index and the NaN-key deletion it
  triggered), but **no chimera was fixed** and none is claimed.
- **`qachat` is the PURCHASED quantity, not a total.** The old YAML fed it as
  `Quantity` into `purchased = Quantity − Produced`. The source carries three
  PARALLEL (qty, unit, value) triples — `qachat/uachat/achat`,
  `qautocons/uautocons/autocons`, `qcadeau/ucadeau/cadeau` — that are near-disjoint
  (only 286 p1 rows have both `qachat` and `qautocons`; 49 of those have
  `qautocons > qachat`). This was NOT in the diagnosis. It also tagged produced
  quantities with `uachat` (NaN), which is what made them NaN-keyed. The `cadeau`
  (in-kind) stream — 9,723,324 CFA — was never read at all.
- **Code `21` in `s16bq01` is a BARE UNLABELED Stata code**, not the "Autres
  semences" slot the `categorical_mapping.org` comment asserted (`Autres semences`
  carries its own text label in the same variable). What crop it denotes is not
  determinable, so it is labelled honestly as unidentified rather than merged.
- **2014 shocks duplicates = 38, not 47** (the brief's figure); confirmed on both
  the parquet and the raw source.

## §8 Open / not fixed (reported, not silently left)

- **`aggregation:` is dead config** (§4). Implementing it as real policy is a
  framework change beyond this issue's blast radius; flagged for a follow-up.
  No decorative `aggregation:` block was added here.
- **`GroupBy.first()` per-column-first-non-null** remains the framework's default
  reducer. Changing it globally to true-first-row would alter output in many
  countries and could not be validated inside this task's scope. `reduce_to_agreed`
  gives country builds an honest opt-in; the class defect is documented in
  `_normalize_dataframe_index`.
- 2014 derived `food_expenditures` is now a total over FOUR non-contiguous recall
  weeks (the derived transforms sum `visit`), NOT a 7-day figure, and is not
  comparable to the EHCVM waves' single-week totals. Documented in
  `data_scheme.yml` and `CONTENTS.org`. Work from `food_acquired` for a
  per-quarter basis.
