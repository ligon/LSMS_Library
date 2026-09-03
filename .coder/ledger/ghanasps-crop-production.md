# Prior-Art Ledger — ghanasps-crop-production (GH #729, #140)

> Per-task ledger.  Inherits the repo §0 baseline in `STANDING.md`; cites it,
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git floor (gitnexus MCP failed to connect this
session: `CONNECT_TIMEOUT`).

## §1 Task, restated
Wire the canonical `crop_production` table for GhanaSPS in all three waves
(`2009-10`, `2013-14`, `2017-18`) at the grain `(t, i, plot_id, j, u, season)`
-- no `condition` level (never asked; Nigeria's shape), no `v` (joined from
`sample()` at API time).  Columns: `Quantity` (reported harvest in the native
unit `u`), `Quantity_sold` / `Value_sold` (where the instrument records a sale
at the plot-crop grain), `harvest_month` (str).  **Mixed build path**:
2009-10 is a two-season WIDE file pair (`S4AV1` major, `S4AV2` minor, five
harvest slots each) that YAML cannot melt, so it is a wave script
`2009-10/_/crop_production.py` writing an L2-wave parquet via `to_parquet`;
2013-14 / 2017-18 are long plot-crop files and use the YAML path plus one
`crop_production` `df_edit` hook in `ghanasps.py`.  `materialize: make` is
declared as the brief instructs.  Crop labels are mapped on the DECODED label
through one `harmonize_crop` table and unit spellings through a
`harvest_units` table in `_/categorical_mapping.org`.  Verify cold on a
private `LSMS_DATA_DIR` under `LSMS_GRAIN_STRICT=1`, with the read-strict
condition asserted directly (`null_read_reports(country='GhanaSPS',
table='crop_production') == []`) because `LSMS_READ_STRICT=1` aborts inside
`sample()` on this country (documented, PR #752).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._aggregate_wave_data` → `load_from_waves` | `lsms_library/country.py:~3610-3700` | per wave: `getattr(wave, table)()` (YAML `grab_data`) and, when that returns empty (no `data_info.yml` block), `run_make_target(table, wave=w)` (Makefile → direct `python script.py` fallback).  This is the MIXED-MODE dispatch; it ignores the `materialize` flag under the default `dvc` backend | Tanzania mixed tables, Iraq `assets` | **reuse** (W1 script + W2/W3 YAML) |
| `run_make_target` / `try_script` | `country.py:3119-3270` | builds `data_root(country)/{wave}/_/{table}.parquet`; runs the wave script with cwd = script dir and `LSMS_DATA_DIR` in env | GhanaSPS `food_acquired` | reuse |
| `Country._evict_hashless_wave_caches` | `country.py:2966` | deletes script-written (hashless) L2-wave parquets before every rebuild descent | cache tests | reuse (nothing to do) |
| `Wave.grab_data` `dfs:` branch + `Wave._merge_subframes` | `country.py:1273-1390`, `1095` | multi-file merge on `merge_on`, `merge_how: left`, `final_index`, then the per-table `df_edit` hook; GH #323 site-4 cartesian test | roster / plot_features | **reuse** for W3 (04n primary + 04o secondary on `(i, plot_id, j)`) |
| `Wave.column_mapping` → `mappings: [table, key, value]` | `country.py:939-947` | extraction-time lookup in `_/categorical_mapping.org` ONLY (`self.categorical_mapping`), keyed exactly; unmatched values pass through unchanged | plot_features `Tenure`/`AreaUnit`, livestock `animal` | **reuse** for `j` (`harmonize_crop`) and `u` (`harvest_units`) in W2/W3 |
| `Wave.formatting_functions` / `df_edit` dispatch | `country.py:871`, `978`, `1263` | a callable named after the table in `ghanasps.py` is the table's hook, bound for every YAML wave | `ghanasps.individual_education` / `plot_features` / `livestock` | **reuse** the pattern (new hook body) |
| `df_data_grabber` / `format_id` | `local_tools.py:1325`, `2107` | idxvars auto-`format_id` (NaN → `None`) | yes | reuse; null `u` / `j` must be resolved BEFORE the index collapse |
| `to_parquet` / `_resolve_data_path` | `local_tools.py:1570` | wave script writes `crop_production.parquet` → redirected to `data_root()/GhanaSPS/2009-10/_/` by call-stack inference | GhanaSPS `2009-10/_/food_acquired.py` | **reuse** |
| `get_dataframe` | `local_tools.py:805` | the only sanctioned reader (3-5 s per GhanaSPS file here); `data_access.get_data_file` is the DVCFS walk (1,599 s measured on a cached file) -- inspection only, never in a script | yes | reuse |
| `df_from_orgfile` / `all_dfs_from_orgfile` | `local_tools.py:2904` | org table parser (`---` → NaN, cells stripped) | yes | reuse in the W1 script for `harmonize_crop`, `units.org` `unit09` + `harmonizedunit` |
| GhanaSPS `2009-10/_/food_acquired.py` unit decode | that file, l.55-64 | W1 unit CODE → `units.org` `unit09` label → `harmonizedunit['2009-10']` → Preferred Label (singular: `Maxi bag`, `Tuber`, ...) -- the vocabulary `food_acquired.u` delivers in all three waves | food_acquired tests | **reuse the exact chain** so `crop_production.u == food_acquired.u` vocabulary |
| `Country._normalize_dataframe_index` / `_audit_index_collapse` | `country.py:5358` | `groupby(dropna=True)` DELETES null-key rows and `.first()`s duplicates with a `GrainCollapseWarning` (fatal under strict) | `tests/test_gh323_*` | the reason the script/hook resolve null `u`/`j` and the same-key collisions THEMSELVES (§5) |
| `null_read_audit` Site B | `lsms_library/null_read_audit.py:375` | required declared column 100% null in a wave's `t` slice → report / fatal | yes | `Quantity_sold` (W1+W2 never asked) and `Value_sold` (W2) must be `optional: true` -- the livestock `HeadSold` precedent |
| `Country._join_v_from_sample` | `country.py:1633` | joins `v` for household tables | every GhanaSPS table | reuse (STANDING §5) |
| Uganda `crop_production` (`Uganda/_/data_scheme.yml:127`, `Uganda/_/uganda.py crop_production_for_wave`, `Uganda/2009-10/_/crop_production.py`) | | index `(t, i, plot, j, u, condition, season)`, wave scripts + country concatenator, `harmonize_crop` reuses food labels, `harvest_units` table, `'Unknown'` where no unit label | script | precedent for label reuse, `harvest_units` name, `Unknown` sentinel, the wave-script shape |
| Nigeria `crop_production` (`Nigeria/_/data_scheme.yml:228`, `Nigeria/_/nigeria.py:300-420`) | | `(t, i, plot, crop)` no `condition`; `Quantity_sold`/`Value_sold` only where sales are at plot-crop grain, NaN otherwise (W3-W5); `harvest_month` as a `str` (`_month_str`, zero-padded `MM`) | script | precedent for the sales asymmetry and the month string |
| `ghanasps.livestock` hook | `GhanaSPS/_/ghanasps.py` | bounded, pinned SUM of duplicate index lines (`min_count=1`); keep-rule; null-key drop | `tests/test_ghanasps_livestock.py` | precedent for the same-crop-same-unit collision sum |
| `slurm_logs/ghanasps/FINDINGS_agriculture.org` §3, §4, §9, §"crop_production" | | two-season W1; the `S4AV2` label error settled at A120-A122 (C4); `04o_cropsalesstoresservices` is NOT sales; the S4BI plot-crop sales, the nine channels | research note | source of record |

## §3 Definitions & conventions in force
- Canonical `crop_production`: `lsms_library/data_info.yml:441-500` declares ONLY `condition` (an index level; a spellings vocabulary seeded from Uganda; `unknown_condition` is a sentinel for a record whose condition code is missing *from a scheme the survey asked*).  GhanaSPS never asks condition, so the level is omitted (Nigeria precedent) and the sentinel does not apply.  The plot-level ag features are deliberately NOT in `index_info` (`data_info.yml:32`) -- "their per-country index NAMES diverge (plot vs plot_id, crop vs j)".
- `v` is joined from `sample()`; never declared in a feature index: `CLAUDE.md` §"`sample()` and Cluster Identity"; STANDING §4.
- Grain: core never aggregates; NaN in a declared index level is deleted-and-reported; a REDUCER in core is forbidden, a documented bounded reducer in a country script/hook is what Uganda / Malawi / GhanaSPS-livestock do: `CLAUDE.md` §"Grain Collapse", `ghanasps.py` livestock comment.
- `dfs:` merge whose key is non-unique in BOTH frames is a cartesian; null keys count because `pd.merge` matches them: `CLAUDE.md` §"Gotchas with Teeth".
- Site B null-read guard and `optional: true`: `CLAUDE.md` §"The Silent All-Null Read".
- Household id form per wave (W1 9-digit `hhno`; W2/W3 `FPrimary`, 10-char split form on 9 W3 harvest rows): `GhanaSPS/_/CONTENTS.org` §"Cluster identity IS available".
- `plot_id` = `(hhno, plot_no)` / `(FPrimary, plotid)` exactly as `plot_features` keys it (`GhanaSPS/{wave}/_/data_info.yml` `plot_features` blocks); a panel id W1→W2 but NOT W2→W3: `CONTENTS.org` §"The unit of observation is the PLOT".
- Every W1 money variable is a cedis / pesewas pair: `CONTENTS.org` §"Every 2009-10 money variable is split cedis / pesewas".
- Module 4 is agriculture BY CONTENT in every wave: `CONTENTS.org` §"Module 4 IS agriculture in every wave -- tested, not assumed".
- `LSMS_READ_STRICT=1` aborts every GhanaSPS household table inside `sample()`: `CONTENTS.org` §"LSMS_READ_STRICT=1 cannot build ANY GhanaSPS household table on this branch".
- Unit decoding convention (never fabricate a label; undecodable codes stay as the code and are documented as accepted residuals): `.claude/skills/add-feature/food-acquired/units/SKILL.md`.
- Categorical-table lookup syntax and "each Alternate Spelling exactly once": `.claude/skills/add-feature/SKILL.md`; `tests/test_ghanasps_livestock.py::test_species_table_has_each_spelling_exactly_once`.

## §4 Invariants & assumptions
- **`S4AV2` is the MINOR season although every one of its quantity/value labels says "major"** -- questionnaire Part A, A120: "CROPS LAST MINOR SEASON: CROP (HARVESTS) 1 ... A122. What is the quantity harvested in the last minor season?" (FINDINGS §4, C4), corroborated by `S4AVI2`'s own label ("In the past minor season, have you used chemical 1").  `season` is assigned from the FILE.  Measured: crop-1 quantity non-null on 4,825 (`S4AV1`) vs 1,412 (`S4AV2`) plots; melted records with any datum 9,535 major / 2,528 minor.
- W1 harvest slot map (per season file): crop id `a80i a88i a96i a104i a112i` / `a121i a129i a137i a145i a153i` (a bare CODE with no value labels; the A78 crop-grown columns carry the 43-label list); part `*ii`; quantity `a81i a89i a97i a105i a113i` / `a122i a130i a138i a146i a154i`; unit `a81ii ...` / `a122ii ...` (codes, no labels); revenue `a83 a91 a99 a107 a115` / `a124 a132 a140 a148 a156` (cedis `i` + pesewas `ii`).  Slot-3 minor pesewas is misnamed `s4v_a102ii` in the major file (A101.2).
- W1 crop codes are defined 01-43 only (`CODE_BOOK.pdf` "CROP CODES"; the value labels agree).  Records carrying 0 / 44 / 45 / 46 / 47 / 90 (12 rows) and a null crop id with a quantity (30 rows) have NO crop identity → dropped and counted.  (44 = `Soya bean(s)` only in the W2/W3 instruments; not asserted for W1.)
- W1 unit codes: `units.org` `unit09` (02-44, 95); the codebook's own list has a typo (`Gallon ... 14`, `Kilogram ... 14`; `unit09` gives Gallon 13).  Off-list codes in the harvest records: 0 (39), -1 (13), 1 (3), 46, 50, 51, 53 (9), 57 (4), 58 (2), 61, 80, 94 → 0 / -1 are treated as missing (`Unknown`, with the 194 quantity-but-no-unit rows); positive undefined codes stay as the code string (accepted residuals, units-skill convention).
- W1 harvest records collide at `(hh, plot, crop, unit, season)`: 98 rows in 47 groups (82 major / 16 minor); in 18 groups the harvested PART differs (cocoyam `Roots/tuber` vs `Leaves`, both in baskets).  The part is NOT `condition` and is not force-fitted; a non-principal part is a different PRODUCT and qualifies `j` (`Cocoyam` + `Leaves` → `Cocoyam Leaves`, an existing `food_items` Preferred Label; others `Crop (part)`); same-product collisions are two harvest events and are SUMMED (bounded, pinned).
- W1 `Quantity_sold` is never asked (S4BI B71 and A83 are revenue only).  `Value_sold` = A83/A124 (in-block, season-specific) rather than S4BI B71: S4BI is a season-less 15-slot marketing roster whose crop rows match a major harvest slot on `(plot, crop)` for only 2,976 of 4,752 rows, and B71 == A83 for only 891 of 2,156 rows where both exist.
- W1 `harvest_month` is a PROXY: `S4AIX1` A289.2 / `S4AIX5` A326.2 "Ending month for the major/minor season of dominant crop" (plot-season level); W2/W3 record the month(s) per crop.
- W2/W3 instrument: "If there has been more than one harvest from the plot, give the total quantity which has been harvested over the last 12 months" → `season` there is a 12-month recall, NOT a major/minor season; constant `annual`.
- W2 `04o_cropsalesstoresquestions` has NO `plotid` → `Quantity_sold` / `Value_sold` NaN by design.  W3's `04o` is `(FPrimary, plotid, cropcode)` unique with NINE buyer channels, each `quantity` / `unit` / `price`, and the price is per unit ("the price for which [Name] sold one [unit] of [crop type]") → `Value_sold = Σ qty × price` (money is additive across channels), `Quantity_sold = Σ qty` ONLY when every selling channel uses one unit equal to the row's harvest `u` (3,062 rows; 432 differ; 7 multi-unit; 12 rows have a quantity with no price → `Value_sold` NaN).  1,237 W3 sale records have no harvest row and are not delivered.  One sale value is 42.8M GHS -- left as reported, flagged.
- W2 `04n_harvestquestions` 8,229 rows: 1,341 with neither quantity nor unit (1,322 `harvest == 5` "all yet to be harvested", 19 null) and no month → dropped; 10 duplicate `(FPrimary, plotid, cropcode)` of which 2 survive as same-crop-same-unit pairs (107250012 cassava baskets 10 + 20; 109315002 millet maxi bags 0.5 + 0.5) → summed, months unioned.  W3: 1,926 null-`cropcode` placeholders (`cultivated` null, 0 quantities) and 963 both-null rows (794 status 5, 169 null) → dropped; 0 collisions.
- W2 unit list is a 20-code subset with `95 Other - specify` (226 rows; free text `tubers` 51, `ropes` ~26, `pan` 15, ...); W3 `-666 Other` (122).  Specify text is folded ONLY on an exact case-insensitive match to a `harmonizedunit` label or its plural (`tubers` → `Tuber`); everything else stays `Other`.
- W2/W3 `intercropped*` is a plot-level multi-select list (which crops were intercropped), not a per-crop flag → NOT wired.
- Every harvest plot is in its wave's `04h_agsection` / `S4AII` keyed plot set: 5,034 / 5,034 (W1), 3,826 / 3,826 (W2), 3,440 / 3,440 (W3).
- The W3 `dfs:` merge key `(i, plot_id, j)` must be unique in BOTH sub-frames INCLUDING null-key rows (`pd.merge` matches nulls) -- asserted in the verify script; if it fails W3 becomes a wave script.
- `materialize: make` on a MIXED table costs the `LSMS_BUILD_BACKEND=make` backend (country-level target required; none exists); the default `dvc` backend is unaffected.  Tanzania's mixed tables omit the flag; the brief asks for it; recorded in `data_scheme.yml`.
- Editing `ghanasps.py` / `categorical_mapping.org` / `Makefile` moves `Country._table_cache_hash` for EVERY GhanaSPS table: one cold rebuild of the shared cache after merge.
- `get_data_file` = DVCFS walk (1,599 s on a cached file, on this node); `get_dataframe` 3-5 s.  Inspection reads bypassed the library (cached blob by sidecar md5) -- never in a script.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| index `(t, i, plot_id, j, u, season)` | reuse (`idxvars` + `format_id`; script sets the same names) | brief; `plot_id` derivation identical to `plot_features` (`plot_no` / `plotid`) |
| W1 two-season melt | **new** -- `2009-10/_/crop_production.py` wave script + helpers in `ghanasps.py` | YAML cannot melt five wide slots × two files; precedent Uganda wave scripts |
| W2/W3 extraction | reuse YAML + `mappings:` + `dfs:` (W3) | one file (W2) / two files 1:1 (W3); the framework's cache hashing and null-read guard |
| W2/W3 post-processing (no-harvest drop, `Other`-unit fold, month string, W3 nine-channel sale aggregation, constant `season`, duplicate sum) | **new** -- `ghanasps.crop_production` df_edit hook | YAML maps one column to one column; the livestock / plot_features hook precedent |
| `j` vocabulary | reuse `mappings: [harmonize_crop, Alternate Spelling, Preferred Label]` + new table | Preferred Label = the GhanaSPS `food_items` Preferred Label where the harvested crop IS that food (so `j` joins `food_acquired.j` within the country -- Uganda's stated purpose), else Uganda's `harmonize_crop` label, else a coinage; spelling harmonised, no crop folded |
| `u` vocabulary | reuse `units.org` `unit09` → `harmonizedunit` chain (W1, script) + new `harvest_units` table (W2/W3 spellings) | identical to `food_acquired.u`; NOT a table named `u` (API-time auto-dispatch onto every `u` level + row-union with the global `u.org`) |
| non-principal harvested PART → product label | **new** -- ~10-entry principal-part dict + one food-label override in code | the part is not `condition`; an unconditional sum would add leaves to tubers in 18 groups; `Cocoyam Leaves` is a real product with an existing food label |
| same-product collisions (W1 29 groups; W2 2) | **new** bounded reducer, `sum(min_count=1)`, months unioned, counts pinned by test | two harvest events of one product in one unit are additive at this grain (`_ADDITIVE_MEASURE_COLUMNS` semantics); livestock precedent |
| W1 `Value_sold` cedis + pesewas/100 | **new** in script | every W1 money variable is a pair (CONTENTS.org) |
| `Quantity_sold` / `Value_sold` `optional: true` | reuse the livestock `HeadSold` pattern | Site B would otherwise fire on W1 (`Quantity_sold` never asked) and W2 (both NaN by design) |
| `v` | reuse `_join_v_from_sample` | STANDING §5 |
| `condition`, `intercropped`, `planting_month`, `perennial`, `part` as a level | not declared | never asked / not a straight flag / not canonical |

## §6 Open questions for the human
- `season` for 2013-14 / 2017-18 is the constant `annual` (the instrument sums harvests over the last 12 months).  If the owner prefers a different recall label, that is a one-line config change.
- `Cocoyam Leaves` (and the rarer `Crop (part)` labels) exist only in 2009-10 because only that instrument records the harvested part -- a cross-wave instrument artefact like `sharecropped_in`.
- W1 `Value_sold` is A83/A124, not S4BI B71 (both the brief and CONTENTS.org point at S4BI).  The measured disagreement between the two is in §4.

---
### Phase 3 — verification (fill at task end)
- `GhanaSPS/2009-10/_/crop_production.py` (wave script) — OK (anchored on §5): melts S4AV1/S4AV2 five slots each, `season` from the FILE, decodes crop/unit codes through `ghanasps` helpers and `units.org`, qualifies `j` by the harvested part, sums same-product duplicates (bounded, pinned: 33 groups), writes via `to_parquet`; reads only via `get_dataframe`.  Not a REINVENTION: Uganda's `crop_production_for_wave` is UNPS-specific (condition level, AGSEC column maps) and was used as the shape precedent, not copied.
- `ghanasps.crop_production` (df_edit hook) + helpers — OK (§5): no-harvest drop, `Other`-unit fold on exact vocabulary match, month-token string (month NAMES accepted — the first cold build served 2013-14 `harvest_month` 0% non-null because `harvestmonths1..6` decode to `January`; caught by Site B, fixed), W3 nine-channel aggregation with the unit-agreement rule, constant `season`, bounded duplicate sum (2 groups in 2013-14).
- `GhanaSPS/{wave}/_/data_info.yml` `crop_production` — OK (§2/§4): W2 single file; W3 `dfs:` left-merge on `(i, plot_id, j)` measured 1:1 in both frames INCLUDING blank-crop rows (0 duplicated keys on either side).
- `GhanaSPS/_/categorical_mapping.org` `harmonize_crop` / `harvest_units` — OK (§5): every spelling once; `harvest_units` targets ⊆ `harmonizedunit` Preferred Labels (pinned by test); delivered `j` ⊆ `harmonize_crop` Preferred Labels (+ 2009-10 part-qualified products), delivered `u` ⊆ vocabulary + `Unknown` + `Kg` (the framework's global `u.org` API-time fold of `Kilogram`) + the ten 2009-10 residual code strings.
- `GhanaSPS/_/data_scheme.yml` `crop_production` — OK (§3): index `(t, i, plot_id, j, u, season)` without `v` or `condition`; `Quantity_sold` / `Value_sold` `optional`; `materialize: make` with the make-backend cost stated.
- `GhanaSPS/_/Makefile` — OK (§2): wave-script rule only (no country target, by the reasoning in §4).
- `tests/test_ghanasps_crop_production.py` — OK (§4): data-gated via `requires_s3`; pins two seasons in W1 with minor the smaller and every S4AV2 minor-quantity plot delivered as `minor`; the per-wave dedup group counts recomputed from source; the sales asymmetry; `j`/`u` subsets; the plot_features join.
- Verification env — same deviation as PRs #752 / #760, recorded: `LSMS_READ_STRICT=1` is fatal inside `GhanaSPS/sample`; STEP 3 ran under `LSMS_GRAIN_STRICT=1` with `null_read_reports(country='GhanaSPS', table='crop_production') == []` asserted directly.
- Deviations from the brief's letter, each reasoned in §4/§5: W1 `Value_sold` from A83/A124 rather than S4BI; the unit table is `harvest_units`, not `u`; the harvested part qualifies `j` rather than being dropped.
