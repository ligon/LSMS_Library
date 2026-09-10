# Prior-Art Ledger — 852-ethiopia-shipped-factors (GH #852, #853)

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED) and this mirror carries no
`.gitnexus/` index, so per `CLAUDE.md` §"Code intelligence: GitNexus is
OPTIONAL" the substitute for `gitnexus_impact` was an `rg` sweep of every
reference to `crop_cf` / `Crop_CF` / `local_area_unit` / `plot_features_for_wave`
/ `crop_production_for_wave` / `yield_kg` / `_map_int_codes` /
`_harmonize_wave_keyed` across `*.py`, `*.yml`, `*.org`, `*.md` and every
`Makefile`. Declared here rather than skipped. Result: **zero references to
either shipped file anywhere in the tree** (which is the issue), and
`yield_kg`'s only callers are `tests/`.

## §1 Task, restated

Ethiopia holds two World Bank conversion tables in DVC that no script opens.
Wire both, without turning either into stored data that hides its own
decisions.

* **#852 — crop factors.** `Crop_CF_Wave{2,3,4}.dta` / `crop_cf_wave5.dta`
  (crop x unit x region kg-per-unit). Served as an **analyst-callable loader**,
  `ethiopia.crop_conversion_factors()`, whose output goes to
  `transformations.harvest_kg(cp, shipped_factors=...)` — the `shipped` layer
  merged as `675fc34a`. **Nothing is stored**: `crop_production`'s parquet is
  byte-identical with and without this work (verified, §Phase 3).
* **#853 — area units.** `ET_local_area_unit_conversion.dta` (woreda x local
  unit -> square metres) fills `plot_features.Area` for a field with **no GPS
  measurement** whose only area is a farmer estimate in a local unit. This
  half **does** move stored content, and the hash, deliberately.

Neither is a registered or derived table; `harvest_kg` / `yield_kg` are
analyst-callable transforms (`CLAUDE.md` §"Derived Tables"), so no
`data_scheme.yml` entry is created for either loader.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `harvest_kg` / `harvest_kg_factors` | `lsms_library/transformations.py:2477` / `:2179` | the layer stack `reported > shipped > survey_median > inferred > none` | yes | **reuse** (the loader feeds it) |
| `_shipped_factor_lookup` | `transformations.py:2000` | joins a shipped table on the `SHIPPED_FACTOR_JOIN_LEVELS` overlap; **refuses an ambiguous table**; warns on a dropped key level and on a zero match | yes (`tests/test_shipped_factors.py`) | **reuse** |
| `SHIPPED_FACTOR_JOIN_LEVELS` | `transformations.py:1971` | `('country','t','j','u','condition','region')`; `region` is LOWER CASE and matched exactly | yes | **reuse** — the loader keys on `(t, j, u[, region])` |
| `yield_kg` | `transformations.py:2653` | harvest / area, on the parcel grain | yes | **extend** (the `shipped_factors=` / `min_reports=` pass-through the shipped-factors ledger §6 Q3 deferred to this PR) |
| `ethiopia._eth_crop_label_map` | `Ethiopia/_/ethiopia.py:886` | `harmonize_crop` code -> Preferred Label, the SAME map `crop_production_for_wave` uses for `j` | via the crop_production build | **reuse** |
| `ethiopia._clean_unit_label` | `Ethiopia/_/ethiopia.py:891` | normalises a §9 unit label to the shared `u` vocabulary | via the build | **reuse** |
| `ethiopia._harmonize_wave_keyed` | `Ethiopia/_/ethiopia.py:551` | `{(wave, code): label}` for a WAVE-KEYED table (`harmonize_area_unit`, `harmonize_acquire`) | via the build | **reuse** — and see §4, its misuse was a live bug |
| `ethiopia.plot_features_for_wave` | `Ethiopia/_/ethiopia.py:621` | the shared per-wave `plot_features` builder | via the build | **extend** (`area_est` colmap key + the conversion) |
| `local_tools.get_dataframe` | `local_tools.py:1179` | the only sanctioned reader; a **countries-root-relative** path (`Ethiopia/2018-19/Data/x.dta`) resolves from ANY cwd (`:1225`, `_COUNTRIES_DIR / fn_path`) | yes | **reuse** — this is what lets a country-level loader be callable from anywhere |
| an external factor-table loader for a country | — | **none existed.** `rg 'crop_cf\|Crop_CF\|local_area_unit'` over the whole tree returned nothing outside the two `.dvc` sidecar sets | — | so `new` is not reinvention |
| EPAR's `fillin` / median-imputation ladders | `EPAR_UW_Ethiopia_ESS_W5.do:776-797`, `:657-711` | fills the holes in both tables from its own means / weighted medians | n/a | **deliberately NOT reproduced** — imputation, not a shipped factor (#852, #853 "What it must NOT do") |

## §3 Definitions & conventions in force

- **`KgFactor` is kg per ONE unit of the row's `u`**, and a shipped table is
  applied **at read time and never written to the column** —
  `lsms_library/data_info.yml` `Columns.crop_production.KgFactor` (tightened in
  the shipped-factors PR, `.coder/ledger/harvest-kg-shipped-factors.md` §7.5).
  This is why #852 lands as a loader and not as a `crop_production` column.
- **`AreaUnit` is the "original survey unit before conversion to hectares"** —
  `lsms_library/data_info.yml:237-239`. So a converted row keeps its native
  unit name and `'hectares'` continues to mean exactly "GPS-measured".
- **Count, never clip** — `transformations._screen_reported_factors`
  docstring; `AGENTS.md:353`; #853 "Never resolve an implausible converted
  `Area` by clipping it".
- **A duplicate on a key is REFUSED, never reduced** — `CLAUDE.md` §"Grain
  Collapse"; `_shipped_factor_lookup`'s refusal; #852 "Never average the
  duplicate row".
- **Core never aggregates** — `SkunkWorks/grain_aggregation_policy.org` §3a.
  A median across the table's region columns would be exactly that, which is
  why the national default reads the file's **own** `mean_cf_nat` instead.
- **Ethiopia idiosyncrasies cross-referenced** from `Ethiopia/_/CONTENTS.org`:
  the "Crop-side WB conversion factors: held, unwired" section, the corrected
  "Area and the local-unit gap" paragraph (2026-09-09, `ef633b12`), the §11/§12
  fold bounding GH #820 to W1-W3, `holder_id` folded into `plot_id`, the W1
  rural-and-small-towns universe, the `cluster_features` Region/District
  caveat, and the note that `Crop_CF_Wave4` / `crop_cf_wave5` are one blob.

## §4 Invariants & assumptions

- **`crop_production` carries NO `region` level**, and the framework will not
  give it one: `_join_v_from_sample` joins `v`, and `cluster_features.Region`
  is an un-harmonised STRING (21 spellings for 11 regions;
  `CONTENTS.org` §"cluster_features District is not a district"), while the WB
  table keys on the numeric `saq01` code. Measured: 99.6% of crop rows can be
  given a Region *string* via `v`; there is no name->code table in the repo.
- **`sect9_ph` carries `saq01` on 100% of rows in all five waves** — so the
  clean fix is upstream (emit a native `region` level), which is a
  `crop_production` schema change and out of scope here.
- **The `u` vocabularies are two sides of the same Stata value labels** but
  not identical: the CF file's units cover 69-82% of each wave's §9 harvest
  ROWS. That share is the CEILING on `shipped_matched`, and a silent drop to
  zero is this layer's dominant failure mode
  (`.coder/ledger/harvest-kg-shipped-factors.md`, "the `j`-vocabulary trap
  generalises to every key"), so it is pinned by test.
- **W2's CF labels are bare caps (`KERCHAT/KEMBA`) and W3-W5's are prefixed
  (`1. Kilogram`).** `_clean_unit_label` normalises both; verified.
- **The mojibake units do not match and must not be "fixed" here.** 550
  `crop_production` rows carry `Zorba/Akara Ï¿½ Large` (double-encoded) where
  the CF file has the clean form (`CONTENTS.org` §"The unit axis ... two small
  blemishes"). Repairing `_clean_unit_label` would change `crop_production`'s
  stored content, which Task A must not.
- **`_harmonize_wave_keyed` returns TUPLE keys and `_map_int_codes` maps BARE
  codes.** `plot_features_for_wave` passed the tuple-keyed
  `harmonize_area_unit` dict straight to `_map_int_codes`, so every lookup
  missed and `AreaUnit` was `<NA>` on **100% of the 9,727 non-GPS fields** —
  silently, because the column was still *present* (`CLAUDE.md` §"The Silent
  All-Null Read" catches an all-null **required** column, and `AreaUnit` is
  100% populated overall because the GPS rows carry `'hectares'`). Found while
  wiring #853, whose join is ON that label. See §Phase 3.
- **EPAR's own area merge is mis-keyed.** Their general helper leaves
  `saq02`/`saq03` unpadded when concatenating (`W5.do:195-225`) while the
  conversion-file block pads them to two characters (`:325-341`), so zone 1 of
  region 1 is `"011"` on one side and `"0101"` on the other. Their merge
  matched 1,566 of 14,878 rows and changed 19 field sizes (their own inline
  counts, `:694-703`). We join the raw `(region, zone, woreda)` integer triple.
- Repo-wide landmines per `STANDING.md` §4 (sanctioned IO only, pandas 3.0,
  never the `dvc` CLI, no in-tree parquets). Plus **GH #866**: a config change
  to a script-path country table does not rebuild an existing `var/` parquet —
  every rebuild in this task `rm`-ed the target first.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| the `shipped` layer + its join, screen, counts | **reuse** | `675fc34a` built it for exactly this; §2 |
| crop code -> `j` | **reuse** `_eth_crop_label_map` | the loader must speak the built frame's vocabulary, and only the frame's own map guarantees that |
| unit code -> `u` | **reuse** `_clean_unit_label` on the CF file's own Stata labels | same reason; a parallel unit table would fragment the `u` axis |
| local-unit code -> `AreaUnit` | **reuse** `_harmonize_wave_keyed('harmonize_area_unit')`, sliced to the wave | the join is ON that label, so "consistent by construction" beats "consistent by coincidence" (the file's own labels do agree) |
| the national factor | **reuse the file's `mean_cf_nat`** | it is the WB's own published national figure — transcription, not an aggregate. A median across region columns would be an aggregation core forbids |
| region expansion of the pooled `99` cell | **not implemented** | the variable label says "SOMALIE, DIRE DAWA, & HARAR"; EPAR expands it to codes 5/13/15 (`W5.do:766-773`). We serve what the file says and leave the expansion to a caller |
| the crop-74 / unit-62 duplicate | **new, targeted rule** (keep 4.34, drop > 5.0), then **assert** the remainder is unique | EPAR reaches the same answer from W3 continuity (`W5.do:800-802`). A general "keep min" would silently absorb a *future* duplicate; the assert makes a new one loud |
| the `Leafy Greens` decode collision | **new: DROP the key, warn, never pick** | KALE 56 and SPINACH 69 carry different factors and `harmonize_crop` folds them onto one label; `crop_production` no longer records which crop the row was, so there is nothing to choose on |
| filling the holes in either table | **refused** | EPAR's `fillin` + per-`(unit, region)` mean, and its weighted zone/region/national area medians, are imputation. Out of scope and named as such in both issues |
| metric self-conversions (`Hectare`, `Square Meters`) in `plot_features` | **not implemented** | they need no external table and are a separate defect; 2,107 rows corpus-wide. Recorded in `CONTENTS.org`, not wired, so #853's "newly converted" count stays attributable to the WB table |
| borrowing a 2021-22 area table | **refused (default)** | #853 "What it must NOT do"; the loader raises when asked for that wave |
| `yield_kg` pass-through | **extend** | the deferred item, `.coder/ledger/harvest-kg-shipped-factors.md` §6 Q3 |

## §6 Open questions for the human

1. **Should `crop_production` emit a native `region` level?** `sect9_ph`
   carries `saq01` on 100% of rows in every wave, and 10.2% / 45.6% / 38.3% /
   28.7% of the rows the WB table can serve (by wave) land on a `(j, u)` whose
   factor actually VARIES by region — so the national default is a real
   approximation, not a formality. Emitting `region` is a `crop_production`
   schema change (a new index level or column, a `data_scheme.yml` edit and a
   corpus rebuild) and was deliberately not taken inside a read-time-layer
   issue. **Blocks**: retiring the national default in favour of `region=True`.
2. **`AreaUnit` was `<NA>` on every non-GPS Ethiopia field until this PR** (§4).
   Two questions follow: is the same `_harmonize_wave_keyed` /
   `_map_int_codes` mismatch present in another country, and should the
   null-read guard reach a **partially** null declared column (it is
   whole-column / whole-`t`-slice today, and `AreaUnit` was 93% populated
   country-wide because the GPS rows carry `'hectares'`)? **Blocks**: nothing
   here; filed as an observation.
3. **The metric self-conversion** (`Hectare` 216 rows, `Square Meters` 1,891
   rows still NaN) needs no table at all — EPAR does it (`W5.do:701-702`).
   Worth its own issue? **Blocks**: nothing.

---
### Phase 3 — verification (measured)

- `ethiopia.crop_conversion_factors` — **OK (anchored on §3, §4, §5)**: keys on
  `(t, j, u)` from `SHIPPED_FACTOR_JOIN_LEVELS`, index unique, decodes through
  the frame's own maps, refuses 2011-12, resolves the WB duplicate to 4.34 and
  the decode collision by a loud drop. Not a reinvention: §2 row 10.
- `ethiopia.local_area_unit_factors` / `_convert_local_area_units` — **OK
  (anchored on §3, §4)**: GPS is never overridden; `AreaUnit` keeps the native
  label per `data_info.yml:237-239`; an implausible conversion is counted and
  refused, never clipped.
- `yield_kg(..., shipped_factors=, min_reports=)` — **OK (anchored on §5)**:
  two forwarded kwargs, default path unchanged.
- **Cache impact, measured** (`_table_cache_hash(table, c.waves)`, base
  `675fc34a` vs this branch, same isolated `LSMS_DATA_DIR`):
  **all 25 Ethiopia tables move**, because `ethiopia.py` is the country module
  and is in every table's fingerprint. Only ONE moves in *content*:
  `plot_features`. `crop_production` rebuilt **byte-identical** to the
  pre-edit parquet (`DataFrame.equals` True, 85,519 x 7) — the Task A claim,
  verified rather than asserted. The `transformations.py` edit moves **0 of
  11** probed Uganda values (`build_transforms_fingerprint(None)`, four
  `build_transforms_fingerprint(table)`, five `_table_cache_hash`) — `yield_kg`
  carries no `@build_transform()`.
- **#852, measured cold on the isolated build** (`Country('Ethiopia').crop_production()`, 85,519 rows):

  | | before | after |
  |---|---|---|
  | `shipped_matched` | 0 | **62,164** (72.7% of rows) |
  | `shipped` (post-rank) | 0 | 62,164 |
  | `inferred` | 26,546 | 7,766 |
  | `none` | 58,973 | **15,589** |
  | `reported` / `survey_median` | 0 / 0 | 0 / 0 |
  | `shipped_implausible` | 0 | **0** |
  | rows returned | 22,672 | 66,049 |
  | Σ `Harvest_kg` | 1,905,711.5 | **12,680,616.3** |

  By wave (Σ `Harvest_kg`, before -> after): 2011-12 653,661.0 ->
  **653,661.0 (unchanged — W1 ships no factor file)**; 2013-14 625,076.0 ->
  3,612,044.1; 2015-16 331,120.4 -> 3,920,357.0; 2018-19 103,685.4 ->
  2,657,402.1; 2021-22 192,168.7 -> 1,837,152.1.

  `reported_vs_shipped` is `{both: 0}` and always will be for Ethiopia: the
  ESS asks no per-row conversion factor, so `crop_production` has no
  `KgFactor` column. The available corroboration is **shipped vs inferred**:
  on the 18,780 rows where both exist (the metric units) the median ratio is
  **1.0000** and **0.0%** fall outside `[0.5, 2]` — per wave 9,151 / 3,826 /
  2,576 / 3,227 rows, median ratio 1.000 in each. A mis-keyed join shows as a
  wild ratio, not as an empty result, so this is the check that the keys are
  right rather than merely non-empty.

  Unit-label match rate (share of each wave's §9 harvest ROWS whose `u` the
  table covers — the ceiling on `shipped_matched`): 69.3% / 82.5% / 82.0% /
  77.6%. Crop-code match rate: **100% in all four waves** (0 unmapped codes).
  The reverse direction: the units the table has and §9 never uses are inert;
  the units §9 uses and the table lacks are `Other (Specify)` (5,498 of the
  15,589 still-unserved rows), `Chinet` / `Chinet Medium`, and the mojibake
  `Zorba/Akara Ï¿½ *` forms.

- **#853, measured cold**: fields gaining an `Area`, by wave — 2011-12 **964**,
  2013-14 **1,152**, 2015-16 **135**, 2018-19 **19**, 2021-22 **0** (no shipped
  table; not borrowed) = **2,270**. `Area` NaN falls 9,727 -> 7,457
  (5,933->4,969 / 2,485->1,333 / 1,072->937 / 99->80 / 138->138).
  **0** previously-non-null `Area` values changed — GPS is never overridden.
  Implausibility (converted area above the wave's own largest GPS-measured
  field): **3 refused in 2011-12** (ceiling 9.91 ha), 0 elsewhere; nothing
  clipped. Of the 7,457 still NaN, 1,891 are `Square Meters` and 216
  `Hectare` — metric, convertible with no table at all (§5, §6 Q3) — and
  1,098 are `Other`.
- **Tests**: `tests/test_ethiopia_shipped_factors.py` **20 passed** (new);
  `tests/test_shipped_factors.py` + `tests/test_crop_kg_factor.py` **72
  passed**, including `test_uganda_harvest_kg_baseline`, which RAN and is
  unchanged (130,606 in / 36,095 out, sum `10 868 272.24500081`).
