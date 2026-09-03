# Prior-Art Ledger — ghanasps-livestock (GH #729, #736, #140)

> Per-task ledger.  Inherits the repo §0 baseline in `STANDING.md`; cites it,
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git floor (gitnexus MCP failed to connect this
session: `CONNECT_TIMEOUT`).

## §1 Task, restated
Wire the canonical `livestock` table for GhanaSPS in all three waves
(`2009-10`, `2013-14`, `2017-18`) on the **YAML path**: a `livestock:` block
in each wave's `data_info.yml` (one source file per wave; wave 3 lists its
`_osp` "other -- specify" file as a second `file:` entry so the free-text tail
row-concatenates), a `livestock` entry in `GhanaSPS/_/data_scheme.yml` at the
canonical grain `(t, i, animal)` with columns `HeadCount` (float),
`HeadSold` (float, `optional` -- asked in 2017-18 only) and `HerdValue`
(float, monetary).  `v` is NOT declared and is NOT joined: `livestock`'s
canonical index in `data_info.yml`'s `index_info` omits `v` and the table is
in `Join v from sample > skip_extra`, so `_no_v_join_tables()` skips it (as
every precedent country documents).  The `animal` index level is harmonised
at EXTRACTION through a new `harmonize_species` table in
`_/categorical_mapping.org` (the corpus name -- Uganda / Nigeria / Malawi all
use it).  The one Python piece is a `livestock(df)` `df_edit` hook in
`GhanaSPS/_/ghanasps.py` for what YAML cannot do (§5).  `ValuePerAnimal`,
`SalesValue`, `HeadAcquired` are deliberately NOT declared.  Verify cold on a
private `LSMS_DATA_DIR` under `LSMS_GRAIN_STRICT=1`, with the read-strict
condition asserted directly (`null_read_reports(country='GhanaSPS',
table='livestock') == []`) because `LSMS_READ_STRICT=1` aborts inside
`sample()` on this country (documented, PR #752).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.grab_data` single-/multi-file branch | `lsms_library/country.py:1214-1266` | per file: `df_data_grabber` -> `check_adding_t` -> concat; `missing_ok=True` when several files are listed; the table's `df_edit` hook runs AFTER the concat on the `(t, i, animal)`-indexed frame | roster/housing/plot_features | **reuse** (W3: main + `_osp` as two `file:` entries) |
| `Wave.column_mapping` -> `mappings: [table, key, value]` on an idxvar | `country.py:939-947` | `cat_table.set_index(key)[value].to_dict()`; `df_data_grabber` applies `.map(lambda x: f.get(x, x))` (unmatched values pass through UNCHANGED) and THEN `format_id` | GhanaSPS `plot_features` (`Tenure`/`AreaUnit`), Mali food | **reuse** for `animal` |
| `df_data_grabber` | `local_tools.py:1325-1443` | idxvars auto-`format_id` (strips whitespace, NaN -> `None`) | yes | reuse; a null `animal_id` becomes a null index level -> the hook must DROP those rows |
| `Wave.formatting_functions` / `df_edit` dispatch | `country.py:871`, `978`, `1216`, `1263` | a callable named after the table in `ghanasps.py` is the table's hook, bound for every wave | `ghanasps.individual_education`, `ghanasps.plot_features` | **reuse** the pattern (new hook body) |
| `all_dfs_from_orgfile` | `local_tools.py:2904` | org table parser: cells `.strip()`ped, `---` -> NaN, `_to_numeric` per column; internal double spaces preserved | yes | reuse; a purely numeric spelling (`'50'`) would be coerced, so it is deliberately NOT in the table |
| `Country._apply_categorical_mappings` | `country.py:2403` | API-time auto-dispatch on a table whose NAME matches a column/index | housing tests | not triggered (table is `harmonize_species`, no such column) -- extraction-time only, like the precedents |
| `Country._normalize_dataframe_index` / `_audit_index_collapse` | `country.py:5358` | `groupby(dropna=True)` DELETES null-key rows and `.first()`s duplicates, filing a `GrainCollapseWarning` (fatal under `LSMS_GRAIN_STRICT`) | `tests/test_gh323_*` | the reason the hook drops null-`animal` rows and resolves duplicate `(t, i, animal)` lines ITSELF (§5) |
| `_no_v_join_tables()` | `country.py:4589-4616` | skips the `v` join for tables whose `index_info` index omits `v` or that are in `skip_extra` -- `livestock` is both | GH #436 tests | reuse (nothing to do) |
| `Country._finalize_result` `dropna(how='all')` | `country.py` (GH #645) | removes rows with every declared column null | yes | interacts with the keep-rule: a row the hook keeps always has >= 1 positive value, so nothing is silently removed downstream |
| `null_read_audit` Site B | `lsms_library/null_read_audit.py:375` | required declared column 100% null in a wave's `t` slice -> warning / fatal | yes | `HeadSold` is `optional: true` (W1/W2 never ask it) |
| `currency.currency_for` + `_DEFAULT_MONETARY['livestock']` | `lsms_library/currency.py:543`, `:512`; `data_info.yml` `Currency: GhanaSPS: GHS` (l.22 of that section) | GhanaSPS resolves to `GHS` in every wave; `HerdValue` is in the registry (PR #736) | `tests/test_currency_livestock_registry.py` | reuse -- nothing to add, no STOP |
| Uganda `livestock_for_wave` | `countries/Uganda/_/uganda.py:2185-2225` | keep-rule: any head measure > 0; SUMS duplicate species lines | script | precedent for the keep-rule and the duplicate-line sum |
| Malawi `_livestock_block` / `assemble_livestock` | `countries/Malawi/_/malawi.py:1687-1780` | keep-rule: owned flag OR any positive head measure; `groupby(['t','i','animal']).agg(sum)` on duplicate lines | script | precedent (its bare `sum` turns NaN+NaN into 0 -- this hook uses `min_count=1`) |
| Nigeria `livestock_for_wave` | `countries/Nigeria/_/nigeria.py:856-940` | keep-rule: owned flag OR ANY reported quantity non-null and nonzero (value included) | script | **the rule adopted** -- see §5 |
| Nigeria `harmonize_species` | `countries/Nigeria/_/categorical_mapping.org:756` | Preferred Labels incl. `Guinea Fowl`, `Duck`, `Turkey`, `Fish`, `Other Livestock`; a `Species` rollup column | -- | label precedents |
| Uganda `harmonize_species` | `countries/Uganda/_/categorical_mapping.org:1029` | species-level labels `Cattle Goats Sheep Pigs Donkeys Horses Chicken Other Poultry Rabbits Bees` | -- | label precedents |
| Malawi `harmonize_species` | `countries/Malawi/_/categorical_mapping.org:670` | `Dove/Pigeon`, `Guinea Fowl`, `Duck`, `Turkey`, `Chicken`; drops `Other (Specify)` to NaN | -- | label precedents |
| `slurm_logs/ghanasps/FINDINGS_agriculture.org` | §7, §8, §"livestock -- CONSTRUCTIBLE", §"Ranked recommendation / 1" | the C4 wording on all three instruments; W2 grid + free text; W1 cedis/pesewas | research note | source of record |

## §3 Definitions & conventions in force
- `livestock` canonical columns and the ADDITIVITY split: `lsms_library/data_info.yml:314-437` -- `ValuePerAnimal` (per head, NOT additive), `HerdValue` (stock total, additive over `animal`, "values HeadCount"), `SalesValue` (flow total).  Which one a country has is settled on the QUESTIONNAIRE WORDING.  Nothing in the block is `required`.
- `index_info: livestock: (t, i, animal)` at `data_info.yml:35`; `skip_extra` includes `livestock` at `:68`.
- Monetary registry: `currency._DEFAULT_MONETARY['livestock'] == {ValuePerAnimal, HerdValue, SalesValue}` pinned to the YAML by `tests/test_currency_livestock_registry.py`; a bare `Value` in livestock is forbidden by `test_bare_Value_is_no_longer_a_livestock_column`.
- Currency: `data_info.yml` `Currency: GhanaSPS: GHS` ("all post-reform").
- Grain: core never aggregates; NaN in a declared index level is deleted-and-reported: `CLAUDE.md` §"Grain Collapse".  A reducer in CORE is forbidden; a documented, bounded reducer in a COUNTRY script is what Uganda/Malawi do (§2).
- Household id form per wave: `GhanaSPS/_/CONTENTS.org` §"Cluster identity IS available" (W1 9-digit `hhno`; W2/W3 `FPrimary`).
- Module 03ai is livestock BY CONTENT in every wave; module letters are not stable: `CONTENTS.org` §"Module 4 IS agriculture in every wave -- tested, not assumed" and §"individual_education: the module letter MOVES".
- Every W1 money variable is a cedis / pesewas pair: `CONTENTS.org` §"Every 2009-10 money variable is split cedis / pesewas".
- `LSMS_READ_STRICT=1` aborts every GhanaSPS household table inside `sample()`: `CONTENTS.org` §"LSMS_READ_STRICT=1 cannot build ANY GhanaSPS household table on this branch".
- Extraction-time table mapping syntax: `.claude/skills/add-feature/SKILL.md` §"Categorical mapping tables".

## §4 Invariants & assumptions
- **The value question is a HERD TOTAL in all three instruments** (FINDINGS C4, quoted in `data_scheme.yml`): W1 `S3AI` label "Q3.1 What is the current value of these animals if you sold all of them (cedis)"; W2 Part A "if you sold all of them?  Gh Cedis and pesewas"; W3 "if you sold all of them?  Indicate amount as a decimal value".  Therefore `HerdValue`, never `ValuePerAnimal`.  Corroborating conditional test, measured: **0 of 10,884** W2 zero-head grid rows carry a value (a herd total vanishes with the herd; a reservation price per head would not).
- W1 `S3AI.dta` 4,280 x 28, key `(hhno, animal_id)`: `animal_id` decodes to 8 labels (`Drought Animal` [sic], `Cattle`, `Sheep`, `Goats`, `Pigs`, `Rabbits`, `Chicken/Rosters`, `Other Farm Animals`); **8 rows have a null `animal_id`** (181 head, one household `108280011` with four such lines) -> null index level -> dropped by the hook, recorded as a loss; **19 households list `Other Farm Animals` on 2-3 lines each** (38 rows) -> duplicate `(t, i, animal)` -> summed by the hook.  `s3ai_1` (head) has 0 zeros and 11 nulls (9 of the 11 carry a value).  `s3ai_3ii` (pesewas) is int8, 0 nulls, non-zero on 25 rows; `s3ai_3i` (cedis) has 4 nulls, ONE of which has pesewas = 1 (goat, 1 head -> 0.01 GHS, kept as reported).
- W2 `03ai_animalquestions.dta` 14,860 x 44, key `(FPrimary, animal)` unique: a FIXED 7-species grid asked of every one of 2,098 households (7 x 2,098 = 14,686) + 174 free-text rows.  **10,884 rows have `quantity == 0`**; 13 have `quantity` null and no value.  The tail's `animal` is free text with 62 distinct spellings beyond the 7 core labels; 0 leading/trailing whitespace; no household writes the same species twice under two spellings (post-mapping duplicates: 0).  `currentvalue` is already decimal cedis (chicken herd median 100 vs 40 in W1 and 200 in W3 -- inflation, not a x100).
- W3 `03ai_animalquestions.dta` 4,203 x 44 (8 clean labels, owned only, `quantity` min 1) + `03ai_animalquestions_osp.dta` 91 x 43 (11 title-case free-text species, 78 households); same column names (`quantity`, `currentvalue`, `quantitysold`); the main file has NO "Other" placeholder row, so concatenating does not double count.  **One household (`106183008`) has `Chickens/roosters` in the main file and `Chickens` in `_osp`** -> one duplicate after mapping -> summed.  23 main rows have `quantity` null; 11 of them carry a value or a sale and are kept with `HeadCount` NaN.  `quantitysold` > `quantity` on 72 rows (a 12-month flow against a current stock); two outliers left as reported (`107258039` chickens 1,000 head / 40,000 sold; `101025002` cattle 10 head / 3,300 sold).
- Every livestock household is in its wave's `sample()`: 2,142/2,142, 2,098/2,098, 2,363/2,363 (100.0%).  `v` is nonetheless NOT attached -- by canonical design, not by omission.
- `mappings:` on an idxvar runs BEFORE `format_id`, and the lookup is exact: every Alternate Spelling must appear exactly once in the table (`set_index().to_dict()` keeps the last on duplicates), and internal double spaces (`GUINE  FOWL`) must be reproduced.
- Editing `ghanasps.py` moves `Country._table_cache_hash` for EVERY GhanaSPS table (country module is hashed): one cold rebuild of the shared cache after merge.
- `HeadSold` must be `optional: true` or the Site B null-read guard fires on W1/W2 (never asked there).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| key / index `(t, i, animal)` | reuse (`idxvars` + `format_id`) | STANDING §2; `index_info` |
| `animal` harmonisation | reuse `mappings: [harmonize_species, Alternate Spelling, Preferred Label]` at extraction + new table in `_/categorical_mapping.org` | corpus name; precedent label wins where one exists (Uganda's species-level `Cattle Sheep Goats Pigs Rabbits Chicken Donkeys Other Poultry`; Nigeria/Malawi's `Guinea Fowl Duck Turkey Fish Dove/Pigeon Other Livestock`); plural coinages where none (`Draught Animals`, `Dogs`, `Cats`, `Grasscutters`, `Guinea Pigs`, `Peacocks`, `Quails`).  Spelling harmonised, species NEVER folded; only labels that name no species (`BIRDS`/`Bird` -> `Other Poultry`, `Other Farm Animals` -> `Other Livestock`) go to a catch-all.  `'50'`, `'cut'`, `'dake'` are genuinely unmappable and pass through as their own labels. |
| `HerdValue` W1 = cedis + pesewas/100 | **new** -- `ghanasps.livestock` df_edit hook | YAML maps one column to one column; `derived:` has one registered kind and adding one edits `build_transforms.py` (stop-listed).  NaN only where cedis is null AND pesewas is 0. |
| W2 zero-head grid rows | **new** -- same hook, Nigeria's rule: keep iff any of `HeadCount` / `HeadSold` / `HerdValue` is non-null and > 0 | the roster grid's never-owned filler is not a holding (all three precedents drop it).  Nigeria's INCLUSIVE form (a value alone keeps the row) is the right one here because `HerdValue` is a stock valuation of the herd; Uganda excludes value-alone rows because ITS value is a per-head price, which is not evidence of ownership -- that reasoning does not transfer.  Measured, the two forms differ on 9 W1 rows and 11 W3 rows (null head, positive value/sale) and on 0 W2 rows. |
| null-`animal` rows (W1: 8) | **new** -- same hook | `format_id(NaN)` -> `None` index level -> NaN-key deletion + `GrainCollapseWarning`, fatal under strict.  Dropped deliberately and counted. |
| duplicate `(t, i, animal)` lines (W1: 19 hh `Other Farm Animals`; W3: 1 hh chickens main+osp) | **new** -- same hook, `groupby(['t','i','animal']).sum(min_count=1)` | Malawi `assemble_livestock` / Uganda precedent.  Legal because every declared column is ADDITIVE at this grain (`HeadCount`, `HeadSold` counts of the same label; `HerdValue` a herd total by definition).  BOUNDED: the verify script prints the groups collapsed per wave and the test pins the counts, so a NEW duplicate turns red instead of being summed.  W1's collapse adds DIFFERENT unnamed species into one `Other Livestock` row -- the level the W1 instrument never recorded. |
| `v` | not joined | canonical design (`index_info` + `skip_extra`); sample-join share reported instead |
| `ValuePerAnimal` / `SalesValue` / `HeadAcquired` | not declared | per-head value never asked; sales revenue (`s3ai_12`, `animalrevenue`) is animals AND products together, not `SalesValue`; purchases never asked |
| expenses / revenue per species | not wired, recorded in CONTENTS.org | no canonical home |

## §6 Open questions for the human
- The `animal` vocabulary mixes Uganda's plurals with Nigeria/Malawi's singular `Duck` / `Turkey` so that `Feature('livestock')` agrees across countries on every label that has a precedent; the seven coinages (`Draught Animals`, `Dogs`, `Cats`, `Grasscutters`, `Guinea Pigs`, `Peacocks`, `Quails`) have none.  If the owner prefers a shared cross-country species list, that is a `lsms_library/categorical_mapping/` change.
- Dogs and cats are reported with sale values in W2/W3 and are delivered under their own labels rather than folded into `Other Livestock`, so an analyst can exclude them from a wealth aggregate.
- Brief STEP 3 check 2 asks for `v` 100% non-null; livestock has no `v` by canonical design.  The brief should be corrected.

---
### Phase 3 — verification (fill at task end)
- `ghanasps.livestock` (df_edit hook) — OK (anchored on §5): recombines W1 cedis + pesewas, drops the temporaries, drops null-`animal` rows, applies the Nigeria keep-rule, sums duplicate `(t, i, animal)` lines with `min_count=1`; no other edit, no fill, no clip.
- `GhanaSPS/{wave}/_/data_info.yml` `livestock` — OK (§2/§4): one source file per wave (W3 two, same schema); `animal` mapped at extraction; W1 reads BOTH `s3ai_3i` and `s3ai_3ii`.
- `GhanaSPS/_/categorical_mapping.org` `harmonize_species` — OK (§5): every Alternate Spelling once; precedent labels reused verbatim.
- `GhanaSPS/_/data_scheme.yml` `livestock` — OK (§3): index `(t, i, animal)` without `v`; `HeadCount`, `HeadSold` (optional), `HerdValue`; the three instruments' wording quoted; no `ValuePerAnimal` / `SalesValue` / `Value`.
- `tests/test_ghanasps_livestock.py` — OK (§4): data-gated via `requires_s3`; pins `HerdValue` not `ValuePerAnimal`, the per-wave duplicate-collapse counts, the keep-rule, and the delivered `animal` values as a subset of the table's Preferred Labels plus the three listed pass-throughs.
- Verification env — same deviation as PR #752, recorded: `LSMS_READ_STRICT=1` is fatal inside `GhanaSPS/sample`; STEP 3 ran under `LSMS_GRAIN_STRICT=1` with `null_read_reports(country='GhanaSPS', table='livestock') == []` asserted directly.
