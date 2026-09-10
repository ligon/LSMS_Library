# Prior-Art Ledger — GH #863 (the remaining transforms, Phase 2 of WORKPLAN.org)

> Per-task ledger. Inherits `STANDING.md` §0 — cites it rather than re-copying.
> Sibling of `.coder/ledger/rcsi-transform.md`, which covered `rcsi` /
> `rcsi_phase` (already merged; untouched here).

**Search tier used:** ripgrep + git (floor). The `gitnexus` MCP server did not
connect this session (`CONNECTION_CLOSED`) and this worktree has no
`.gitnexus/` index, so per `CLAUDE.md` §"Code intelligence: GitNexus is
OPTIONAL" the substitutes were used and are declared here rather than skipped
silently: `rg`/`grep` over `lsms_library/transformations.py`,
`lsms_library/data_info.yml`, all 40 `lsms_library/countries/*/_/data_scheme.yml`
and the `CONTENTS.org` of Uganda / Malawi / Ethiopia / Nigeria / GhanaSPS, in
place of `gitnexus_query` / `gitnexus_impact`.

**Blast radius of the six new symbols** (`gross_crop_revenue`,
`livestock_sales_value`, `hdds`, `fcs`, `fertilizer_rate`, `crop_diversity`, plus
`_CROP_LEVELS` / `_resolve_crop_level` / `_resolve_food_groups` /
`_consumed_mask` / `HDDS_GROUPS` / `FCS_WFP_WEIGHTS`): **none**. `rg -n` for
each name across `lsms_library/` (package and `countries/*/_/*.py`) before the
change returned zero hits; every one is a new top-level name called by nothing.
No existing call site to break, no rename, no signature change to anything
that already exists.

The EPAR `.do` files were read directly (`sed -n` over
`reference/epar/LSMS-Agricultural-Indicators-Code/…`), not trusted from the
issue body — see §3 for the lines and what each actually says.

## §1 Task, restated

GH #863 / WORKPLAN.org Phase 2 asks for six analyst-callable transforms in
`lsms_library/transformations.py`, in the "Parity TRANSFORMS" house style
(plain functions over an already-loaded item feature, NOT registered in
`_FOOD_DERIVED` / `_ROSTER_DERIVED`, NOT auto-surfaced as `Country` features):
`gross_crop_revenue` and `fertilizer_rate` (MECHANICAL reductions over
`crop_production` / `plot_inputs`+`plot_features`), `livestock_sales_value`, `hdds`,
`fcs` and `crop_diversity` (METHODOLOGY — each encodes a specific analytic
choice: a price basis, a food-group taxonomy, an entropy weight). Each
docstring must name the EPAR/WB analogue and state the divergence rather than
force a match, per the section banner at `transformations.py:1578-1620`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_resolve_plot_level` / `_PLOT_LEVELS` | `transformations.py:1626-1640` | resolves `plot` vs `plot_id` across countries | via `yield_kg`/`nitrogen_kg` | **reuse**; `_CROP_LEVELS`/`_resolve_crop_level` are its twin for `j` vs `crop` (new, and necessary — see §4) |
| `nitrogen_kg` | `transformations.py:2469` | plot-level kg of N: unit→kg via `_kg_factor_series`, times `_NITROGEN_CONTENT` share | corpus sanity | **reuse as the numerator of `fertilizer_rate`**, including for `nutrient='product'` (shares forced to 1.0) — no second unit-conversion implementation |
| `yield_kg` | `transformations.py:2256` | the grain-bridge pattern: reduce both features to a common land key, `merge(how='inner')`, divide, drop non-finite | corpus sanity | **reuse the pattern** (`on='plot'`/`'parcel'`, `_parcel_from_crop_plot` / `_parcel_from_feature_plot`) for `fertilizer_rate` |
| `median_price_valuation` | `transformations.py:3140` | the geographic median-price ladder (finest cell with N≥10 → national) | `tests/test_median_price_weight_col.py` | **reuse by reference**: `gross_crop_revenue`'s docstring points at it for EPAR's *valued-harvest* construct rather than reimplementing a ladder; `livestock_income(price=<Series>)` is the seam a caller feeds it through |
| `rcsi` | `transformations.py:2800` | never-zero-fill rule, symmetric vocabulary check, `ValueError` naming the offending labels | `tests/test_rcsi.py` | **reuse the discipline**: `_resolve_food_groups` raises on an unrecognised group, a missing group, and an unmapped `j`, all naming names |
| `tlu` | `transformations.py:2629` | animal level-or-column resolution, species→factor map | corpus sanity | **reuse the shape** for `livestock_sales_value`'s `animal` handling |
| `farm_size` / `nb_plots` | `transformations.py:2981` / `:3020` | the `(t,i)`-grain reduction house pattern | — | pattern reused; the new block is inserted immediately after `nb_plots` per the task brief |

**Reuse-search result:** no prior `gross_crop_revenue`, `livestock_sales_value`,
`hdds`, `fcs`, `fertilizer_rate`, `crop_diversity`, Shannon/entropy,
`HDDS`/`FCS`/food-group symbol anywhere in `lsms_library/` (`rg -i
'shannon|diversity_index|\bhdds\b|\bfcs\b|food_group|fertilizer_rate|
livestock_income|gross_crop'` = 0 outside docs/`slurm_logs`). Building new is
correct; three of the six are thin wrappers over machinery that already exists
and say so in code.

## §3 Definitions & conventions in force (cite, don't paraphrase)

- **`livestock.ValuePerAnimal` is TWO instrument questions**, per
  `lsms_library/data_info.yml` (`Columns: livestock: ValuePerAnimal`): a
  RESERVATION price ("if you would sell one today…", reported whether or not
  anything sold — Nigeria `s11iq3`/`s11iq7`, Malawi `ag_r04`, Uganda 2009-10 /
  2010-11 `a6aq6`) and a REALISED average ("what was, on average, the value of
  each sold?" — Uganda 2011-12 onward `a6aq14b`/`s6aq14b`, non-null rate
  falling ~96% → ~21%). Cross-checked against the countries' own
  `data_scheme.yml` (`Nigeria:337-343`, `Uganda:291-292`) and
  `Malawi/_/CONTENTS.org:336-344`.
- **`livestock.SalesValue` is the REPORTED gross sale value** and is a
  different column, not a synonym: Ethiopia `ls_s8aq60`
  (`Ethiopia/_/data_scheme.yml:196-206`) and Mali `s4aq24`/`s8b1q14`. Both
  countries report it *instead of* a per-head price.
- **GhanaSPS has no reported PRICE at all**: its value question is a HERD
  total (`HerdValue`, "current value of these animals if you sold all of
  them"), and its revenue questions bundle animals with products, so neither
  `ValuePerAnimal` nor `SalesValue` is declared — deliberately
  (`GhanaSPS/_/data_scheme.yml:406-414`). It DOES declare `HeadSold`
  (`:465`, `optional: true`), so only a caller-supplied `price=` Series works
  there. (Verified with `grep -n '^    HeadSold:' countries/*/_/data_scheme.yml`
  — 14 countries declare it. An earlier whole-file grep matched the column
  name inside GhanaSPS's *comment* listing what it does NOT declare, and would
  have shipped a false claim in two docstrings.)
- **`plot_features.Area` is hectares** — "plot area in hectares (canonical
  unit)", `data_info.yml`, `Columns: plot_features: Area`, `required: true`.
  That is what makes `fertilizer_rate` a kg/ha figure without a unit kwarg.
- **`crop_production` carries NO per-crop area**: the canonical block declares
  `condition` and `KgFactor` only, and no country's `data_scheme.yml` declares
  an `AreaShare` / `Area_planted`. Recorded independently in
  `slurm_logs/2026-09-09_epar_curation/LEARNINGS.org` L8 and
  `FINDINGS_taxonomy.org` (SHANNON DIVERSITY INDEX row, "THEIRS-ONLY(agg)").
- **EPAR's `livestock_sales_value`** (`Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:5959`,
  read directly): `value_slaughtered + value_lvstck_sold -
  value_livestock_purchases + (milk + eggs + other + manure) - (hired labour +
  fodder + vaccines)`. Its `value_lvstck_sold` is built at `:3320-3346` from a
  weighted-median price ladder ea → ta → district → region → country at N ≥ 10.
- **EPAR's HDDS mapping** (`Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:2538-2560`)
  is NOT a partition: itemcode `704` appears in both FRUITS and SWEETS,
  itemcode `1003` in both OILS AND FATS and SPICES/CONDIMENTS/BEVERAGES.
  Stata's `recode` takes the first matching rule, so 704 silently became
  FRUITS and 1003 silently became OILS — the second assignment never fired.
- **EPAR's FCS** (`:2574-2600`) reads a *separate* module (`hh_sec_j3.hh_j08`,
  days) and blends cereals with tubers by an uncited averaging rule
  (`(max + min(sum,7))/2`) instead of summing-then-capping; its item-group 2
  is left unweighted and contributes nothing.
- **EPAR's Shannon** (`Uganda UNPS Wave 5/EPAR_UW_Uganda_UNPS_W5.do:3603,3617`)
  is `Σ p·ln p` — **never negated**, so its published `sdi` is non-positive and
  equals `−H`. It weights by area PLANTED, drops `area_plan == 0` rows (which
  its own comment says excludes permanent/tree crops), and takes shares over
  plot-crop ROWS rather than over crops.
- **FAO HDDS** = 12 groups, **24-hour** recall (Kennedy, Ballard & Dop, FAO
  2011, Table 1). **WFP FCS** = 8 groups with weights 2/3/1/1/4/4/0.5/0.5 and
  condiments at 0 (WFP VAM Technical Guidance Sheet, 2008).

## §4 Invariants & assumptions (the landmines)

- Per STANDING.md §4: pandas 3.0 targets (no `inplace=`), sanctioned IO only.
  **Not exercised by this task** — all six transforms take an already-loaded
  DataFrame and do no IO whatsoever.
- **The crop level is `j` in Uganda/Niger/Togo and `crop` in Malawi.**
  `data_info.yml`'s `index_info` says exactly why the plot-level ag features
  are not registered there: "their per-country index NAMES diverge (plot vs
  plot_id, crop vs j)". So any transform that groups by crop MUST resolve the
  name (`_resolve_crop_level`) or it silently fails on half the corpus.
  `by='j'` therefore means "keep the crop level", not "keep a level called j",
  and the output keeps the table's own name — pinned by
  `test_gross_crop_revenue_by_j_resolves_malawis_crop_level` and
  `test_crop_diversity_resolves_malawis_crop_level`.
- **Never zero-fill a household that reported nothing.** Same rule, same
  reason, as `rcsi`: every reduction uses `sum(min_count=1)` + `dropna()`, so a
  household with rows but all-NaN `Value_sold` is ABSENT from
  `gross_crop_revenue`, not 0. `HeadSold == 0` beside a real price IS a
  genuine zero and is kept. The one deliberate exception is `fcs`, where a
  group with no row scores 0 days — because in a 7-day *frequency* recall
  "no row" is the instrument's own zero; stated in its docstring, with the
  acquisition-vs-consumption caveat that qualifies it.
- **`crop_production` is one row per `(u, condition)` as well as per plot and
  season.** A raw row count is therefore NOT a crop count: `crop_diversity`'s
  `'count'` basis de-duplicates to the distinct land×season occurrences the
  table carries before taking shares. Pinned by
  `test_crop_diversity_count_dedups_the_u_and_condition_rows`.
- **`gross_crop_revenue` does NOT de-duplicate, deliberately (count, never
  clip).** Measured on the warm corpus, 2026-09-09: Uganda has 895 of 45,592
  non-zero sale rows that exactly repeat an earlier row within `(t, i, crop)`
  on BOTH `Quantity_sold` and `Value_sold` — 1.9% of summed value (198.8M of
  10.71bn UGX); Malawi has 14 of 18,285 (314,100 of 1.158bn MWK). At the
  finest key Uganda carries (`t, i, plot, j, season`) only 964 of 44,618
  groups have more than one row and 926 of those carry *differing* values, so
  the pattern is genuinely repeated facts, not a household total stamped
  across rows. Two plots of one crop sold in equal quantity for equal money
  is an ordinary fact; dropping it would be the transform choosing for the
  analyst. Documented in the docstring, not silently handled.
- **The `j` labels `hdds`/`fcs` see are whatever the frame carries.** If the
  caller passed `labels='Aggregate'` they are Aggregate labels; otherwise the
  country's fine Preferred Labels (Uganda: 175). See the `labels=` contract in
  `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md` — only
  4 of 16 food countries curate an Aggregate column, so a mapping written
  against one is not portable to the other.
- **`fcs` is unusable on any shipped table today, and says so.** No country's
  `food_acquired` carries a days-consumed column: the canonical schema
  declares `Quantity`/`Expenditure`/`Price`, and `rg 'Days'` over all 40
  `data_scheme.yml` finds it only under `food_coping`. Tanzania's
  `hh_sec_j3.hh_j08` (which EPAR reads) is unwired on our side. `days=` is
  therefore required with no default and the error names the reason —
  deriving a consumption frequency from an acquisition amount would be
  inventing data.
- **No cache-hash movement — measured, not argued.** `build_transforms_fingerprint`
  for 10 tables and `Country._table_cache_hash` for 30 `(country, table)`
  cells (Uganda / Malawi / Nigeria / Ethiopia / Tanzania × six tables) are
  **byte-identical** before (`git stash push -- lsms_library/transformations.py`)
  and after this change: 42 of 42 values unmoved, **0 of them an error
  string** (`_table_cache_hash` takes `(method_name, waves)` — a first probe
  that omitted `waves` returned a TypeError for all 30 country cells and would
  have compared errors to errors, which is not a measurement; re-run with the
  real signature). Expected — none of the new functions carries a
  `@build_transform` tag and nothing on a build path calls them — but the walk
  is from every tagged root, so it is checked rather than assumed (same
  protocol as `.coder/ledger/rcsi-transform.md`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| sale revenue per household | **new** (`gross_crop_revenue`) | no prior symbol; a plain `sum(min_count=1)` over a reported column — the *valued-harvest* alternative is `median_price_valuation`, reused by reference in the docstring rather than reimplemented |
| value of livestock sold | **new** (`livestock_sales_value`), with `price=` as an explicit seam | no prior symbol; the price ladder itself is NOT rebuilt — a caller feeds `median_price_valuation`'s output in as a Series |
| kg of fertilizer / N per plot | **reuse** `nitrogen_kg` for both bases | `nutrient='product'` passes `{k: 1.0}` shares to the same function; a second unit-conversion path would be a REINVENTION of `_kg_factor_series` |
| plot ↔ plot_features grain bridge | **reuse** `yield_kg`'s `on='plot'`/`'parcel'` machinery verbatim | measured: Malawi matches 66,298 of 66,368 `(t,i,plot)` keys literally (99.9%), so `'plot'` is the right default and `'parcel'` covers Uganda's two vocabularies |
| crop-level name resolution | **new** (`_resolve_crop_level`), modelled on `_resolve_plot_level` | the `j`/`crop` divergence is documented in `data_info.yml` and had no helper |
| food-group taxonomy | **new group vocabulary only** (`HDDS_GROUPS`, `FCS_WFP_WEIGHTS`); the *mapping* is deliberately NOT shipped | LEARNINGS L8: EPAR's mapping is "a starting point to re-derive, not copy"; a default would be silently wrong for 15 of 16 food countries. Shipped instead: the group names, the standard weights, and a partition check with teeth |
| Shannon index | **new** (`crop_diversity`); `weight='area'` raises | the area basis needs a column no country carries; splitting plot area evenly across a plot's crops would MANUFACTURE the shares the index is made of (on an intercropped plot the even split is the maximum-diversity answer by construction). A `NotImplementedError` naming the missing column is the honest answer; the fix is a column, not a transform |

**Decisions recorded here rather than escalated (all three are §5, not open
questions):** (a) the `groups=` mapping is taken **group-first**
(`{group: [j, …]}`), not `{j: group}` — a `j`-keyed dict *cannot express* the
EPAR 704/1003 defect because Python dedupes the key, so the partition check
would have no teeth; (b) the mapping's key set must equal the canonical group
set **exactly**, with an empty list as the explicit way to say "this survey
fields nothing here" (rcsi's symmetric rule); (c) an unmapped `j` present in
the frame raises, but a mapped `j` *absent* from the frame does not — the
asymmetry is deliberate, because a single-wave or filtered slice legitimately
lacks most of a 175-item vocabulary, unlike rcsi's five-item battery.

## §6 Open questions for the human

- ~~**`livestock_income` is named for a construct it does not compute**~~
  **CLOSED by the red-team pass, 2026-09-09: renamed to
  `livestock_sales_value`**, which matches its output column
  `Livestock_sales_value` and is unambiguous about gross-vs-net. It returns
  EPAR's `value_lvstck_sold` term only: a GROSS SALES VALUE. Slaughtered head, purchase values, livestock products
  (milk/eggs/manure) and expenses (fodder/water/vaccines/hired labour) are all
  absent from our `livestock` schema — `FINDINGS_taxonomy.org` (LIVESTOCK
  INCOME row) files them as THEIRS-ONLY at the item layer, and GhanaSPS's
  `data_scheme.yml:406-414` records holding per-species expense/revenue
  questions that are deliberately unwired. The docstring says all of this in
  its first Notes paragraph and the output column is
  `Livestock_sales_value`, not `Livestock_income`. **If the six missing terms
  are wanted, they are a schema question (new columns), not a transform one.**
- **`fcs` cannot be run on anything we ship** until a consumption-frequency
  column is wired (Tanzania `hh_sec_j3.hh_j08` is the nearest source). Shipped
  now because the issue asks for it and because the constants + partition
  check are the reusable part; whether to wire that column is a separate call.
- Whether `crop_diversity`'s `'count'` occurrence key should be
  `(plot, season)` (chosen: the finest land×season grain the table carries) or
  distinct plots only. Chosen key is documented and pinned by test; changing
  it changes published numbers, so it is flagged rather than assumed settled.

---
### Phase 3 — verification

- `gross_crop_revenue` — OK (anchored on §2, §4): new symbol; the *other*
  construct (valued harvest) is delegated to the existing tested
  `median_price_valuation` rather than duplicated, and the no-de-dup decision
  is anchored on the measurement in §4, not on taste.
- `livestock_sales_value` — OK (anchored on §3): the two-instrument reading of
  `ValuePerAnimal`, the `SalesValue` alternative and GhanaSPS's exclusion are
  all quoted from `data_info.yml` / the countries' own `data_scheme.yml`, not
  inferred. No REINVENTION: the price ladder is a caller-supplied Series.
- `hdds` / `fcs` / `_resolve_food_groups` — OK (anchored on §3, §5): the
  partition check is built *because of* a defect verified by direct read of
  EPAR's `.do` file; no mapping is shipped, per LEARNINGS L8.
- `fertilizer_rate` — OK (anchored on §2, §5): REUSES `nitrogen_kg` for both
  numerator bases and `yield_kg`'s grain bridge; contains no new unit
  conversion and no new area handling.
- `crop_diversity` — OK (anchored on §3, §4): refuses the area basis naming
  the missing column rather than approximating it; sign convention stated
  against EPAR's un-negated `sdi`.
- `_CROP_LEVELS` / `_resolve_crop_level` — OK (anchored on §2): twin of the
  existing `_PLOT_LEVELS` / `_resolve_plot_level`, same shape, different axis.


---
## Addendum — red-team corrections (2026-09-09, after `61053864`)

Red-team report: `slurm_logs/2026-09-09_epar_curation/REDTEAM_P2_863_transforms.org`.
`development` merged in first (the harvest_kg shipped-factor family and the
#850 design landed; the red team's own conflict analysis confirmed no hunk
overlap with this block, and the merge was clean).

### A. `livestock_income` -> `livestock_sales_value` (§6 open question, closed)

Renamed; the output column already was `Livestock_sales_value`, so the call
site and the column now agree, and "income" no longer promises a net figure
the schema cannot produce. The output index is also reordered to the
canonical `(t, i, animal)`: warm Malawi arrives as `(i, t, animal)` and the
function used to preserve it, so a `.join()` or `.reindex()` against
`gross_crop_revenue` / `crop_diversity` (which build an explicit `['t','i']`
group list) would silently misalign. Pinned by an index-name assertion in the
warm test.

### B. `_NITROGEN_CONTENT` label mismatch — a REAL BUG in the shipped `nitrogen_kg`

Not introduced by this branch, and the more serious of the two findings.
`nitrogen_kg` keyed its nutrient share off `input.lower()` by **exact
equality**, and Malawi writes `Urea Fertilizer` where the map's key is
`urea`. Measured on the warm corpus:

| Malawi `input` label | rows | old fate |
|---|---|---|
| NPK Fertilizer | 26,775 | unmatched |
| Urea Fertilizer | 24,125 | unmatched |
| CAN Fertilizer | 2,037 | unmatched |
| DAP Fertilizer | 273 | unmatched |
| **Other Fertilizer** | 403 | **matched, nominal share 0.0** |

So the ONLY label that resolved carried zero nitrogen: `nitrogen_kg(Malawi)`
returned **331 plots of exactly 0.0**, and `fertilizer_rate` inherited it.
The branch's own warm test asserted `len(out) > 0`, `isfinite` and `>= 0` —
**all three pass on a column of zeros**. That is CLAUDE.md's "right shape, no
content" (`null_read_audit`), reproduced in a new transform.

*Fix, in `nitrogen_kg` (the shared machinery), not in `fertilizer_rate`:*
`_normalise_input_label` tries three candidates in order — the normalised
label, the label with a trailing `' fertilizer'`/`' fertiliser'` REMOVED, and
the label with `' fertilizer'` ADDED (the map mixes both conventions because
the countries do: bare `urea`/`npk`/`dap` beside `phosphate fertilizer`/
`other fertilizer`). Matching stays **exact after normalisation** — no
substring, no fuzzy — so `Organic Fertilizer`, `D Compound Fertilizer` and
Ethiopia's `NPS` stay unmatched rather than being assigned an invented
nominal share.

*Movement, measured over all 11 warm `plot_inputs` tables (old rule vs new,
same frames):*

| country | plots old -> new | kg N old -> new |
|---|---|---|
| **Malawi** | **331 -> 33,113** | **0 -> 835,012** |
| Benin | 1,798 -> 1,804 | 322,993 -> 322,993 |
| Burkina Faso | 2,426 -> 2,451 | 255,358 -> 255,358 |
| CotedIvoire | 1,638 -> 1,655 | 444,388 -> 444,388 |
| Niger | 1,360 -> 1,370 | 52,093 -> 52,093 |
| Senegal | 882 -> 908 | 78,412 -> 78,412 |
| Ethiopia / Mali / Tanzania / Togo | unchanged | unchanged |
| Guinea-Bissau | 0 -> 0 | 0 -> 0 |

**Malawi is the only country whose numbers move.** The five EHCVM countries
gain a handful of plot rows and **zero** kilograms: those are the bare
`Phosphate` label (349 rows corpus-wide) resolving to `phosphate fertilizer`,
whose nominal share is 0.0 — it moves them out of the unmatched tally, which
is what makes the tally readable, and moves no number. Guinea-Bissau still
returns nothing (its labels are Portuguese: `Adubos inorgânicos - ureia`,
190 rows of real urea, unmatched) and now WARNS instead of returning silence.

*Loudness*, two tiers, mirroring `harvest_kg_factors`'s `shipped_matched`:
`attrs['nitrogen_input_match']` always carries the join tally
(`matched_rows` / `unmatched_rows` / `matched_labels` / `unmatched_labels`),
and `NutrientCoverageWarning` fires when an unmatched label *names itself* a
fertilizer input (`_FERTILIZER_LABEL_HINTS`: fertilizer / fertiliser /
manure / compost / adubo / engrais), escalating to a louder message when the
whole column is empty or identically zero.

> **Deliberate deviation from "warn naming every label that got no N share".**
> Warning on *all* unmatched labels is a firehose that would bury the signal:
> `Seed` alone is 112,296 rows on Malawi and 108,713 on Ethiopia, and
> `Pesticide`/`Herbicide`/`Fungicide` are correctly outside a nitrogen map.
> CLAUDE.md's null-read section is explicit that "a warning nobody reads is
> exactly how #323 survived its first fix", and the per-column form there was
> rejected for the same 884-of-887 reason. The hint list is the corpus's own
> vocabulary and is measured: it fires on 1-3 labels per country, every one
> of them a genuine fertilizer input contributing zero. It never affects a
> nutrient share, so a stem missing from it costs a warning, never a number.
> **Separately filed** as its own issue per the red team's item 4: the
> `_NITROGEN_CONTENT` table has no entry for `NPS` (Ethiopia, 5,264 rows),
> `D Compound` (Malawi, 726) or any organic input — those are content
> decisions, not matching bugs.

### C. `fertilizer_rate` silent emptiness on Uganda

`on='plot'` was the default while the sibling `yield_kg` defaults to
`on='parcel'`, and Uganda's two plot vocabularies share no literal key:
`fertilizer_rate(Uganda)` returned `(0, 1)` in silence, `on='parcel'` returns
509 rows. **Default changed to `'parcel'`** (the disagreement between two
sibling functions was itself the trap) and a zero-row inner merge now raises
`PlotGrainMismatchWarning` naming both key vocabularies with examples.
`'parcel'` is a measured **no-op on Malawi** — its two features already share
the literal key, and both parcel helpers are documented no-ops on a
vocabulary with no `{hhid}-` prefix or `_suffix`.

### D. Two docstring statements that were false

- `gross_crop_revenue` cited Malawi's "attached to single-plot crops only" as
  a *stamping* (double-count) risk. It is the opposite — a systematic
  **lower bound**: 26.5% of single-plot `(t, i, crop)` groups carry a sale
  row against 2.7% of multi-plot ones, so the shortfall concentrates in the
  households farming the most land. Reframed, with the numbers. The Uganda
  paragraph now also carries the red team's decisive measurement (only 37 of
  963 multi-row groups at the finest key show the stamping shape) and the
  instrument citation (`s5bq07a_2`/`s5bq08_2` ride the same harvest record).
- `crop_diversity`'s Notes claimed EPAR's plot-crop row basis was "not
  reproduced here". **False**: a crop on three plots contributes three terms
  here too. Only the `u`/`condition` multiplicity *within* a land-season unit
  is de-duplicated. Sentence removed and replaced with the basis as
  implemented (with the worked 0.5004-vs-ln2 case that the existing test
  pins), plus the previously undocumented caveat that the `'count'` basis is
  **not cross-country comparable** — the occurrence key includes `season`
  where the country has that level (Uganda) and cannot where it does not
  (Malawi).

### E. Warm-gate over-strictness

`_warm` required the L2-**country** parquet, so the Uganda smoke test skipped
even though `Country('Uganda').crop_production()` served 130,606 rows from
the L2-**wave** parquets with zero cache writes. The gate now accepts either
tier. `assume_cache_fresh=True` is retained — existence is not freshness, and
a stale-hash rebuild would write to a cache other agents share.

### F. Re-verification

`build_transforms_fingerprint` x 10 tables + `Country._table_cache_hash(t,
waves)` x 5 countries x 6 tables = **42 values, 0 moved, 0 error strings**,
against the merged `development` baseline (`git stash` of this diff).
`nitrogen_kg` is an analyst transform with no `@build_transform` tag and no
in-corpus caller (`rg 'nitrogen_kg'` outside `transformations.py` finds only
prose in country docstrings), so changing it invalidates nothing.
