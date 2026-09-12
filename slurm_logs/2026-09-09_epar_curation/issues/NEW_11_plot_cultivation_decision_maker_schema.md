Title: Schema proposal: plot_cultivation, a per-season plot decision-maker table (#167's reserved name, #439's convention)
Labels: enhancement, framework

EPAR's Uganda, Tanzania and Ethiopia do-files each build a person-grain
"who decided what to farm on this plot" intermediate, then collapse it
away into a single plot-season gender categorical before the final
tables are saved. All twenty-one of the raw decision-maker variables
that intermediate is built from are completely unwired in this repo, and
the source files that carry them are already open in our own wave
scripts for other columns -- this is a column-add, not a new
acquisition, for three countries. But the repo has already reserved a
table name and a filing rule for exactly this construct
(`data_info.yml:228-232`, `plot_cultivation`), and any implementation
must use that grain, not a narrower one that omits `season`.

## What is wrong or missing

- Zero wiring today. `grep -rIni` across the whole of `lsms_library/` for
  every raw variable name below returns zero hits, each.
- The natural grain for "who farmed this plot this season" is `(t, i,
  plot, season, pid, role)`. A grain that drops `season` -- as an
  unqualified `(t, i, plot, pid, role)` proposal would -- collides
  Uganda's two seasons and Tanzania's two seasons onto one row and lets
  `_normalize_dataframe_index`'s `groupby().first()` silently discard the
  second season's decision-makers. That is the CLAUDE.md failure mode
  verbatim: "Duplicates on a declared index mean the IDENTIFIER IS BROKEN
  or a LEVEL IS MISSING."

## Evidence

**Ours -- the reservation already in the repo**
- `lsms_library/data_info.yml:228-232` -- the comment under `plot_features:`
  (`:227`): "Lasting
  plot/parcel characteristics (GH #167). Per-season detail (which crop,
  who farmed it this season, season-specific inputs, crop-share %)
  belongs in a future `plot_cultivation` table. See
  `slurm_logs/DESIGN_erhs_plot_features_2026-05-20.org` for the design
  rationale; Uganda is the Phase-1 pilot."
- `slurm_logs/DESIGN_erhs_plot_features_2026-05-20.org:54-57` -- "If
  someone later wants a per-plot, per-season `plot_cultivation` table
  (one row per `(t, v, i, plot_id, crop_seq)`, recording what was
  planted), that's a separate canonical-schema proposal -- file a new
  issue under #167's umbrella, do not retrofit `plot_features`."
- `crop_production`'s canonical grain already names `season`:
  `data_info.yml:577-578` writes it "(t, i, plot, j, u, condition, season)".
  But only two countries declare it as an index level today -- Uganda
  (`Uganda/_/data_scheme.yml`, `(t, i, plot, j, u, condition, season)`) and
  GhanaSPS. Ethiopia declares `(t, i, plot_id, j, u)` and Tanzania
  `(t, i, plot_id, j)`, so a `plot_cultivation` with a `season` level will
  NOT join those two countries' `crop_production` on season; that
  reconciliation is part of the proposal, not an assumption it can make.
- Ethiopia's `holder_id` is NOT a missing level despite first appearances:
  `lsms_library/countries/Ethiopia/_/ethiopia.py:747` and `:1000` already
  build `plot_id = format_id(holder_id)_format_id(parcel_id)_format_id(field_id)`
  (documented again at `:524`, `:849`, `:1181`), so Ethiopia's join key
  lines up as-is against `plot_features`/`crop_production`.

**Ours -- the three countries' source files already open**
- Uganda 2015-16: `lsms_library/countries/Uganda/2015-16/_/plot_labor.py:25-26`
  and `plot_inputs.py:22-23` already open `AGSEC3A.dta` / `AGSEC3B.dta` (the
  file carrying `a3aq3_3`, `a3aq3_4a`, `a3aq3_4b` and the season-B twins
  `a3bq3_3`, `a3bq3_4a`, `a3bq3_4b`); 2018-19, 2010-11, 2011-12, 2013-14
  use the same file/line shape.
- Ethiopia 2018-19: `lsms_library/countries/Ethiopia/2018-19/_/plot_inputs.py:28`,
  `plot_labor.py:21`, `plot_features.py:20` already open `sect3_pp_w4.dta`
  (the file carrying `s3q13`, `s3q15_1`, `s3q15_2`).
- Tanzania 2019-20/2020-21: `lsms_library/countries/Tanzania/2019-20/_/plot_inputs.py:21`,
  `plot_features.py:17`, `plot_labor.py:24` (and the 2020-21 equivalents
  `plot_inputs.py:17`, `plot_labor.py:21`, `plot_features.py:18`) already
  open `AG_SEC_3A.dta` / `ag_sec_3a.dta` (2020-21's file is **lowercase** --
  `2020-21/_/plot_labor.py:9-10` records the spelling already). That file
  carries `ag3a_08_1/_2/_3` in 2019-20 and the NPS W5/SDD spelling
  `ag3a_08b_1/_2/_3` in 2020-21 (plus the `ag3b_08_*`/`ag3b_08b_*` season-B
  fallbacks EPAR also reads). Tanzania 2012-13 is a new acquisition, not a
  column-add -- it lives in the `2008-15/` multi-round tree with no
  `AG_SEC_*` files at all.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/`)
- `Uganda UNPS/Uganda UNPS Wave 5/EPAR_UW_Uganda_UNPS_W5.do:413-424` --
  the saved person-grain intermediate `Uganda_NPS_W5_dm_ids.dta`, columns
  `(hhid, parcel_id, plot_id, season, id_no, pid, formal_land_rights,
  personid, female, age, hh_head)`.
- `EPAR_UW_Uganda_UNPS_W5.do:433-439` -- the collapse rule: `collapse
  (mean) female (firstnm) dm1_gender, by(hhid parcel_id plot_id season)`
  (`:433`), then thresholds (`:435-439`)
  (male-only / female-only / anything fractional is mixed / the head's
  sex where nothing matched). Uganda collapses by `(hhid, parcel_id,
  plot_id, season)` -- `season` is explicitly in the `by()` list, not
  optional.
- `Tanzania NPS/Tanzania NPS Wave 3/EPAR_UW_Tanzania_NPS_W3.do:261` (the
  saved intermediate `Tanzania_NPS_W3_plot_dm_ids.dta`) and `:263`
  (`collapse (mean) female (firstnm) dm1_gender, by(hhid plot_id season
  cultivated)`) -- Tanzania's twin, `season` likewise in the `by()` list.
- `Ethiopia ESS/Ethiopia ESS Wave 4/EPAR_UW_Ethiopia_ESS_W4.do:513-515` --
  Ethiopia's twin: `keep hhid holder_id parcel_id plot_id personid female`
  then the save. No role slot, and no `season` in the key -- the reshape at
  `:510` is `i(parcel_id plot_id hhid holder_id dummy)`. Whether an Ethiopia
  row should carry a null `season` or the level should be country-optional is
  part of what the proposal must settle.

## What a fix would touch

- A design proposal document, written against
  `DESIGN_lsms_plus_individual_disaggregation_2026-06-14.org` and
  `DESIGN_erhs_plot_features_2026-05-20.org`) for a `plot_cultivation`
  table at grain `(t, i, plot, season, pid, role)`, filed under #167's
  reserved name and #439's owner-expansion convention.
- Roles: decision-maker slots 1-3 (Uganda has two decision-maker slots
  for some sections, per `a3aq3_3`/`a3aq3_4a`/`a3aq3_4b`), with the
  head-of-household fallback documented as a **transform rule** applied
  at read time (mirroring EPAR's mean-of-female-then-threshold logic),
  never as a stored row -- the raw per-slot rows are what gets stored.
- Column-adds in the three wave scripts named above, once the schema
  proposal is accepted; then the other waves of each country.
- Reconciliation with `crop_production`'s `season` level where it exists
  (Uganda, GhanaSPS) and a stated rule for the countries where it does not
  (Ethiopia, Tanzania), plus Ethiopia's `holder_id` fold-in (already handled,
  no extra work needed there).

## What it must NOT do

- Never propose or accept a grain that omits `season` -- both Uganda and
  Tanzania collapse by season in the source pipeline; dropping it here
  reproduces the exact grain-collapse bug class CLAUDE.md documents.
- Never retrofit `plot_features` with this detail -- the design doc
  (`DESIGN_erhs_plot_features_2026-05-20.org:54-57`) already rules that
  out explicitly.
- Never store the head-of-household fallback value as if it were a
  reported row -- it is a derived/transform result and must be
  computable, not baked into the parquet.
- Never assume Ethiopia needs a `holder_id`-to-`plot_id` reconciliation
  step -- it doesn't; the existing fold-in already produces the matching
  key.

## Cross-references

Filed under #167 (plot_features, closed) and #439 (LSMS+
individual-disaggregation, open) per both issues' own filing
instructions. Companion Phase-3 items (not this issue): a `dm_gender`
transform reducing over this new table, and a separate `individual_assets
(t, i, j, pid)` feature from Ethiopia `sect11_hh_w4` / Uganda `AGSEC2A`,
`AGSEC6A`, `gsec14` -- #439's flagship, built from data we already hold. The
transforms-umbrella issue filed alongside this one carries the rest of the
Phase-3 transform list.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L4)
