Title: Ethiopia: ET_local_area_unit_conversion.dta is held in DVC and never opened; local-unit plots get Area NaN
Labels: enhancement, data-gap

Ethiopia carries a WB local-area-unit conversion table (Timad, Kert, Boy,
Senga, Tilm, Medeb, Rope, Ermija -> hectares) in four of five waves, and
`plot_features.Area` is NaN for every plot reported only in a local unit.

## Evidence

**Ours**
- `lsms_library/countries/Ethiopia/_/CONTENTS.org:1042-1049` -- "Area and the
  local-unit gap": "Where only a farmer estimate in a non-metric local unit
  (Timad, Kert, Boy, Senga, Tilm, Medeb, Rope, Ermija) exists, we cannot
  convert -- `Area` is NaN and `AreaUnit` carries the native unit name. GPS
  area covers ~80-97% of fields per wave."
- The same file's `:1051-1062` correction paragraph (landed 2026-09-09,
  commit `ef633b12`) already records the state this issue is filed against,
  and ends "GH #TBD (2026-09-09 EPAR survey) to wire it into
  `plot_features.Area`" -- this issue is that number.
- `ET_local_area_unit_conversion.dta.dvc` sidecars exist in `2011-12/`,
  `2013-14/`, `2015-16/` and `2018-19/`; `2021-22/` has none. Those four
  sidecars carry THREE distinct md5s (2015-16 and 2018-19 share
  `f556b66e...`; 2011-12 is `ad8ca021...`, 2013-14 `1680c547...`), and all
  three blobs are already in the local DVC blob cache -- no download needed
  for any wave.
- Widened grep (`*.py`, `*.yml`, `*.org`, `*.json`, `Makefile`) across the
  whole Ethiopia tree: zero references to the file.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Ethiopia ESS/Ethiopia ESS Wave 5/`)
- `EPAR_UW_Ethiopia_ESS_W5.do:324` -- "Note that this file does not exist in
  the WB W5 download. EPAR borrowed the land unit conversion factor file from
  previous ESS waves" -- the reason our 2021-22 wave has no sidecar is a
  WB-side gap, not a repo gap.
- `:325-341` -- what EPAR does with the file before using it: pad `region`,
  `zone` and `woreda` to fixed width and concatenate them into hierarchical
  string keys, because the raw codes are not unique on their own. Any loader
  we write joins on the same three-level geography and hits the same problem.

## What a fix would touch

- A loader for `ET_local_area_unit_conversion.dta`, wired into
  `plot_features.Area` for rows where the wave reports a local unit and no GPS
  measurement exists.
- `Ethiopia/_/CONTENTS.org:1061` -- replace `GH #TBD` with this issue number,
  and record the outcome once the loader lands.
- Ethiopia re-warm and regrade of the affected `plot_features` cells.

## What it must NOT do

- Never invent a 2021-22 factor by copying an earlier wave's table into that
  wave's build without saying so. EPAR does exactly this borrow and states it
  in the do-file; if we do the same it goes in `CONTENTS.org`, not a comment.
- Never override a GPS-measured `Area` with a local-unit conversion -- GPS
  stays the preferred source per the existing precedence.
- Never resolve an implausible converted `Area` by clipping it to a
  plausibility range; count and name the rows, as `harvest_kg`'s reported-
  factor screen does.

## Cross-references

Companion to the Ethiopia crop-side conversion-table issue filed alongside
this one (`Crop_CF_Wave{2..5}`) -- both are the same class of gap: WB-shipped
factor tables sitting unread in DVC.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L1, N1)
