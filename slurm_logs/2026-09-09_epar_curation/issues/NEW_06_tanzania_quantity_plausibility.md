Title: Tanzania: report-only plausibility count on crop_production.Quantity (no screen exists today)
Labels: enhancement, diagnostics

Tanzania's `crop_production_for_wave` applies no plausibility screen to raw
harvest `Quantity`. 2020-21 alone carries quantities up to 7,500,000 kg with
nothing flagging them, while the corpus already has a working pattern for this
shape of problem (`harvest_kg`'s reported-factor screen: count and name the
rows, never clip).

## Evidence

**Ours**
- `lsms_library/countries/Tanzania/_/tanzania.py:1163` --
  `def crop_production_for_wave(t, seas, fruit, peren, sales, colmaps)`. The
  function body contains no clip, clamp or winsorize call on `Quantity`.
- Measured against the warm parquet
  `~/.local/share/lsms_library/Tanzania/2020-21/_/crop_production.parquet`:
  11,125 rows; `Quantity` max 7,500,000 with `u='kg'` (Coconuts,
  `i=9640-001-99`, plot 1); mean 2,218.41; std 85,732.98. Next three:
  Timber 4,000,000 (`i=2010-001-01`, plot 6), Other Fruits 2,400,000
  (same household as the coconuts), Ripe Bananas 1,505,400.
- By contrast, `tanzania.py:1046-1051` already screens plot `Area` in
  `plot_features_for_wave`: "Plausibility clamp: parcels > 2500 acres
  (~1000 ha) are data-entry errors for smallholder plots ... drop to NaN so
  AreaUnit follows rather than poisoning area-weighted aggregates downstream
  (GH #167)". The pattern of screening a raw column exists in this exact
  file, just not for `Quantity` -- and the `Area` screen NULLS the value,
  where the newer `harvest_kg` screen counts it instead.
- `_screen_reported_factors` (`transformations.py:1714`, thresholds
  `KG_FACTOR_MAX` at `:1690`) is the report-only shape to reuse: reject,
  count under `reported_implausible`, never clip or rescale
  (`:1926-1931`).

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Tanzania NPS/Tanzania NPS Wave 5/`)
- `EPAR_UW_Tanzania_NPS_W5.do:578-579` -- EPAR's remedy for the same class of
  problem is a hand-coded single-household deletion: `replace kg_harvest = .
  if strmatch(hhid, "0015-001-001") & plot_id==4 //1 very strange outlier,
  800000 kg cabbage`, with `value_harvest` nulled the same way at `:579`.
  That household id does not appear in our 2020-21 `crop_production` parquet
  at all (2,708 distinct `i` values checked), so the fix is
  wave/household-specific and does not generalise.
- `EPAR_UW_Tanzania_NPS_W5.do:254-255` -- the same file's two competing
  readings of implausible GPS areas. The UNIT-error reading is the commented-
  out line `:254` ("there's a discontinuity between 85.52 and 10432
  indicating that several areas are being reported in units other than
  acres"); the rule EPAR actually applies is `:255`, a decimal rescale
  (`replace area_acres_meas = area_acres_meas/10000 if area_acres_meas>=1000
  //Based on comparing farmer reported area and the actual measurement this is
  assumed to be in decimal acres`). Both readings are candidates when our
  `Area` clamp is next revisited; neither is settled.

## What a fix would touch

- A plausibility count over `crop_production.Quantity`, same shape as
  `harvest_kg`'s factor screen: count the rows, name them (household, plot,
  crop, unit, wave), never clip. Decide whether it lives in
  `transformations.py` as a query-time diagnostic or as a
  `null_read_audit.py`-style warning -- read that module first, since it is
  the corpus's existing pattern for a report-only content check with its own
  strictness lever.
- A threshold informed by the corpus's own per-crop or per-unit distribution
  rather than a single hard-coded cutoff.

## What it must NOT do

- Never clip or delete the flagged rows -- report only, matching "core does
  not aggregate" and the `harvest_kg` precedent.
- Do NOT hand-code a household-specific exclusion list the way
  `W5.do:578-579` does -- it does not generalise across waves or countries,
  and the corpus avoids per-cell allowlists (CLAUDE.md's "no allowlist of
  known-bad cells" for grain collapse is the same anti-pattern).
- Do NOT reuse the `Area` clamp's 2500-acre threshold for `Quantity`; it is a
  different column in a different unit, and the two screens should not share a
  number by accident.

## Cross-references

Same shape as the `harvest_kg` reported-factor screen (landed 2026-09-09,
commits `0d3a4241` / `cf1e0334`); read that implementation before building
this one.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L10, N9)
