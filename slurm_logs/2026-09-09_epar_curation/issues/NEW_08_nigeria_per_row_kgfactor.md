Title: Nigeria: carry the per-row crop conversion factor (sa3iq6_conv and its siblings) as KgFactor
Labels: enhancement, data-gap

Nigeria's harvest modules ship a per-row crop conversion factor, EPAR prefers
it over its own merged lookup table, and we carry no equivalent column for
Nigeria `crop_production`. `harvest_kg` already prefers a `KgFactor` column
where one exists; Nigeria supplies none.

## Evidence

**Ours**
- `lsms_library/countries/Nigeria/_/crop_production.py:1-28` -- builds
  item-level `crop_production` at grain `(t, i, plot, crop)` from
  `secta3_harvestw{1,2}.dta`, `secta3i_harvestw{3,4,5}.dta` and
  `secta3iii_harvestw{4,5}.dta`; the docstring at `:5-7` reads "Stores
  REPORTED item-level fields only -- no kg conversion, no yield, no
  main_crop, no value shares (those are transformations)". No `*_conv` column
  is read anywhere.
- `grep -rn 'sa3iq6_conv\|sa3iiq1_conv\|sa3iiiq13_conv' lsms_library/countries/Nigeria/`
  returns zero hits. `grep -c KgFactor` on both
  `Nigeria/_/crop_production.py` and `Nigeria/_/data_scheme.yml` returns 0.
- `harvest_kg`'s precedence already treats a row's own reported `KgFactor` as
  the first layer -- `transformations.py:1923-1933`: "The row's own
  ``KgFactor`` column -- what the instrument wrote down ... where it is
  finite, > 0 and PLAUSIBLE". Nigeria has nothing to feed it.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Nigeria GHS/Nigeria GHS Wave 4/`)
- `EPAR_UW_Nigeria_GHS_W4.do:982` -- `replace conv_fact = sa3iiq1_conv if
  sa3iiq1_conv!=.`, applied immediately AFTER the merge at `:980` that brings
  in EPAR's own `Nigeria_GHS_W4_cf.dta` lookup. EPAR overwrites the table
  value with the survey's per-row number wherever the per-row number exists --
  the same precedence our `reported` layer already implements.
- `W4.do:575-592` -- a coalesce that builds that central table out of the
  harvest sections' own `*_conv` columns. It is FOUR columns, not three:
  `sa3iq6_conv` (`:575`), `sa3iiq1_conv` (`:580`), `sa3iiiq13_conv` (`:585`)
  and `sa3iq6d_conv` (`:592`), each paired with its section's own unit / size
  / condition columns.
- `:595-597` -- EPAR notes it skips the usual state/zone/country median
  collapse "because ... the values are the same across geographies", which is
  what makes these per-row survey data rather than a derived geographic table.

## What a fix would touch

- Each Nigeria wave's harvest script: read that wave's `*_conv` column(s) and
  carry them into `crop_production` as an optional canonical `KgFactor`
  column, per section.
- `Nigeria/_/data_scheme.yml` -- declare `KgFactor` as an optional column,
  as Uganda does after #849 (`Uganda/_/data_scheme.yml:200-202`,
  `KgFactor: {type: float, optional: true}`).
- A Nigeria re-warm once wired: `harvest_kg` already prefers `KgFactor`, so
  `Harvest_kg` moves for every row with a non-null per-row factor with no
  other code change.

## What it must NOT do

- Never construct a `KgFactor` for a wave or section that has no `*_conv`
  column of its own -- leave those rows to the existing `survey_median` /
  `inferred` layers, which already count what they could not serve.
- Never overwrite a reported per-row factor with a table lookup; the survey's
  own number wins, which is the whole point.
- Never average the sections' `*_conv` columns together; each section
  (`secta3i` / `secta3ii` / `secta3iii`, plus W4's `sa3iq6d_conv`) is wired
  independently per its own harvest module, and a row belongs to exactly one.
- Never assume the sale-side unit matches the harvest unit when applying a
  factor to `Quantity_sold` -- that is #824's finding, and it applies here.

## Cross-references

Sibling of #849 (Uganda's per-row `KgFactor` wiring, landed 2026-09-09) and
of the Ethiopia crop-CF issue filed alongside this one (a table-sourced layer
rather than a per-row one). Relevant to #850's generalisation of
`_get_kg_factors`.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L11, N11)
