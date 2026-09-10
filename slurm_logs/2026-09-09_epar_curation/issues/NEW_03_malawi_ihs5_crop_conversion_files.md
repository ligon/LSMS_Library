Title: Malawi: wire the two IHS5 crop-side conversion files; decide what _harvest_block's condition parameter is for
Labels: enhancement, data-gap, cleanup

Malawi's 2019-20 wave ships two crop-side conversion tables alongside the
food-side one, and only the food-side table is wired. Separately, all four
Malawi wave scripts pass a real source column into `_harvest_block`'s
`condition=` parameter, which discards it.

## Evidence

**Ours**
- `lsms_library/countries/Malawi/2019-20/Data/Cross_Sectional/` has
  `ihs_seasonalcropconversion_factor_2020.dta.dvc` and
  `ihs_treeconversion_factor_2020.dta.dvc` alongside
  `ihs_foodconversion_factor_2020.dta.dvc` and
  `caloric_conversionfactor.dta.dvc`. Grepping all four names across
  `lsms_library/countries/Malawi/` finds exactly one wiring: the FOOD file, at
  `Malawi/2019-20/_/food_acquired.py:17`
  (`conversions = get_dataframe('../Data/Cross_Sectional/ihs_foodconversion_factor_2020.dta', ...)`).
- `Malawi/_/CONTENTS.org:652-664` already lists both crop-side files in the
  DVC source inventory, marked "2026-09-09: held, unwired; only the food file
  is read" (`:659-662`). The inventory entry exists; the loader does not.
- `lsms_library/countries/Malawi/_/malawi.py:915-917` --
  `def _harvest_block(df, *, hhid, plotkey, cropcode, qty, unit, condition=None, ...)`.
  `grep -nw condition _/malawi.py` returns line 915 only; a substring grep adds
  `:1266` and `:2493`, both the English word "unconditionally". The parameter
  is accepted and never read anywhere in the module.
- All four wave scripts DO pass it: `2010-11/_/crop_production.py:36`,
  `2013-14/:36`, `2016-17/:46`, `2019-20/:41`, each `condition='ag_g13c'`. So
  the wave configs assert a condition exists and the module drops it on the
  floor.
- `Malawi/_/CONTENTS.org:201-207` already records this: "`ag_g13c` is passed
  to `_harvest_block` as `condition=` but the parameter is *accepted and never
  used*. A shelled/unshelled mismatch between harvest and sale is a second,
  smaller basis error on legumes and groundnut; it needs a `condition` level
  on the grain, which is a schema change and out of scope here."

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/`)
- `Malawi IHS/Nonstandard Unit Conversion Factors/README.md` -- "The IHS
  Agricultural Conversion Factor Database.dta files originate from the Malawi
  Fifth Integrated Household Survey 2019-2020 ... We borrow this survey's
  conversion factor files for the remaining Waves in the absence of having
  similar files for earlier versions of the survey." Wiring them once has
  cross-wave leverage on our side too.
- The same README's HOW TO USE section prescribes the merge key
  `region crop_code_long unit condition`, and every Malawi factor lookup in
  the wave do-file uses it: `Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:946`,
  `:1326`, `:2006` (all `merge m:1 region crop_code_long unit condition using
  ... MWI_IHS_cf.dta, keep(1 3)`). `condition` is load-bearing on EPAR's side
  and inert on ours.
- `Malawi IHS/Nonstandard Unit Conversion Factors/EPAR_UW_conversionfactors.do`
  (493 lines) is the builder for `Malawi_IHS_cf.dta`: hard-coded `input`
  blocks scraped from the IHS3 food-conversion PDF, appended to the two IHS5
  raw tables, then a fallback ladder -- shelled/unshelled ratio, regional
  mean, national mean, mean of tuber crops, cowpea borrow -- each stamped into
  a `source` column (`:399-432` for the ratio and the regional/national
  means, `:469-487` for the tuber and cowpea borrows). The ladder is
  `imputed`; only the two raw IHS5 tables are `reported`.

## What a fix would touch

- A loader for `ihs_seasonalcropconversion_factor_2020.dta` and
  `ihs_treeconversion_factor_2020.dta`, feeding a `harvest_kg` layer with its
  own fallback ladder, separate from Ethiopia's.
- `Malawi/_/CONTENTS.org:659-662` -- update the "held, unwired" note once the
  loader lands.
- `_harvest_block`'s `condition` parameter: either wire it to a `condition`
  index level (the schema change `CONTENTS.org:201-207` scopes out) or delete
  the parameter and the four `condition='ag_g13c'` call-site arguments. It
  must not keep existing as a no-op that four wave scripts feed.

## What it must NOT do

- Do NOT port EPAR's interpolation ladder (shelled/unshelled ratio, regional
  and national means, tuber and cowpea borrows) into a stored table. Those are
  `imputed` values with a `source` tag; if we want them they belong in a
  transform, layered and counted like `harvest_kg`'s.
- Do NOT port EPAR's crop allowlist for the shelled/unshelled ratio
  (`EPAR_UW_conversionfactors.do:436-444`, `shunsh_na`) -- that allowlist
  decision belongs to a later, principled layer, not to this wiring job.
- Do NOT make `condition` live by changing what `_harvest_block` returns for
  existing callers without a docstring note and a re-warm; the four waves
  already pass `ag_g13c`, so the change is visible the moment it is read.
- Never average or silently resolve a table lookup miss; report it.

## Cross-references

Related to the Malawi shelled/unshelled basis error (`CONTENTS.org:201-207`),
for which a live `condition` is a precondition, and to #824 (sold-unit
conversion, which Malawi closed 2026-09-08 with a similar fallback-ladder
shape).

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L1, N4; L9, N5)
