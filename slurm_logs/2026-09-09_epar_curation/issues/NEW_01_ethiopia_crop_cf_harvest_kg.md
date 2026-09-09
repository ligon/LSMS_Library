Title: Ethiopia: wire Crop_CF_Wave{2..5} (WB crop x unit x region conversion factors) into harvest_kg
Labels: enhancement, data-gap

Four Ethiopia ESS waves ship a World Bank crop-by-unit-by-region conversion
table that no Ethiopia script reads, and `harvest_kg` has no layer for it.
`Ethiopia/_/data_scheme.yml:120-121` states outright "NO unit->kg conversion"
for `crop_production`, and that statement is currently true: the files sit in
DVC, unread.

## Evidence

**Ours**
- `lsms_library/countries/Ethiopia/_/data_scheme.yml:113-134` -- the
  `crop_production` block; the comment at `:120-121` reads "NO unit->kg
  conversion, NO yield / main-crop / value-share rollups (those are
  transformations)."
- Zero references anywhere in the Ethiopia tree: `grep -rni crop_cf` across
  `*.py`, `*.yml`, `*.org`, `*.json` and `Makefile` returns nothing.
- Four sidecars, three distinct blobs, all three already in the local DVC
  blob cache -- no acquisition needed:

  | wave | sidecar | md5 |
  |---|---|---|
  | 2013-14 | `Ethiopia/2013-14/Data/Crop_CF_Wave2.dta.dvc` | `dbfd5dc8...` |
  | 2015-16 | `2015-16/Data/Crop_CF_Wave3.dta.dvc` | `bb4042a8...` |
  | 2018-19 | `2018-19/Data/Crop_CF_Wave4.dta.dvc` | `299c2700...` |
  | 2021-22 | `2021-22/Data/crop_cf_wave5.dta.dvc` | `299c2700...` |

  2018-19 and 2021-22 carry the SAME md5: our W4 and W5 crop-CF files are one
  byte-identical blob under two names. A loader must not treat them as two
  independent tables. There is no sidecar for 2011-12 (W1).
- `harvest_kg`'s layer stack and precedence (`reported` -> `survey_median` ->
  `inferred` -> `none`) is documented at
  `lsms_library/transformations.py:1920-1948`; `KG_FACTOR_LAYERS` at `:1822`
  is the same list in code, and `KgFactorSource` records which layer served
  each row.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/`)
- `Ethiopia ESS/Ethiopia ESS Wave 5/EPAR_UW_Ethiopia_ESS_W5.do:762-804` --
  EPAR's crop-CF processing: reshape to per-region rows, synthesise unit 52
  from units 51/53, `fillin` the crop x unit x region grid, then fill the
  holes from a per-(unit, region) mean across crops (`:789`, its own comment:
  "this isn't perfect but it's a rough estimate of the capacity of the unit
  when the crop code is unknown"). Everything after `:794` is `imputed`, not
  `reported` -- a loader that reads the raw table gets only the reported part,
  which is what we want.
- The duplicate row in that table (crop_code 74 "ENSET ESIR MEDIUM", unit 62,
  factors 4.34 and 6.125) is resolved in LIVE code at `W5.do:800-802`
  (`duplicates tag region crop_code unit`, then `drop if crop_code==74 &
  unit==62 & conversion>5 //8 observations deleted`), documented at `:801`,
  and identically at `Ethiopia ESS Wave 4/EPAR_UW_Ethiopia_ESS_W4.do:744` --
  consistent with the two waves sharing one blob.
  A second, EARLIER block at `W5.do:345-353` states the same resolution in
  prose (`:349`, "Based on W3 data, we have chosen to retain cf=4.34") but is
  commented out (`/*` at `:345`, `*/` at `:353`) and its output
  `Crop_CF_Wave5_adj.dta` is read nowhere in the file. The de-dup that runs is
  the one at `:802`.

## What a fix would touch

- A new Ethiopia-specific loader for `Crop_CF_Wave{2,3,4}.dta` /
  `crop_cf_wave5.dta` (crop x unit x region), feeding a new `harvest_kg`
  layer ("WB-shipped factor").
- A decision on that layer's rank relative to `reported` and `survey_median`
  (proposal: after `reported`, before `survey_median`, since it is externally
  sourced rather than survey-internal) -- record the decision in the prior-art
  ledger and the `harvest_kg` docstring.
- `Ethiopia/_/data_scheme.yml:120-121`'s comment, once the gap it names is
  closed.

## What it must NOT do

- Never average the duplicate row (crop 74 / unit 62, 4.34 vs 6.125) --
  de-duplicate it with a stated rule rather than silently averaging or letting
  a `groupby().first()` pick one at random.
- Never load the derived rows EPAR builds after `fillin` (`W5.do:794-797`) as
  if they were shipped factors; read the WB table, not EPAR's product.
- Never fabricate a factor for 2011-12 (W1), which has no `Crop_CF` sidecar at
  all -- that wave stays on the existing layers.
- Never clip or override a row's own `reported` `KgFactor` with this layer.

## Cross-references

Sibling of #849 (Uganda's per-row `KgFactor`) and #850 (generalising
`_get_kg_factors` / `conversion_to_kgs` beyond the food side) -- this is the
crop-side, externally-sourced-table analogue. Also touches #820 (Ethiopia
section 11 / section 12 harvest wiring), since both live in `harvest_kg`'s
input path. Companion to the Ethiopia local-area-unit issue filed alongside
this one (the same class of gap: WB-shipped factor tables held in DVC and
never opened).

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L1, N2)
