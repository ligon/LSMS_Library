Title: Nigeria: no factors for heaps/ridges/stands areas; EPAR's two grids disagree by a median of 4.9x
Labels: enhancement, data-gap

Nigeria plots reported in non-standard local area units (heaps, ridges,
stands) cannot be converted to hectares in our pipeline; `Area` is NaN for
those rows and `AreaUnit` carries the native label. EPAR carries zone-specific
factors for exactly this, from the Wave 2 Basic Information Document -- and
records, in the same comment block, a second set observed from the data that
disagrees with the BID set by up to 72x. It applies the BID set.

## Evidence

**Ours**
- `lsms_library/countries/Nigeria/_/CONTENTS.org:738-750` -- "Area
  conversion": "`Area` is in hectares. Preference: GPS measurement
  (m^2 / 10000). Where GPS is missing, fall back to the farmer estimate ONLY
  for convertible units: acres (x0.404686), hectares (x1), square metres
  (/10000). The NON-STANDARD local units (heaps, ridges, stands, plots, and
  W5's square-foot 8/9 and football-field 10) have no in-repo conversion
  factor -- for those rows `Area` is taken from GPS, and where GPS is missing
  `Area` is NaN while `AreaUnit` carries the native label." Net: ~86.6% of
  rows have a non-null Area; convertible-unit share is W1 29% / W2 28% /
  W3 25% / W4 56% / W5 49%.
- `Nigeria/_/crop_production.py` and `Nigeria/_/data_scheme.yml` carry no
  area-factor table of any kind for these units.
- Note one unit we treat as non-standard that EPAR converts with an
  all-zone factor: "plots" (`W4.do:386`, x0.0667).

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Nigeria GHS/`)
- `Nigeria GHS Wave 4/EPAR_UW_Nigeria_GHS_W4.do:355-372` -- the source and
  the BID table: "using conversion factors from LSMS-ISA Nigeria Wave 2 Basic
  Information Document (Wave 3 unavailable, but Waves 1 & 2 are identical)",
  then a 6-zone x {heaps, ridges, stands} grid of hectare factors.
- `:374-382` -- a second grid, labelled "ALT observed from the data", same
  shape, different numbers.
- `:385-388` -- the standard-unit conversions actually applied (hectares,
  plots x0.0667, acres x0.404686, square metres x0.0001);
  `:390-409` -- the zone-specific ones actually applied: heaps `:390-395`,
  ridges `:397-402`, stands `:404-409`. The applied numbers are the BID grid;
  the observed grid is never applied and EPAR does not comment on the gap
  between them.
- The gap, computed cell by cell (observed / BID over all 18 cells): minimum
  0.16x, maximum 71.6x, median 4.9x. A single cell -- heaps, zone 1, 0.00281
  against 0.00012 -- is 23.4x; that one number is not the disagreement.
  Heaps disagree worst (0.16x to 71.6x),
  ridges mostly least (0.35x to 2.2x, except zone 6 at 50x), stands 3.8x to 69x.
- `Nigeria GHS Wave 1/EPAR_UW_Nigeria_GHS_W1.do:309-357` -- the W1 twin,
  structurally consistent and using the identical BID numbers, but with NO
  "observed" grid: the alternative set appears only in the later wave file.

## What a fix would touch

- Nothing yet, pending a decision. Given that EPAR's own file carries two sets
  differing by a median factor of 4.9, this may be a "do not wire" outcome
  rather than a wiring job. If a fix proceeds it would be a zone x unit factor
  table for Nigeria's `Area` / `AreaUnit`, filling only the currently-NaN
  non-standard-unit rows (GPS and convertible-unit rows unaffected).
- The "plots" unit is a separate, smaller question: EPAR converts it with a
  single all-zone factor and we do not convert it at all. That one has no
  competing factor set and could be decided on its own.
- `Nigeria/_/CONTENTS.org:738-750` -- update once a decision is made, either
  "evaluated and rejected because the two available factor sets disagree by a
  median of 4.9x" or "wired, using factor set X, with the disagreement
  recorded".

## What it must NOT do

- Never adopt either grid without stating the disagreement in `CONTENTS.org`.
  Picking a side silently would hide the whole reason this is a hard call.
- Never override a GPS-measured or convertible-unit `Area` with a
  heaps/ridges/stands factor -- those rows are already handled and stay on the
  existing precedence.
- Never clip the resulting `Area` to a plausibility range as a substitute for
  deciding which factor set to trust; a wide spread is the signal, not noise
  to be trimmed.

## Cross-references

Same Nigeria crop-production pass as the per-row `KgFactor` issue filed
alongside this one. Related to #841 (Nigeria W5 GPS coordinates unavailable),
which is why the farmer-estimate fallback chain matters more in that wave.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L11)
