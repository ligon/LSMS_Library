Title: Uganda: strip the 2009-10 99999 harvest-quantity sentinel
Labels: bug, cleanup

Uganda 2009-10's harvest `Quantity` uses `99999` as a missing-value sentinel
that is never stripped, inflating the wave's total by roughly 300x every other
wave. This is already an open TODO in `CONTENTS.org` with no code fix; EPAR
names the exact column and value and shows two removal rules.

## Evidence

**Ours**
- `lsms_library/countries/Uganda/_/CONTENTS.org:2105-2109` -- "TODO 2009-10
  harvest quantities carry an unstripped 99999 sentinel. 2009-10's `Quantity`
  sums to 3.18e8 native units, ~300x every other wave, because `99999` is used
  as a missing-value sentinel and never removed. Any analysis touching 2009-10
  `crop_production` quantities must filter it."
- `grep -rn '99999' lsms_library/countries/Uganda/` returns hits in exactly
  three files: that `CONTENTS.org` TODO, and float artifacts in
  `Uganda/_/fct_uganda.csv` (43 lines) and `Uganda/_/fct_usda.csv` (7 lines),
  all of the form `47.199999999999996` and unrelated. No sentinel-handling
  code exists in `uganda.py`, any wave `_/*.py` script, or any
  `data_info.yml`. The TODO is unfixed.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Uganda UNPS/Uganda UNPS Wave 1/`; EPAR's
UNPS Wave 1 is the 2009-10 round, per `EPAR_UW_Uganda_UNPS_W1.do:4`)
- `EPAR_UW_Uganda_UNPS_W1.do:483` -- `recode A5aq6a (99999=.a) if A5aq11==.`,
  with the inline count "1410 quantities (99999) changed to missing since the
  entire row doesn't have any information". EPAR's rule is conditional: the
  recode fires only where `A5aq11` is also empty, which is narrower than
  "strip every 99999".
- `EPAR_UW_Uganda_UNPS_W1.do:558` -- `replace quantity_harv=. if
  quantity_harv==99999`, an unconditional rule applied later in the same
  pipeline to the derived `quantity_harv` (built at `:556-557` by coalescing
  the season-A column `A5aq6a` with the season-B column `A5bq6a`). Between them the two
  rules remove every 99999 from `quantity_harv`, the first only on the subset
  where `A5aq11` is also empty.

## What a fix would touch

- A wave-level rule for Uganda 2009-10's harvest-quantity column (`A5aq6a`
  and its season-B twin `A5bq6a`, or their canonical equivalents), mapping
  `99999` to missing.
- A count of how many rows the strip removes per season, recorded in
  `CONTENTS.org` alongside the corrected `Quantity` total, so the ~300x claim
  can be checked after the fix.
- A test asserting the strip is exact-value, not a range.
- `CONTENTS.org:2105-2109` -- close the TODO with the measured before/after.
- A Uganda re-warm.

## What it must NOT do

- Never strip a large quantity that is merely near 99999 (99998, 100000) --
  target the literal sentinel value, not a range.
- Never zero out or impute the stripped rows; they become missing, as both of
  EPAR's rules do.
- Do NOT adopt EPAR's `A5aq11`-conditional form alone (`:483`) and stop there.
  EPAR itself follows it with the unconditional strip at `:558`; picking only
  the narrow rule would leave sentinel rows in a wave whose total is already
  known to be wrong by 300x.

## Cross-references

Independent of the recent Uganda `KgFactor` work (#849, #848), which touches
different columns; this is a 2009-10-only defect.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (N12)
