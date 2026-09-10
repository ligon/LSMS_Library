EPAR's Uganda conversion tables are a template with two traps in them. Paths below are relative to `reference/epar/LSMS-Agricultural-Indicators-Code/` at HEAD `abfbdc9301242527b7543606aad1ad6eed2e6530`.

**Scope.** `Uganda UNPS/Nonstandard Unit Conversion Factors/Master_Conversionfactor_table.do:271` and `:286` both read `keep if wave=="wave 3" | wave=="wave 4" | wave=="wave 5"` immediately before the regional and national SOLD-unit collapses; the harvest collapses at `:248` and `:260` carry no such filter. EPAR's sold-unit table therefore pools waves 3-5 only. Both sold collapses key on `sold_unit_code` with `condition_sold` commented out, and both harvest collapses key on `unit_code` with `condition_harv` commented out -- so the separation of sale and harvest units is real, but the wave coverage is not symmetric. A `Unit_sold` template taken from it must not be silently extended to waves 1-2.

**The condition-as-unit trap.** `EPAR_UW_Uganda_UNPS_W7.do:520-523` (2018-19):

```
gen condition_harv=a5aq6b
replace condition_harv =  a5bq6b if condition_harv==.
gen unit_code_harv=a5aq6c
replace unit_code_harv = a5bq6c if unit_code_harv==.
```

AGSEC5A in that wave has no unit column. It carries `a5aq6b` and a second column also titled "6c. Condition / state", both on the 20-code condition scheme, disagreeing on 158 of 7,144 rows (`Uganda/_/CONTENTS.org:1016` in the per-wave table at `:1004-1020`; the two-6c-columns note at `:1031-1035`). So `a5aq6c` exists, the `gen` does not error, and season A's condition code arrives as `unit_code_harv`.

Re-derived for this comment: all 20 harvest-condition codes -- `{11,12,13,14,20,21,22,23,24,31,32,33,35,40,41,42,43,44,45,99}`, `Uganda/_/categorical_mapping.org:982-1001` -- also occur as `unit_code` values in EPAR's own `UG_Conv_fact_harvest_table.dta` (2,181 rows, 89 distinct `unit_code`; 20 of 20 present). The merge at `W7.do:579`, `merge m:1 crop_code unit_code region`, will therefore match on a condition code wherever that (crop, code, region) cell exists, producing a kilogram figure instead of a missing one.

For any `Unit_sold` wiring in Uganda or Nigeria (Nigeria's per-row reported factor is #859): take the column from the survey's own value-label vocabulary. `CONTENTS.org:997-1020` records the per-wave assignment, including the one file (2013-14 AGSEC5A) where the VARIABLE labels are swapped and the value labels are not. Do not let a condition column stand in for a missing unit column because it merges cleanly.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
