# Tanzania Panel Design

Tanzania's NPS has an unusual panel structure starting from 2014-15.
This knowledge is essential for interpreting panel attrition results
and debugging ID linkage.

## Sub-panel split (2014-15 onward)

In Round 4 (2014-15), the sample split into two **disjoint
sub-panels**:

| Sub-panel | Tracked in | Origin |
|-----------|-----------|--------|
| Extended Panel | 2019-20 (NPS-SDD) | ~20% of original 2008-13 cohort |
| Refresh Panel | 2020-21 (NPS Y5) | New sample drawn in 2014-15 (3,352 HH) |

**2019-20 and 2020-21 have zero household overlap.** This is correct
by design, not a data error.  `panel_attrition()` will show 0 in the
2019-20 x 2020-21 cell.

## ID formats by wave

| Wave | ID variable | Format | Example |
|------|------------|--------|---------|
| 2008-09 (R1) | r_hhid | 14-digit | `01010140020171` |
| 2010-11 (R2) | r_hhid | 16-digit (R1 + 2-digit suffix) | `0101014002017101` |
| 2012-13 (R3) | r_hhid | NNNN-NNN | `0001-001` |
| 2014-15 (R4) | r_hhid | NNNN-NNN | `0001-001` (extended), `1000-001` (refresh) |
| 2019-20 | sdd_hhid | NNNN-NNN-NNN | `0001-001-001` |
| 2020-21 | y5_hhid | NNNN-NNN-NN | `1000-001-01` |

## Split-off detection

Within the 2008-15 multi-round data, household splits are identified
by the r_hhid suffix:

- **Round 2** (16-digit): suffix `01` = primary, `02`+ = split-off
- **Rounds 3-4** (NNNN-NNN): suffix `001` = primary, others = split-off

`map_08_15()` in `tanzania.py` only links primary households backward;
split-offs start as new households.

## ID chain through updated_ids

- Extended panel: `sdd_hhid` -> `y4_hhid` (0001-001 format) ->
  `r_hhid` (2014-15) -> chained to base UPHI-derived ID
- Refresh panel: `y5_hhid` -> `y4_hhid` (1000-001 format) ->
  `r_hhid` in 2014-15 (first appearance, no further chaining)

The `y4_hhid` in 2020-21 uses the refresh panel's numbering
(1000-001+), which matches `r_hhid` values that first appear in
Round 4.

## Residual duplicate indices

After fixing split-off handling, ~216 duplicate `(i, t, j)` indices
remain (out of 250k rows).  These are cases where distinct UPHIs
share the same Round 1 `r_hhid` --- a deeper ambiguity in the UPD4
data documented in `Tanzania/_/CONTENTS.org`.

## Crosswalk files

- `npssdd.panel.key.dta` (2019-20) --- individual-level linkage
- `npsy5.panel.key.dta` (2020-21) --- individual-level linkage
- Both are DVC-tracked in the respective `Data/` directories

## Verification

```python
from lsms_library.local_tools import panel_attrition
from lsms_library.diagnostics import check_panel_consistency

tz = ll.Country('Tanzania')
check_panel_consistency(tz)           # should pass
panel_attrition(tz.food_expenditures(), tz.waves)
```

Source: NPS 2020/21 Basic Information Document, pp. 7-8.
