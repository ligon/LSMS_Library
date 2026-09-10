#!/usr/bin/env python
"""crop_production for Uganda UNPS 2018-19 (GAP 1 item-level build).

Reads AGSEC5A (season 1), AGSEC5B (season 2) and AGSEC4A (intercrop
flag) via get_dataframe and emits a canonical (t,i,plot,j,u,condition,season)
parquet of REPORTED harvest values.

2018-19 is the odd wave, and the two visits disagree with each other AND
with the questionnaire about which column is the unit -- so the columns are
identified by their VALUE RANGE against the `harvest_units` (46 codes) and
`harvest_conditions` (20 codes) tables, never by their labels.

AGSEC5A carries NO harvest-unit column at all (-> u='Unknown').  Both
a5aq6b and a5aq6c are titled "6c. Condition / state" and both hold the
20-code condition scheme (100% inside it, 0% matching a unit-only code);
they disagree on 158 of 7 144 rows and a5aq6b is used.  The unit question
was asked (form p.13) and was lost in the extract, so this is
`asked-not-distributed`, not `not-asked`.

AGSEC5B does carry the unit in a5bq6b, with the condition in a5bq6c.  It
is now WIRED (GH #842; it used to be `unit: None`, serving u='Unknown' on
all 7 041 rows).  The clinching evidence is the kg factor, not the label:
the median a5bq6d per a5bq6b code reproduces the weight named in that
code's own label -- Kg->1, Sack (100 kgs)->100, Sack (120 kgs)->120,
Sack (50 kgs)->50, Basin (15 lts)->15, Basket (20/10/5 kg)->20/10/5.

Both visits' q6d ("conversion factor into kg") is carried as `KgFactor`,
a survey-reported kg-per-unit RATE.  That is what keeps season A usable
despite having no unit: its kilograms are Quantity * KgFactor.

The harvest CONDITION is an index level in its own right (GH #323/#637).
See uganda.CROP_COLMAPS for the per-wave column map.

The SOLD unit and SOLD condition ARE both present in this wave, in BOTH
visits, and are carried as Unit_sold / Condition_sold (GH #824).  AGSEC5A has
no HARVEST unit but s5aq07c_1 IS a unit -- 84.8% unit-only codes, 0.0%
condition-only, the full 40-label harvest_units set, and the questionnaire
agrees -- so a farmgate price for this wave-season is reachable.  The
condition-as-unit trap that EPAR_UW_Uganda_UNPS_W7.do:520-523 falls into on
the harvest side does NOT recur here; the columns were re-derived from the
value ranges, not copied.
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import crop_production_for_wave, CROP_COLMAPS

t = '2018-19'

def _try(path):
    try:
        return get_dataframe(path, convert_categoricals=False)
    except Exception:
        return None

df5a = _try('../Data/AGSEC5A.dta')
df5b = _try('../Data/AGSEC5B.dta')
df4a = _try('../Data/AGSEC4A.dta')

df = crop_production_for_wave(t, df5a, df5b, df4a, CROP_COLMAPS[t])
assert len(df) > 0, f"crop_production produced no rows for {t}"
assert df.index.is_unique, f"Non-unique crop_production index for {t}"
to_parquet(df, 'crop_production.parquet')
