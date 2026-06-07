"""Build shocks table for Albania 2012.

Source: ``Modul_6D_Shocks to the household.sav`` — LONG form, one row per
(household, shock-type).  Columns:
  psu, hh         : household identifiers
  M6D_Q00         : shock-type label (Albanian) -> Shock index
  M6D_Q01         : did the household EVER experience this shock 2008-2012? (Yes/No)
  M6D_2008..2012  : did it occur in that specific year? (Yes/No, only filled when Q01=Yes)

This module is OCCURRENCE-ONLY: it records *whether* and *in which year(s)* each
shock happened, but carries NO impact (income/assets/production/consumption)
nor coping-strategy detail.  None of the canonical Affected* / HowCoped*
columns exist in the source, so they are not emitted.  The honest payload is
``Years`` — a comma-separated list of the years in which the shock was reported.

We keep only rows where the shock actually occurred (M6D_Q01 == 'Yes'), so the
table is a genuine shocks roster ("did the household experience <shock>") rather
than the full 9-shock-types x 6671-households product.  Filtering also removes
the 15 duplicate (psu, hh, 'Tjeter'/Other) rows present in the raw product
(each duplicate is an occurred 'Other' shock paired with a not-occurred one).

CRITICAL: the household id ``i`` must be built as ``format_id(psu*100 + hh)`` to
match the household identity in Albania/2012/_/sample.py (where i = format_id(hhid)
and hhid == psu*100 + hh).  This lets _join_v_from_sample() resolve ``v``.  This
mirrors the composite-i idiom used by housing / individual_education in this wave.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

t = '2012'

# English labels for the 9 Albanian shock-type strings (truncated in the source).
SHOCK_LABELS = {
    'Semundje serioze': 'Serious illness',
    'Humbje(Shperdorim) parash/kursimesh': 'Loss/embezzlement of money or savings',
    'Burgosje e nje anetari te familjes qe si': 'Imprisonment of a family member',
    'Humbje pune': 'Job loss',
    'Shpronesim toke': 'Land expropriation',
    'Shtepia u shkaterrua/dogj': 'Home destroyed/burned',
    'Vdekje e papritur e nje anetari te famij': 'Sudden death of a family member',
    'Deme nga permbytje': 'Flood damage',
    'Tjeter': 'Other',
}

YEARS = [2008, 2009, 2010, 2011, 2012]

df = get_dataframe('../Data/Modul_6D_Shocks to the household.sav')

# Normalise the yes/no fields to plain strings (they arrive as categoricals).
df['M6D_Q01'] = df['M6D_Q01'].astype(str)
for y in YEARS:
    df[f'M6D_{y}'] = df[f'M6D_{y}'].astype(str)

# Keep only shocks that actually occurred.
occurred = df[df['M6D_Q01'] == 'Yes'].copy()

# Comma-joined list of years in which each shock was reported.
def years_string(row):
    yrs = [str(y) for y in YEARS if row[f'M6D_{y}'] == 'Yes']
    return ','.join(yrs) if yrs else pd.NA

shocks = pd.DataFrame({
    't': t,
    'i': occurred[['psu', 'hh']].apply(
        lambda r: format_id(int(r.iloc[0]) * 100 + int(r.iloc[1])), axis=1),
    'Shock': occurred['M6D_Q00'].astype(str).map(
        lambda x: SHOCK_LABELS.get(x, x)),
    'Years': occurred.apply(years_string, axis=1).astype('string'),
})

shocks = shocks.set_index(['t', 'i', 'Shock'])

to_parquet(shocks, 'shocks.parquet')
