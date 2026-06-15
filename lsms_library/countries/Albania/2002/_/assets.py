"""Build assets (durable goods) table for Albania 2002.

Source: durables_cl.dta — LONG, one row per (household, durable item INSTANCE).
A household owning 2 refrigerators appears as 2 rows with distinct value/age.
  j     = m3c_q02  (item label, string)
  Age   = m3c_q03  (years since acquired)
  Value = m3c_q05  (current estimated value)
No quantity column is recorded in the source.

We aggregate per (t, i, j) before writing so the framework's groupby().first()
does not silently drop 2nd+ instances of the same item in a household:
  Quantity = count of instances (the count IS the quantity owned)
  Value    = sum of instance values
  Age      = min (most recent acquisition)

CRITICAL: the household id ``i`` must be built as ``format_id(psu)-format_id(hh)``
to match Albania/2002/_/sample.py.  A naive ``i = hh`` produces ~0% overlap with
sample() (the household_roster's ``i:hh`` is a latent bug — do NOT mirror it).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

t = '2002'

df = get_dataframe('../Data/durables_cl.dta')

assets = pd.DataFrame({
    't': t,
    'i': df[['psu', 'hh']].apply(
        lambda r: format_id(r.iloc[0]) + '-' + format_id(r.iloc[1]), axis=1),
    # 41 source rows have no recorded item label; bucket them under a sentinel
    # so they are not silently dropped by groupby(dropna=True) and do not leave
    # a NaN in the j index level.
    'j': df['m3c_q02'].astype('string').fillna('Other/Unspecified'),
    'Age': df['m3c_q03'],
    'Value': df['m3c_q05'],
})

assets = assets.groupby(['t', 'i', 'j']).agg(
    Quantity=('j', 'size'),
    Value=('Value', lambda s: s.sum(min_count=1)),
    Age=('Age', 'min'),
)

to_parquet(assets, 'assets.parquet')
