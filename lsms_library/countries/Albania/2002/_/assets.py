"""Build assets (durable goods) table for Albania 2002.

Source: durables_cl.dta — LONG, one row per (household, durable item).
  j     = m3c_q02  (item label, string)
  Age   = m3c_q03  (years since acquired)
  Value = m3c_q05  (current estimated value)
No quantity column is recorded.

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
    'j': df['m3c_q02'].astype(str),
    'Age': df['m3c_q03'],
    'Value': df['m3c_q05'],
})

assets = assets.set_index(['t', 'i', 'j'])

to_parquet(assets, 'assets.parquet')
