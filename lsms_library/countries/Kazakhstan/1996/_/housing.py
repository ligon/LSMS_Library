"""
Kazakhstan 1996 housing.

Source: ../Data/KZ96HSG_PUF.dta (the dwelling module).  The file is stored at
person level (one row per personnr within a household), but every dwelling
characteristic is constant within the household key ``rn`` (verified: a single
distinct value per rn for all extracted columns).  We therefore collapse to one
row per household with ``drop_duplicates`` on the ``rn`` index.

``rn`` is the household key used by household_roster (idxvars i: rn), so the
canonical housing index is (t, i) with i = rn.

Value labels in the .dta are truncated to 8 characters by Stata; we map those
truncated labels to full human-readable names.  Columns drawn from the canonical
housing schema:
  Walls       <- b41  "main construction material of outer walls"
  Toilet      <- b11  "kind of toilet"
  Tenure      <- b03  "Do you own or do you rent"
  Electricity <- b21_01 "electricity: have you at home?"
  Rooms       <- b12  "how many rooms?" (numeric)

The brief named b40/b41 as roof/floor and b42 as area; inspection of the
variable labels shows b40 is "elevator", b41 is the outer-wall material, b42 is
"number of apartments in the building", and b43/b44 are building-location codes.
There is no roof- or floor-material question in this module, so Roof/Floor are
not emitted.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet


def clean(x):
    if pd.isna(x):
        return pd.NA
    return x


walls_mapping = {
    'brick, s': 'Brick/Stone',
    'concrete': 'Concrete',
    'timber': 'Timber',
    'clay and': 'Clay and Wattle',
    'other': 'Other',
}

toilet_mapping = {
    'flush to': 'Flush Toilet',
    'letrine': 'Latrine',
    'open toi': 'Open Toilet',
}

tenure_mapping = {
    'own': 'Owned',
    'state, r': 'State Rented',
    'legal en': 'Legal Entity Rented',
    'coop., r': 'Cooperative Rented',
    'privat c': 'Private Cooperative',
    'privat p': 'Private Person Rented',
    'rel. or': 'Relative or Other',
}

electricity_mapping = {
    'yes': 'Yes',
    'no': 'No',
}

df = get_dataframe('../Data/KZ96HSG_PUF.dta')

cols = {
    'Walls': ('b41', walls_mapping),
    'Toilet': ('b11', toilet_mapping),
    'Tenure': ('b03', tenure_mapping),
    'Electricity': ('b21_01', electricity_mapping),
}

out = pd.DataFrame({'i': df['rn'].astype(int).astype(str)})

for name, (src, mapping) in cols.items():
    out[name] = df[src].astype(object).map(lambda x: mapping.get(x, clean(x)))

# Rooms is numeric (count of rooms), keep as float.
out['Rooms'] = pd.to_numeric(df['b12'], errors='coerce')

out['t'] = '1996'

# Housing is constant within household; collapse person-level rows to one per hh.
out = out.drop_duplicates(subset=['t', 'i']).set_index(['t', 'i']).sort_index()

# Drop rows where every characteristic is missing.
out = out.dropna(how='all')

to_parquet(df=out, fn='housing.parquet')
