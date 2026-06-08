"""
Kazakhstan 1996 household assets (durable goods).

Source: ../Data/KZ96HSG_PUF.dta, the dwelling/durables module.  Block b37 is a
WIDE durable-goods roster -- 16 items x 4 attributes, one column per
(item, attribute):

    b37_NN_1  "<item>: do you have?"        (1 = yes, 2 = no)
    b37_NN_2  "<item>: how many pieces?"    -> Quantity
    b37_NN_3  "<item>: what year did you get it?"   (not emitted)
    b37_NN_4  "<item>: how much does one piece cost?" (per-piece price)

The columns carry no usable per-item *value* label of their own (the j key is
buried in the variable label), so the wide block is unusable as-is.  The item
labels ARE recoverable from the variable labels (b37_NN_1 == "<item>: do you
have?"); we hard-code the recovered names here and melt wide -> long to the
canonical assets schema (t, i, j) with j = the item name.

Like housing.py, the file is person-level (one row per personnr) but every b37
value is constant within the household key ``rn``; we collapse to one row per
household with drop_duplicates on rn before melting.

i = rn (the household key used by household_roster idxvars i: rn).  v is NOT
baked in; it is joined from sample() at API time by _join_v_from_sample.

Quantity = pieces owned (b37_NN_2; missing among owned households is treated as
1 -- the modal answer -- since ownership is affirmed).  Value = pieces x
per-piece price (b37_NN_4), the household's total reported value for the item.
Only items the household reports owning (b37_NN_1 == 1) are emitted.
"""
import pandas as pd
import numpy as np
from lsms_library.local_tools import get_dataframe, to_parquet

# b37 item suffix -> canonical item label (j), recovered from the variable
# labels of the b37_NN_1 "<item>: do you have?" columns.
ITEM_LABELS = {
    '01': 'Refrigerator',
    '02': 'Freezer',
    '03': 'Washing machine',
    '04': 'B/W television',
    '05': 'Colour television',
    '06': 'Musical center',
    '07': 'Record-player',
    '08': 'Tape recorder',
    '09': 'Video player',
    '10': 'Computer',
    '11': 'Sewing/knitting machine',
    '12': 'Passenger car',
    '13': 'Truck',
    '14': 'Motorcycle/moped',
    '15': '(Mini-)tractor',
    '16': 'Carpets',
}

# convert_categoricals=False: keep the b37 block numeric (b37_NN_1 = 1/0 have-flag,
# counts/prices numeric).  With the default decode, b37_NN_1 comes back as
# 'Yes'/'No' -> to_numeric NaN -> `owned` never true -> empty records.
df = get_dataframe('../Data/KZ96HSG_PUF.dta', convert_categoricals=False)

# Durables are constant within household; collapse person-level rows to one per hh.
keep_cols = ['rn'] + [f'b37_{n}_{a}' for n in ITEM_LABELS for a in ('1', '2', '4')]
hh = df[keep_cols].drop_duplicates(subset=['rn']).copy()
hh['i'] = hh['rn'].astype(int).astype(str)

records = []
for suffix, label in ITEM_LABELS.items():
    have = pd.to_numeric(hh[f'b37_{suffix}_1'], errors='coerce')
    owned = have == 1
    if not owned.any():
        continue
    sub = hh.loc[owned, ['i']].copy()
    pieces = pd.to_numeric(hh.loc[owned, f'b37_{suffix}_2'], errors='coerce')
    # Ownership affirmed but piece count missing -> treat as 1 (the modal value).
    pieces = pieces.fillna(1.0)
    price = pd.to_numeric(hh.loc[owned, f'b37_{suffix}_4'], errors='coerce')
    sub['j'] = label
    sub['Quantity'] = pieces.values
    sub['Value'] = (pieces * price).values
    records.append(sub)

out = pd.concat(records, axis=0, ignore_index=True)
out['t'] = '1996'
out['j'] = out['j'].astype('string')

# Value is genuinely missing where the household didn't report a price.
out['Value'] = out['Value'].replace(0, np.nan)

out = out.set_index(['t', 'i', 'j']).sort_index()

to_parquet(df=out, fn='assets.parquet')
