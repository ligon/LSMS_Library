#!/usr/bin/env python
"""community_prices for GhanaLSS 2016-17 (GLSS7) -- GH #562 phase 3a.

Source: ``Data/g7price.dta`` -- one row per (clust, ln, itname): the item
line ``ln`` (644 codes; ``bname`` the item name), the brand line ``itname``,
and three observations ``price{a,b,c}`` / ``quantity{a,b,c}`` /
``unit{a,b,c}`` (numeric code with the file's OWN value labels: the
``unit_9b`` list with 72=Service and 75=Visit) / ``unito{a,b,c}`` (free
text when the unit code is 99 "Other unit").

* v  = clust (70001-70998); all 398 price clusters are in the household
       cover's 1,000 EAs.  Eleven (clust, region) pairs in the price file
       carry a region the cover contradicts -- clusters 70002 and 70909 hold
       a mis-keyed sibling EA's rows.  Kept native (no cluster id is
       fabricated), told apart by obs / Description; named in CONTENTS.org.
* j  = ln decoded through harmonize_price_item (every food code placed
       explicitly on the 2016-17 harmonize_food axis; non-foods own labels).
* u  = the observation's unit code -> the file's value label -> the shared
       unit_labels.org Preferred Label; code 99 -> the other-unit text
       mapped the same way (an unmapped spelling stays visible, title-cased).
* NumberOfUnits = quantity{a,b,c}.
* Description   = 'bname | itname' (the brand line), plus the other-unit
       text where the code was 99.
"""
import re
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from glss_prices import (assemble, canon_unit, melt_observations,
                         price_item_table, unit_label_map, v_from_clust)

WAVE = '2016-17'
SRC = '../Data/g7price.dta'


def build():
    raw = get_dataframe(SRC, convert_categoricals=False)
    labels = get_dataframe(SRC, convert_categoricals=True, categories_only=True)
    ulab = {int(k): str(v).strip() for k, v in labels['unita'].items()}
    umap = unit_label_map()
    items = price_item_table(WAVE)

    code = pd.to_numeric(raw['ln'], errors='coerce').astype('Int64')
    known = code.isin(items.index)
    # ln == 0 is a single blank line in the file; nothing else may be unknown.
    assert (known | (code == 0)).all(), \
        f'{WAVE}: unknown ln {sorted(code[~known & (code != 0)].dropna().unique())}'

    brand = raw['itname'].astype('string').str.strip().fillna('')
    rec = pd.DataFrame({
        'v': v_from_clust(raw['clust']),
        'j': code.map(items['Preferred Label']),
        'label': code.map(items['Label']).astype('string'),
        'brand': brand,
    })
    rec['Description'] = (rec['label'] + ' | ' + rec['brand']).str.rstrip(' |')

    def unit_for(ucol, ocol):
        uc = pd.to_numeric(raw[ucol], errors='coerce')
        other = raw[ocol].astype('string').str.strip().fillna('')
        out = []
        for c, o in zip(uc, other):
            if pd.isna(c):
                out.append(pd.NA)
            elif int(c) == 99 and o:
                out.append(canon_unit(o, umap))
            else:
                out.append(canon_unit(ulab.get(int(c)), umap))
        return pd.Series(out, index=raw.index, dtype='string'), other

    slots = []
    for k, s in enumerate('abc', start=1):
        u, other = unit_for(f'unit{s}', f'unito{s}')
        rec[f'u{k}'] = u
        rec[f'Price{k}'] = pd.to_numeric(raw[f'price{s}'], errors='coerce')
        rec[f'Quan{k}'] = pd.to_numeric(raw[f'quantity{s}'], errors='coerce')
        rec[f'Desc{k}'] = rec['Description'].where(other == '', rec['Description'] + ' | ' + other)
        slots.append((k, {'Price': f'Price{k}', 'NumberOfUnits': f'Quan{k}', 'u': f'u{k}'}))

    long = melt_observations(rec, slots=slots,
                             base_cols={'v': 'v', 'j': 'j', 'brand': 'brand'})
    # per-slot Description (the other-unit text differs by slot)
    long['Description'] = pd.concat(
        [rec[f'Desc{k}'] for k, _ in slots], ignore_index=True).astype('string')
    long['brand_key'] = long['brand'].fillna('')
    return assemble(WAVE, long, sort_keys=['brand_key'])


if __name__ == '__main__':
    df = build()
    to_parquet(df, 'community_prices.parquet')
