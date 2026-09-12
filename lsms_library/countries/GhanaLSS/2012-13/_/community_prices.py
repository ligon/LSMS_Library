#!/usr/bin/env python
"""community_prices for GhanaLSS 2012-13 (GLSS6) -- GH #562 phase 3a.

Sources (``Data/PRICES/``):
  * ``price_sec1.dta`` -- foods: one row per (clust, fcode) with three
    observations ``s1stkg``/``s1stpx``, ``s1ndkg``/``s1ndpx``,
    ``s1rdkg``/``s1rdpx`` (the form's KG/LITRE + PRICE).
  * ``price_sec2.dta`` -- non-foods: one row per (clust, nfcode) with the
    reader's free-text basis ``s2desc`` and ``s2stpx``/``s2ndpx``/``s2rdpx``.
  * ``price_sec0.dta`` -- the cover (region, district, clust, market number,
    locality name, date); not needed for the table.

* v  = clust (60001-61200), the EA the form is keyed on; all 1,015 price
       EAs are household clusters (84.6% of the 1,200).  The survey stamps
       prices per EA but collected them at 41 market numbers (323
       region/district/market keys); EAs sharing a market carry identical
       prices -- kept native.
* j  = fcode / nfcode decoded through harmonize_price_item, which carries the
       .dta value labels (NOT the printed form's numbering).
* u  = Kilogram or Liter per food item (the KG/LITRE column; liquids in
       litres -- a per-item judgement recorded in the table); 'Other Unit'
       for non-foods, whose basis is the free text in Description.
* NumberOfUnits = the KG/LITRE figure for foods; NaN for non-foods (the
       basis is text).
* Description = the item label ('label | s2desc' for non-foods).
"""
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from glss_prices import (assemble, melt_observations, price_item_table,
                         v_from_clust)

WAVE = '2012-13'


def _decode(raw, code_col, items):
    code = pd.to_numeric(raw[code_col], errors='coerce').astype('Int64')
    known = code.isin(items.index)
    assert known.all(), f'{WAVE}: unknown {code_col} {sorted(code[~known].dropna().unique())}'
    return code


def build():
    items = price_item_table(WAVE)

    s1 = get_dataframe('../Data/PRICES/price_sec1.dta', convert_categoricals=False)
    c1 = _decode(s1, 'fcode', items)
    food = pd.DataFrame({
        'v': v_from_clust(s1['clust']),
        'j': c1.map(items['Preferred Label']),
        'u': c1.map(items['Unit']),
        'Description': c1.map(items['Label']),
    })
    for k, (kg, px) in {1: ('s1stkg', 's1stpx'), 2: ('s1ndkg', 's1ndpx'),
                        3: ('s1rdkg', 's1rdpx')}.items():
        food[f'Price{k}'] = pd.to_numeric(s1[px], errors='coerce')
        food[f'Quan{k}'] = pd.to_numeric(s1[kg], errors='coerce')
    food_long = melt_observations(
        food, slots=[(k, {'Price': f'Price{k}', 'NumberOfUnits': f'Quan{k}'}) for k in (1, 2, 3)],
        base_cols={'v': 'v', 'j': 'j', 'u': 'u', 'Description': 'Description'})

    s2 = get_dataframe('../Data/PRICES/price_sec2.dta', convert_categoricals=False)
    c2 = _decode(s2, 'nfcode', items)
    desc = s2['s2desc'].astype('string').str.strip().fillna('')
    nonfood = pd.DataFrame({
        'v': v_from_clust(s2['clust']),
        'j': c2.map(items['Preferred Label']),
        'u': c2.map(items['Unit']),
        'Description': (c2.map(items['Label']).astype('string') + ' | ' + desc).str.rstrip(' |'),
    })
    for k, px in {1: 's2stpx', 2: 's2ndpx', 3: 's2rdpx'}.items():
        nonfood[f'Price{k}'] = pd.to_numeric(s2[px], errors='coerce')
    nonfood_long = melt_observations(
        nonfood, slots=[(k, {'Price': f'Price{k}'}) for k in (1, 2, 3)],
        base_cols={'v': 'v', 'j': 'j', 'u': 'u', 'Description': 'Description'})

    rows = pd.concat([food_long, nonfood_long], ignore_index=True)
    rows['Description'] = rows['Description'].astype('string')
    # Description (free text) orders same-(v, j, u) records deterministically
    # before the slot; it is dropped from the output.
    rows['desc_key'] = rows['Description'].fillna('')
    return assemble(WAVE, rows, sort_keys=['desc_key'])


if __name__ == '__main__':
    df = build()
    to_parquet(df, 'community_prices.parquet')
