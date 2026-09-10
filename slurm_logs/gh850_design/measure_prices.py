"""GH #850 -- median price per kilogram of the ten highest-expenditure items.

Restricted to the LATEST wave: a median pooled over all of a country's waves
mixes currencies (GhanaLSS spans the 2007 cedi redenomination) and reports
five-figure "changes" that are an artefact of the pooling.

Both sides come from the library's own arithmetic on the same frame: the
"before" factor map is ``before_factors.csv`` (what ``_get_kg_factors``
returned on the base commit, which is what ``_apply_kg_conversion`` looked
each row up in), the "after" one is ``food_kg_factors``.  The price is
``Expenditure / Quantity_kg`` either way -- ``food_prices(units='kgvalue')``.

Usage
-----
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<worktree> python measure_prices.py
"""
from __future__ import annotations

import os
import sys
import warnings

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library import transformations as T

HERE = os.path.dirname(os.path.abspath(__file__))
COUNTRIES = ['Uganda', 'Malawi', 'Nigeria', 'Ethiopia', 'Tanzania', 'Niger',
             'Mali', 'EthiopiaRHS', 'GhanaLSS']


def price_per_kg(fa, factors):
    v = T._apply_kg_conversion(fa, factors)
    with np.errstate(divide='ignore', invalid='ignore'):
        p = T._as_float(v['Expenditure']) / T._as_float(v['Quantity_kg'])
    p = np.where(np.isfinite(p) & (p > 0), p, np.nan)
    return pd.Series(p, index=fa.index)


def main():
    old_all = pd.read_csv(os.path.join(HERE, 'before_factors.csv'))
    rows = []
    for c in sys.argv[1:] or COUNTRIES:
        old = dict(zip(old_all.loc[old_all.country == c, 'u'],
                       old_all.loc[old_all.country == c, 'kg_per_unit']))
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            fa = ll.Country(c, assume_cache_fresh=True).food_acquired()
            new = T.food_kg_factors(fa)['kg_per_unit']
        p_old = price_per_kg(fa, old)
        p_new = price_per_kg(fa, new)
        t = fa.index.get_level_values('t').astype(str)
        j = fa.index.get_level_values('j')
        last = sorted(pd.unique(t))[-1]
        keep = (t == last)
        exp = fa['Expenditure'][keep].groupby(j[keep]).sum().sort_values(
            ascending=False)
        mo = p_old[keep].groupby(j[keep]).median()
        mn = p_new[keep].groupby(j[keep]).median()
        for item in exp.head(10).index:
            a, b = mo.get(item, np.nan), mn.get(item, np.nan)
            rows.append({'country': c, 't': last, 'j': item,
                         'exp_share': float(exp[item] / exp.sum()),
                         'price_kg_before': float(a), 'price_kg_after': float(b),
                         'pct_change': (float(b / a - 1) * 100
                                        if pd.notna(a) and pd.notna(b) and a
                                        else np.nan)})
        print(c, 'done', flush=True)
        pd.DataFrame(rows).to_csv(os.path.join(HERE, 'top10_prices.csv'),
                                  index=False)
    print('wrote top10_prices.csv')


if __name__ == '__main__':
    main()
