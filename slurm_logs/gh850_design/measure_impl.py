"""GH #850 -- measure the SHIPPED code, before and after the fix.

``measure.py`` prototyped the correction in memory.  This script measures the
library itself: it calls the public transforms (``_get_kg_factors``,
``food_quantities_from_acquired``, ``food_prices_from_acquired``,
``harvest_kg_factors``) on warm frames and writes one row per country.  Run it
on the UNEDITED base commit to get ``before_*.csv``, then again after each
commit to get ``after_*.csv``; the movement table is the diff.

Doing it this way rather than re-using ``measure.py``'s re-implementation for
the "before" side means both halves of the movement table come from the same
pipeline (same zero-drop, same currency-sentinel drop, same carry rule), so a
discrepancy is a real change and not a difference of harness.

Read-only.  Point ``LSMS_DATA_DIR`` at a scratch root holding COPIES of the
L2-country parquets (never symlinks to the shared cache -- a write would go
through the link), and construct ``Country(..., assume_cache_fresh=True)`` so
nothing rebuilds.

Usage
-----
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<checkout> \
        python measure_impl.py <tag> [Country ...]

``<tag>`` is the output prefix, e.g. ``before`` or ``after_c``.
"""
from __future__ import annotations

import os
import sys
import time
import warnings

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library import transformations as T

HERE = os.path.dirname(os.path.abspath(__file__))
COUNTRIES = ['Uganda', 'Malawi', 'Nigeria', 'Ethiopia', 'Tanzania', 'Niger',
             'Mali', 'EthiopiaRHS', 'GhanaLSS']
CROP_COUNTRIES = ['Uganda', 'Malawi', 'Nigeria', 'Ethiopia', 'Tanzania',
                  'Niger', 'Mali', 'EthiopiaRHS']


def load(country, table='food_acquired'):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return getattr(ll.Country(country, assume_cache_fresh=True), table)()


def _median_price_per_kg(prices, wave):
    """Median ``Price`` per item over the ``u == 'kg'`` rows of one wave."""
    idx = prices.index
    keep = ((idx.get_level_values('t').astype(str) == wave)
            & (idx.get_level_values('u').astype(str) == 'kg'))
    s = prices.loc[keep, 'Price']
    return s.groupby(s.index.get_level_values('j')).median()


def measure(country):
    fa = load(country)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        factors = T._get_kg_factors(fa)
        q = T.food_quantities_from_acquired(fa, units='kgs')
        p = T.food_prices_from_acquired(fa, units='kgvalue')
        try:
            pk = T.food_prices_from_acquired(fa, units='kgprice')
        except Exception:                                     # noqa: BLE001
            pk = None

    uq = q.index.get_level_values('u').astype(str)
    row = {
        'country': country,
        'fa_rows': len(fa),
        'n_factors': len(factors),
        'fq_rows': len(q),
        'fq_kg_rows': int((uq == 'kg').sum()),
        'fq_kg_total': float(q.loc[uq == 'kg', 'Quantity'].sum()),
        'fq_native_total': float(q.loc[uq != 'kg', 'Quantity'].sum()),
        'fp_rows': len(p),
        'fp_kg_rows': int((p.index.get_level_values('u').astype(str)
                           == 'kg').sum()),
        'fp_kgprice_rows': (len(pk) if pk is not None else -1),
        'kg_factor_sources': str(q.attrs.get('kg_factor_sources', '')),
    }

    # per-unit factor table, for a key-by-key diff
    fac = pd.DataFrame({'country': country,
                        'u': list(factors),
                        'kg_per_unit': [float(v) for v in factors.values()]})

    # top-10 expenditure items in the LATEST wave (a cross-wave median pools
    # redenominated currencies -- GhanaLSS spans the 2007 cedi redenomination)
    tvals = sorted(fa.index.get_level_values('t').astype(str).unique())
    last = tvals[-1]
    exp = (fa['Expenditure']
           .groupby([fa.index.get_level_values('t').astype(str),
                     fa.index.get_level_values('j')]).sum())
    exp = exp.loc[last].sort_values(ascending=False)
    med = _median_price_per_kg(p, last)
    top = pd.DataFrame({
        'country': country, 't': last, 'j': list(exp.head(10).index),
        'expenditure_share': [float(exp[j] / exp.sum())
                              for j in exp.head(10).index],
        'median_price_kg': [float(med.get(j, np.nan))
                            for j in exp.head(10).index],
    })
    return row, fac, top


def measure_crop(country):
    try:
        cp = load(country, 'crop_production')
    except Exception as exc:                                   # noqa: BLE001
        return {'country': country, 'error': f'{type(exc).__name__}: {exc}'}
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f = T.harvest_kg_factors(cp)
        res = T.harvest_kg(cp)
    row = {'country': country, 'crop_rows': len(cp), 'harvest_rows': len(res),
           'harvest_kg_total': float(res['Harvest_kg'].sum())}
    row.update({f'layer_{k}': v
                for k, v in f.attrs['kg_factor_sources'].items()})
    dis = f.attrs['kg_factor_disagreement']
    for k, v in dis.items():
        if isinstance(v, dict):
            row[f'dis_{k}_both'] = v.get('both')
            row[f'dis_{k}_share'] = v.get('share')
    return row


def main():
    tag = sys.argv[1] if len(sys.argv) > 1 else 'run'
    countries = sys.argv[2:] or COUNTRIES
    rows, facs, tops, crops = [], [], [], []
    for c in countries:
        t0 = time.time()
        try:
            r, fac, top = measure(c)
        except Exception as exc:                               # noqa: BLE001
            print(f'--- {c} FAILED: {type(exc).__name__}: {exc}', flush=True)
        else:
            rows.append(r); facs.append(fac); tops.append(top)
            print(f'--- {c} {r["fa_rows"]:,} rows, {r["n_factors"]} factors, '
                  f'kg total {r["fq_kg_total"]:.4g}  ({time.time()-t0:.0f}s)',
                  flush=True)
        if c in CROP_COUNTRIES:
            cr = measure_crop(c)
            crops.append(cr)
            print(f'    crop: {cr}', flush=True)
        for name, obj in [('summary', pd.DataFrame(rows)),
                          ('factors', pd.concat(facs) if facs else pd.DataFrame()),
                          ('top10', pd.concat(tops) if tops else pd.DataFrame()),
                          ('crop', pd.DataFrame(crops))]:
            if len(obj):
                obj.to_csv(os.path.join(HERE, f'{tag}_{name}.csv'), index=False)
    print('wrote', tag, 'csvs to', HERE)


if __name__ == '__main__':
    main()
