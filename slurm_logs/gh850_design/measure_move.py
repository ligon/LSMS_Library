"""GH #850 -- per-ROW factor movement, old vs new, from the shipped code.

``measure_impl.py`` measures the delivered TABLES.  This measures the factor
each row is served, which is the number the issue is about.

The "old" factor is read from ``before_factors.csv`` -- the ``{u: kg}`` dict
``_get_kg_factors`` returned on the base commit, which is exactly what
``_apply_kg_conversion`` used to look each row's factor up in.  The "new" one
is ``food_kg_factors(...)['kg_per_unit']`` from the branch.  So both sides are
the library's own output, not a re-implementation.

Restricted to the rows whose kilograms the factor DECIDES: no survey
``Quantity_kg`` (which takes precedence either way).

Usage
-----
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<worktree> python measure_move.py
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


def main():
    old_all = pd.read_csv(os.path.join(HERE, 'before_factors.csv'))
    rows = []
    for c in sys.argv[1:] or COUNTRIES:
        old = dict(zip(old_all.loc[old_all.country == c, 'u'],
                       old_all.loc[old_all.country == c, 'kg_per_unit']))
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            fa = ll.Country(c, assume_cache_fresh=True).food_acquired()
            new = T.food_kg_factors(fa)
        units = fa.index.get_level_values('u').astype(str).str.lower()
        f_old = pd.Series(np.asarray(units)).map(old).astype(float).to_numpy()
        f_new = new['kg_per_unit'].to_numpy(dtype=float)
        decides = np.ones(len(fa), dtype=bool)
        if 'Quantity_kg' in fa.columns:
            decides = ~np.isfinite(T._as_float(fa['Quantity_kg']))
        both = decides & np.isfinite(f_old) & np.isfinite(f_new) & (f_old > 0)
        rel = f_new[both] / f_old[both]
        n = max(both.sum(), 1)
        src = new['KgFactorSource'].to_numpy()
        rows.append({
            'country': c, 'rows': len(fa),
            'rows_decided': int(decides.sum()),
            'rows_compared': int(both.sum()),
            'rows_gained': int((decides & ~np.isfinite(f_old)
                                & np.isfinite(f_new)).sum()),
            'rows_lost': int((decides & np.isfinite(f_old)
                              & ~np.isfinite(f_new)).sum()),
            'share_gt_10pct': float((np.abs(rel - 1) > .10).sum() / n),
            'share_gt_2x': float(((rel > 2) | (rel < .5)).sum() / n),
            'share_gt_10x': float(((rel > 10) | (rel < .1)).sum() / n),
            'median_rel': float(np.median(rel)) if both.any() else np.nan,
            **{f'src_{k}': v for k, v in new.attrs['kg_factor_sources'].items()},
        })
        print(rows[-1], flush=True)
        pd.DataFrame(rows).to_csv(os.path.join(HERE, 'movement.csv'),
                                  index=False)
    print('wrote movement.csv')


if __name__ == '__main__':
    main()
