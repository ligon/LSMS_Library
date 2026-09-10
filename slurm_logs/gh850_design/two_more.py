"""Two numbers the design's remaining decisions turn on.

D6  Does a (j, u) factor need a ``t`` axis?  For every (j, u) cell estimated in
    three or more waves, how far apart are the per-wave estimates?

D7  Does defect (c) -- the metric spellings ``KNOWN_METRIC`` and
    ``_parse_explicit_metric`` miss -- reach ``crop_production``?  It would,
    through ``_kg_factor_series``, which is ``harvest_kg``'s ``inferred``
    layer.  Count the crop rows whose ``u`` is one of those labels.

Read-only.  Run with LSMS_DATA_DIR pointed at a scratch root.
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure as M                                            # noqa: E402
import lsms_library as ll                                      # noqa: E402
from lsms_library.transformations import (                     # noqa: E402
    KNOWN_METRIC, _parse_explicit_metric)

HERE = os.path.dirname(os.path.abspath(__file__))


def t_spread(country):
    flat = M.prep(M.load(country))
    ratio = flat['Expenditure'] / flat['Kgs']
    gb = ratio.groupby([flat['t'], flat['j']])
    pkg, nb = gb.median(), gb.count()
    pkg.index.names = nb.index.names = ['t', 'j']
    pkg = pkg.where(nb >= M.DEFAULT_BASELINE)
    infer = flat[flat['Quantity_kg'].isna()].copy()
    infer['up'] = infer['Expenditure'] / infer['Quantity']
    est = (infer.groupby(['t', 'j', 'u'])['up'].median() / pkg)
    est = est.replace([np.inf, -np.inf], np.nan).dropna()
    g = est.groupby(['j', 'u'])
    n, lo, hi, med = g.count(), g.min(), g.max(), g.median()
    keep = n >= 3
    spread = (hi / lo)[keep & (lo > 0)]
    return {'country': country,
            'cells_ju_with_3plus_waves': int(keep.sum()),
            'waves': int(flat['t'].nunique()),
            'max_over_min_p25': float(spread.quantile(.25)) if len(spread) else np.nan,
            'max_over_min_median': float(spread.median()) if len(spread) else np.nan,
            'max_over_min_p75': float(spread.quantile(.75)) if len(spread) else np.nan,
            'share_within_2x': float((spread <= 2).mean()) if len(spread) else np.nan,
            'share_within_10x': float((spread <= 10).mean()) if len(spread) else np.nan}


def crop_hit(country):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = ll.Country(country).crop_production()
    u = (cp.index.get_level_values('u') if 'u' in (cp.index.names or [])
         else cp['u'])
    lab = pd.Series([str(x) for x in u])
    seeded_now, seeded_wide = [], []
    for s in lab.unique():
        k = s.lower()
        now = k in KNOWN_METRIC or _parse_explicit_metric(s) is not None
        wide = now or k in M.MISSING_BARE or M.MISSING_CONTENT.search(s)
        if now:
            seeded_now.append(s)
        if wide and not now:
            seeded_wide.append(s)
    n_new = int(lab.isin(seeded_wide).sum())
    return {'country': country, 'crop_rows': len(cp),
            'labels_seeded_now': len(seeded_now),
            'labels_seeded_by_c': len(seeded_wide),
            'rows_moved_by_c': n_new,
            'share_rows_moved': n_new / max(len(cp), 1),
            'examples': '; '.join(sorted(seeded_wide)[:6])}


def main():
    warnings.simplefilter('ignore')
    rows_t, rows_c = [], []
    for c in sys.argv[1:] or M.COUNTRIES:
        try:
            rows_t.append(t_spread(c))
        except Exception as exc:                                # noqa: BLE001
            print(f'{c}: t_spread FAILED {type(exc).__name__}: {exc}')
        try:
            rows_c.append(crop_hit(c))
        except Exception as exc:                                # noqa: BLE001
            print(f'{c}: crop FAILED {type(exc).__name__}: {exc}')
    a, b = pd.DataFrame(rows_t), pd.DataFrame(rows_c)
    a.to_csv(os.path.join(HERE, 'd6_t_axis.csv'), index=False)
    b.to_csv(os.path.join(HERE, 'd7_crop_reach.csv'), index=False)
    with pd.option_context('display.width', 220, 'display.max_columns', 30):
        print(a.to_string(index=False)); print(); print(b.to_string(index=False))


if __name__ == '__main__':
    main()
