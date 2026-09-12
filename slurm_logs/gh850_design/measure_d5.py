"""GH #850 D5 -- what a dispersion-gated baseline exception buys, and at what
tolerance.

@ligon's D5: the ``(t, j)`` price-per-kg baseline floor is 5 reports, WITH an
exception -- a baseline of 3 or 4 reports is accepted when the reports agree
closely.  This script measures the two candidate spreads and four candidate
tolerances so the default is chosen from a table rather than asserted.

Spread candidates, over the per-report price-per-kg (``Expenditure / Kgs``)
inside one ``(t, j)`` cell:

  ``max/min``     -- scale-free, reads as "the reports lie within X% of each
                     other", and defined for N = 3 as well as it is for N = 40.
  ``IQR/median``  -- at N = 3 or 4 the quartiles are interpolations over two
                     gaps, so this is the range wearing a robust name.

Also measures, symmetrically, what the same exception would buy on the
``(t, j, u)`` step-2 rung (D4), and the two candidate definitions of the
``item_unit_tight`` provenance tag.

Read-only; same scratch-root discipline as ``measure_impl.py``.

Usage
-----
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<checkout> python measure_d5.py [Country ...]
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

from measure import COUNTRIES, load, prep, seeded_factors  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
TOLERANCES = (1.10, 1.25, 1.50, 2.00)
MIN_BASELINE = 5
MIN_BASELINE_TIGHT = 3
MIN_REPORTS = 5


def baseline_cells(flat):
    """Per-``(t, j)`` price-per-kg reference: median, N, and two spreads."""
    ratio = (flat['Expenditure'] / flat['Kgs']).replace(
        [np.inf, -np.inf], np.nan)
    gb = ratio.groupby([flat['t'], flat['j']])
    out = pd.DataFrame({'median': gb.median(), 'n': gb.count(),
                        'max': gb.max(), 'min': gb.min(),
                        'q25': gb.quantile(.25), 'q75': gb.quantile(.75)})
    out.index.names = ['t', 'j']
    out['max_over_min'] = out['max'] / out['min']
    out['iqr_over_median'] = (out['q75'] - out['q25']) / out['median']
    return out


def factors(flat, cells, admit, floor=MIN_REPORTS):
    """``(j, u)`` factors from the admitted baselines.  Returns (factors,
    per-cell tight flags, step-2 support)."""
    pkg = cells['median'].where(admit)
    infer = flat[flat['Quantity_kg'].isna()].copy()
    infer['up'] = infer['Expenditure'] / infer['Quantity']
    g = infer.groupby(['t', 'j', 'u'])['up']
    est = (g.median() / pkg).replace([np.inf, -np.inf], np.nan).dropna()
    n = g.count().reindex(est.index)
    sup = n.groupby(['j', 'u']).sum()
    per = est.groupby(['j', 'u']).median()
    keep = (sup >= floor) & np.isfinite(per) & (per > 0)
    return per[keep], est, sup


def rows_served(flat, per_ju, need):
    key = pd.Series(list(zip(flat['j'].astype(str), flat['u'])),
                    index=flat.index)
    return int((key.map(per_ju.to_dict()).notna() & need).sum())


def measure(country):
    df = load(country)
    flat = prep(df)
    seed = seeded_factors(df, wide=True)
    need = flat['Quantity_kg'].isna() & ~flat['ul'].isin(seed)

    cells = baseline_cells(flat)
    strict = cells['n'] >= MIN_BASELINE
    band = (cells['n'] >= MIN_BASELINE_TIGHT) & (cells['n'] < MIN_BASELINE)

    per_strict, est_strict, _ = factors(flat, cells, strict)
    base_rows = rows_served(flat, per_strict, need)
    n_need = int(need.sum())

    out = []
    for spread_col in ('max_over_min', 'iqr_over_median'):
        for tol in TOLERANCES:
            tight = band & (cells[spread_col] <= tol)
            admit = strict | tight
            per, est, _sup = factors(flat, cells, admit)
            served = rows_served(flat, per, need)
            # provenance-tag definitions, on the ADMITTED map
            tightmap = tight.reindex(est.index.droplevel('u')).to_numpy()
            any_tight = (pd.Series(tightmap, index=est.index)
                         .groupby(['j', 'u']).max().reindex(per.index)
                         .fillna(False).astype(bool))
            only_tight = ~per.index.isin(per_strict.index)
            adm = cells.loc[tight, spread_col]
            out.append({
                'country': country, 'spread': spread_col, 'tolerance': tol,
                'cells_total': len(cells),
                'cells_strict': int(strict.sum()),
                'cells_in_band': int(band.sum()),
                'cells_admitted': int(tight.sum()),
                'share_of_band_admitted': float(tight.sum() / max(band.sum(), 1)),
                'admitted_spread_median': float(adm.median()) if len(adm) else np.nan,
                'admitted_spread_p90': float(adm.quantile(.9)) if len(adm) else np.nan,
                'ju_cells_strict': len(per_strict),
                'ju_cells': len(per),
                'ju_cells_new': int((~per.index.isin(per_strict.index)).sum()),
                'rows_need_inference': n_need,
                'rows_served_strict': base_rows,
                'rows_served': served,
                'rows_gained': served - base_rows,
                'share_served_strict': base_rows / max(n_need, 1),
                'share_served': served / max(n_need, 1),
                'tag_any_tight': int(any_tight.sum()),
                'tag_only_tight': int(only_tight.sum()),
            })

    # D4, symmetrically: the same exception on the (t, j, u) step-2 rung.
    infer = flat[flat['Quantity_kg'].isna()].copy()
    infer['up'] = (infer['Expenditure'] / infer['Quantity']).replace(
        [np.inf, -np.inf], np.nan)
    g = infer.groupby(['t', 'j', 'u'])['up']
    sup_cells = pd.DataFrame({'n': g.count(), 'max': g.max(), 'min': g.min()})
    sup_cells['max_over_min'] = sup_cells['max'] / sup_cells['min']
    est = (g.median() / cells['median'].where(strict)).replace(
        [np.inf, -np.inf], np.nan).dropna()
    n_ju = sup_cells['n'].reindex(est.index).groupby(['j', 'u']).sum()
    per5 = est.groupby(['j', 'u']).median()
    d4 = []
    for tol in TOLERANCES:
        # a (j,u) cell below the floor is admitted when its own step-2 reports
        # agree within `tol`
        tight_sup = (sup_cells['max_over_min'] <= tol).reindex(
            est.index).fillna(False)
        n_tight = (sup_cells['n'].where(tight_sup, 0).reindex(est.index)
                   .groupby(['j', 'u']).sum())
        keep5 = (n_ju >= MIN_REPORTS)
        keep_t = keep5 | ((n_ju >= MIN_BASELINE_TIGHT) & (n_tight >= MIN_BASELINE_TIGHT))
        a = per5[keep5 & np.isfinite(per5) & (per5 > 0)]
        b = per5[keep_t & np.isfinite(per5) & (per5 > 0)]
        d4.append({'country': country, 'tolerance': tol,
                   'ju_cells_floor5': len(a), 'ju_cells_with_tight': len(b),
                   'rows_served_floor5': rows_served(flat, a, need),
                   'rows_served_with_tight': rows_served(flat, b, need),
                   'rows_need_inference': n_need})
    return out, d4


def main():
    countries = sys.argv[1:] or COUNTRIES
    A, B = [], []
    for c in countries:
        print(f'--- {c}', flush=True)
        try:
            a, b = measure(c)
        except Exception as exc:                              # noqa: BLE001
            print(f'    FAILED: {type(exc).__name__}: {exc}', flush=True)
            continue
        A += a; B += b
        for r in a:
            if r['spread'] == 'max_over_min':
                print(f"    max/min tol={r['tolerance']}: "
                      f"+{r['cells_admitted']} cells of {r['cells_in_band']} in band, "
                      f"served {r['share_served_strict']:.3f} -> {r['share_served']:.3f}, "
                      f"tag any={r['tag_any_tight']} only={r['tag_only_tight']}",
                      flush=True)
        pd.DataFrame(A).to_csv(os.path.join(HERE, 'd5_tolerance.csv'), index=False)
        pd.DataFrame(B).to_csv(os.path.join(HERE, 'd4_tolerance.csv'), index=False)
    print('wrote d5_tolerance.csv / d4_tolerance.csv to', HERE)


if __name__ == '__main__':
    main()
