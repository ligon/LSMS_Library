"""GH #850 red-team follow-ups -- the numbers behind items 2, 3 and 4.

2. `min_reports` now gates the PER-WAVE `(t, j, u)` estimate rather than the
   support summed over waves.  Re-measures coverage and the kg total.
3. D6: how many rows are served by a factor whose per-wave estimates disagree,
   and how many by a factor estimated in a single wave.
4. `baseline_max_spread` -- a dispersion gate at EVERY N on the `(t, j)`
   baseline, OFF by default.  Sweeps the candidates so @ligon can choose, and
   answers the specific question the red team raised: is Niger's nine-report
   millet baseline (2,286 FCFA/kg against a retail 250-450) refused?

Read-only; scratch `LSMS_DATA_DIR` holding COPIES of the L2-country parquets.

Usage
-----
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<worktree> python measure_redteam.py
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
CANDIDATES = (3, 5, 10, 30)


def kg_total(fa, factors):
    v = T._apply_kg_conversion(fa, factors)
    return float(v['Quantity_kg'].sum(skipna=True))


def niger_millet_spread(fa):
    """The `(2021-22, Mil)` baseline the red team singled out: N, max/min and
    p90/p10 of its own per-report price per kilogram."""
    v = T._kg_inference_frame(fa, 'Quantity', 'u')
    ppk = (v['Expenditure'] / v['Kgs']).replace([np.inf, -np.inf], np.nan)
    t = v.index.get_level_values('t').astype(str)
    j = v.index.get_level_values('j').astype(str)
    out = {}
    for item in ('Mil', 'Mais en grain', 'Niebe/Haricots secs'):
        m = (t == '2021-22') & (j.str.startswith(item[:4]))
        s = ppk[m].dropna()
        if not len(s):
            continue
        out[item] = {'n': int(len(s)), 'median': float(s.median()),
                     'max_over_min': float(s.max() / s.min()),
                     'p90_over_p10': (float(s.quantile(.9) / s.quantile(.1))
                                      if s.quantile(.1) > 0 else np.inf)}
    return out


def main():
    rows, sweep, niger = [], [], []
    for c in sys.argv[1:] or COUNTRIES:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            fa = ll.Country(c, assume_cache_fresh=True).food_acquired()
            f = T.food_kg_factors(fa)
        src = f.attrs['kg_factor_sources']
        ws = f.attrs['kg_factor_wave_spread']
        base_kg = kg_total(fa, f['kg_per_unit'])
        rows.append({'country': c, 'rows': len(fa), 'kg_total': base_kg,
                     **{f'src_{k}': v for k, v in src.items()},
                     'item_rows': ws['rows_served_by_item_rung'],
                     'wave_spread_over_2x': ws['rows_over_threshold'],
                     'single_wave_rows': ws['rows_single_wave']})
        print(rows[-1], flush=True)

        for cand in CANDIDATES:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                g = T.food_kg_factors(fa, baseline_max_spread=cand)
            s2 = g.attrs['kg_factor_sources']
            sweep.append({
                'country': c, 'baseline_max_spread': cand,
                'item_unit': s2['item_unit'],
                'item_unit_tight': s2['item_unit_tight'],
                'unit': s2['unit'], 'none': s2['none'],
                'rows_moved_to_unit_or_none':
                    (s2['unit'] + s2['none']) - (src['unit'] + src['none']),
                'kg_total': kg_total(fa, g['kg_per_unit']),
                'kg_pct_vs_branch':
                    (kg_total(fa, g['kg_per_unit']) / base_kg - 1) * 100
                    if base_kg else np.nan,
            })
            print('   ', sweep[-1], flush=True)

        if c == 'Niger':
            for item, d in niger_millet_spread(fa).items():
                stat = d['max_over_min'] if d['n'] < 10 else d['p90_over_p10']
                niger.append({'item': item, **d, 'gate_statistic': stat,
                              **{f'refused_at_{k}': bool(stat > k)
                                 for k in CANDIDATES}})
                print('    NIGER', niger[-1], flush=True)

        pd.DataFrame(rows).to_csv(os.path.join(HERE, 'redteam_coverage.csv'),
                                  index=False)
        pd.DataFrame(sweep).to_csv(os.path.join(HERE, 'redteam_spread_sweep.csv'),
                                   index=False)
        if niger:
            pd.DataFrame(niger).to_csv(
                os.path.join(HERE, 'redteam_niger_baseline.csv'), index=False)
    print('wrote redteam_*.csv')


if __name__ == '__main__':
    main()
