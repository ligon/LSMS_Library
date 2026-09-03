"""STEP-3 verification for GhanaLSS community_prices (GH #562 phase 3a).

Builds Country('GhanaLSS').community_prices() through the API (LSMS_GRAIN_STRICT
and LSMS_READ_STRICT are expected in the environment) and prints, per wave:
rows / distinct v, j, u / index uniqueness / obs distribution / rows dropped
for no price / v <-> sample().v shares / share of j on harmonize_food and the
overlap with food_acquired.j (from a WARM cache only -- never built) / staple
medians / u labels not on the unit_labels.org axis.  Read-only apart from the
library's own caches.
"""
import glob
import os
import sys
import warnings
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import df_from_orgfile

assert 'wt-glss-prices' in str(countries_root()), countries_root()
ROOT = countries_root() / 'GhanaLSS'
sys.path.insert(0, str(ROOT / '_'))
from glss_prices import price_item_table, unit_label_map  # noqa: E402

STAPLES = {
    'maize': ['Maize', 'Maize (cob)', 'Maize (grain)'],
    'rice': ['Rice', 'Rice (local)', 'Rice (imported)'],
    'cassava': ['Cassava', 'Cassava (fresh)'],
    'plantain': ['Plantain'],
    'palm oil': ['Oil (palm)', 'Oil (red palm)'],
}


def main():
    pd.set_option('display.width', 220)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        c = ll.Country('GhanaLSS')
        cp = c.community_prices()
        fired = [str(x.message)[:200] for x in w
                 if 'Grain' in type(x.message).__name__ or 'NullRead' in type(x.message).__name__
                 or 'null' in str(x.message).lower()]
    print('index:', cp.index.names, ' shape:', cp.shape, ' unique:', cp.index.is_unique)
    print('columns:', list(cp.columns), cp.dtypes.to_dict())
    print('grain/null warnings fired:', len(fired))
    for f in fired[:10]:
        print('   ', f)
    from lsms_library.country import grain_reports
    try:
        print('grain_reports:', len(grain_reports()))
    except Exception as e:
        print('grain_reports unavailable:', e)

    sample = c.sample()
    sv = sample.reset_index()[['t', 'v']].drop_duplicates()
    umap_vals = set(unit_label_map().values())

    flat = cp.reset_index()
    for t, g in flat.groupby('t', sort=True):
        print(f'\n===== {t}: rows={len(g)}  v={g.v.nunique()}  j={g.j.nunique()}  u={g.u.nunique()}  '
              f'unique(t,v,j,u,obs)={not g.duplicated(["t","v","j","u","obs"]).any()}')
        print('   obs distribution:', g['obs'].value_counts().sort_index().to_dict())
        print('   Price NaN:', g.Price.isna().sum(), ' NumberOfUnits NaN:', g.NumberOfUnits.isna().sum(),
              ' Description NaN:', g.Description.isna().sum())
        s = set(sv.loc[sv.t == t, 'v'])
        pv = set(g.v)
        print(f'   v in sample().v: {len(pv & s)}/{len(pv)} = {len(pv & s)/max(len(pv),1):.3f};  '
              f'sample().v with >=1 price row: {len(pv & s)}/{len(s)} = {len(pv & s)/max(len(s),1):.3f}')
        # j: food share on the wave's harmonize_food axis
        hf = df_from_orgfile(ROOT / t / '_' / 'categorical_mapping.org', name='harmonize_food', to_numeric=False)
        hf.columns = [x.strip() for x in hf.columns]
        axis = set(hf['Preferred Label'].astype(str).str.strip()) - {''}
        js = set(g.j)
        on_axis = js & axis
        items = price_item_table(t)
        print(f'   j labels: {len(js)}; on harmonize_food axis: {len(on_axis)} ({len(on_axis)/len(js):.2f}); '
              f'price rows on axis: {g.j.isin(axis).mean():.3f}; table Food=yes/own/no: '
              f'{items.Food.value_counts().to_dict()}')
        # overlap with a WARM food_acquired.j if any exists in this data root
        fa_paths = [Path(data_root()) / 'GhanaLSS' / t / '_' / 'food_acquired.parquet',
                    Path(data_root()) / 'GhanaLSS' / 'var' / 'food_acquired.parquet']
        fa = next((p for p in fa_paths if p.exists()), None)
        if fa is not None:
            fj = pd.read_parquet(fa, columns=[]).index
            if 't' in fj.names:
                fj = fj[fj.get_level_values('t') == t]
            fj = set(fj.get_level_values('j'))
            print(f'   overlap with WARM food_acquired.j ({fa.name}): {len(js & fj)} labels of {len(js)}; '
                  f'food_acquired has {len(fj)} labels')
        else:
            print('   food_acquired.j: no warm parquet in this data root -- NOT built (brief); '
                  'compared against harmonize_food labels above')
        # u
        us = set(g.u.dropna())
        new_u = sorted(us - umap_vals)
        print(f'   u labels: {len(us)}; not on unit_labels.org axis: {new_u}')
        # staples
        med = {}
        for k, labs in STAPLES.items():
            sub = g[g.j.isin(labs)]
            # a per-unit value (Price / NumberOfUnits) is a TRANSFORMATION --
            # computed here for the report only, never stored.
            kg = sub[sub.u.isin(['Kg', 'Kilogram']) & sub.NumberOfUnits.gt(0)]
            per_kg = (kg.Price / kg.NumberOfUnits).median() if len(kg) else None
            med[k] = dict(n=len(sub), median_Price=round(float(sub.Price.median()), 3) if len(sub) else None,
                          median_per_kg=round(float(per_kg), 3) if per_kg is not None and pd.notna(per_kg) else None,
                          units=sorted(sub.u.dropna().unique())[:4])
        print('   staples:', med)

    print('\n===== overall:', cp.shape, ' t values:', sorted(flat.t.unique()))


if __name__ == '__main__':
    main()
