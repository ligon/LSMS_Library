"""GH #1566: s12q09 (asset value) off the 25 grid, by item, Togo/Benin/CotedIvoire.
usage: python s12_check.py OUT_JSON"""
import json, os, sys, warnings
warnings.filterwarnings('ignore')
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
FILES = {('Togo', '2021-22'): 's12_me_tgo2021.dta', ('Togo', '2018'): 's12_me_tgo2018.dta',
         ('Benin', '2021-22'): 's12_me_ben2021.dta', ('Benin', '2018-19'): 's12_me_ben2018.dta',
         ('CotedIvoire', '2021-22'): 's12_me_civ2021.dta', ('CotedIvoire', '2018-19'): 'Menage/s12_me_CIV2018.dta'}
out = {}
for (c, w), f in FILES.items():
    rel = countries_root() / c / w / ('Data1' if (c, w) == ('Togo', '2018') else 'Data') / f
    try:
        df = get_dataframe(str(rel))
    except Exception as exc:
        out[f'{c}|{w}'] = f'ERR {type(exc).__name__}: {exc}'[:300]; continue
    v = pd.to_numeric(df['s12q09'], errors='coerce')
    pos = v[v > 0]
    off = pos[(pos % 25) != 0]
    nonint = pos[(pos % 1) != 0]
    item = df.loc[pos.index, 's12q01'].astype(str)
    by = pd.DataFrame({'item': item, 'off': (pos % 25) != 0}).groupby('item')['off'].agg(['sum', 'size'])
    by['share'] = by['sum'] / by['size']
    by = by[by['sum'] > 0].sort_values('share', ascending=False)
    out[f'{c}|{w}'] = {'positive': int(len(pos)), 'off_25': int(len(off)), 'off_share': float(len(off) / max(len(pos), 1)),
                       'non_integer': int(len(nonint)), 'non_integer_examples': sorted(map(float, nonint.unique()))[:12],
                       'off_examples': sorted(map(float, off.unique()))[:20],
                       'by_item_top': {k: [int(r['sum']), int(r['size']), round(float(r['share']), 3)] for k, r in by.head(12).iterrows()},
                       'columns': [x for x in df.columns if x.startswith('s12q')]}
    if 'vague' in df.columns:
        out[f'{c}|{w}']['off_share_by_vague'] = {str(k): float(g) for k, g in
            pd.Series((pos % 25) != 0).groupby(df.loc[pos.index, 'vague']).mean().items()}
json.dump(out, open(sys.argv[1], 'w'), indent=1, default=str)
print(json.dumps({k: (v if isinstance(v, str) else {kk: v[kk] for kk in ('positive', 'off_25', 'off_share', 'non_integer')}) for k, v in out.items()}, indent=1))
