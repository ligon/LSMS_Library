"""Corpus sweep of sample() weights: per (country, wave) n/mean/min/max/sum."""
import sys, warnings
warnings.filterwarnings('ignore')
import pandas as pd, numpy as np
import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
from lsms_library.country import Country

out_path = sys.argv[1]
rows = []
for c in sorted(ll.countries()):
    try:
        C = Country(c)
        if 'sample' not in C.data_scheme:
            rows.append(dict(country=c, wave=None, note='no sample in data_scheme'))
            print(c, 'no-sample', flush=True)
            continue
        df = C.sample()
    except Exception as e:
        rows.append(dict(country=c, wave=None, note='ERROR: ' + repr(e)[:300]))
        print(c, 'ERROR', repr(e)[:160], flush=True)
        continue
    if not isinstance(df, pd.DataFrame) or df.empty:
        rows.append(dict(country=c, wave=None, note='empty'))
        print(c, 'empty', flush=True)
        continue
    names = list(df.index.names)
    if 't' in names:
        tv = df.index.get_level_values('t')
    else:
        tv = pd.Index(['ALL'] * len(df))
    g = df.copy()
    g['_t'] = np.asarray(tv, dtype=object)
    for wave, sub in g.groupby('_t', dropna=False, observed=True):
        r = dict(country=c, wave=str(wave), n=len(sub), note='')
        for col in ('weight', 'panel_weight'):
            if col in sub.columns:
                s = pd.to_numeric(sub[col], errors='coerce').dropna()
                r[col + '_n'] = int(s.size)
                r[col + '_mean'] = float(s.mean()) if s.size else np.nan
                r[col + '_min'] = float(s.min()) if s.size else np.nan
                r[col + '_max'] = float(s.max()) if s.size else np.nan
                r[col + '_sum'] = float(s.sum()) if s.size else np.nan
                r[col + '_neg'] = int((s < 0).sum()) if s.size else 0
                r[col + '_zero'] = int((s == 0).sum()) if s.size else 0
            else:
                r[col + '_n'] = 0
        rows.append(r)
    print(c, 'ok', len(df), flush=True)

pd.DataFrame(rows).to_csv(out_path, index=False)
print('WROTE', out_path)
