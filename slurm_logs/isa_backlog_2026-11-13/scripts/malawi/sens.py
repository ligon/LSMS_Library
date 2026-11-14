"""Where do the relabelled cowpea rows sit with respect to the season inference?

usage: python sens.py <clone> <frames> <arm> <out.json>

For every 2016-17 / 2019-20 crop_production / plot_inputs row whose crop is
Cowpea (Khobwe) (harvest rows, not Farm-level): is its household one whose
Module D and Module G started the same day, and one whose dated G lines fall in
one season?  Read-only; the same derivation as diag3.py.
"""
import json
import re
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, fr, arm, outp = sys.argv[1], Path(sys.argv[2]), sys.argv[3], sys.argv[4]
assert_env(clone)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe, format_id  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

ROOT = countries_root() / 'Malawi'
walk = json.loads((ROOT / '_' / 'updated_ids.json').read_text())
HALVES = {'2016-17': [('Data/Cross_Sectional/ag_mod_g.dta', 'Data/Cross_Sectional/ag_metadata.dta', 'case_id', 'cs-17-'),
                      ('Data/Panel/ag_mod_g_16.dta', 'Data/Panel/ag_meta_16.dta', 'y3_hhid', '')],
          '2019-20': [('Data/Cross_Sectional/ag_mod_g.dta', 'Data/Cross_Sectional/ag_mod_meta.dta', 'case_id', 'cs-19-'),
                      ('Data/Panel/ag_mod_g_19.dta', 'Data/Panel/ag_meta_19.dta', 'y4_hhid', '')]}


def to_date(s):
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors='coerce').dt.normalize()
    if pd.api.types.is_numeric_dtype(s):
        x = pd.to_numeric(s, errors='coerce')
        unit = 'ms' if x.dropna().abs().median() > 1e8 else 'D'
        return (pd.Timestamp('1960-01-01') + pd.to_timedelta(x, unit=unit)).dt.normalize()
    return pd.to_datetime(s.astype('string').str.slice(0, 19), errors='coerce').dt.normalize()


res = {}
for w, halves in HALVES.items():
    hh_state = []
    for grel, mrel, hh, prefix in halves:
        g = get_dataframe(str(ROOT / w / grel), convert_categoricals=False)
        g['_h'] = prefix + g[hh].apply(format_id).astype(str)
        mo, yr = pd.to_numeric(g['ag_g05a'], errors='coerce'), pd.to_numeric(g['ag_g05b'], errors='coerce')
        ok = mo.between(1, 12) & yr.between(2000, 2030)
        g['_S'] = np.where(ok, np.where(mo >= 7, yr, yr - 1), np.nan)
        nseason = g.dropna(subset=['_S']).groupby('_h')['_S'].nunique()
        meta = get_dataframe(str(ROOT / w / mrel), convert_categoricals=False)
        dcol = next(c for c in meta.columns if re.fullmatch(r'ag_moduleD_start_?date', c, re.I))
        gcol = next(c for c in meta.columns if re.fullmatch(r'ag_moduleG_start_?date', c, re.I))
        meta['_h'] = prefix + meta[hh].apply(format_id).astype(str)
        mm = meta.drop_duplicates('_h').set_index('_h')
        same = (to_date(mm[gcol]) == to_date(mm[dcol]))
        st = pd.DataFrame({'one_season': nseason.reindex(mm.index.union(nseason.index)).eq(1),
                           'dg_same_day': same.reindex(mm.index.union(nseason.index)).fillna(False)})
        st.index = [walk.get(w, {}).get(x, x) for x in st.index]
        hh_state.append(st)
    st = pd.concat(hh_state)
    st = st[~st.index.duplicated()]
    rec = {}
    for table in ('crop_production', 'plot_inputs'):
        p = fr / f'{arm}.{table}.parquet'
        if not p.exists():
            continue
        df = pd.read_parquet(p).reset_index()
        sub = df[df['t'].astype(str).eq(w) & df['crop'].astype(str).eq('Cowpea (Khobwe)')
                 & df['plot_id'].astype(str).ne('Farm-level')]
        s = st.reindex(sub['i'].astype(str))
        rec[table] = {'rows': int(len(sub)), 'households': int(sub['i'].nunique()),
                      'rows_household_unmatched': int(s['one_season'].isna().sum()),
                      'rows_dg_same_day': int(s['dg_same_day'].fillna(False).astype(bool).sum()),
                      'rows_one_season': int(s['one_season'].fillna(False).astype(bool).sum()),
                      'rows_both': int((s['dg_same_day'].fillna(False).astype(bool)
                                        & s['one_season'].fillna(False).astype(bool)).sum())}
    res[w] = rec
    print(w, rec, flush=True)
Path(outp).write_text(json.dumps(res, indent=1))
