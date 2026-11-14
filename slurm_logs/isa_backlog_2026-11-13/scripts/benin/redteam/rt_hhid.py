import sys, json, warnings
from collections import Counter
from pathlib import Path
import numpy as np, pandas as pd
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe
C = Path(sys.argv[1]) / 'lsms_library' / 'countries' / 'Benin'
key = lambda g, m: f'{int(g)}{int(m):03d}'
c21 = get_dataframe(C / '2021-22/Data/s00_me_ben2021.dta')
c21l = get_dataframe(C / '2021-22/Data/s00_me_ben2021.dta', convert_categoricals=True)
c18 = get_dataframe(C / '2018-19/Data/s00_me_ben2018.dta')
c21['key'] = [key(g, m) for g, m in zip(c21.grappe, c21.menage)]
c18['key'] = [key(g, m) for g, m in zip(c18.grappe, c18.menage)]
panel = c21l['PanelHH'].astype(str).str.strip().to_numpy()
rec = c21.key.isin(set(c18.key)).to_numpy()
c21['grp'] = np.select([(panel == 'Panel') & rec, (panel == 'Panel') & ~rec, (panel != 'Panel') & rec], ['linked', 'panel_nokey', 'nonpanel_recur'], 'new')
R = {}
for wave, name in (('2021-22', 'ehcvm_welfare_ben2021.dta'), ('2021-22', 'ehcvm_individu_ben2021.dta'), ('2018-19', 'ehcvm_welfare_ben2018.dta')):
    df = get_dataframe(C / wave / 'Data' / name)
    r = {'panel_like_columns': [c for c in df.columns if 'panel' in c.lower() or '2018' in c or 'ehcvm1' in c.lower()],
         'hhid_dtype': str(df.hhid.dtype), 'grappe_dtype': str(df.grappe.dtype), 'menage_dtype': str(df.menage.dtype)}
    h = df.drop_duplicates(['grappe', 'menage']).copy()
    h['key'] = [key(g, m) for g, m in zip(h.grappe, h.menage)]
    hs = h.hhid.astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    r['hhid_equals_key_string'] = float((hs == h.key).mean())
    r['mismatch_examples'] = h.loc[hs != h.key, ['grappe', 'menage', 'hhid']].head(6).astype(str).to_dict('records')
    for c in r['panel_like_columns']:
        r[f'values_{c}'] = dict(Counter(df[c].astype(str)).most_common(6))
    R[name] = r
    if name == 'ehcvm_welfare_ben2021.dta':
        h21 = h.assign(hs=hs.values)
    if name == 'ehcvm_welfare_ben2018.dta':
        h18s = set(hs)
g = dict(zip(c21.key, c21.grp))
h21['grp'] = h21.key.map(g)
R['hhid21_in_hhid18_by_group'] = {k: int(h21.loc[h21.grp == k, 'hs'].isin(h18s).sum()) for k in ('linked', 'nonpanel_recur', 'panel_nokey', 'new')}
R['hhid21_ne_key_by_group'] = {k: int((h21.loc[h21.grp == k, 'hs'] != h21.loc[h21.grp == k, 'key']).sum()) for k in ('linked', 'nonpanel_recur', 'panel_nokey', 'new')}
print(json.dumps(R, indent=1, default=str, ensure_ascii=False))
