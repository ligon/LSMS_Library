"""Red-team raw probe of the EHCVM s16b / s16c source files (no library build).

usage: python rt_raw.py <out.json>

Reads each file through get_data_file (local blob) for its Stata metadata
(pyreadstat, metadata only) and through get_dataframe for codes + labels.
"""
import json
import sys
import unicodedata
import warnings
from collections import Counter

import pandas as pd
import pyreadstat

warnings.simplefilter('ignore')
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

OUT = {}
VARS = ['s16bq01', 's16bq02', 's16bq03a', 's16bq03b', 's16bq04', 's16bq05', 's16bq06',
        's16bq07a', 's16bq07b', 's16bq08', 's16bq09a', 's16bq09b', 's16bq09c']
S16B = {
    'Togo/2018': 'Togo/2018/Data1/s16b_me_tgo2018.dta',
    'Togo/2021-22': 'Togo/2021-22/Data/s16b_me_tgo2021.dta',
    'Senegal/2018-19': 'Senegal/2018-19/Data/s16b_me_sen2018.dta',
    'Senegal/2021-22': 'Senegal/2021-22/Data/s16b_me_sen2021.dta',
    'CotedIvoire/2018-19': 'CotedIvoire/2018-19/Data/Menage/s16b_me_CIV2018.dta',
    'CotedIvoire/2021-22': 'CotedIvoire/2021-22/Data/s16b_me_civ2021.dta',
    'Niger/2018-19': 'Niger/2018-19/Data/s16b_me_ner2018.dta',
    'Niger/2021-22': 'Niger/2021-22/Data/s16b_me_ner2021.dta',
    'Burkina_Faso/2018-19': 'Burkina_Faso/2018-19/Data/s16b_me_bfa2018.dta',
}


def j(x):
    if isinstance(x, dict):
        return {str(k): j(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [j(v) for v in x]
    if hasattr(x, 'item'):
        try:
            return x.item()
        except Exception:
            return str(x)
    return x


def counts(s):
    return {repr(k): int(v) for k, v in s.value_counts(dropna=False).items()}


for key, rel in S16B.items():
    rec = OUT[key] = {}
    try:
        local = get_data_file(rel)
        _, meta = pyreadstat.read_dta(str(local), metadataonly=True)
        labels = meta.column_names_to_labels
        vvl = meta.variable_value_labels
        rec['var_labels'] = {v: labels.get(v) for v in VARS if v in labels}
        rec['value_labels'] = {v: j(vvl.get(v)) for v in VARS if v in vvl}
        codes = get_dataframe(countries_root() / rel, convert_categoricals=False)
        lab = get_dataframe(countries_root() / rel, convert_categoricals=True)
        # Applied gate: 1 = oui in every file except CIV 2021-22 where 1 = oui too.
        app = (codes['s16bq02'] == 1).values
        c, l = codes[app], lab[app]
        rec['n_rows'] = int(len(codes)); rec['n_applied'] = int(app.sum())
        rec['codes_applied'] = {v: counts(c[v]) for v in
                                ['s16bq03b', 's16bq05', 's16bq07b', 's16bq08', 's16bq09b'] if v in c}
        rec['unlabelled_codes_applied'] = {}
        for v in ['s16bq03b', 's16bq05', 's16bq07b', 's16bq08', 's16bq09b']:
            if v in c:
                known = set(float(k) for k in (vvl.get(v) or {}))
                vals = set(float(x) for x in c[v].dropna().unique())
                rec['unlabelled_codes_applied'][v] = sorted(vals - known)
        rec['label_reprs_applied'] = {v: sorted(repr(x) for x in l[v].dropna().astype(str).unique())
                                      for v in ['s16bq05', 's16bq08'] if v in l}
        if {'s16bq05', 's16bq08'} <= set(c.columns):
            ct = pd.crosstab(c['s16bq05'].fillna(-1), c['s16bq08'].fillna(-1))
            rec['crosstab_q05_x_q08'] = {f'{a}|{b}': int(ct.loc[a, b]) for a in ct.index for b in ct.columns}
            rec['q07a_notna'] = int(c['s16bq07a'].notna().sum())
            rec['q07a_notna_and_q05_not1'] = int((c['s16bq07a'].notna() & (c['s16bq05'] != 1)).sum())
            rec['q05_1_and_q07a_na'] = int(((c['s16bq05'] == 1) & c['s16bq07a'].isna()).sum())
            rec['q09a_notna'] = int(c['s16bq09a'].notna().sum())
            rec['q09a_notna_and_q08_not1'] = int((c['s16bq09a'].notna() & (c['s16bq08'] != 1)).sum())
            rec['q08_1_and_q09a_na'] = int(((c['s16bq08'] == 1) & c['s16bq09a'].isna()).sum())
            rec['q07b_notna'] = int(c['s16bq07b'].notna().sum())
            rec['q09b_notna'] = int(c['s16bq09b'].notna().sum())
            rec['q03b_na_applied'] = int(c['s16bq03b'].isna().sum())
            rec['q03b_na_applied_qty_pos'] = int((c['s16bq03b'].isna() & (pd.to_numeric(c['s16bq03a'], errors='coerce') > 0)).sum())
            hollow = c['s16bq03a'].isna() & c['s16bq05'].isna() & c['s16bq08'].isna()
            rec['hollow_q03a_q05_q08_na'] = int(hollow.sum())
            # labelled -> boolean mapping as the scripts do (no strip / with strip)
            for v in ('s16bq05', 's16bq08'):
                raw = l[v].astype('string')
                m1 = raw.map({'Oui': True, 'Non': False, 'Sim': True, 'Não': False})
                m2 = raw.str.strip().map({'Oui': True, 'Non': False, 'Sim': True, 'Não': False})
                rec[f'{v}_map_na_nostrip'] = int(m1.isna().sum())
                rec[f'{v}_map_na_strip'] = int(m2.isna().sum())
                rec[f'{v}_code_na'] = int(c[v].isna().sum())
        if key == 'Togo/2018':
            sel = (c['grappe'] == 101) & (c['menage'] == 9)
            cols = ['grappe', 'menage'] + [v for v in VARS if v in c]
            rec['hh_101_9_codes'] = j(c.loc[sel, cols].to_dict(orient='records'))
            rec['hh_101_9_labels'] = j(l.loc[sel, cols].astype(str).to_dict(orient='records'))
            rec['s16bq04_labels_applied'] = counts(l['s16bq04'].astype('string'))
        if key == 'CotedIvoire/2021-22':
            distinct = codes[codes['s16bq01'].notna()].groupby(['grappe', 'menage'])['s16bq01'].transform('nunique')
            damaged = codes.loc[distinct.index[distinct < 24], ['grappe', 'menage']].drop_duplicates()
            dset = set(map(tuple, damaged.values.tolist()))
            blank = c[c['s16bq03b'].isna()]
            rec['blank_unit_rows'] = j(blank[['grappe', 'menage', 's16bq01', 's16bq03a']].to_dict(orient='records'))
            rec['blank_unit_in_damaged'] = [bool((g, m) in dset) for g, m in zip(blank['grappe'], blank['menage'])]
            rec['n_damaged'] = len(dset)
    except Exception as e:  # keep going
        rec['error'] = f'{type(e).__name__}: {e}'

# Senegal s16c crop labels: byte form of maize / cowpea
try:
    rel = 'Senegal/2018-19/Data/s16c_me_sen2018.dta'
    lab = get_dataframe(countries_root() / rel, convert_categoricals=True)
    crops = lab['s16cq04'].astype('string')
    vc = crops.value_counts()
    rec = OUT['Senegal/2018-19/s16c'] = {}
    rec['labels_like_mais_niebe'] = {
        repr(k): {'n': int(v), 'hex': k.encode('utf-8').hex(), 'nfc': unicodedata.is_normalized('NFC', k)}
        for k, v in vc.items() if any(s in k.lower() for s in ('ma', 'ni', 'har'))}
    rec['all_labels'] = {repr(k): int(v) for k, v in vc.items()}
    for target in ('Maïs', 'Niébé'):
        rec[f'target_{target}_hex'] = target.encode('utf-8').hex()
except Exception as e:
    OUT['Senegal/2018-19/s16c'] = {'error': f'{type(e).__name__}: {e}'}

with open(sys.argv[1], 'w') as fh:
    json.dump(OUT, fh, indent=1, ensure_ascii=False, default=str)
print('done', {k: ('error' in v) for k, v in OUT.items()})
