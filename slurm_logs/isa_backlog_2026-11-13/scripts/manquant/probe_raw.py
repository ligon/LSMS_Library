"""Raw s16b probe (base checkout).  Usage: python probe_raw.py <out.json>

Per (country, wave): applied-row counts, missing used-unit counts, value labels
and variable labels of the unit / gift / purchase questions, raw 'manquant'-like
labels, and for Togo 2018 the gift vs purchase crosstab.  Also Senegal s16c crop
labels for maize / cowpea (the #1119-SEN join).
"""
import json, sys, warnings
from collections import Counter
warnings.simplefilter('ignore')
import pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.data_access import get_data_file
from lsms_library.paths import countries_root

CELLS = {
    ('Togo', '2018'): 'Data1/s16b_me_tgo2018.dta',
    ('Togo', '2021-22'): 'Data/s16b_me_tgo2021.dta',
    ('Senegal', '2018-19'): 'Data/s16b_me_sen2018.dta',
    ('Senegal', '2021-22'): 'Data/s16b_me_sen2021.dta',
    ('CotedIvoire', '2018-19'): 'Data/Menage/s16b_me_CIV2018.dta',
    ('CotedIvoire', '2021-22'): 'Data/s16b_me_civ2021.dta',
}
VARS = ['s16bq01', 's16bq02', 's16bq03a', 's16bq03b', 's16bq04', 's16bq05', 's16bq06',
        's16bq07a', 's16bq07b', 's16bq08', 's16bq09a', 's16bq09b', 's16bq09c']
root = countries_root()
out = {}

def _j(x):
    if isinstance(x, dict):
        return {str(k): _j(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_j(v) for v in x]
    if pd.isna(x) if not isinstance(x, (list, dict)) else False:
        return None
    try:
        return x.item()
    except Exception:
        return x if isinstance(x, (int, float, str, bool)) else str(x)

for (c, w), rel in CELLS.items():
    key = f'{c}/{w}'
    rec = out[key] = {}
    try:
        p = get_data_file(f'{c}/{w}/{rel}')
        rec['path'] = str(p)
        with pd.io.stata.StataReader(p) as rd:
            vl = rd.variable_labels()
            vals = rd.value_labels()
            lbl = rd._lbllist if hasattr(rd, '_lbllist') else None
            names = rd._varlist if hasattr(rd, '_varlist') else None
        lblmap = dict(zip(names, lbl)) if names and lbl else {}
        rec['variable_labels'] = {v: vl.get(v) for v in VARS if v in vl}
        rec['value_labels'] = {v: _j(vals.get(lblmap.get(v), {})) for v in VARS
                               if v in vl and v not in ('s16bq01',)}
        lab = get_dataframe(str(root / c / w / rel), convert_categoricals=True)
        cod = get_dataframe(str(root / c / w / rel), convert_categoricals=False)
        rec['rows'] = len(cod)
        app = (cod['s16bq02'] == 1).to_numpy()
        rec['applied'] = int(app.sum())
        la, co = lab[app], cod[app]
        rec['applied_u_code_na'] = int(co['s16bq03b'].isna().sum())
        rec['applied_u_label_na'] = int(la['s16bq03b'].isna().sum())
        ulab = la['s16bq03b'].astype('string')
        rec['applied_u_labels'] = {str(k): int(v) for k, v in ulab.value_counts(dropna=False).items()}
        rec['applied_u_manquant_like'] = int(ulab.str.lower().str.contains('manq|miss|absent', na=False).sum())
        # missing used unit by quantity presence
        q = pd.to_numeric(co['s16bq03a'], errors='coerce')
        miss = co['s16bq03b'].isna()
        rec['applied_u_na_by_qty'] = {'qty_na': int((miss & q.isna()).sum()),
                                      'qty_zero': int((miss & (q == 0)).sum()),
                                      'qty_pos': int((miss & (q > 0)).sum())}
        for v in ['s16bq05', 's16bq08']:
            if v in la:
                rec[f'applied_{v}'] = {str(k): int(n) for k, n in la[v].astype('string').value_counts(dropna=False).items()}
        for v in ['s16bq07a', 's16bq09a', 's16bq09c']:
            if v in co:
                s = pd.to_numeric(co[v], errors='coerce')
                rec[f'applied_{v}_nonnull'] = int(s.notna().sum())
                rec[f'applied_{v}_sum'] = float(s.sum())
        for v in ['s16bq07b', 's16bq09b']:
            if v in la:
                rec[f'applied_{v}'] = {str(k): int(n) for k, n in la[v].astype('string').value_counts(dropna=False).items()}
        if 's16bq05' in la and 's16bq08' in la:
            ct = pd.crosstab(la['s16bq05'].astype('string').fillna('<NA>'),
                             la['s16bq08'].astype('string').fillna('<NA>'))
            rec['crosstab_q05_rows_q08_cols'] = {r: {cc: int(ct.loc[r, cc]) for cc in ct.columns} for r in ct.index}
            # consistency: gift qty present only when q05 Oui? purchase qty only when q08 Oui?
            q7 = pd.to_numeric(co['s16bq07a'], errors='coerce')
            q9 = pd.to_numeric(co['s16bq09a'], errors='coerce') if 's16bq09a' in co else None
            rec['q07a_nonnull_by_q05'] = {str(k): int(n) for k, n in q7.notna().groupby(la['s16bq05'].astype('string').fillna('<NA>')).sum().items()}
            if q9 is not None:
                rec['q09a_nonnull_by_q08'] = {str(k): int(n) for k, n in q9.notna().groupby(la['s16bq08'].astype('string').fillna('<NA>')).sum().items()}
            # hollow under old and new bindings
            old_hollow = q.isna() & la['s16bq05'].isna() & q7.isna()
            rec['hollow_old_binding'] = int(old_hollow.sum())
            if q9 is not None:
                new_hollow = (q.isna() & la['s16bq05'].isna() & q7.isna() & la['s16bq08'].isna() & q9.isna()
                              & la['s16bq07b'].isna() & la['s16bq09b'].isna())
                rec['hollow_new_binding'] = int(new_hollow.sum())
                rec['hollow_used_purchase_only'] = int((q.isna() & la['s16bq08'].isna() & q9.isna()).sum())
    except Exception as e:
        rec['error'] = f'{type(e).__name__}: {e}'

# Senegal s16c crop labels (2018-19) for maize / cowpea, and harmonize_food result.
try:
    from lsms_library.local_tools import get_categorical_mapping
    rel = 'Senegal/2018-19/Data/s16c_me_sen2018.dta'
    c16 = get_dataframe(str(root / rel), convert_categoricals=True)
    crop_cols = [col for col in c16.columns if col.startswith('s16cq04')]
    rec = out['Senegal/2018-19/s16c'] = {'crop_cols': crop_cols}
    for col in crop_cols:
        vc = c16[col].astype('string').value_counts(dropna=False)
        rec[col] = {str(k): int(v) for k, v in vc.items()}
except Exception as e:
    out['Senegal/2018-19/s16c'] = {'error': f'{type(e).__name__}: {e}'}

json.dump(out, open(sys.argv[1], 'w'), indent=1, ensure_ascii=False, default=str)
print('wrote', sys.argv[1])
