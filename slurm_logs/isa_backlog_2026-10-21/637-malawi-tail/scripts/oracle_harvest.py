"""Raw-source oracle for crop_production Quantity (zero-from-blank + 2010-11 repeats).

Reads raw Module G / P with pyreadstat, decodes keys with its own parse of the
org tables, and computes the served Quantity at (i, plot_id, crop, u, condition)
as ONE sum over the raw records with min_count=1 (NA when no record carries a
quantity).  Optionally drops 2010-11 exact repeat records first.  Compares with
<data_root>/Malawi/<wave>/_/crop_production.parquet.
Usage: oracle_harvest.py <data_root> <checkout> <dedupe:0|1> <min_count:0|1> <out.json>
"""
import json, re, sys
from pathlib import Path
import numpy as np, pandas as pd, pyreadstat
from lsms_library.data_access import get_data_file
from lsms_library.local_tools import format_id

data_root, checkout = Path(sys.argv[1]), Path(sys.argv[2])
dedupe, minc, outp = sys.argv[3] == '1', int(sys.argv[4]), sys.argv[5]
ORG = checkout / 'lsms_library/countries/Malawi/_/categorical_mapping.org'

def org_table(name):
    lines = ORG.read_text().splitlines()
    i = next(k for k, l in enumerate(lines) if l.strip().lower() == f'#+name: {name}')
    rows = []
    for l in lines[i + 1:]:
        if not l.strip().startswith('|'):
            break
        if re.match(r'^\|[-+]+\|?$', l.strip()):
            continue
        rows.append([c.strip() for c in l.strip().strip('|').split('|')])
    return pd.DataFrame(rows[1:], columns=rows[0])

def cmap(name):
    t = org_table(name)
    return {int(c): (l or None) for c, l in zip(t['Code'], t['Preferred Label'])}
CROP, UNIT, COND = cmap('harmonize_crop'), cmap('harmonize_crop_unit'), cmap('harmonize_crop_condition')
RECODE = {1100: 1001, 1021: 1018}
NA = '<NA>'

def code(x):
    return int(x) if pd.notna(x) and float(x).is_integer() else None

def rd(rel):
    df, _ = pyreadstat.read_dta(str(get_data_file(rel)), apply_value_formats=False)
    if dedupe and '/2010-11/' in rel:
        df = df[~df.duplicated(keep='first')]
    return df

def garden(df):
    return df['gardenid'].apply(format_id) + '_' + df['plotid'].apply(format_id)

def block(df, hh, plot, crop_col, qty, unit, cond, perennial):
    c = pd.to_numeric(df[crop_col], errors='coerce').map(code)
    if perennial:
        c = c.map(lambda x: RECODE.get(x + 1000, x + 1000) if x is not None else None)
    lab = c.map(lambda x: CROP.get(x) if x is not None else None)
    u = pd.to_numeric(df[unit], errors='coerce').map(lambda x: UNIT.get(code(x)) if code(x) is not None else None)
    if cond:
        cd = pd.to_numeric(df[cond], errors='coerce').map(lambda x: COND.get(code(x), 'unknown_condition') if code(x) is not None else 'unknown_condition')
    else:
        cd = pd.Series('unknown_condition', index=df.index)
    pl = plot(df)
    out = pd.DataFrame({'i': hh(df).astype(str), 'plot_id': pl.astype(object).where(pl.notna(), NA).astype(str),
                        'crop': lab, 'u': u.fillna(NA), 'condition': cd,
                        'Quantity': pd.to_numeric(df[qty], errors='coerce')})
    return out[out['crop'].notna()]

def fid(col):
    return lambda d: d[col].apply(format_id)

def cs(prefix, col):
    return lambda d: prefix + d[col].apply(format_id)

SPEC = {
 '2010-11': [('Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta', 'Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_p.dta',
              fid('case_id'), fid('ag_g0b'), fid('ag_p0b'), 'ag_g0d', 'ag_p0d')],
 '2013-14': [('Malawi/2013-14/Data/AG_MOD_G_13.dta', 'Malawi/2013-14/Data/AG_MOD_P_13.dta',
              fid('y2_hhid'), fid('ag_g00'), fid('ag_p00'), 'ag_g0b', 'ag_p0c')],
 '2016-17': [('Malawi/2016-17/Data/Cross_Sectional/ag_mod_g.dta', 'Malawi/2016-17/Data/Cross_Sectional/ag_mod_p.dta',
              cs('cs-17-', 'case_id'), garden, garden, 'crop_code', None),
             ('Malawi/2016-17/Data/Panel/ag_mod_g_16.dta', 'Malawi/2016-17/Data/Panel/ag_mod_p_16.dta',
              fid('y3_hhid'), garden, garden, 'crop_code', None)],
 '2019-20': [('Malawi/2019-20/Data/Cross_Sectional/ag_mod_g.dta', 'Malawi/2019-20/Data/Cross_Sectional/ag_mod_p.dta',
              fid('case_id'), garden, garden, 'crop_code', None),
             ('Malawi/2019-20/Data/Panel/ag_mod_g_19.dta', 'Malawi/2019-20/Data/Panel/ag_mod_p_19.dta',
              fid('y4_hhid'), garden, garden, 'crop_code', None)],
}
res = {}
for w, halves in SPEC.items():
    parts = []
    for gf, pf, hh, gplot, pplot, gcrop, pcrop in halves:
        g, p = rd(gf), rd(pf)
        pcrop = pcrop or ('ag_p0c' if 'ag_p0c' in p.columns else 'crop_code')
        parts.append(block(g, hh, gplot, gcrop, 'ag_g13a', 'ag_g13b', 'ag_g13c', False))
        parts.append(block(p, hh, pplot, pcrop, 'ag_p09a', 'ag_p09b', None, True))
    R = pd.concat(parts, ignore_index=True)
    K = ['i', 'plot_id', 'crop', 'u', 'condition']
    exp = R.groupby(K)['Quantity'].sum(min_count=minc).rename('Quantity').reset_index()
    built = pd.read_parquet(data_root / 'Malawi' / w / '_' / 'crop_production.parquet').reset_index()
    for c in ('u', 'plot_id'):
        built[c] = built[c].astype(object).where(built[c].notna(), NA).astype(str)
    built['condition'] = built['condition'].astype(str)
    m = exp.merge(built[K + ['Quantity']], on=K, how='outer', suffixes=('_exp', '_got'), indicator=True)
    b = m[m['_merge'] == 'both']
    a1, a2 = pd.to_numeric(b['Quantity_exp']).astype(float), pd.to_numeric(b['Quantity_got']).astype(float)
    mism = ~((a1.isna() & a2.isna()) | (np.isclose(a1.fillna(-1), a2.fillna(-2)) & (a1.isna() == a2.isna())))
    r = dict(oracle_rows=int(len(exp)), built_rows=int(len(built)),
             only_oracle=int((m['_merge'] == 'left_only').sum()), only_built=int((m['_merge'] == 'right_only').sum()),
             quantity_mismatch=int(mism.sum()), oracle_na=int(exp['Quantity'].isna().sum()),
             built_na=int(pd.to_numeric(built['Quantity']).isna().sum()),
             oracle_sum=float(exp['Quantity'].sum()), built_sum=float(pd.to_numeric(built['Quantity']).sum()),
             raw_records=int(len(R)))
    res[w] = r
    print(w, r, flush=True)
json.dump(res, open(outp, 'w'), indent=1)
