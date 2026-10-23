"""Raw-source oracle for plot_inputs SEED rows (defect B + 2010-11 G repeats).

Independent of assemble_plot_inputs: reads raw Module G (seed planted) and
Module H (seed purchased) with pyreadstat, parses harmonize_crop from the org
file itself, and derives the expected wave-level seed rows:
  Quantity           = sum of ag_g04a over the raw G records of (i, plot, crop, u)
                       (bare sum: an all-NA group is 0.0, the #1174 residual, untouched)
  Quantity_purchased = (i, crop) purchase total, attached only where the (i, crop)
                       seed sits on exactly one plot.
Compared with <data_root>/Malawi/<wave>/_/plot_inputs.parquet (pre-finalize).
Usage: oracle_seed.py <data_root> <checkout> <dedupe_2010_G:0|1> <out.json>
"""
import json, re, sys
from pathlib import Path
import numpy as np, pandas as pd, pyreadstat
from lsms_library.data_access import get_data_file
from lsms_library.local_tools import format_id

data_root, checkout, dedupe, outp = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3] == '1', sys.argv[4]

def org_table(path, name):
    lines = Path(path).read_text().splitlines()
    i = next(k for k, l in enumerate(lines) if l.strip().lower() == f'#+name: {name}')
    rows = []
    for l in lines[i + 1:]:
        if not l.strip().startswith('|'):
            break
        if re.match(r'^\|[-+]+\|?$', l.strip()):
            continue
        rows.append([c.strip() for c in l.strip().strip('|').split('|')])
    hdr, body = rows[0], rows[1:]
    return pd.DataFrame(body, columns=hdr)

hc = org_table(checkout / 'lsms_library/countries/Malawi/_/categorical_mapping.org', 'harmonize_crop')
CROP = {int(c): (l if l else None) for c, l in zip(hc['Code'], hc['Preferred Label'])}
SEEDU = {1: 'Gram', 2: 'Kilogram', 3: '2 kg Bag', 4: '3 kg Bag', 5: '3.7 kg Bag', 6: '5 kg Bag',
         7: '10 kg Bag', 8: '50 kg Bag', 9: 'Other (Specify)'}

def rd(rel):
    df, _ = pyreadstat.read_dta(str(get_data_file(rel)), apply_value_formats=False)
    return df

def crop_of(s):
    c = pd.to_numeric(s, errors='coerce')
    return c.map(lambda x: CROP.get(int(x)) if pd.notna(x) else None)

def plot_garden(df):
    return df['gardenid'].apply(format_id) + '_' + df['plotid'].apply(format_id)

HALVES = {
 '2010-11': [dict(g='Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta', h='Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_h.dta',
                  hh=lambda d: d['case_id'].apply(format_id), plot=lambda d: d['ag_g0b'].apply(format_id), gcrop='ag_g0d', hcrop='ag_h0b',
                  q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
 '2013-14': [dict(g='Malawi/2013-14/Data/AG_MOD_G_13.dta', h='Malawi/2013-14/Data/AG_MOD_H_13.dta',
                  hh=lambda d: d['y2_hhid'].apply(format_id), plot=lambda d: d['ag_g00'].apply(format_id), gcrop='ag_g0b', hcrop='ag_h0c',
                  q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
 '2016-17': [dict(g='Malawi/2016-17/Data/Cross_Sectional/ag_mod_g.dta', h='Malawi/2016-17/Data/Cross_Sectional/ag_mod_h.dta',
                  hh=lambda d: 'cs-17-' + d['case_id'].apply(format_id), plot=plot_garden, gcrop='crop_code', hcrop='crop_code',
                  q=['ag_h07a', 'ag_h38a'], v=['ag_h09', 'ag_h40']),
             dict(g='Malawi/2016-17/Data/Panel/ag_mod_g_16.dta', h='Malawi/2016-17/Data/Panel/ag_mod_h_16.dta',
                  hh=lambda d: d['y3_hhid'].apply(format_id), plot=plot_garden, gcrop='crop_code', hcrop='crop_code',
                  q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
 '2019-20': [dict(g='Malawi/2019-20/Data/Cross_Sectional/ag_mod_g.dta', h='Malawi/2019-20/Data/Cross_Sectional/ag_mod_h.dta',
                  hh=lambda d: d['case_id'].apply(format_id), plot=plot_garden, gcrop='crop_code', hcrop='crop_code',
                  q=['ag_h07a', 'ag_h38a'], v=['ag_h09', 'ag_h40']),
             dict(g='Malawi/2019-20/Data/Panel/ag_mod_g_19.dta', h='Malawi/2019-20/Data/Panel/ag_mod_h_19.dta',
                  hh=lambda d: d['y4_hhid'].apply(format_id), plot=plot_garden, gcrop='crop_code', hcrop='crop_code',
                  q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
}
NA = '<NA>'
res = {}
for w, halves in HALVES.items():
    seeds, purch = [], []
    for H in halves:
        g = rd(H['g'])
        if dedupe and w == '2010-11':
            g = g[~g.duplicated(keep='first')]
        pl = H['plot'](g)
        s = pd.DataFrame({'i': H['hh'](g).astype(str), 'plot_id': pl.astype(object).where(pl.notna(), NA).astype(str),
                          'crop': crop_of(g[H['gcrop']]),
                          'u': pd.to_numeric(g['ag_g04b'], errors='coerce').round().map(SEEDU),
                          'Quantity': pd.to_numeric(g['ag_g04a'], errors='coerce')})
        seeds.append(s[s['crop'].notna()])
        h = rd(H['h'])
        if dedupe and w == '2010-11':
            h = h[~h.duplicated(keep='first')]
        tot = []
        for qc, vc in zip(H['q'], H['v']):
            if qc not in h.columns:
                continue
            q = pd.to_numeric(h[qc], errors='coerce')
            v = pd.to_numeric(h[vc], errors='coerce') if vc in h.columns else pd.Series(np.nan, index=h.index)
            tot.append(q.where(v.notna() & (v != 0)))
        p = pd.DataFrame({'i': H['hh'](h).astype(str), 'crop': crop_of(h[H['hcrop']]),
                          'qp': pd.concat(tot, axis=1).sum(axis=1, min_count=1)})
        purch.append(p[p['crop'].notna()])
    S = pd.concat(seeds, ignore_index=True)
    P = pd.concat(purch, ignore_index=True)
    P = P.groupby(['i', 'crop'])['qp'].sum(min_count=1).dropna()            # (i, crop) grain
    S['u'] = S['u'].fillna(NA)
    exp = S.groupby(['i', 'plot_id', 'crop', 'u'])['Quantity'].sum().rename('Quantity').reset_index()
    npl = S[S['plot_id'] != NA].groupby(['i', 'crop'])['plot_id'].nunique()
    single = npl[npl == 1].index
    exp['Quantity_purchased'] = [P.get((i, c), np.nan) if (i, c) in single else np.nan
                                 for i, c in zip(exp['i'], exp['crop'])]
    built = pd.read_parquet(data_root / 'Malawi' / w / '_' / 'plot_inputs.parquet').reset_index()
    built = built[built['input'] == 'Seed'].copy()
    built['u'] = built['u'].astype(object).where(built['u'].notna(), NA)
    built['plot_id'] = built['plot_id'].astype(object).where(built['plot_id'].notna(), NA).astype(str)
    K = ['i', 'plot_id', 'crop', 'u']
    m = exp.merge(built[K + ['Quantity', 'Quantity_purchased']], on=K, how='outer',
                  suffixes=('_exp', '_got'), indicator=True)
    both = m[m['_merge'] == 'both']
    def neq(a, b):
        a, b = pd.to_numeric(a), pd.to_numeric(b)
        return ~((a.isna() & b.isna()) | (np.isclose(a.fillna(-1), b.fillna(-2))))
    r = dict(oracle_rows=int(len(exp)), built_seed_rows=int(len(built)),
             only_oracle=int((m['_merge'] == 'left_only').sum()), only_built=int((m['_merge'] == 'right_only').sum()),
             quantity_mismatch=int(neq(both['Quantity_exp'], both['Quantity_got']).sum()),
             purchased_mismatch=int(neq(both['Quantity_purchased_exp'], both['Quantity_purchased_got']).sum()),
             oracle_seed_qty=float(exp['Quantity'].sum()), built_seed_qty=float(pd.to_numeric(built['Quantity']).sum()),
             oracle_qp_total=float(pd.to_numeric(exp['Quantity_purchased']).sum()),
             built_qp_total=float(pd.to_numeric(built['Quantity_purchased']).sum()),
             oracle_qp_rows=int(exp['Quantity_purchased'].notna().sum()),
             built_qp_rows=int(built['Quantity_purchased'].notna().sum()),
             purchase_ic_with_2plus_codes=None)
    res[w] = r
    print(w, r, flush=True)
json.dump(res, open(outp, 'w'), indent=1)
