"""Base-only raw census for the burkina stream (counts only; no ids, no coordinates).

Run with cwd = a clone, PYTHONPATH = that clone, LSMS_DATA_DIR = a private empty root.
Writes JSON to sys.argv[1].

Sections:
  q3   #1118  plot_inputs u == 'Manquant' per wave (served, cold) and the raw unit labels.
  q7   #1045  the six s00 visit stamps per cover file: patterns, blanks, what coerce nulls.
  q6   #1436/#1437/#824  s16c / s16d 2021-22 shape, fill, keys, labels, sale attachment
       counts, and the harvest_totals rule run as a MEASUREMENT only.
"""
import json
import re
import sys
import warnings
from collections import Counter

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library.local_tools import get_dataframe, get_categorical_mapping, format_id
from lsms_library.paths import countries_root
from lsms_library import data_access

C = 'Burkina_Faso'
ROOT = countries_root() / C
OUT = {}


def save():
    with open(sys.argv[1], 'w') as fh:
        json.dump(OUT, fh, indent=1, default=str, sort_keys=True)


def labels_of(path):
    """Variable labels of a held .dta, through the sanctioned local fetch."""
    rel = str(path.relative_to(countries_root()))
    p = data_access.get_data_file(rel)
    if p is None:
        return {}
    with pd.io.stata.StataReader(str(p)) as r:
        return r.variable_labels()


# ----------------------------------------------------------------------------- q3
q3 = {}
try:
    c = ll.Country(C)
    pi = c.plot_inputs()
    u = pi.index.get_level_values('u').astype('string')
    t = pi.index.get_level_values('t').astype('string')
    q3['served_rows_by_t'] = {k: int(v) for k, v in pd.Series(t).value_counts().items()}
    q3['served_manquant_by_t'] = {k: int(v) for k, v in pd.Series(t[u == 'Manquant']).value_counts().items()}
    q3['served_unknown_by_t'] = {k: int(v) for k, v in pd.Series(t[u == 'Unknown']).value_counts().items()}
    q3['served_u_values'] = sorted(set(u.dropna()))
except Exception as e:  # noqa: BLE001
    q3['served_err'] = f'{type(e).__name__}: {e}'[:500]
for wave, fn in (('2018-19', 's16b_me_bfa2018.dta'), ('2021-22', 's16b_me_bfa2021.dta')):
    try:
        path = ROOT / wave / 'Data' / fn
        lab = get_dataframe(str(path), convert_categoricals=True)
        num = get_dataframe(str(path), convert_categoricals=False)
        applied = (pd.to_numeric(num['s16bq02'], errors='coerce') == 1).to_numpy()
        rec = {'rows': int(len(num)), 'applied': int(applied.sum())}
        for col in ('s16bq03b', 's16bq07b', 's16bq09b'):
            s = lab[col].astype('string')
            rec[col] = {
                'labels': sorted(set(s.dropna())),
                'has_manquant_label': bool(s.str.contains('anquant', case=False, na=False).any()),
                'null_applied': int(s[applied].isna().sum()),
                'codes_null_applied': int(pd.to_numeric(num[col], errors='coerce')[applied].isna().sum()),
            }
        rec['qty_used_present_when_unit_null'] = int(
            (pd.to_numeric(num['s16bq03a'], errors='coerce').notna()
             & pd.to_numeric(num['s16bq03b'], errors='coerce').isna())[applied].sum())
        q3[wave] = rec
    except Exception as e:  # noqa: BLE001
        q3[wave] = f'ERR {type(e).__name__}: {e}'[:500]
OUT['q3'] = q3
save()
print('q3 done', flush=True)

# ----------------------------------------------------------------------------- q7
ISO = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')
q7 = {}
cols = ['s00q23a', 's00q23b', 's00q24a', 's00q24b', 's00q25a', 's00q25b']
for wave, fn in (('2018-19', 's00_me_bfa2018.dta'), ('2021-22', 's00_me_bfa2021.dta'),
                 ('2021-22', 'Cross_Section/s00_me_bfa2021.dta')):
    key = f'{wave}/{fn}'
    try:
        path = ROOT / wave / 'Data' / fn
        df = get_dataframe(str(path))
        labs = labels_of(path)
        rec = {'rows': int(len(df))}
        for col in cols:
            s = df[col]
            r = {'dtype': str(s.dtype), 'label': labs.get(col)}
            o = s.astype(object)
            blank = o.map(lambda v: v is None or (isinstance(v, float) and np.isnan(v))
                          or (isinstance(v, str) and v.strip() == '')).astype(bool)
            txt = o.where(~blank).map(lambda v: str(v).strip(), na_action='ignore')
            iso = txt.map(lambda v: bool(ISO.match(v)), na_action='ignore').fillna(False).astype(bool)
            other = (~blank) & (~iso)
            r['blank'] = int(blank.sum())
            r['iso'] = int(iso.sum())
            r['other'] = int(other.sum())
            # Non-ISO values are survey sentinels or formats, not personal data: counted with
            # their spelling (capped) so the spec can name them.
            r['other_values'] = dict(Counter(txt[other].tolist()).most_common(12))
            coerce = pd.to_datetime(s, errors='coerce')
            r['coerce_null'] = int(coerce.isna().sum())
            r['coerce_dtype'] = str(coerce.dtype)
            fmt = pd.to_datetime(txt.where(iso), format='%Y-%m-%dT%H:%M:%S', errors='coerce')
            r['iso_unparseable'] = int((iso & fmt.isna()).sum())
            both = coerce.notna() & fmt.notna()
            r['coerce_vs_format_differ'] = int((both & (coerce != fmt)).sum())
            r['coerce_not_format'] = int((coerce.notna() & fmt.isna()).sum())
            if fmt.notna().any():
                r['min'] = str(fmt.min())
                r['max'] = str(fmt.max())
                r['by_month'] = {str(k): int(v) for k, v in fmt.dt.to_period('M').value_counts().sort_index().items()}
            rec[col] = r
        q7[key] = rec
    except Exception as e:  # noqa: BLE001
        q7[key] = f'ERR {type(e).__name__}: {e}'[:500]
try:
    idt = ll.Country(C).interview_date()
    flat = idt.reset_index()
    q7['served'] = {
        'rows_by_t': {str(k): int(v) for k, v in flat['t'].value_counts().items()},
        'dtypes': {str(k): str(v) for k, v in flat.dtypes.items()},
        'nat': {c_: int(flat[c_].isna().sum()) for c_ in ('Interview start', 'Interview end') if c_ in flat},
        'visit_by_t': {f'{a}/{b}': int(n) for (a, b), n in flat.groupby(['t', 'visit']).size().items()},
    }
    from lsms_library.datetime_parse import datetime_reports
    q7['datetime_reports'] = [dict((k, v) for k, v in dict(r).items() if k != 'examples')
                              for r in datetime_reports(country=C)]
except Exception as e:  # noqa: BLE001
    q7['served_err'] = f'{type(e).__name__}: {e}'[:500]
OUT['q7'] = q7
save()
print('q7 done', flush=True)

# ----------------------------------------------------------------------------- q6
q6 = {}
try:
    w = ROOT / '2021-22' / 'Data'
    sc = get_dataframe(str(w / 's16c_me_bfa2021.dta'), convert_categoricals=True)
    scn = get_dataframe(str(w / 's16c_me_bfa2021.dta'), convert_categoricals=False)
    sd = get_dataframe(str(w / 's16d_me_bfa2021.dta'), convert_categoricals=True)
    sdn = get_dataframe(str(w / 's16d_me_bfa2021.dta'), convert_categoricals=False)
    lc, ld = labels_of(w / 's16c_me_bfa2021.dta'), labels_of(w / 's16d_me_bfa2021.dta')
    q6['s16c_shape'] = list(sc.shape)
    q6['s16d_shape'] = list(sd.shape)
    q6['s16c_labels'] = {k: v for k, v in lc.items()}
    q6['s16d_labels'] = {k: v for k, v in ld.items()}
    crop_row = sc['s16cq04'].notna()
    q6['s16c_rows_with_crop'] = int(crop_row.sum())
    q6['s16c_rows_no_crop'] = int((~crop_row).sum())
    sc1, scn1 = sc[crop_row], scn[crop_row]
    q6['s16c_plot_key_null'] = int(scn1[['s16cq02', 's16cq03']].isna().any(axis=1).sum())
    key = ['grappe', 'menage', 's16cq02', 's16cq03', 's16cq04']
    q6['s16c_dup_on_hh_field_parcel_crop'] = int(scn1.duplicated(key).sum())
    q6['s16c_exact_dup_rows'] = int(scn1.duplicated().sum())
    fin = sc1['s16cq11'].astype('string').str.strip()
    q6['finished_values'] = {str(k): int(v) for k, v in fin.value_counts(dropna=False).items()}
    qa = pd.to_numeric(scn1['s16cq16a'], errors='coerce')
    q6['s16cq16a_filled_by_finished'] = {str(k): int(v) for k, v in qa.notna().groupby(fin.fillna('<NA>')).sum().items()}
    q6['s16cq16a_positive_by_finished'] = {str(k): int(v) for k, v in qa.gt(0).groupby(fin.fillna('<NA>')).sum().items()}
    q6['s16cq16a_zero'] = int(qa.eq(0).sum())
    for col in ('s16cq16b', 's16cq16c', 's16cq16d'):
        if col in scn1:
            v = scn1[col]
            q6[f'{col}_filled_by_finished'] = {str(k): int(n) for k, n in v.notna().groupby(fin.fillna('<NA>')).sum().items()}
    q6['s16cq16b_labels'] = {str(k): int(v) for k, v in sc1['s16cq16b'].astype('string').value_counts(dropna=False).items()}
    q6['s16cq16d_codes'] = {str(k): int(v) for k, v in pd.to_numeric(scn1['s16cq16d'], errors='coerce').value_counts(dropna=False).items()}
    q6['s16cq16d_labels'] = {str(k): int(v) for k, v in sc1['s16cq16d'].astype('string').value_counts(dropna=False).items()}
    q6['s16cq16c_positive'] = int(pd.to_numeric(scn1['s16cq16c'], errors='coerce').gt(0).sum())
    q6['s16cq16c_positive_with_16a'] = int((pd.to_numeric(scn1['s16cq16c'], errors='coerce').gt(0) & qa.notna()).sum())
    q6['s16cq07_values'] = {str(k): int(v) for k, v in sc1['s16cq07'].astype('string').value_counts(dropna=False).items()}
    # #1437 diagnosis: which s16c columns are filled on FINISHED rows at all?
    fill = {}
    for col in scn1.columns:
        if not str(col).startswith('s16c'):
            continue
        nn = scn1[col].notna()
        fill[col] = {str(k): int(n) for k, n in nn.groupby(fin.fillna('<NA>')).sum().items()}
    q6['s16c_fill_by_finished'] = fill
    # crop / unit labels against the maps
    crop_map = get_categorical_mapping(tablename='harmonize_food', idxvars='Original Label',
                                       dirs=[str(ROOT / '_') + '/'],
                                       **{'Preferred Label': 'Preferred Label'})
    unit_map = get_categorical_mapping(tablename='u', idxvars='Original Label',
                                       dirs=[str(ROOT / '_') + '/'],
                                       **{'Preferred Label': 'Preferred Label'})
    cl = sc1['s16cq04'].astype('string').str.strip()
    q6['s16c_crop_labels_unmapped'] = {k: int(v) for k, v in cl[~cl.isin(crop_map)].value_counts().items()}
    q6['s16c_crop_labels_n'] = int(cl.nunique())
    ul = sc1['s16cq16b'].astype('string').str.strip().dropna()
    q6['s16c_unit_labels_unmapped'] = {k: int(v) for k, v in ul[~ul.isin(unit_map)].value_counts().items()}
    # s16d
    q6['s16d_exact_dup_rows'] = int(sdn.duplicated().sum())
    sdd = sd[~sdn.duplicated()]
    sddn = sdn[~sdn.duplicated()]
    q6['s16d_dup_hh_crop_after_exact'] = int(sddn.duplicated(['grappe', 'menage', 's16dq01']).sum())
    q6['s16dq04_values'] = {str(k): int(v) for k, v in sdd['s16dq04'].astype('string').value_counts(dropna=False).items()}
    qs = pd.to_numeric(sddn['s16dq05a'], errors='coerce')
    vs = pd.to_numeric(sddn['s16dq06'], errors='coerce')
    sale = qs.gt(0) | vs.gt(0)
    q6['sales'] = int(sale.sum())
    q6['sale_value_sum_raw_all'] = float(pd.to_numeric(sdn['s16dq06'], errors='coerce').sum())
    q6['sale_value_sum_dedup'] = float(vs.sum())
    q6['sale_value_sum_dedup_sales'] = float(vs[sale].sum())
    q6['sale_qty_sum_dedup_sales'] = float(qs[sale].sum())
    q6['sales_qty_only'] = int((qs.gt(0) & ~vs.gt(0)).sum())
    q6['sales_value_only'] = int((~qs.gt(0) & vs.gt(0)).sum())
    q6['sale_unit_labels'] = {str(k): int(v) for k, v in sdd.loc[sale.to_numpy(), 's16dq05b'].astype('string').value_counts(dropna=False).items()}
    q6['s16dq05c_positive_on_sales'] = int(pd.to_numeric(sddn['s16dq05c'], errors='coerce')[sale].gt(0).sum())
    q6['s16dq05d_filled_on_sales'] = int(sddn['s16dq05d'][sale].notna().sum()) if 's16dq05d' in sddn else None
    dl = sdd['s16dq01'].astype('string').str.strip()
    q6['s16d_crop_labels_unmapped'] = {k: int(v) for k, v in dl[~dl.isin(crop_map)].value_counts().items()}
    q6['s16d_crop_labels_not_in_s16c'] = {k: int(v) for k, v in dl[~dl.isin(set(cl))].value_counts().items()}
    # household ids: country i (grappe + menage.zfill(3)), as plot_inputs 2021-22 does
    from importlib import util
    spec = util.spec_from_file_location('bfa', ROOT / '_' / 'burkina_faso.py')
    bfa = util.module_from_spec(spec)
    sys.path.insert(0, str(ROOT / '_'))
    spec.loader.exec_module(bfa)
    hh_c = [bfa.i(pd.Series([g, m])) for g, m in zip(scn1['grappe'], scn1['menage'])]
    hh_d = [bfa.i(pd.Series([g, m])) for g, m in zip(sddn['grappe'], sddn['menage'])]
    crop_c = cl.map(lambda x: crop_map.get(x, x)).astype('string')
    crop_d = dl.map(lambda x: crop_map.get(x, x)).astype('string')
    lines = pd.DataFrame({'i': hh_c, 'crop': crop_c.to_numpy()}).groupby(['i', 'crop']).size()
    sk = pd.MultiIndex.from_arrays([np.asarray(hh_d, dtype=object)[sale.to_numpy()], crop_d.to_numpy()[sale.to_numpy()]])
    n = pd.Series(lines.reindex(sk).to_numpy()).fillna(0)
    q6['sale_lines_dist'] = {'0': int((n == 0).sum()), '1': int((n == 1).sum()), '2': int((n == 2).sum()), '3+': int((n >= 3).sum())}
    q6['harvest_pairs_lines_dist'] = {str(k): int(v) for k, v in lines.value_counts().sort_index().items()}
    # households against served sample() 2021-22 (after the walk)
    samp = ll.Country(C).sample(waves=['2021-22'])
    s_i = set(samp.index.get_level_values('i').astype(str))
    upd = ll.Country(C).updated_ids
    walk = {}
    for k_, m_ in (upd or {}).items():
        if str(k_) == '2021-22' and isinstance(m_, dict):
            walk = {str(a): str(b) for a, b in m_.items()}
    hw = {h: walk.get(h, h) for h in set(hh_c)}
    q6['s16c_households'] = len(hw)
    q6['s16c_households_orphan_after_walk'] = sum(1 for v in hw.values() if v not in s_i)
    q6['s16c_households_orphan_native'] = sum(1 for h in hw if h not in s_i)
    q6['updated_ids_2021_entries'] = len(walk)
    # #1437: harvest_totals AS A MEASUREMENT ONLY (not served; not ruled for Burkina)
    from lsms_library.ehcvm import harvest_totals
    disp = sdd[['s16dq02a', 's16dq02b', 's16dq03a', 's16dq03b', 's16dq05a', 's16dq05b', 's16dq13a', 's16dq13b']].copy()
    disp['i'] = hh_d
    disp['crop'] = crop_d.to_numpy()
    harvest_lines = pd.DataFrame({'i': hh_c, 'crop': crop_c.to_numpy(), 'Quantity': qa.to_numpy(),
                                  'finished': fin.to_numpy()})
    try:
        tot = harvest_totals(harvest_lines, disp)
        q6['harvest_totals_pairs'] = int(len(tot))
        q6['harvest_totals_pairs_single_line'] = int(tot['lines'].eq(1).sum())
        q6['harvest_totals_pairs_multi_line'] = int(tot['lines'].gt(1).sum())
        q6['harvest_totals_rows_single_line'] = int(tot.loc[tot['lines'].eq(1), 'lines'].sum())
        q6['harvest_totals_units'] = {str(k): int(v) for k, v in tot['u'].value_counts().items()}
        pairs = harvest_lines.groupby(['i', 'crop']).agg(open=('finished', lambda s: s.astype('string').str.strip().ne('Oui').fillna(True).any()))
        q6['pairs_all'] = int(len(pairs))
        q6['pairs_all_finished'] = int((~pairs['open']).sum())
        long = []
        for var in ('s16dq02', 's16dq03', 's16dq05', 's16dq13'):
            long.append(pd.DataFrame({'i': disp['i'], 'crop': disp['crop'],
                                      'q': pd.to_numeric(disp[f'{var}a'], errors='coerce'),
                                      'u': disp[f'{var}b'].astype('string')}))
        long = pd.concat(long)
        long = long[long['q'].gt(0)]
        nu = long.groupby(['i', 'crop'])['u'].nunique()
        fin_pairs = pairs.index[~pairs['open']]
        nu_f = nu.reindex(fin_pairs)
        q6['finished_pairs_disp_one_unit'] = int(nu_f.eq(1).sum())
        q6['finished_pairs_disp_multi_unit'] = int(nu_f.gt(1).sum())
        q6['finished_pairs_disp_none'] = int(nu_f.isna().sum())
    except Exception as e:  # noqa: BLE001
        q6['harvest_totals_err'] = f'{type(e).__name__}: {e}'[:500]
except Exception as e:  # noqa: BLE001
    import traceback
    q6['err'] = traceback.format_exc()[-2000:]
OUT['q6'] = q6
save()
print('q6 done', flush=True)
