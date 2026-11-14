"""Red team (malawi): an independent oracle from the raw files.

usage: python oracle.py <clone> <framesdir> <outdir>

1. The Module D question-20 join for every 2016-17 / 2019-20 G code-48 line, under the
   ruling's literal words and under the candidate's narrower rule; text inventories.
2. The served candidate rows against it (needs <framesdir>/cand.crop_production.parquet
   and cand.plot_inputs.parquet from build_arm.py).
3. 2010-11 and 2013-14: does Module D carry an Other-crop text, and in 2010-11 does the
   plot's single D text agree with the G line's own ag_g0d_os (a test of the method)?
4. GH #1712: the six Module H columns in every held H file.
5. 2016-17 XS ag_mod_d.dta: does get_dataframe read it whole with encoding='latin1'?
Pure reads; nothing is written outside <outdir>.
"""
import json
import os
import re
import sys
import traceback
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

clone, frames, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
out.mkdir(parents=True, exist_ok=True)
import lsms_library  # noqa: E402
from lsms_library import paths  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(clone) + os.sep)
dr = os.path.realpath(str(paths.data_root()))
assert dr.startswith('/local/job') or '/isa-sm-20261113/private/' in dr, dr
from lsms_library.local_tools import get_dataframe, format_id  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
import pyreadstat  # noqa: E402

root = paths.countries_root()
MW = root / 'Malawi'
res = {}


def norm(s):
    s = pd.Series(s).astype('string').str.strip().str.replace(r'\s+', ' ', regex=True).str.upper()
    return s.where(s.notna() & (s != ''), pd.NA)


def restricted(rel, cols):
    p = get_data_file(rel)
    _, meta = pyreadstat.read_dta(str(p), metadataonly=True)
    use = [c for c in cols if c in meta.column_names]
    df, _ = pyreadstat.read_dta(str(p), usecols=use, apply_value_formats=False)
    return df, meta


def read(rel):
    return get_dataframe(str(root / rel), convert_categoricals=False)


COW = re.compile(r'COW|KHOB|NSEU|MSEW|NKHOB|KHOBWE|C0W|CAW', re.I)
walk_all = json.loads((MW / '_' / 'updated_ids.json').read_text())

# ---------------------------------------------------------------- 5. latin-1 route
try:
    d5 = get_dataframe(str(MW / '2016-17/Data/Cross_Sectional/ag_mod_d.dta'), convert_categoricals=False,
                       encoding='latin1')
    res['latin1_route'] = {'ok': True, 'shape': list(d5.shape)}
except Exception as e:  # noqa: BLE001
    res['latin1_route'] = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:400]}'}
try:
    get_dataframe(str(MW / '2016-17/Data/Cross_Sectional/ag_mod_d.dta'), convert_categoricals=False)
    res['plain_route_2016_xs_d'] = 'ok'
except Exception as e:  # noqa: BLE001
    res['plain_route_2016_xs_d'] = f'{type(e).__name__}: {str(e)[:300]}'
print('latin1', res['latin1_route'], res['plain_route_2016_xs_d'], flush=True)

# ---------------------------------------------------------------- 1. the later-wave join
HALVES = {'2016-17': [('Cross_Sectional', 'ag_mod_g.dta', 'ag_mod_d.dta', 'case_id', 'cs-17-'),
                      ('Panel', 'ag_mod_g_16.dta', 'ag_mod_d_16.dta', 'y3_hhid', '')],
          '2019-20': [('Cross_Sectional', 'ag_mod_g.dta', 'ag_mod_d.dta', 'case_id', 'cs-19-'),
                      ('Panel', 'ag_mod_g_19.dta', 'ag_mod_d_19.dta', 'y4_hhid', '')]}
SLOTS = ['ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e']
oracle_lines = []
res['join'] = {}
for wave, halves in HALVES.items():
    walk = walk_all.get(wave, {})
    for half, gf, df_, hh, prefix in halves:
        key = f'{wave}/{half}'
        rec = {}
        try:
            g = read(f'Malawi/{wave}/Data/{half}/{gf}')
            drel = f'Malawi/{wave}/Data/{half}/{df_}'
            if wave == '2016-17' and half == 'Cross_Sectional':
                _, meta = pyreadstat.read_dta(str(get_data_file(drel)), metadataonly=True)
                dcols = [c for c in meta.column_names if c.startswith('ag_d20') or c in (hh, 'gardenid', 'plotid')]
                d, _ = restricted(drel, dcols)
            else:
                d = read(drel)
            d20 = sorted(c for c in d.columns if c.startswith('ag_d20'))
            rec['d20_columns'] = d20
            texts = [c for c in d20 if re.search(r'(oth|os)$', c)]
            rec['text_columns'] = texts
            slots = [c for c in SLOTS if c in d.columns]
            rec['slots'] = slots
            for x in (g, d):
                x['_h'] = prefix + x[hh].apply(format_id)
                x['_g'] = x['gardenid'].apply(format_id)
                x['_p'] = x['plotid'].apply(format_id)
            K = ['_h', '_g', '_p']
            T = pd.DataFrame({c: norm(d[c]).values for c in texts}, index=d.index)
            d['_ntext'] = T.notna().sum(axis=1)
            d['_text'] = T.apply(lambda r: ' | '.join(v for v in r if pd.notna(v)), axis=1)
            d['_textcols'] = T.apply(lambda r: ','.join(c for c, v in r.items() if pd.notna(v)), axis=1)
            d['_n48'] = sum(pd.to_numeric(d[c], errors='coerce').eq(48).astype(int) for c in slots)
            d['_n49'] = sum(pd.to_numeric(d[c], errors='coerce').eq(49).astype(int) for c in slots)
            d['_nrows'] = d.groupby(K)['_h'].transform('size')
            rec['d_rows_with_slot49'] = int((d['_n49'] > 0).sum())
            rec['d_slots_x_textcols'] = {f'n48={a} n49={b} text={c}': int(v) for (a, b, c), v in
                                         d.loc[(d['_n48'] > 0) | (d['_n49'] > 0) | (d['_ntext'] > 0),
                                               ['_n48', '_n49', '_textcols']].astype(str).value_counts().items()}
            dd = d.drop_duplicates(K)[K + ['_ntext', '_text', '_textcols', '_n48', '_n49', '_nrows']]
            g['_code'] = pd.to_numeric(g['crop_code'], errors='coerce')
            rec['g_code49_lines'] = int(g['_code'].eq(49).sum())
            g['_ng48'] = g.groupby(K)['_code'].transform(lambda s: int(s.eq(48).sum()))
            g48 = g[g['_code'].eq(48)].copy()
            m = g48.merge(dd, on=K, how='left', indicator=True)
            rec['g48_lines'] = int(len(m))
            rec['g48_no_d_row'] = int((m['_merge'] == 'left_only').sum())
            rec['g48_d_rows_gt1'] = int((m['_nrows'] > 1).sum())
            rec['g48_ng48_gt1'] = int((m['_ng48'] > 1).sum())
            lit = (m['_nrows'] == 1) & (m['_ntext'] == 1)
            narrow = lit & (m['_n48'] == 1) & (m['_ng48'] == 1)
            rec['literal_single_text'] = int(lit.sum())
            rec['narrow_single_text'] = int(narrow.sum())
            rec['literal_not_narrow'] = m.loc[lit & ~narrow, ['_text', '_n48', '_ng48']].astype(str).value_counts().to_dict()
            rec['literal_not_narrow'] = {' / '.join(k): int(v) for k, v in rec['literal_not_narrow'].items()}
            rec['ntext_dist'] = {str(k): int(v) for k, v in m['_ntext'].value_counts(dropna=False).items()}
            rec['n48_dist'] = {str(k): int(v) for k, v in m['_n48'].value_counts(dropna=False).items()}
            rec['single_text_from_cols'] = {str(k): int(v) for k, v in m.loc[lit, '_textcols'].value_counts().items()}
            rec['narrow_by_textcol_n49'] = {f'{c} n49={b}': int(v) for (c, b), v in
                                            m.loc[narrow, ['_textcols', '_n49']].astype(str).value_counts().items()}
            rec['narrow_cowpea_by_textcol_n49'] = {f'{c} n49={b}': int(v) for (c, b), v in
                                                   m.loc[narrow & m['_text'].isin(['COWPEAS', 'COW PEAS']),
                                                         ['_textcols', '_n49']].astype(str).value_counts().items()}
            rec['single_texts_top'] = {str(k): int(v) for k, v in m.loc[narrow, '_text'].value_counts().head(80).items()}
            rec['cow_like_texts_on_g48'] = {f'{t} | ntext={n} n48={a} ng48={b}': int(v) for (t, n, a, b), v in
                                            m.loc[m['_text'].astype(str).str.contains(COW, na=False),
                                                  ['_text', '_ntext', '_n48', '_ng48']].astype(str).value_counts().items()}
            # cow-like D texts on plots whose G lines are NOT code 48
            gm = g.merge(dd, on=K, how='left')
            ncow = gm[gm['_text'].astype(str).str.contains(COW, na=False) & ~gm['_code'].eq(48)]
            rec['cow_like_text_on_non48_g_lines'] = {str(k): int(v) for k, v in ncow['_code'].value_counts().items()}
            # all cow-like texts anywhere in D
            rec['cow_like_texts_anywhere_in_d'] = {str(k): int(v) for k, v in
                                                   d.loc[d['_text'].astype(str).str.contains(COW, na=False), '_text'].value_counts().items()}
            sel = m[narrow & m['_text'].isin(['COWPEAS', 'COW PEAS'])].copy()
            rec['oracle_cowpea_lines'] = int(len(sel))
            sel['i'] = sel['_h'].map(lambda x: walk.get(x, x))
            sel['plot_id'] = sel['_g'] + '_' + sel['_p']
            sel['wave'] = wave
            sel['half'] = half
            sel['qty'] = pd.to_numeric(sel['ag_g13a'], errors='coerce')
            oracle_lines.append(sel[['wave', 'half', 'i', 'plot_id', 'qty', '_text']])
            # households: code-48 lines per household and how many are cowpea by the narrow rule
            m['_cow'] = narrow & m['_text'].isin(['COWPEAS', 'COW PEAS'])
            m['i'] = m['_h'].map(lambda x: walk.get(x, x))
            hh_tab = m.groupby('i')['_cow'].agg(['sum', 'size'])
            rec['hh_all_cow'] = int((hh_tab['sum'] == hh_tab['size']).sum())
            rec['hh_some_cow'] = int(((hh_tab['sum'] > 0) & (hh_tab['sum'] < hh_tab['size'])).sum())
            res.setdefault('_hh_tab', {})[key] = hh_tab.reset_index().astype(str).values.tolist()
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['tb'] = traceback.format_exc()[-2500:]
        res['join'][key] = rec
        print(key, {k: v for k, v in rec.items() if k in ('g48_lines', 'literal_single_text', 'narrow_single_text',
                                                          'oracle_cowpea_lines', 'error')}, flush=True)
orc = pd.concat(oracle_lines, ignore_index=True) if oracle_lines else pd.DataFrame()
orc.to_csv(out / 'oracle_cowpea_lines.csv', index=False)

# ---------------------------------------------------------------- 2. served vs oracle
try:
    cp = pd.read_parquet(frames / 'cand.crop_production.parquet').reset_index()
    cpb = pd.read_parquet(frames / 'base.crop_production.parquet').reset_index()
    cmp = {}
    for wave in HALVES:
        s = cp[(cp['t'].astype(str) == wave) & (cp['crop'].astype(str) == 'Cowpea (Khobwe)')]
        h = s[s['plot_id'].astype(str) != 'Farm-level']
        f = s[s['plot_id'].astype(str) == 'Farm-level']
        o = orc[orc['wave'] == wave]
        sk = set(zip(h['i'].astype(str), h['plot_id'].astype(str)))
        ok = set(zip(o['i'].astype(str), o['plot_id'].astype(str)))
        cmp[wave] = {'served_harvest_rows': int(len(h)), 'served_harvest_keys': len(sk), 'oracle_keys': len(ok),
                     'served_not_oracle': sorted(map(list, sk - ok))[:20], 'oracle_not_served': sorted(map(list, ok - sk))[:20],
                     'served_qty': float(pd.to_numeric(h['Quantity'], errors='coerce').sum()),
                     'oracle_qty': float(o['qty'].sum()),
                     'production_obs': {str(k): int(v) for k, v in h['production_obs'].astype(str).str.split('/').str[0].value_counts().items()},
                     'farm_level_rows': int(len(f)),
                     'farm_level_obs': {str(k): int(v) for k, v in f['production_obs'].astype(str).str.split('/').str[0].value_counts().items()},
                     'rows_with_sale': int((pd.to_numeric(s['Value_sold'], errors='coerce').gt(0)
                                            | pd.to_numeric(s['Quantity_sold'], errors='coerce').gt(0)).sum()),
                     'value_sold': float(pd.to_numeric(s['Value_sold'], errors='coerce').sum())}
        # farm-level cowpea sales: are the household's code-48 harvest lines all cowpea (oracle)?
        hht = pd.DataFrame(res['_hh_tab'].get(f'{wave}/Cross_Sectional', []) + res['_hh_tab'].get(f'{wave}/Panel', []),
                           columns=['i', 'sum', 'size'])
        hht['sum'] = hht['sum'].astype(int); hht['size'] = hht['size'].astype(int)
        allcow = set(hht.loc[hht['sum'] == hht['size'], 'i'])
        cmp[wave]['farm_level_cowpea_hh_not_all_cow'] = sorted(set(f['i'].astype(str)) - allcow)[:20]
        # farm-level code-48 sales left Other, and why
        fo = cp[(cp['t'].astype(str) == wave) & (cp['plot_id'].astype(str) == 'Farm-level')
                & cp['production_obs'].astype(str).str.match(r'reported:I:48(/|$)')]
        cmp[wave]['farm_level_code48_total'] = int(len(fo))
        cmp[wave]['farm_level_code48_by_crop'] = {str(k): int(v) for k, v in fo['crop'].astype(str).value_counts().items()}
        # every crop change base -> cand is Other -> cowpea on a code-48 observation
        idx = [c for c in cp.columns if c in ('t', 'i', 'v', 'plot_id', 'u', 'condition', 'production_obs')]
        mb = cpb.merge(cp, on=idx, how='outer', suffixes=('_b', '_c'), indicator=True)
        mv = mb[(mb['_merge'] == 'both') & (mb['crop_b'].astype(str) != mb['crop_c'].astype(str))]
        cmp[wave]['crop_changes'] = {f'{a}->{b} @ {o}': int(v) for (a, b, o), v in
                                     mv.loc[mv['t'].astype(str) == wave, ['crop_b', 'crop_c', 'production_obs']]
                                     .assign(production_obs=lambda x: x['production_obs'].astype(str).str.split('/').str[0])
                                     .astype(str).value_counts().items()}
        cmp['merge_only'] = {str(k): int(v) for k, v in mb['_merge'].value_counts().items()}
    res['served_vs_oracle'] = cmp
except Exception as e:  # noqa: BLE001
    res['served_vs_oracle'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-2500:]}
print('served_vs_oracle', json.dumps(res['served_vs_oracle'], default=str)[:3000], flush=True)

# ---------------------------------------------------------------- 3. 2010-11 / 2013-14 Module D text
early = {}
for wave, rel_d, rel_g, hh, dplot, gplot, gcode, gtext in [
        ('2010-11', 'Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_d.dta',
         'Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta', 'case_id', 'ag_d00', 'ag_g0b', 'ag_g0d', 'ag_g0d_os'),
        ('2013-14', 'Malawi/2013-14/Data/AG_MOD_D_13.dta', 'Malawi/2013-14/Data/AG_MOD_G_13.dta', 'y2_hhid',
         'ag_d00', 'ag_g00', 'ag_g0b', None)]:
    rec = {}
    try:
        d = read(rel_d)
        g = read(rel_g)
        rec['d_columns_d20'] = sorted(c for c in d.columns if c.startswith('ag_d20'))
        rec['d_text_like_columns'] = sorted(c for c in d.columns if re.search(r'(_os|_oth)$', c))
        tcols = [c for c in d.columns if c.startswith('ag_d20') and re.search(r'(_os|_oth)$', c)]
        rec['d20_text_columns'] = tcols
        if tcols:
            T = pd.DataFrame({c: norm(d[c]).values for c in tcols}, index=d.index)
            d['_ntext'] = T.notna().sum(axis=1)
            d['_text'] = T.apply(lambda r: ' | '.join(v for v in r if pd.notna(v)), axis=1)
            slots = [c for c in d.columns if re.fullmatch(r'ag_d20[a-e]', c)]
            d['_n48'] = sum(pd.to_numeric(d[c], errors='coerce').eq(48).astype(int) for c in slots) if slots else np.nan
            for x, pc in ((d, dplot), (g, gplot)):
                x['_h'] = x[hh].apply(format_id)
                x['_p'] = x[pc].apply(format_id)
            d['_nrows'] = d.groupby(['_h', '_p'])['_h'].transform('size')
            dd = d.drop_duplicates(['_h', '_p'])
            g48 = g[pd.to_numeric(g[gcode], errors='coerce').eq(48)].copy()
            g48['_ng48'] = g48.groupby(['_h', '_p'])['_h'].transform('size')
            m = g48.merge(dd[['_h', '_p', '_ntext', '_text', '_n48', '_nrows']], on=['_h', '_p'], how='left')
            rec['g48'] = int(len(m))
            narrow = (m['_nrows'] == 1) & (m['_ntext'] == 1) & (m['_n48'] == 1) & (m['_ng48'] == 1)
            rec['narrow'] = int(narrow.sum())
            if gtext:
                m['_gt'] = norm(m[gtext])
                both = narrow & m['_gt'].notna()
                rec['narrow_with_g_text'] = int(both.sum())
                rec['agree_exact'] = int((both & (m['_gt'] == m['_text'])).sum())
                rec['g_cowpeas_vs_d'] = {str(k): int(v) for k, v in
                                         m.loc[both & m['_gt'].eq('COWPEAS'), '_text'].value_counts().items()}
                rec['d_cowpea_vs_g'] = {str(k): int(v) for k, v in
                                        m.loc[both & m['_text'].isin(['COWPEAS', 'COW PEAS']), '_gt'].value_counts().items()}
                rec['disagree_examples'] = {f'G={a} | D={b}': int(v) for (a, b), v in
                                            m.loc[both & (m['_gt'] != m['_text']), ['_gt', '_text']]
                                            .astype(str).value_counts().head(40).items()}
            rec['narrow_texts_top'] = {str(k): int(v) for k, v in m.loc[narrow, '_text'].value_counts().head(40).items()}
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
        rec['tb'] = traceback.format_exc()[-2000:]
    early[wave] = rec
    print('early', wave, {k: v for k, v in rec.items() if k not in ('tb',)}, flush=True)
res['early_d_text'] = early

# ---------------------------------------------------------------- 4. GH #1712
h1712 = {}
SIX = ['ag_h17_os', 'ag_h26b_os', 'ag_h27_os', 'ag_h36b_os', 'ag_h26_1b', 'ag_h36_1b']
PARENT = {'ag_h17_os': ('ag_h17', 5), 'ag_h26b_os': ('ag_h26b', 9), 'ag_h27_os': ('ag_h27', 5),
          'ag_h36b_os': ('ag_h36b', 9), 'ag_h26_1b': ('ag_h26_1a', None), 'ag_h36_1b': ('ag_h36_1a', None)}
hfiles = sorted(p.with_suffix('') for p in MW.rglob('*.dvc') if re.search(r'ag_mod_h', p.name, re.I)
                and p.name.lower().endswith('.dta.dvc'))
for src in hfiles:
    rel = src.relative_to(root).as_posix()
    rec = {}
    try:
        h = read(rel)
        hraw, _ = pyreadstat.read_dta(str(get_data_file(rel)), apply_value_formats=False)
        rec['rows'] = int(len(h))
        rec['rows_raw'] = int(len(hraw))

        def nonblank(s):
            if pd.api.types.is_numeric_dtype(s):
                return s.notna()
            return s.astype('string').str.strip().replace('', pd.NA).notna()
        for c in SIX:
            if c in h.columns or c in hraw.columns:
                pc, pv = PARENT[c]
                par = None
                if pc in hraw.columns:
                    pn = pd.to_numeric(hraw[pc], errors='coerce')
                    par = int(pn.eq(pv).sum()) if pv is not None else int(pn.notna().sum())
                rec[c] = {'nonblank_get_dataframe': int(nonblank(h[c]).sum()) if c in h.columns else None,
                          'nonblank_raw_pyreadstat': int(nonblank(hraw[c]).sum()) if c in hraw.columns else None,
                          'dtype': str(h[c].dtype) if c in h.columns else None,
                          'parent': pc, 'parent_routing_rows': par}
        h1712[rel] = rec
    except Exception as e:  # noqa: BLE001
        h1712[rel] = {'error': f'{type(e).__name__}: {e}'}
    print('1712', rel, h1712[rel], flush=True)
def _allblank(s):
    if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s):
        return bool(s.isna().all())
    return bool(s.astype('string').str.strip().replace('', pd.NA).isna().all())


try:
    pi = pd.read_parquet(frames / 'cand.plot_inputs.parquet')
    res['plot_inputs_all_null_columns'] = [c for c in pi.columns if _allblank(pi[c])]
    pib = pd.read_parquet(frames / 'base.plot_inputs.parquet')
    res['plot_inputs_all_null_columns_base'] = [c for c in pib.columns if _allblank(pib[c])]
except Exception as e:  # noqa: BLE001
    res['plot_inputs_all_null_columns'] = f'{type(e).__name__}: {e}'

# ---------------------------------------------------------------- 6. season
season = {}


def g_season(g):
    mth = pd.to_numeric(g['ag_g05a'], errors='coerce')
    yr = pd.to_numeric(g['ag_g05b'], errors='coerce')
    start = yr.where(mth >= 7, yr - 1)
    return start.astype('Int64')          # season start year: Jul-Dec of Y -> Y/Y+1


cow_hh = {}
if len(orc):
    for (w, half), sub in orc.groupby(['wave', 'half']):
        cow_hh[(w, half)] = set(sub['i'])
for wave, x02first in (('2016-17', 2014), ('2019-20', 2017)):
    walk = walk_all.get(wave, {})
    for half, gf, df_, hh, prefix in HALVES[wave]:
        key = f'{wave}/{half}'
        rec = {}
        try:
            g = read(f'Malawi/{wave}/Data/{half}/{gf}')
            g['_h'] = prefix + g[hh].apply(format_id)
            g['_i'] = g['_h'].map(lambda x: walk.get(x, x))
            g['_s'] = g_season(g)
            dated = g[g['_s'].notna()]
            ns = dated.groupby('_h')['_s'].nunique()
            rec['hh_dated'] = int(len(ns))
            rec['hh_one_season'] = int((ns == 1).sum())
            rec['g_season_dist'] = {str(k): int(v) for k, v in dated['_s'].value_counts().sort_index().items()}
            if half == 'Cross_Sectional':
                xrel = f'Malawi/{wave}/Data/Cross_Sectional/' + ('hh_mod_x.dta' if wave == '2016-17' else 'HH_MOD_X.dta')
                x = read(xrel)
                x['_h'] = prefix + x['case_id'].apply(format_id)
                x['_rs'] = pd.to_numeric(x['hh_x02'], errors='coerce').map({1: x02first, 2: x02first + 1}).astype('Int64')
                rec['hh_x02_dist'] = {str(k): int(v) for k, v in x['_rs'].value_counts(dropna=False).items()}
                gx = dated.merge(x[['_h', '_rs']].drop_duplicates('_h'), on='_h', how='left')
                rec['g_lines_dated'] = int(len(gx))
                rec['g_lines_with_x02'] = int(gx['_rs'].notna().sum())
                rec['g_lines_season_eq_x02'] = int((gx['_s'] == gx['_rs']).fillna(False).sum())
                rec['g_lines_season_vs_x02'] = {f'G={a} X02={b}': int(v) for (a, b), v in
                                                gx[['_s', '_rs']].astype(str).value_counts().head(20).items()}
                hh_ok = gx.groupby('_h').apply(lambda s: bool((s['_s'] == s['_rs']).fillna(False).all()))
                rec['hh_all_lines_match_x02'] = int(hh_ok.sum())
                rec['hh_with_x02_and_dated'] = int(len(hh_ok))
                # relabelled households: does their x02 agree with their G lines?
                ch = cow_hh.get((wave, half), set())
                sub = gx[gx['_i'].isin(ch)]
                rec['cowpea_hh'] = len(ch)
                rec['cowpea_hh_lines_eq_x02'] = int((sub['_s'] == sub['_rs']).fillna(False).sum())
                rec['cowpea_hh_lines_dated'] = int(len(sub))
                rec['cowpea_hh_all_match'] = int(sub.groupby('_h').apply(
                    lambda s: bool((s['_s'] == s['_rs']).fillna(False).all())).sum()) if len(sub) else 0
            # D vs G start dates in the agriculture meta file
            mrel = {('2016-17', 'Cross_Sectional'): 'ag_metadata.dta', ('2016-17', 'Panel'): 'ag_meta_16.dta',
                    ('2019-20', 'Cross_Sectional'): 'ag_mod_meta.dta', ('2019-20', 'Panel'): 'ag_meta_19.dta'}[(wave, half)]
            mfile = f'Malawi/{wave}/Data/{half}/{mrel}'
            mm, _ = pyreadstat.read_dta(str(get_data_file(mfile)), apply_value_formats=False)
            dcol = [c for c in mm.columns if re.fullmatch(r'ag_moduleD_start_?date', c, re.I)]
            gcol = [c for c in mm.columns if re.fullmatch(r'ag_moduleG_start_?date', c, re.I)]
            rec['meta_cols'] = [dcol, gcol]
            if dcol and gcol and hh in mm.columns:
                dd_ = pd.to_datetime(mm[dcol[0]], errors='coerce')
                gg_ = pd.to_datetime(mm[gcol[0]], errors='coerce')
                both = dd_.notna() & gg_.notna()
                rec['meta_hh_both_dates'] = int(both.sum())
                rec['meta_same_day'] = int((dd_.dt.normalize() == gg_.dt.normalize())[both].sum())
                gap = (gg_ - dd_).dt.days[both]
                rec['meta_gap_days_dist'] = {str(k): int(v) for k, v in gap.value_counts().sort_index().head(15).items()}
                mm['_h'] = prefix + mm[hh].apply(format_id)
                mm['_i'] = mm['_h'].map(lambda x: walk.get(x, x))
                ch = cow_hh.get((wave, half), set())
                sel = mm['_i'].isin(ch) & both
                rec['cowpea_hh_meta_both'] = int(sel.sum())
                rec['cowpea_hh_same_day'] = int((dd_.dt.normalize() == gg_.dt.normalize())[sel].sum())
                rec['cowpea_hh_gap_dist'] = {str(k): int(v) for k, v in (gg_ - dd_).dt.days[sel].value_counts().sort_index().items()}
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['tb'] = traceback.format_exc()[-2500:]
        season[key] = rec
        print('season', key, {k: v for k, v in rec.items() if k != 'tb'}, flush=True)
# 2024-25: the household's X02 against Module E's own E00, per household
try:
    for half, x_f, e_f, hh, has_x in (('Cross_Sectional', 'hh_mod_x.dta', 'ag_mod_e1.dta', 'case_id', True),
                                      ('Panel', 'hh_mod_x_24.dta', 'ag_mod_e1_24.dta', 'y5_hhid', False)):
        rec = {}
        e = read(f'Malawi/2024-25/Data/{half}/{e_f}')
        e['_h'] = e[hh].apply(format_id)
        e1 = e.groupby('_h')['ag_e00'].agg(lambda s: tuple(sorted(set(pd.to_numeric(s, errors='coerce').dropna().astype(int)))))
        rec['e00_per_hh_value_sets'] = {str(k): int(v) for k, v in e1.value_counts().items()}
        if has_x:
            x = read(f'Malawi/2024-25/Data/{half}/{x_f}')
            x['_h'] = x[hh].apply(format_id)
            xx = x.drop_duplicates('_h').set_index('_h')['hh_x02']
            j = pd.DataFrame({'e': e1}).join(xx, how='inner')
            j['x'] = pd.to_numeric(j['hh_x02'], errors='coerce')
            j['e1'] = j['e'].map(lambda t: t[0] if len(t) == 1 else np.nan)
            rec['hh_joined'] = int(len(j))
            rec['e00_eq_x02'] = int((j['e1'] == j['x']).sum())
            rec['e00_vs_x02'] = {f'E00={a} X02={b}': int(v) for (a, b), v in j[['e1', 'x']].astype(str).value_counts().items()}
        season[f'2024-25/{half}'] = rec
        print('season 2024-25', half, rec, flush=True)
except Exception as e:  # noqa: BLE001
    season['2024-25'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-2000:]}
res['season'] = season

# ---------------------------------------------------------------- 8. 2010-11 Module I's own text vs the household rule
try:
    rec = {}
    g = read('Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta')
    i_ = read('Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_i.dta')
    g['_h'] = g['case_id'].apply(format_id)
    i_['_h'] = i_['case_id'].apply(format_id)
    g48 = g[pd.to_numeric(g['ag_g0d'], errors='coerce').eq(48)].copy()
    g48['_cow'] = norm(g48['ag_g0d_os']).eq('COWPEAS').fillna(False)
    agg = g48.groupby('_h')['_cow'].agg(['sum', 'size'])
    allcow = set(agg.index[agg['sum'] == agg['size']])
    rec['hh_all_code48_cowpea'] = len(allcow)
    i48 = i_[pd.to_numeric(i_['ag_i0b'], errors='coerce').eq(48)].copy()
    i48['_t'] = norm(i48['ag_i0b_os'])
    i48['_val'] = pd.to_numeric(i48['ag_i03'], errors='coerce')
    sel = i48[i48['_h'].isin(allcow)]
    rec['i_code48_lines_in_allcow_hh'] = int(len(sel))
    rec['their_i_text'] = {str(k): int(v) for k, v in sel['_t'].value_counts(dropna=False).items()}
    pos = sel[sel['_val'] > 0]
    rec['with_positive_value'] = int(len(pos))
    rec['their_i_text_positive_value'] = {str(k): int(v) for k, v in pos['_t'].value_counts(dropna=False).items()}
    res['i_text_vs_household_rule_2010'] = rec
except Exception as e:  # noqa: BLE001
    res['i_text_vs_household_rule_2010'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1500:]}
print('2010 I text', res['i_text_vs_household_rule_2010'], flush=True)

# ---------------------------------------------------------------- 7. the restricted reader's branch
try:
    import importlib.util
    from lsms_library import local_tools as LT
    sys.path.insert(0, str(MW / '_'))
    spec = importlib.util.spec_from_file_location('malawi_rt_or', MW / '_' / 'malawi.py')
    mal = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mal)
    if hasattr(mal, 'read_module_d_columns'):
        target = MW / '2016-17/Data/Cross_Sectional/ag_mod_d.dta'
        LT._ensure_dvc_pulled(str(target))
        cache = LT._dvc_cache_path(str(target))

        class _Boom:
            def open(self, *a, **k):
                raise AssertionError('DVCFS.open reached')
        saved = LT.DVCFS
        LT.DVCFS = _Boom()
        try:
            got = mal.read_module_d_columns(target, ['case_id', 'gardenid', 'plotid', 'ag_d20a', 'ag_d20_oth'])
            res['restricted_reader_cached_branch'] = {'ok': True, 'cache_path': str(cache), 'shape': list(got.shape)}
        except AssertionError as e:
            res['restricted_reader_cached_branch'] = {'ok': False, 'cache_path': str(cache), 'error': str(e)}
        finally:
            LT.DVCFS = saved
        # relative-path form, as a wave script calls it (cwd = the wave's _/)
        cwd = os.getcwd()
        os.chdir(MW / '2016-17' / '_')
        try:
            LT.DVCFS = _Boom()
            got = mal.read_module_d_columns('../Data/Cross_Sectional/ag_mod_d.dta', ['case_id', 'ag_d20_oth'])
            res['restricted_reader_cached_branch_relative'] = {'ok': True, 'shape': list(got.shape)}
        except AssertionError as e:
            res['restricted_reader_cached_branch_relative'] = {'ok': False, 'error': str(e)}
        finally:
            LT.DVCFS = saved
            os.chdir(cwd)
    else:
        res['restricted_reader_cached_branch'] = 'not in this checkout'
except Exception as e:  # noqa: BLE001
    res['restricted_reader_cached_branch'] = {'error': f'{type(e).__name__}: {e}', 'tb': traceback.format_exc()[-1500:]}
print('reader', res.get('restricted_reader_cached_branch'), res.get('restricted_reader_cached_branch_relative'), flush=True)
res['gh1712'] = h1712
res.pop('_hh_tab', None)
(out / 'oracle.json').write_text(json.dumps(res, indent=1, default=str))
print('ORACLE DONE', flush=True)
