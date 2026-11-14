"""Red team (malawi delta): registration stamps, joins, summaries, #1712 columns.

usage: python delta_check.py <clone> <framesdir> <out.json>
Reads base/cand frames and dinp parquets written by build_arm.py; rebuilds nothing
except a warm Country read for attrs.
"""
import json
import os
import re
import sys
import warnings
from pathlib import Path

import pandas as pd

clone, fr, outp = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
import lsms_library  # noqa: E402
from lsms_library import paths  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(clone) + os.sep)
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.derivations import records_for  # noqa: E402
import pyreadstat  # noqa: E402

COW = 'Cowpea (Khobwe)'
K_CP = 'Malawi::crop_production::cowpea-from-crop-text'
K_PI = 'Malawi::plot_inputs::cowpea-from-crop-text'
res = {}


def keys(s):
    return s.astype('string').fillna('').str.split('+').map(lambda ks: set(k for k in ks if k))


# --- crop_production
b = pd.read_parquet(fr / 'base.crop_production.parquet').reset_index()
c = pd.read_parquet(fr / 'cand.crop_production.parquet').reset_index()
key = ['i', 't', 'v', 'plot_id', 'u', 'condition', 'production_obs']
assert not b.duplicated(key).any() and not c.duplicated(key).any()
m = b.merge(c, on=key, how='outer', suffixes=('_b', '_c'), indicator=True)
rec = {'merge': m['_merge'].value_counts().to_dict()}
kb, kc = keys(m['Derivation_b']), keys(m['Derivation_c'])
has = kc.map(lambda s: K_CP in s)
cow = m['crop_c'].astype(str).eq(COW)
rec['keyed'] = int(has.sum())
rec['cowpea_rows'] = int(cow.sum())
rec['keyed_not_cowpea'] = int((has & ~cow).sum())
rec['cowpea_not_keyed'] = int((cow & ~has).sum())
rec['keyed_by_t_obs'] = (m.loc[has, ['t', 'production_obs']].astype(str)
                         .assign(production_obs=lambda x: x['production_obs'].str.split('/').str[0])
                         .value_counts().rename(lambda x: '|'.join(x) if isinstance(x, tuple) else x).to_dict())
rec['keyed_by_t_obs'] = {('|'.join(k) if isinstance(k, tuple) else str(k)): int(v) for k, v in
                         m.loc[has, ['t', 'production_obs']].astype(str)
                         .assign(production_obs=lambda x: x['production_obs'].str.split('/').str[0])
                         .value_counts().items()}
rest = kc.map(lambda s: s - {K_CP})
rec['other_keys_equal_base'] = bool((rest == kb).all())
rec['other_keys_differ_rows'] = int((rest != kb).sum())
rec['doubly_keyed'] = int((has & kc.map(len).gt(1)).sum())
rec['doubly_keyed_by_other'] = {', '.join(sorted(k)): int(v) for k, v in
                                rest[has & kc.map(len).gt(1)].map(lambda s: tuple(sorted(s))).value_counts().items()}
sep_ok = m['Derivation_c'].dropna().astype(str)
rec['values_sorted_unique'] = bool(sep_ok.map(lambda v: v.split('+') == sorted(set(v.split('+')))).all())
others = [x for x in b.columns if x in c.columns and x not in key + ['Derivation']]
diffcols = {}
for col in others:
    bb, cc = m[f'{col}_b'], m[f'{col}_c']
    d = ~((bb.isna() & cc.isna()) | (bb.astype(str) == cc.astype(str)))
    if d.any():
        diffcols[col] = m.loc[d, 't'].astype(str).value_counts().to_dict()
rec['other_columns_moved'] = diffcols
rec['cols_only'] = [sorted(set(b.columns) - set(c.columns)), sorted(set(c.columns) - set(b.columns))]
res['crop_production'] = rec
print('crop_production', json.dumps(rec, default=str), flush=True)

# --- plot_inputs
b = pd.read_parquet(fr / 'base.plot_inputs.parquet').reset_index()
c = pd.read_parquet(fr / 'cand.plot_inputs.parquet').reset_index()
key = ['i', 't', 'v', 'plot_id', 'input', 'u', 'input_obs']
m = b.merge(c, on=key, how='outer', suffixes=('_b', '_c'), indicator=True)
rec = {'rows': [len(b), len(c)], 'merge': m['_merge'].value_counts().to_dict(),
       'cols_only_base': sorted(set(b.columns) - set(c.columns)),
       'cols_only_cand': sorted(set(c.columns) - set(b.columns))}
kc = keys(m['Derivation']) if 'Derivation' in m.columns else keys(m.get('Derivation_c', pd.Series(dtype='string')))
has = kc.map(lambda s: K_PI in s)
cow = m['crop_c'].astype(str).eq(COW)
seed = m['input'].astype(str).eq('Seed')
rec['keyed'] = int(has.sum())
rec['cowpea_rows'] = int(cow.sum())
rec['cowpea_seed_rows'] = int((cow & seed).sum())
rec['keyed_not_cowpea_seed'] = int((has & ~(cow & seed)).sum())
rec['cowpea_seed_not_keyed'] = int((cow & seed & ~has).sum())
rec['cowpea_nonseed_rows'] = int((cow & ~seed).sum())
rec['derivation_values'] = {', '.join(sorted(k)): int(v) for k, v in kc.map(lambda s: tuple(sorted(s))).value_counts().items()}
rec['keyed_by_t_plot'] = {('|'.join(k)): int(v) for k, v in
                          m.loc[has, ['t']].assign(p=m.loc[has, 'plot_id'].astype(str).eq('Farm-level').map({True: 'Farm', False: 'plot'}))
                          .astype(str).value_counts().items()}
others = [x for x in b.columns if x in c.columns and x not in key]
diffcols = {}
for col in others:
    bb, cc = m[f'{col}_b'], m[f'{col}_c']
    d = ~((bb.isna() & cc.isna()) | (bb.astype(str) == cc.astype(str)))
    if d.any():
        diffcols[col] = m.loc[d, 't'].astype(str).value_counts().to_dict()
rec['other_columns_moved'] = diffcols
for col in rec['cols_only_base']:
    s = b[col]
    rec.setdefault('dropped_nonblank', {})[col] = int((~(s.isna() | s.astype('string').str.strip().eq('').fillna(False))).sum())
for col in ('ag_h33', 'ag_h42b_os', 'ag_h36b_oth'):
    if col in c.columns:
        s = c[col]
        rec.setdefault('three_served', {})[col] = {'dtype': str(s.dtype), 'isna': int(s.isna().sum()),
                                                   'blank_str': int(s.astype('string').str.strip().eq('').fillna(False).sum()),
                                                   'rows': len(s)}
res['plot_inputs'] = rec
print('plot_inputs', json.dumps(rec, default=str), flush=True)

# --- the new key's inputs: joins per route
J = {}
for table, k, frame in (('crop_production', K_CP, 'cand.crop_production.parquet'),
                        ('plot_inputs', K_PI, 'cand.plot_inputs.parquet')):
    s = pd.read_parquet(fr / frame).reset_index()
    ks = keys(s['Derivation'])
    s = s[ks.map(lambda x: k in x).to_numpy(dtype=bool)]
    for w in ('2010-11', '2013-14', '2016-17', '2019-20'):
        p = fr / f'cand.dinp__{table}__cowpea-from-crop-text__{w}.parquet'
        sub = s[s['t'].astype(str) == w]
        r = {'served_keyed': int(len(sub))}
        if not p.exists():
            r['inputs'] = 'missing (callable raised or wave not covered)'
        else:
            inp = pd.read_parquet(p)
            r['inputs_rows'] = int(len(inp))
            r['inputs_dup_i_crop_plot'] = int(inp.duplicated(['i', 'crop', 'plot_id']).sum())
            plot = sub[sub['plot_id'].astype(str) != 'Farm-level']
            farm = sub[sub['plot_id'].astype(str) == 'Farm-level']
            L3 = set(zip(inp['i'].astype(str), inp['crop'].astype(str), inp['plot_id'].astype(str)))
            L2 = set(zip(inp['i'].astype(str), inp['crop'].astype(str)))
            r['plot_rows'] = int(len(plot))
            r['plot_rows_join_tiplot'] = int(sum(x in L3 for x in zip(plot['i'].astype(str), plot['crop'].astype(str), plot['plot_id'].astype(str))))
            r['farm_rows'] = int(len(farm))
            r['farm_rows_join_ti'] = int(sum(x in L2 for x in zip(farm['i'].astype(str), farm['crop'].astype(str))))
            # every input line served? (harvest table: each relabelled G line has a served row)
            S3 = set(zip(plot['i'].astype(str), plot['crop'].astype(str), plot['plot_id'].astype(str)))
            r['input_lines_not_served'] = int(sum(x not in S3 for x in L3))
            if 'ag_d20_oth' in inp.columns:
                r['d_text_values'] = inp['ag_d20_oth'].astype('string').str.strip().str.upper().value_counts(dropna=False).to_dict()
            for col in ('ag_g0d_os',):
                if col in inp.columns:
                    r['g_text_values'] = inp[col].astype('string').str.strip().str.upper().value_counts(dropna=False).to_dict()
        J[f'{table}|{w}'] = r
        print('join', table, w, json.dumps(r, default=str), flush=True)
res['new_key_joins'] = J

# --- summaries: which tables list the new keys
res['records_for'] = {t: sorted(k for k in records_for('Malawi', t) if 'cowpea' in k)
                      for t in ('crop_production', 'plot_inputs', 'plot_features', 'food_acquired', 'sample')}
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    from lsms_library.country import Country
    C = Country('Malawi')
    for t in ('crop_production', 'plot_inputs'):
        summ = getattr(C, t)().attrs.get('derivations', {}).get('Malawi', {})
        res.setdefault('attrs_summary', {})[t] = {k: v for k, v in summ.items() if 'cowpea' in k}
print('records_for', res['records_for'], 'attrs', res.get('attrs_summary'), flush=True)

# --- the three other blank H columns, raw
raw = {}
for rel in ('Malawi/2010-11/Data/Full_Sample/Agriculture/ag_mod_h.dta', 'Malawi/2010-11/Data/Panel/Agriculture/ag_mod_h.dta',
            'Malawi/2013-14/Data/AG_MOD_H_13.dta', 'Malawi/2016-17/Data/Cross_Sectional/ag_mod_h.dta',
            'Malawi/2016-17/Data/Panel/ag_mod_h_16.dta', 'Malawi/2019-20/Data/Cross_Sectional/ag_mod_h.dta',
            'Malawi/2019-20/Data/Panel/ag_mod_h_19.dta'):
    h, meta = pyreadstat.read_dta(str(get_data_file(rel)), apply_value_formats=False)
    r = {}
    for col in ('ag_h33', 'ag_h42b_os', 'ag_h36b_oth', 'ag_h17_os', 'ag_h26b_os', 'ag_h27_os', 'ag_h36b_os', 'ag_h26_1b', 'ag_h36_1b'):
        if col in h.columns:
            s = h[col]
            nb = s.notna() if pd.api.types.is_numeric_dtype(s) else s.astype('string').str.strip().replace('', pd.NA).notna()
            r[col] = {'nonblank': int(nb.sum()), 'dtype': str(s.dtype),
                      'label': dict(zip(meta.column_names, meta.column_labels)).get(col)}
    raw[rel] = r
    print('raw', rel, r, flush=True)
res['h_raw'] = raw
outp.write_text(json.dumps(res, indent=1, default=str))
print('DELTA DONE', flush=True)
