"""GH #637 round 3: raw-source oracle for the maintainer's rulings.

Independent field bindings from the held AG_SEC_3A/4A/5A/6A/6B/7A/7B
(get_dataframe, convert_categoricals=False), compared with the WAVE parquets
of a cold build (pre-finalize; raw sdd_hhid / y5_hhid as ``i``).

  crop_production : one row per (module, hhid, plot, cropid), production_obs
                    'reported:<module>:<cropid>'; sales module-matched
                    (5A->4A, 7A->6A, 7B->6B) and attached where the
                    (hhid, cropid) is on ONE plot of that module.  Asserts the
                    parquet equals the oracle row for row, and that
                    per-household Quantity_sold / Value_sold equal the raw
                    attachable sums.
  plot_inputs     : Unit_purchased from ag4a_10c_2 (seed) / (KG) (organic),
                    canonical labels written out independently; coverage and
                    the purchase-unit != total-unit counts.

Usage (cwd = checkout under test; PYTHONPATH = same):
    python oracle.py <data_root_of_cold_build> <aggregate_json>
Aggregates only; nothing row-level is written.
"""
import contextlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

import lsms_library as ll
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

built, out = Path(sys.argv[1]), Path(sys.argv[2])
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
TZ = countries_root() / 'Tanzania'
spec = importlib.util.spec_from_file_location('tz_oracle3', TZ / '_/tanzania.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
with contextlib.chdir(TZ / '_'):
    cropmap = mod._crop_labels()

SEED_PU = {1: 'Kg', 2: '20l Bucket', 3: 'Small Cup', 4: 'Large Cup', 5: 'Unknown'}
SEED_U = {1: 'kg', 2: '20 liter bucket', 3: 'small cup', 4: 'large cup'}
SEED_U_SERVED = {'kg': 'Kg', '20 liter bucket': '20l Bucket', 'small cup': 'Small Cup',
                 'large cup': 'Large Cup'}
W = {'2019-20': dict(pre='AG_SEC_', hh='sdd_hhid', plot='plotnum'),
     '2020-21': dict(pre='ag_sec_', hh='y5_hhid', plot='plot_id')}
res = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}


def load(w, m):
    p = W[w]['pre'] + (m.upper() if W[w]['pre'].isupper() else m.lower())
    return get_dataframe(TZ / w / 'Data' / f'{p}.dta', convert_categoricals=False)


def num(s):
    return pd.to_numeric(s, errors='coerce')


for w, c in W.items():
    R = {}
    # ---------------- crop_production ----------------
    H = []
    for m, q, hv, hm, ic in [('4A', 'ag4a_27', 'ag4a_19', 'ag4a_24_2', 'ag4a_04'),
                             ('6A', 'ag6a_09', 'ag6a_06a', 'ag6a_07_4', 'ag6a_05'),
                             ('6B', 'ag6b_09', 'ag6b_06a', 'ag6b_07_4', 'ag6b_05')]:
        d = load(w, m)
        code = num(d['cropid']).astype('Int64')
        qty = num(d[q]).astype('Float64').mask(num(d[hv]).eq(2).fillna(False).to_numpy(bool), 0.0)
        month = num(d[hm]).astype('Int64')
        H.append(pd.DataFrame({
            'mod': m, 'i': d[c['hh']].apply(format_id), 'plot_id': d[c['plot']].apply(format_id),
            'code': code, 'j': code.map(cropmap).astype('string'), 'Quantity': qty,
            'harvest_month': month.where((month >= 1) & (month <= 12), pd.NA),
            'intercropped': num(d[ic]).astype('Int64').map({1: True, 2: False}).astype('boolean'),
            'perennial': m != '4A'}))
    H = pd.concat(H, ignore_index=True)
    H = H[~(H['j'].isna() & H['Quantity'].isna())].copy()
    H['j'] = H['j'].where(H['j'].notna(), H['code'].astype('string').radd('crop_'))
    H['u'] = pd.Series('kg', index=H.index, dtype='string').where(H['Quantity'].notna(), pd.NA)
    R['cp_native_rows'] = len(H)
    R['cp_groups_tipj'] = int(H.groupby(['i', 'plot_id', 'j']).ngroups)
    R['cp_groups_tipcode'] = int(H.groupby(['i', 'plot_id', 'code']).ngroups)
    two_mod = H[H.duplicated(['i', 'plot_id', 'code'], keep=False)]
    R['cp_code_in_two_modules_on_one_plot'] = {
        'pairs': int(two_mod.groupby(['i', 'plot_id', 'code']).ngroups),
        'modules': sorted(two_mod['mod'].unique().tolist()),
        'quantities': sorted(map(float, two_mod['Quantity'].dropna())),
        'pairs_alone_in_their_tipj_group': int(sum(
            H[(H['i'] == k[0]) & (H['plot_id'] == k[1]) & (H['j'] == g['j'].iloc[0])]['code'].nunique() == 1
            for k, g in two_mod.groupby(['i', 'plot_id', 'code'])))}
    S = []
    for m, hmod, q, v in [('5A', '4A', 'ag5a_02', 'ag5a_03'), ('7A', '6A', 'ag7a_03', 'ag7a_04'),
                          ('7B', '6B', 'ag7b_03', 'ag7b_04')]:
        d = load(w, m)
        S.append(pd.DataFrame({'smod': m, 'mod': hmod, 'i': d[c['hh']].apply(format_id),
                               'code': num(d['cropid']).astype('Int64'),
                               'Quantity_sold': num(d[q]).astype('Float64'),
                               'Value_sold': num(d[v]).astype('Float64')}))
    S = pd.concat(S, ignore_index=True).dropna(subset=['code'])
    assert not S.duplicated(['smod', 'i', 'code']).any()
    nplot = H.groupby(['i', 'code', 'mod'])['plot_id'].nunique().rename('nplot').reset_index()
    S = S.merge(nplot, on=['i', 'code', 'mod'], how='left')
    att = S[S['nplot'].eq(1)]
    R['sales_raw'] = {'rows': len(S), 'Quantity_sold': float(S.Quantity_sold.sum()),
                      'Value_sold': float(S.Value_sold.sum()),
                      'attachable_rows': len(att), 'attachable_Quantity_sold': float(att.Quantity_sold.sum()),
                      'attachable_Value_sold': float(att.Value_sold.sum()),
                      'multi_plot_Quantity_sold': float(S[S.nplot.gt(1)].Quantity_sold.sum()),
                      'multi_plot_Value_sold': float(S[S.nplot.gt(1)].Value_sold.sum()),
                      'no_matching_harvest_row': int(S.nplot.isna().sum())}
    O = H.merge(att[['i', 'code', 'mod', 'Quantity_sold', 'Value_sold']], on=['i', 'code', 'mod'], how='left')
    O['production_obs'] = ('reported:' + O['mod'] + ':' + O['code'].astype('string')).astype('string')
    O['t'] = w
    O = O.set_index(['t', 'i', 'plot_id', 'j', 'production_obs'])[
        ['Quantity', 'u', 'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped', 'perennial']]
    P = pd.read_parquet(built / 'Tanzania' / w / '_' / 'crop_production.parquet')
    assert list(P.index.names) == ['t', 'i', 'plot_id', 'j', 'production_obs'], P.index.names
    assert P.index.is_unique
    O = O.sort_index()
    P = P.sort_index()
    assert O.index.equals(P.index), 'index differs from the oracle'
    for col in O.columns:
        a, b = O[col], P[col]
        if col in ('Quantity', 'Quantity_sold', 'Value_sold', 'harvest_month'):
            a, b = a.astype('Float64'), b.astype('Float64')
        elif col in ('intercropped', 'perennial'):
            a, b = a.astype('boolean'), b.astype('boolean')
        else:
            a, b = a.astype('string'), b.astype('string')
        same = (a == b).fillna(False) | (a.isna() & b.isna())
        assert same.all(), (col, int((~same).sum()))
    R['cp_oracle_equal_rows'] = len(P)
    hh_o = O.groupby(level='i')[['Quantity_sold', 'Value_sold']].sum(min_count=1).dropna(how='all')
    hh_r = att.groupby('i')[['Quantity_sold', 'Value_sold']].sum(min_count=1).dropna(how='all')
    hh_p = P.groupby(level='i')[['Quantity_sold', 'Value_sold']].sum(min_count=1).dropna(how='all')
    assert_frame_equal(hh_p.sort_index(), hh_r.sort_index(), check_dtype=False, check_names=False)
    R['per_household_sales_equal_raw_attachable'] = {'households': len(hh_p),
                                                     'Quantity_sold': float(hh_p.Quantity_sold.sum()),
                                                     'Value_sold': float(hh_p.Value_sold.sum())}
    # ---------------- plot_inputs: Unit_purchased ----------------
    d4 = load(w, '4A')
    code = num(d4['cropid']).astype('Int64')
    crop = code.map(cropmap).astype('string')
    q = num(d4['ag4a_10_1'])
    keep = (q.notna() | crop.notna()).to_numpy(bool)
    uc, pu, pq = num(d4['ag4a_10_2']).astype('Int64'), num(d4['ag4a_10c_2']).astype('Int64'), num(d4['ag4a_10c_1'])
    X = pd.DataFrame({'i': d4[c['hh']].apply(format_id), 'plot_id': d4[c['plot']].apply(format_id),
                      'crop': crop.where(crop.notna(), code.astype('string').radd('crop_')),
                      'u': uc.map(SEED_U).astype('string').map(SEED_U_SERVED).fillna('Unknown'),
                      'pq': pq, 'Unit_purchased': pu.map(lambda x: pd.NA if pd.isna(x) else SEED_PU.get(int(x), 'Unknown'))
                      .astype('string')})[keep]
    R['seed_records'] = len(X)
    R['seed_unit_purchased_recorded'] = int(X.Unit_purchased.notna().sum())
    R['seed_unit_purchased_recorded_iff_pq_positive'] = bool((X.Unit_purchased.notna() == X.pq.gt(0).fillna(False)).all())
    R['seed_records_unit_purchased_ne_u'] = int((X.Unit_purchased.notna() & (X.Unit_purchased != X.u).fillna(False)).sum())
    R['seed_records_unit_purchased_ne_u_pairs'] = {
        f'{a} -> {b}': int(n) for (a, b), n in
        X[X.Unit_purchased.notna() & (X.Unit_purchased != X.u).fillna(False)][['u', 'Unit_purchased']]
        .value_counts().sort_index().items()}
    X['input'] = 'Seed'
    X['t'] = w
    Xg = X.groupby(['t', 'i', 'plot_id', 'input', 'crop', 'u'])['Unit_purchased'].agg(
        lambda s: sorted(set(s.dropna())))
    R['seed_6key_groups_with_two_purchase_units'] = int((Xg.map(len) > 1).sum())
    exp = Xg.map(lambda v: v[0] if len(v) else pd.NA).astype('string')
    d3 = load(w, '3A')
    org = pd.DataFrame({'t': w, 'i': d3[c['hh']].apply(format_id), 'plot_id': d3[c['plot']].apply(format_id),
                        'input': 'Organic Fertilizer', 'crop': pd.NA, 'u': 'Kg',
                        'pq': num(d3['ag3a_44'])})[num(d3['ag3a_41']).eq(1).to_numpy(bool)]
    org['Unit_purchased'] = pd.Series('Kg', index=org.index, dtype='string').where(org.pq.notna(), pd.NA)
    R['organic_rows'] = len(org)
    R['organic_unit_purchased_kg'] = int(org.Unit_purchased.notna().sum())
    P = pd.read_parquet(built / 'Tanzania' / w / '_' / 'plot_inputs.parquet')
    flat = P.reset_index()
    flat['u_served'] = flat['u'].astype('string').map(lambda x: SEED_U_SERVED.get(x, x))
    seed = flat[flat.input.eq('Seed')].set_index(['t', 'i', 'plot_id', 'input', 'crop', 'u_served'])['Unit_purchased']
    seed.index = seed.index.set_names('u', level='u_served')
    seed = seed.astype('string').sort_index()
    exp = exp.sort_index()
    assert seed.index.equals(exp.index), 'seed keys differ from the oracle'
    same = (seed == exp).fillna(False) | (seed.isna() & exp.isna())
    assert same.all(), int((~same).sum())
    o = flat[flat.input.eq('Organic Fertilizer')]
    assert len(o) == len(org)
    assert int(o.Unit_purchased.notna().sum()) == int(org.Unit_purchased.notna().sum())
    assert set(o.Unit_purchased.dropna()) <= {'Kg'}
    rest = flat[~flat.input.isin(['Seed', 'Organic Fertilizer'])]
    assert rest.Unit_purchased.isna().all()
    R['pi_rows'] = len(P)
    R['pi_seed_rows_equal_oracle'] = len(seed)
    R['pi_unit_purchased_by_value'] = {str(k): int(v) for k, v in
                                       P['Unit_purchased'].astype('string').fillna('<NA>').value_counts().sort_index().items()}
    R['pi_served_rows_unit_purchased_ne_u'] = int(
        (flat.Unit_purchased.notna() & (flat.Unit_purchased.astype('string') != flat.u_served).fillna(False)).sum())
    res[w] = R
out.write_text(json.dumps(res, indent=1, default=str) + '\n')
print(json.dumps(res, indent=1, default=str))
