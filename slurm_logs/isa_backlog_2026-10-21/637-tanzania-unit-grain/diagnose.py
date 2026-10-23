"""GH #637 Tanzania tail: classify every group the two helper reducers collapse.

Usage (cwd = checkout under test, PYTHONPATH = same checkout):
    python diagnose.py <private_out_dir> <aggregate_json>

Reads held sources with get_dataframe (L1 blob), runs the helper IN-PROCESS on
those frames with a capture of the frame handed to the groupby, and builds an
independent provenance-carrying long frame (module / channel / source row /
raw code) to classify each duplicate group.  Writes ONLY aggregate counts to
<aggregate_json>; row-level detail (ids) goes to <private_out_dir>.
"""
import contextlib
import importlib.util
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.paths import countries_root

priv = Path(sys.argv[1]).resolve()
agg_out = Path(sys.argv[2]).resolve()
priv.mkdir(parents=True, exist_ok=True)

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
assert countries_root().resolve() == Path(top, 'lsms_library/countries').resolve()
TZ = Path(top, 'lsms_library/countries/Tanzania')

spec = importlib.util.spec_from_file_location('tz_diag', TZ / '_/tanzania.py')
tz = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tz)


@contextlib.contextmanager
def cwd(p):
    old = Path.cwd()
    os.chdir(p)
    try:
        yield
    finally:
        os.chdir(old)


# Capture the frame each helper hands to its collapse.
_captured = {}
_orig_groupby = pd.DataFrame.groupby


def _spy(self, *a, **k):
    lv = k.get('level')
    if isinstance(lv, list) and lv[:3] == ['t', 'i', 'plot_id'] and len(self.index.names) == len(lv):
        _captured[tuple(lv)] = self.copy()
    return _orig_groupby(self, *a, **k)


WAVES = {
    '2019-20': dict(hh='sdd_hhid', plot='plotnum',
                    f=dict(s3a='AG_SEC_3A', s4a='AG_SEC_4A', s6a='AG_SEC_6A', s6b='AG_SEC_6B',
                           s5a='AG_SEC_5A', s7a='AG_SEC_7A', s7b='AG_SEC_7B')),
    '2020-21': dict(hh='y5_hhid', plot='plot_id',
                    f=dict(s3a='ag_sec_3a', s4a='ag_sec_4a', s6a='ag_sec_6a', s6b='ag_sec_6b',
                           s5a='ag_sec_5a', s7a='ag_sec_7a', s7b='ag_sec_7b')),
}

with cwd(TZ / '_'):
    cropmap = tz._crop_labels()
    fertmap = tz._plot_harmonized_codes('harmonize_input')

summary = {}
for wave, W in WAVES.items():
    raw = {k: get_dataframe(TZ / wave / 'Data' / f'{v}.dta', convert_categoricals=False)
           for k, v in W['f'].items()}
    hh, plot = W['hh'], W['plot']
    S = summary.setdefault(wave, {})

    # ------------------------------------------------------------------
    # Source grain / identifier namespace
    # ------------------------------------------------------------------
    ns = {}
    for k, d in raw.items():
        ent = {'rows': len(d), 'null_hhid': int(d[hh].isna().sum())}
        if plot in d.columns:
            ent['null_plot'] = int(d[plot].isna().sum())
        keycols = [hh] + ([plot] if plot in d.columns else []) + (['cropid'] if 'cropid' in d.columns else [])
        ent['key'] = keycols
        ent['dup_key_rows'] = int(d.duplicated(keycols, keep=False).sum())
        ent['null_cropid'] = int(d['cropid'].isna().sum()) if 'cropid' in d.columns else None
        ns[k] = ent
    # household ids present in sample-cover?  (namespace check vs the 4A/3A frames)
    S['source'] = ns

    # ------------------------------------------------------------------
    # plot_inputs: helper in-process with capture
    # ------------------------------------------------------------------
    _captured.clear()
    pd.DataFrame.groupby = _spy
    try:
        with cwd(TZ / wave / '_'):
            pi = tz.plot_inputs_for_wave(wave, raw['s3a'], raw['s4a'], dict(hhid=hh, plot=plot, crop='cropid'))
    finally:
        pd.DataFrame.groupby = _orig_groupby
    pre = _captured.get(('t', 'i', 'plot_id', 'input', 'crop'))
    # Independent provenance long frame (same field bindings as the helper).
    recs = []
    d4 = raw['s4a']
    for idx, r in d4.iterrows():
        code = r['cropid']
        crop = cropmap.get(int(code), pd.NA) if pd.notna(code) else pd.NA
        q = r['ag4a_10_1']
        if pd.isna(q) and pd.isna(crop):
            continue
        if pd.isna(crop) and pd.notna(code):
            crop = f'crop_{int(code)}'
        uc = r['ag4a_10_2']
        recs.append(dict(t=wave, i=format_id(r[hh]), plot_id=format_id(r[plot]), input='Seed', crop=crop,
                         Quantity=q, ucode=uc, u=tz._SEED_UNIT_LABELS.get(int(uc), pd.NA) if pd.notna(uc) else pd.NA,
                         channel='seed', src=f's4a:{idx}', rawcode=code,
                         Quantity_purchased=r['ag4a_10c_1'], pucode=r['ag4a_10c_2'],
                         paid=r['ag4a_12'], seedtype=r['ag4a_08']))
    d3 = raw['s3a']
    for ch, used, qc, ucc, tc in [('organic', 'ag3a_41', 'ag3a_42', None, None),
                                  ('inorganic1', 'ag3a_47', 'ag3a_49', None, 'ag3a_48'),
                                  ('inorganic2', 'ag3a_54', 'ag3a_56', None, 'ag3a_55'),
                                  ('herbicide', 'ag3a_60', 'ag3a_62_1', 'ag3a_62_2', None),
                                  ('pesticide', 'ag3a_65a', 'ag3a_65b_1', 'ag3a_65b_2', None)]:
        for idx, r in d3.loc[pd.to_numeric(d3[used], errors='coerce').eq(1)].iterrows():
            if ch == 'organic':
                name = 'Organic Fertilizer'
            elif tc:
                typ = r[tc]
                name = fertmap.get(int(typ), pd.NA) if pd.notna(typ) else pd.NA
                name = 'Other Fertilizer' if pd.isna(name) else name
            else:
                name = ch.capitalize()
            uc = r[ucc] if ucc else pd.NA
            u = 'kg' if ucc is None else (tz._LIQUID_UNIT_LABELS.get(int(uc), pd.NA) if pd.notna(uc) else pd.NA)
            recs.append(dict(t=wave, i=format_id(r[hh]), plot_id=format_id(r[plot]), input=name, crop=pd.NA,
                             Quantity=r[qc], ucode=uc, u=u, channel=ch, src=f's3a:{ch}:{idx}', rawcode=r[tc] if tc else pd.NA))
    L = pd.DataFrame(recs)
    L.to_pickle(priv / f'plot_inputs_long_{wave}.pkl')
    assert pre is not None or pi.index.is_unique
    S['plot_inputs'] = pinfo = {'source_long_rows': len(L), 'helper_pre_collapse_rows': None if pre is None else len(pre),
                                'helper_rows': len(pi)}
    key = ['t', 'i', 'plot_id', 'input', 'crop']
    nullkeys = {k: int(L[k].isna().sum()) for k in key}
    pinfo['null_keys_long'] = nullkeys
    cls = Counter()
    sizes = Counter()
    conflicts = Counter()
    patterns = Counter()
    rows_in_class = Counter()
    for kv, g in L.groupby(key, dropna=False):
        if len(g) == 1:
            continue
        sizes[len(g)] += 1
        units_q = set(g.loc[g['Quantity'].notna(), 'u'].dropna())
        units_all = set(g['u'].dropna())
        any_na_u_with_q = bool((g['u'].isna() & g['Quantity'].notna()).any())
        n_src = g['src'].nunique()
        n_codes = g['rawcode'].nunique(dropna=False)
        payload = g[['Quantity', 'u']].astype('string').fillna('<NA>')
        identical = len(payload.drop_duplicates()) == 1
        if n_src < len(g):
            c = 'replication(same source record)'
        elif len(units_all) > 1:
            c = 'incommensurable(distinct recorded units)'
        elif any_na_u_with_q and len(units_all) >= 1:
            c = 'unknown+recorded unit with quantity'
        elif g['Quantity'].notna().sum() <= 1:
            c = 'completion(<=1 quantity; nothing summed)'
        else:
            c = 'commensurable sum(same unit)'
        cls[c] += 1
        rows_in_class[c] += len(g)
        if len(units_all) > 1:
            patterns[(kv[3], kv[4], tuple(sorted(units_all)))] += 1
        conflicts['u_distinct_nonnull>1'] += int(len(units_all) > 1)
        conflicts['u_mixed_null_and_recorded'] += int(g['u'].isna().any() and len(units_all) >= 1)
        conflicts['distinct_raw_codes>1'] += int(n_codes > 1)
        conflicts['identical_qty_u_payload'] += int(identical)
        if kv[3] == 'Seed':
            # kept-first / OR'd fields
            st = g['seedtype'].dropna().map({1: True, 2: False, 3: True}).dropna().unique()
            conflicts['Improved_disagree'] += int(len(st) > 1)
            pu = set(g['pucode'].dropna())
            conflicts['purchase_unit_distinct>1'] += int(len(pu) > 1)
        else:
            conflicts['non_seed_group'] += 1
    pinfo['dup_groups_by_class'] = dict(cls)
    pinfo['dup_rows_by_class'] = dict(rows_in_class)
    pinfo['dup_group_sizes'] = {str(k): v for k, v in sorted(sizes.items())}
    pinfo['dup_group_flags'] = dict(conflicts)
    pinfo['incommensurable_patterns'] = [dict(input=k[0], crop=k[1], units=list(k[2]), groups=v)
                                         for k, v in sorted(patterns.items())]
    # what keying on u would do: groups at (key + u) after NA->Unknown
    L2 = L.copy()
    L2['u'] = L2['u'].astype('string').fillna('Unknown')
    g5 = L.groupby(key, dropna=False).ngroups
    g6 = L2.groupby(key + ['u'], dropna=False).ngroups
    pinfo['groups_at_5key'] = g5
    pinfo['groups_at_6key_u_unknown_filled'] = g6
    pinfo['dup_groups_at_6key'] = int((L2.groupby(key + ['u'], dropna=False).size() > 1).sum())
    # consistency with the helper
    if pre is not None:
        pinfo['helper_pre_rows_eq_long_rows'] = len(pre) == len(L)

    # ------------------------------------------------------------------
    # crop_production: helper in-process with capture
    # ------------------------------------------------------------------
    cm = lambda q, harvested, hm, ic: dict(hhid=hh, plot=plot, crop='cropid', qty=q, harvested=harvested,
                                           harvest_month=hm, intercrop=ic)
    colmaps = dict(
        seasonal=cm('ag4a_27', 'ag4a_19', 'ag4a_24_2', 'ag4a_04'),
        fruit=cm('ag6a_09', None, 'ag6a_07_4', 'ag6a_05'),
        perennial=cm('ag6b_09', None, 'ag6b_07_4', 'ag6b_05'),
        sales_seasonal=dict(hhid=hh, crop='cropid', qty='ag5a_02', value='ag5a_03'),
        sales_fruit=dict(hhid=hh, crop='cropid', qty='ag7a_03', value='ag7a_04'),
        sales_perennial=dict(hhid=hh, crop='cropid', qty='ag7b_03', value='ag7b_04'),
    )
    _captured.clear()
    pd.DataFrame.groupby = _spy
    try:
        with cwd(TZ / wave / '_'):
            cp = tz.crop_production_for_wave(wave, raw['s4a'], raw['s6a'], raw['s6b'],
                                             sales=dict(seasonal=raw['s5a'], fruit=raw['s7a'], perennial=raw['s7b']),
                                             colmaps=colmaps)
    finally:
        pd.DataFrame.groupby = _orig_groupby
    cpre = _captured.get(('t', 'i', 'plot_id', 'j'))
    C = {'helper_rows': len(cp), 'helper_pre_collapse_rows': None if cpre is None else len(cpre)}
    S['crop_production'] = C
    # provenance long frame
    recs = []
    for mod, key_, q, hv, hm, ic in [('seasonal', 's4a', 'ag4a_27', 'ag4a_19', 'ag4a_24_2', 'ag4a_04'),
                                    ('fruit', 's6a', 'ag6a_09', None, 'ag6a_07_4', 'ag6a_05'),
                                    ('perennial', 's6b', 'ag6b_09', None, 'ag6b_07_4', 'ag6b_05')]:
        d = raw[key_]
        for idx, r in d.iterrows():
            code = r['cropid']
            j = cropmap.get(int(code), pd.NA) if pd.notna(code) else pd.NA
            qty = r[q]
            if hv is not None and pd.notna(r[hv]) and r[hv] == 2:
                qty = 0.0
            if pd.isna(j) and pd.isna(qty):
                continue
            if pd.isna(j):
                j = f'crop_{int(code)}' if pd.notna(code) else pd.NA
            recs.append(dict(t=wave, i=format_id(r[hh]), plot_id=format_id(r[plot]), j=j, rawcode=code,
                             module=mod, src=f'{key_}:{idx}', Quantity=qty,
                             harvest_month=r[hm], intercrop=r[ic]))
    LC = pd.DataFrame(recs)
    # sales per (i, code)
    sp = []
    for key_, qc, vc in [('s5a', 'ag5a_02', 'ag5a_03'), ('s7a', 'ag7a_03', 'ag7a_04'), ('s7b', 'ag7b_03', 'ag7b_04')]:
        d = raw[key_]
        s = pd.DataFrame({'i': d[hh].apply(format_id), 'rawcode': pd.to_numeric(d['cropid'], errors='coerce'),
                          'qs': pd.to_numeric(d[qc], errors='coerce'), 'vs': pd.to_numeric(d[vc], errors='coerce'),
                          'smod': key_})
        sp.append(s.dropna(subset=['rawcode']))
    SP = pd.concat(sp)
    C['sales_rows'] = len(SP)
    C['sales_dup_i_code'] = int(SP.duplicated(['i', 'rawcode'], keep=False).sum())
    sales = SP.groupby(['i', 'rawcode'])[['qs', 'vs']].sum(min_count=1).reset_index()
    LC['rawcode'] = pd.to_numeric(LC['rawcode'], errors='coerce')
    nplots = LC.groupby(['i', 'rawcode'])['plot_id'].transform('nunique')
    LC = LC.merge(sales, on=['i', 'rawcode'], how='left')
    LC.loc[(nplots != 1).values, ['qs', 'vs']] = pd.NA
    LC.to_pickle(priv / f'crop_production_long_{wave}.pkl')
    C['source_long_rows'] = len(LC)
    key = ['t', 'i', 'plot_id', 'j']
    C['null_keys_long'] = {k: int(LC[k].isna().sum()) for k in key}
    cls = Counter(); rows_in_class = Counter(); flags = Counter(); sizes = Counter(); modpairs = Counter()
    jlumps = Counter()
    for kv, g in LC.groupby(key, dropna=False):
        if len(g) == 1:
            continue
        sizes[len(g)] += 1
        n_src = g['src'].nunique()
        codes = g['rawcode'].nunique()
        mods = tuple(sorted(g['module']))
        modpairs[mods] += 1
        qn = g['Quantity'].notna().sum()
        if n_src < len(g):
            c = 'replication(same source record)'
        elif codes == 1 and len(set(mods)) > 1:
            c = 'same crop code in two modules'
        elif codes == 1:
            c = 'same crop code repeated within a module'
        else:
            c = 'distinct crop codes lumped by harmonize_crop'
        cls[c] += 1
        rows_in_class[c] += len(g)
        if codes > 1:
            jlumps[kv[3]] += 1
        flags['all_Quantity_NA(sum->0.0)'] += int(qn == 0)
        flags['>=2 quantities summed'] += int(qn >= 2)
        sold = g[['qs', 'vs']].dropna(how='all').drop_duplicates()
        flags['sale_rows_with_value'] = flags['sale_rows_with_value'] + int(g['qs'].notna().any() or g['vs'].notna().any())
        flags['sale_distinct_nonnull>1 (first drops one)'] += int(len(sold) > 1)
        # sale attached from >1 crop code (would be separate sales facts)
        flags['sale_from_>1_code'] += int(g.dropna(subset=['qs', 'vs'], how='all')['rawcode'].nunique() > 1)
        hmv = pd.to_numeric(g['harvest_month'], errors='coerce')
        hmv = hmv[(hmv >= 1) & (hmv <= 12)]
        flags['harvest_month_disagree'] += int(hmv.nunique() > 1)
        icv = pd.to_numeric(g['intercrop'], errors='coerce').dropna().unique()
        flags['intercrop_disagree'] += int(len(icv) > 1)
    C['dup_groups_by_class'] = dict(cls)
    C['dup_rows_by_class'] = dict(rows_in_class)
    C['dup_group_sizes'] = {str(k): v for k, v in sorted(sizes.items())}
    C['dup_group_module_mix'] = {'+'.join(k): v for k, v in sorted(modpairs.items())}
    C['dup_group_flags'] = dict(flags)
    C['lumped_labels'] = dict(jlumps)
    C['u_values_helper'] = {str(k): int(v) for k, v in cp['u'].value_counts(dropna=False).items()}
    if cpre is not None:
        C['helper_pre_rows_eq_long_rows'] = len(cpre) == len(LC)

agg_out.write_text(json.dumps(summary, indent=1, default=str) + '\n')
print(json.dumps(summary, indent=1, default=str))
