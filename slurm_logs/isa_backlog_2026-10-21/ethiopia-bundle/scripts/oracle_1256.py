"""Raw oracle for GH #1256: recompute which served W4/W5 crop_production rows carry a
permanent-crop flag, from the raw §9 + §4 files, independently of the builder,
and compare with the served Derivation stamp.  Also cross-tabulate the stamp
against the WB perennial list and the §11 permanent-block marker.
usage: oracle_1256.py <checkout> <api.parquet> <out.json>"""
import os, sys, json, warnings
checkout, api, out = sys.argv[1:4]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe, format_id
R = f'{checkout}/lsms_library/countries/Ethiopia'
sys.path.insert(0, f'{R}/_'); import ethiopia as E
KEY = 'Ethiopia::crop_production::permanent-crop-12-month-window'
cp = pd.read_parquet(api)
res = {}
for w, n, c11 in [('2018-19', 4, 's11q01'), ('2021-22', 5, 's11q01b')]:
    cm = E._eth_crop_label_map_for_wave(w)
    s9 = get_dataframe(f'{R}/{w}/Data/sect9_ph_w{n}.dta', convert_categoricals=False)
    s4 = get_dataframe(f'{R}/{w}/Data/sect4_pp_w{n}.dta', convert_categoricals=False)
    s11 = get_dataframe(f'{R}/{w}/Data/sect11_ph_w{n}.dta', convert_categoricals=False)
    pid = lambda d: (d['holder_id'].apply(format_id).astype(str) + '_' + d['parcel_id'].apply(format_id).astype(str) + '_' + d['field_id'].apply(format_id).astype(str))
    F4 = pd.DataFrame({'plot_id': pid(s4).values, 'code': pd.to_numeric(s4['s4q01b'], errors='coerce').values,
                       'flag': pd.to_numeric(s4['s4q15'], errors='coerce').values}).dropna(subset=['code'])
    g = F4.groupby(['plot_id', 'code'])['flag']
    perm_keys = set(k for k, v in g.apply(lambda x: bool((x == 1).all())).items() if v)
    conflict = int((g.nunique(dropna=False) > 1).sum())
    H = pd.DataFrame({'plot_id': pid(s9).values, 'code': pd.to_numeric(s9['s9q00b'], errors='coerce').values}).dropna(subset=['code'])
    H['j'] = H['code'].map(lambda c: cm.get(int(c)))
    H['perm'] = [(p, c) in perm_keys for p, c in zip(H.plot_id, H.code)]
    k4 = set(zip(F4.plot_id, F4.code))
    H['has4'] = [(p, c) in k4 for p, c in zip(H.plot_id, H.code)]
    # (plot_id, j) whose §9 codes disagree on permanence (folded labels) are not identifiable
    pj = H.groupby(['plot_id', 'j'])['perm'].agg(['all', 'any'])
    expected = set(pj[pj['all']].index); ambiguous = set(pj[pj['any'] & ~pj['all']].index)
    sv = cp[cp.index.get_level_values('t') == w].reset_index()
    sv['any_val'] = sv[['Quantity', 'Quantity_sold', 'Value_sold']].notna().any(axis=1)
    sv['stamped'] = (sv['Derivation'] == KEY).fillna(False)
    keys = list(zip(sv.plot_id, sv.j))
    sv['exp'] = [k in expected for k in keys]; sv['amb'] = [k in ambiguous for k in keys]
    cmp_ = sv[~sv.amb]
    r = {
        's9_rows': int(len(H)), 's9_rows_flag_yes': int(H.perm.sum()), 's9_rows_no_s4_line': int((~H.has4).sum()),
        's4_keys_with_disagreeing_lines': conflict,
        'served_rows': int(len(sv)), 'served_rows_stamped': int(sv.stamped.sum()),
        'served_rows_expected_by_oracle': int((cmp_.exp & cmp_.any_val).sum()),
        'served_rows_compared': int(len(cmp_)), 'served_rows_ambiguous_folded_label': int(sv.amb.sum()),
        'mismatch_stamped_not_expected': int((cmp_.stamped & ~(cmp_.exp & cmp_.any_val)).sum()),
        'mismatch_expected_not_stamped': int((~cmp_.stamped & (cmp_.exp & cmp_.any_val)).sum()),
        'flagged_rows_with_no_value_not_stamped': int((cmp_.exp & ~cmp_.any_val).sum()),
    }
    # crosstab vs perennial and vs the §11 permanent block (Q2 answered) on served rows
    r['stamped_x_perennial'] = {f'stamped={a} perennial={b}': int(v) for (a, b), v in sv.groupby(['stamped', sv['perennial'].fillna(False).astype(bool)]).size().items()}
    L = pd.DataFrame({'holder': s11['holder_id'].apply(format_id).values, 'code': pd.to_numeric(s11[c11], errors='coerce').values,
                      'q2': pd.to_numeric(s11['s11q02a1'], errors='coerce').notna().values}).dropna(subset=['code'])
    L['j'] = L['code'].map(lambda c: cm.get(int(c)))
    q2 = set(L.groupby(['holder', 'j'])['q2'].any().loc[lambda x: x].index)
    sv['holder'] = sv.plot_id.str.split('_').str[0]
    sv['q2'] = [k in q2 for k in zip(sv.holder, sv.j)]
    r['stamped_x_s11_q2_answered'] = {f'stamped={a} q2={b}': int(v) for (a, b), v in sv.groupby(['stamped', 'q2']).size().items()}
    pos = (sv.Value_sold > 0).fillna(False)
    r['stamped_rows_with_positive_Value_sold'] = int((sv.stamped & pos).sum())
    r['stamped_rows_by_crop_top12'] = {k: int(v) for k, v in sv[sv.stamped].j.value_counts().head(12).items()}
    r['unstamped_perennial_rows_by_crop_top8'] = {k: int(v) for k, v in sv[~sv.stamped & sv.perennial.fillna(False).astype(bool)].j.value_counts().head(8).items()}
    r['stamped_nonperennial_rows_by_crop_top8'] = {k: int(v) for k, v in sv[sv.stamped & ~sv.perennial.fillna(False).astype(bool)].j.value_counts().head(8).items()}
    res[w] = r
json.dump(res, open(out, 'w'), indent=1); print(json.dumps(res, indent=1))
