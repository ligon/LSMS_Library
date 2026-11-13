"""GH #637 Tanzania round 2: re-measure the five claims the review challenged.
Aggregates only.  Reads the private pickles written by api_snapshot.py
(development 'base-cold', branch 'branch-final') and source_oracle.py, and the
held sources via get_dataframe.

Usage (cwd = branch checkout, PYTHONPATH = same):
    python round2_measure.py <private_dir> <out_json>
"""
import json
import os
import subprocess
import sys
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
from lsms_library.quantity_audit import audit_quantities
from lsms_library.transformations import U_UNKNOWN

priv, out = Path(sys.argv[1]), Path(sys.argv[2])
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
TZ = countries_root() / 'Tanzania'
res = {}
K5 = ['t', 'i', 'plot_id', 'input', 'crop']

# --- 1. duplicate groups at the 6-level key: recorded unit vs Unknown -------
seed_units_code = {}
for w in ('2019-20', '2020-21'):
    o = pd.read_pickle(priv / f'oracle-source-long-{w}.pkl')
    g6 = o.groupby(K5 + ['u'], dropna=False)
    size = g6.size()
    dup = size[size > 1]
    nq = g6['Quantity'].count()
    u_of = dup.index.get_level_values('u')
    unk = dup[u_of == U_UNKNOWN]
    rec = dup[u_of != U_UNKNOWN]
    unk_q = nq.reindex(unk.index)
    # 5-key classes
    nu = o.groupby(K5, dropna=False)['u'].agg(lambda s: tuple(sorted(set(s))))
    s5 = o.groupby(K5, dropna=False).size()
    d5 = nu[s5 > 1]
    cls = Counter()
    for us in d5:
        if len(us) > 1:
            cls['two recorded units' if U_UNKNOWN not in us else 'Unknown + recorded unit'] += 1
        elif us == (U_UNKNOWN,):
            cls['both Unknown (one row, summed)'] += 1
        else:
            cls['same recorded unit (commensurable)'] += 1
    res.setdefault('plot_inputs_groups', {})[w] = {
        'dup_groups_5key': int((s5 > 1).sum()), 'classes_5key': dict(cls),
        'dup_groups_6key': len(dup), 'dup_6key_recorded_unit': len(rec),
        'dup_6key_unknown': len(unk),
        'dup_6key_unknown_with_>=2_quantities': int((unk_q >= 2).sum()),
        'dup_6key_unknown_inputs_crops': sorted(Counter(zip(unk.index.get_level_values('input'),
                                                            unk.index.get_level_values('crop'))).items()),
    }

# Unit codes behind those Unknown groups (held source).
for w, n4, hh, plot in [('2019-20', 'AG_SEC_4A', 'sdd_hhid', 'plotnum'), ('2020-21', 'ag_sec_4a', 'y5_hhid', 'plot_id')]:
    d4 = get_dataframe(TZ / w / 'Data' / f'{n4}.dta', convert_categoricals=False)
    res['plot_inputs_groups'][w]['seed_unit_code_counts_where_label_missing'] = {
        str(k): int(v) for k, v in d4.loc[~d4['ag4a_10_2'].isin([1, 2, 3, 4]), 'ag4a_10_2']
        .astype('string').fillna('<NA>').value_counts().items()}

# --- 2. seed_kg decomposition over the split groups -------------------------
bpi = pd.read_pickle(priv / 'base-cold_plot_inputs.pkl').reset_index()
rpi = pd.read_pickle(priv / 'branch-final_plot_inputs.pkl').reset_index()
bsk = pd.read_pickle(priv / 'base-cold_seed_kg.pkl')['Seed_kg']
rsk = pd.read_pickle(priv / 'branch-final_seed_kg.pkl')['Seed_kg']
keys = ['t', 'i', 'plot_id', 'input', 'crop']
for f in (bpi, rpi):
    for c in keys + ['u']:
        f[c] = f[c].astype('string')
nunits = rpi.groupby(keys, dropna=False)['u'].nunique()
split = nunits[nunits > 1].index
rows = []
for k in split:
    b = bpi.set_index(keys).loc[[k]]
    r = rpi.set_index(keys).loc[[k]]
    plot = (k[0], k[1], k[2])
    bk = float(bsk.get(plot, 0.0)) if plot in bsk.index else 0.0
    rk = float(rsk.get(plot, 0.0)) if plot in rsk.index else 0.0
    rows.append({
        't': k[0], 'crop': k[4],
        'base_served': f"{float(b['Quantity'].iloc[0]):g} {b['u'].iloc[0]}",
        'branch_served': ' + '.join(f"{float(q):g} {u}" for q, u in sorted(zip(r['Quantity'], r['u']), key=lambda x: x[1])),
        'seed_kg_base': bk, 'seed_kg_branch': rk, 'delta': rk - bk})
rows.sort(key=lambda x: (x['t'], x['delta']))
res['seed_kg_split_groups'] = rows
res['seed_kg_summary'] = {
    'groups': len(rows), 'plots_moved': sum(abs(x['delta']) > 1e-9 for x in rows),
    'total_delta': sum(x['delta'] for x in rows),
    'delta_where_base_served_bucket': sum(x['delta'] for x in rows if x['base_served'].endswith('20l Bucket')),
    'delta_where_base_served_kg': sum(x['delta'] for x in rows if x['base_served'].endswith('Kg')),
    'delta_other': sum(x['delta'] for x in rows if not (x['base_served'].endswith('20l Bucket') or x['base_served'].endswith('Kg'))),
    'positive_deltas': sum(x['delta'] > 0 for x in rows)}

# --- 3/4. Site Q: full vs positive-only reference, both checkouts -----------
def offenders(df):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        reps = audit_quantities(df, 'Quantity', country='Tanzania', table='crop_production')
    s = {}
    for rp in reps:
        for o in rp['offenders']:
            s[(rp['wave'], o['i'], o.get('plot_id', o.get('plot')), o['j'], float(o['value']))] = o
    return s

q = {}
for lab in ('base-cold', 'branch-final'):
    cp = pd.read_pickle(priv / f'{lab}_crop_production.pkl')
    pos = cp[~(pd.to_numeric(cp['Quantity'], errors='coerce') <= 0).fillna(False)]
    q[lab] = {'full': offenders(cp), 'positive_only': offenders(pos)}
B, R = q['base-cold'], q['branch-final']
new = set(R['full']) - set(B['full'])
gone = set(B['full']) - set(R['full'])
res['site_q'] = {
    'base_full': len(B['full']), 'branch_full': len(R['full']),
    'base_positive_only': len(B['positive_only']), 'branch_positive_only': len(R['positive_only']),
    'base_positive_only_equals_base_full': set(B['positive_only']) == set(B['full']),
    'branch_positive_only_equals_base_full': set(R['positive_only']) == set(B['full']),
    'new_offenders': len(new), 'dropped_offenders': len(gone),
    'new_by_wave_j': {f'{w}|{j}': n for (w, j), n in Counter((k[0], k[3]) for k in new).items()},
    'new_survive_positive_only': len(new & set(R['positive_only'])),
}
for w in ('2019-20', '2020-21'):
    cp = pd.read_pickle(priv / 'branch-final_crop_production.pkl')
    sub = cp[(cp.index.get_level_values('t') == w) & (cp.index.get_level_values('j') == 'Timber')]
    qq = pd.to_numeric(sub['Quantity'], errors='coerce')
    res['site_q'][f'timber_kg_cell_{w}'] = {'rows_with_quantity': int(qq.notna().sum()), 'zeros': int((qq == 0).sum()),
                                             'p90_all': float(qq.quantile(0.9)),
                                             'p90_positive': float(qq[qq > 0].quantile(0.9))}

# --- 5. recorded zero harvests with harvested == YES ------------------------
z = {}
for w, mods in [('2019-20', [('AG_SEC_4A', 'ag4a_27', 'ag4a_19'), ('AG_SEC_6A', 'ag6a_09', 'ag6a_06a'), ('AG_SEC_6B', 'ag6b_09', 'ag6b_06a')]),
                ('2020-21', [('ag_sec_4a', 'ag4a_27', 'ag4a_19'), ('ag_sec_6a', 'ag6a_09', 'ag6a_06a'), ('ag_sec_6b', 'ag6b_09', 'ag6b_06a')])]:
    for f, qc, hc in mods:
        d = get_dataframe(TZ / w / 'Data' / f'{f}.dta', convert_categoricals=False)
        qv = pd.to_numeric(d[qc], errors='coerce')
        hv = pd.to_numeric(d[hc], errors='coerce')
        z[f'{w}|{f}'] = {'q0_harvested_yes': int(((qv == 0) & (hv == 1)).sum()),
                          'q0_other': int(((qv == 0) & (hv != 1)).sum())}
res['recorded_zero_quantities'] = z
res['recorded_zero_quantities_total_yes'] = sum(v['q0_harvested_yes'] for v in z.values())
out.write_text(json.dumps(res, indent=1, default=str) + '\n')
print(json.dumps(res, indent=1, default=str))
