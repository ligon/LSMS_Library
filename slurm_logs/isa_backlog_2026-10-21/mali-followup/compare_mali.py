"""Compare base vs branch Mali plot_inputs (pickles from measure_mali.py).

Rule under test (round 2): a NON-SEED row whose base `crop` was NaN now
carries '(not crop-specific)'; a SEED row whose base `crop` was NaN (its
recorded crop is not mapped by harmonize_food) now carries its recorded
label.  Asserts that is the ONLY difference: same rows, identical values
(incl. Applied) and dtypes, the marker on exactly the base-NaN non-seed rows,
and the base-NaN seed rows identical once `crop` is set aside.  Same for the
Feature('plot_inputs')(['Mali']) frames on `j`.  Aggregate counts only.

Usage: python compare_mali.py <base_dir> <branch_dir> <out.json>
"""
import json
import sys

import pandas as pd

base_dir, branch_dir, out_path = sys.argv[1:4]
MARKER = '(not crop-specific)'
res = {}

for name, level in (('mali_country.pkl', 'crop'), ('mali_feature.pkl', 'j')):
    b = pd.read_pickle(f'{base_dir}/{name}')
    n = pd.read_pickle(f'{branch_dir}/{name}')
    key = list(b.index.names)
    r = {'base_rows': len(b), 'branch_rows': len(n),
         'index_names_equal': key == list(n.index.names),
         'columns_equal': list(b.columns) == list(n.columns),
         'dtypes_equal': b.dtypes.astype(str).to_dict() == n.dtypes.astype(str).to_dict()}
    bf, nf = b.reset_index(), n.reset_index()
    base_labels = set(bf[level].dropna())
    b_nan = bf[level].isna()
    b_seed = bf['input'] == 'Seed'
    n_seed = nf['input'] == 'Seed'
    n_pass = n_seed & ~nf[level].isin(base_labels) & nf[level].notna()
    r['base_level_nan'] = int(b_nan.sum())
    r['base_level_nan_nonseed'] = int((b_nan & ~b_seed).sum())
    r['base_level_nan_seed'] = int((b_nan & b_seed).sum())
    r['branch_level_nan'] = int(nf[level].isna().sum())
    r['branch_marker'] = int((nf[level] == MARKER).sum())
    r['branch_marker_on_seed'] = int(((nf[level] == MARKER) & n_seed).sum())
    r['branch_seed_passthrough'] = int(n_pass.sum())
    r['branch_seed_passthrough_by_label'] = {
        str(k): int(v) for k, v in nf.loc[n_pass, level].value_counts().sort_index().items()}
    r['branch_seed_passthrough_by_wave'] = {
        str(k): int(v) for k, v in nf.loc[n_pass].groupby('t').size().items()}

    def _sorted(df):
        df = df.copy()
        df[level] = df[level].astype('string')
        return df.sort_values(key, na_position='first').reset_index(drop=True)

    # Everything except the base-NaN seed rows: identical after NaN -> marker.
    rest_b = bf[~(b_nan & b_seed)].copy()
    rest_b[level] = rest_b[level].astype('string').fillna(MARKER)
    rest_n = nf[~n_pass]
    pd.testing.assert_frame_equal(_sorted(rest_b), _sorted(rest_n), check_exact=True)
    r['identical_outside_seed_passthrough'] = True
    # The base-NaN seed rows: identical once `crop` is set aside.
    other = [c for c in bf.columns if c != level]
    sb = bf.loc[b_nan & b_seed, other].sort_values([k for k in key if k != level]).reset_index(drop=True)
    sn = nf.loc[n_pass, other].sort_values([k for k in key if k != level]).reset_index(drop=True)
    pd.testing.assert_frame_equal(sb, sn, check_exact=True)
    r['seed_rows_identical_but_crop'] = True
    r['marker_by_input'] = {str(k): int(v) for k, v in
                            nf.loc[nf[level] == MARKER, 'input'].value_counts().sort_index().items()}
    full = list(n.index.names)
    r['branch_full_groupby_size_sum'] = int(n.groupby(full).size().sum())
    r['base_full_groupby_size_sum'] = int(b.groupby(full).size().sum())
    res[name] = r

p = res['mali_country.pkl']
res['all_ok'] = (p['base_rows'] == p['branch_rows'] and p['dtypes_equal']
                 and p['branch_level_nan'] == 0 and p['branch_marker_on_seed'] == 0
                 and p['branch_marker'] == p['base_level_nan_nonseed']
                 and p['branch_seed_passthrough'] == p['base_level_nan_seed']
                 and p['branch_full_groupby_size_sum'] == p['branch_rows'])
with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True, ensure_ascii=False)
print(json.dumps(res, indent=1, sort_keys=True, ensure_ascii=False))
