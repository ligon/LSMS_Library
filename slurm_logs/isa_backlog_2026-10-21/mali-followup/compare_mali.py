"""Compare base vs branch Mali plot_inputs (pickles from measure_mali.py).

Asserts the ONLY difference is `crop` NaN -> '(not crop-specific)', on
exactly the rows where base had NaN: same row count, same order after the
base NaN is mapped to the marker, identical values (incl. Applied) and
dtypes.  Same for the Feature('plot_inputs')(['Mali']) frames on `j`.
Writes aggregate counts only.

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
    r = {'base_rows': len(b), 'branch_rows': len(n),
         'index_names_equal': list(b.index.names) == list(n.index.names),
         'columns_equal': list(b.columns) == list(n.columns),
         'dtypes_equal': b.dtypes.astype(str).to_dict() == n.dtypes.astype(str).to_dict()}
    bf = b.reset_index()
    nf = n.reset_index()
    was_nan = bf[level].isna()
    r['base_level_nan'] = int(was_nan.sum())
    r['branch_level_nan'] = int(nf[level].isna().sum())
    r['branch_marker'] = int((nf[level] == MARKER).sum())
    r['base_marker'] = int((bf[level] == MARKER).sum())
    mapped = bf.copy()
    mapped[level] = mapped[level].astype('string').fillna(MARKER)
    nf2 = nf.copy()
    nf2[level] = nf2[level].astype('string')
    key = [c for c in b.index.names]
    mapped = mapped.sort_values(key, na_position='first').reset_index(drop=True)
    nf2 = nf2.sort_values(key, na_position='first').reset_index(drop=True)
    pd.testing.assert_frame_equal(mapped, nf2, check_exact=True)
    r['identical_after_nan_to_marker'] = True
    # The marker sits on exactly the base-NaN rows (by full key).
    bk = set(map(tuple, mapped.loc[mapped.index[mapped[level] == MARKER], key].astype(str).values))
    r['marker_rows_are_base_nan_rows'] = (
        len(bk) == r['base_level_nan'] == r['branch_marker'])
    r['marker_by_input'] = {str(k): int(v) for k, v in
                            nf.loc[nf[level] == MARKER, 'input'].value_counts().sort_index().items()}
    r['marker_by_input_wave'] = {f'{t}|{i}': int(v) for (t, i), v in
                                 nf.loc[nf[level] == MARKER].groupby(['t', 'input']).size().items()}
    r['applied_equal'] = bool(b['Applied'].reset_index(drop=True).astype('string').fillna('<NA>')
                              .value_counts().equals(n['Applied'].reset_index(drop=True)
                                                     .astype('string').fillna('<NA>').value_counts()))
    full = list(n.index.names)
    r['branch_full_groupby_size_sum'] = int(n.groupby(full).size().sum())
    r['base_full_groupby_size_sum'] = int(b.groupby(full).size().sum())
    res[name] = r

# Seed rows under the marker: which raw crop, which wave, with a quantity?
n = pd.read_pickle(f'{branch_dir}/mali_country.pkl').reset_index()
seed = n[(n['input'] == 'Seed') & (n['crop'] == MARKER)]
res['seed_marker_rows'] = {'total': int(len(seed)),
                           'by_wave': {str(k): int(v) for k, v in seed.groupby('t').size().items()},
                           'with_positive_quantity': int((pd.to_numeric(seed['Quantity'], errors='coerce') > 0).sum())}

with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True)
print(json.dumps(res, indent=1, sort_keys=True))
