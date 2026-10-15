"""Read-only country-source review; aggregate evidence only in committed JSON."""
import json
import os
import warnings
from pathlib import Path

import pandas as pd
import lsms_library as ll

root = Path(os.environ['PYTHONPATH'])
assert Path(ll.__file__).is_relative_to(root), ll.__file__
assert not list((root / 'lsms_library/countries').rglob('*.parquet'))
label = os.environ['PROBE_LABEL']
out = Path(os.environ['LSMS_REVIEW_OUT'])
out.mkdir(exist_ok=True)
records = {}
for country, waves in {'Ethiopia': ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22'], 'Tanzania': ['2019-20', '2020-21']}.items():
    c = ll.Country(country)
    for wave in waves:
        print('BUILD', label, country, wave, flush=True)
        w = c[wave]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            pre = w.grab_data('cluster_features')
            served = w.cluster_features()
        pre.to_pickle(out / f'{label}-{country}-{wave}-pre.pkl')
        served.to_pickle(out / f'{label}-{country}-{wave}-served.pkl')
        flat = pre.reset_index().drop(columns=['i', 't'], errors='ignore')
        group = flat.groupby('v', dropna=False, observed=True)
        cols = [k for k in flat if k != 'v']
        real = {k: int((group[k].nunique(dropna=True) > 1).sum()) for k in cols}
        mixed = {k: int(((group[k].count() > 0) & (group[k].count() < group.size())).sum()) for k in cols}
        row_conflicts = sum(len(g.drop_duplicates()) > 1 for _, g in group)
        conflict_loss = sum(len(g) - 1 for _, g in group if len(g.drop_duplicates()) > 1)
        r = {'pre_shape': list(pre.shape), 'pre_index': pre.index.names, 'served_shape': list(served.shape), 'served_index': served.index.names, 'real_conflicts': real, 'missing_and_value_groups': mixed, 'row_conflict_groups': row_conflicts, 'conflicting_rows_removed_by_core': conflict_loss, 'served_nonnull': {k: int(served[k].notna().sum()) for k in served}, 'warning_categories': [type(x.message).__name__ for x in caught], 'warnings': [str(x.message) for x in caught]}
        records[country + '/' + wave] = r
        print('DONE', country, wave, r, flush=True)
(out / f'{label}-probe.json').write_text(json.dumps(records, indent=2))
