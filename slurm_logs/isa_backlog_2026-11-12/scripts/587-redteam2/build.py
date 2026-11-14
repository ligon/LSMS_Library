"""Cold-build Nigeria tables for one side into a private root (GH #587 delta re-review).

usage: python build.py <clone> <side> <framesdir> <resultsdir>
Aggregates -> <resultsdir>/<side>.build.json; frames -> <framesdir> (private).
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env, Recorder  # noqa: E402

clone, side, frames, results = sys.argv[1], sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4])
frames.mkdir(parents=True, exist_ok=True)
results.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)

import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

R = Recorder(results / f'{side}.build.json')
R.data['head'] = head


def save(name, df):
    d = df.copy()
    d.attrs = {}
    p = frames / f'{side}.{name}.parquet'
    d.to_parquet(p)
    return str(p)


def describe(name, df):
    out = {'rows': int(len(df)), 'columns': list(map(str, df.columns)),
           'index': list(map(str, df.index.names)), 'unique': bool(df.index.is_unique),
           'dtypes': {str(k): str(v) for k, v in df.dtypes.items()}}
    if 't' in df.index.names:
        out['rows_by_t'] = {str(k): int(v) for k, v in
                            df.index.get_level_values('t').value_counts().sort_index().items()}
    out['attrs_keys'] = sorted(map(str, df.attrs))
    for k in ('derivations', 'labels_unmatched'):
        if k in df.attrs:
            out[f'attrs_{k}'] = json.loads(json.dumps(df.attrs[k], default=str))
    out['path'] = save(name, df)
    return out


c = ll.Country('Nigeria')
R.step('waves', lambda: list(c.waves))

CALLS = [
    ('community_prices', lambda: c.community_prices()),
    ('community_prices_aggregate', lambda: c.community_prices(labels='Aggregate')),
    ('community_prices_2012Q3', lambda: c.community_prices(labels='2012Q3')),
    ('sample', lambda: c.sample()),
    ('household_roster', lambda: c.household_roster()),
    ('food_acquired', lambda: c.food_acquired()),
    ('food_acquired_aggregate', lambda: c.food_acquired(labels='Aggregate')),
    ('food_acquired_2012Q3', lambda: c.food_acquired(labels='2012Q3')),
    ('crop_production', lambda: c.crop_production()),
    ('plot_features', lambda: c.plot_features()),
    ('plot_inputs', lambda: c.plot_inputs()),
    ('food_expenditures_aggregate', lambda: c.food_expenditures(labels='Aggregate')),
    ('food_quantities', lambda: c.food_quantities()),
]

for name, fn in CALLS:
    def run(name=name, fn=fn):
        t0 = time.time()
        df = fn()
        out = describe(name, df)
        out['seconds'] = round(time.time() - t0, 1)
        return out
    R.step(name, run)


def audit_reports():
    out = {}
    from lsms_library.null_read_audit import null_read_reports
    from lsms_library.null_index_audit import null_index_reports
    from lsms_library.quantity_audit import quantity_reports
    from lsms_library.country import grain_reports
    for nm, fn in (('null_read', null_read_reports), ('null_index', null_index_reports),
                   ('quantity', quantity_reports), ('grain', grain_reports)):
        try:
            reps = fn(country='Nigeria')
        except TypeError:
            reps = fn('Nigeria')
        by = {}
        for r in reps:
            key = str(r.get('table', '?')) if isinstance(r, dict) else '?'
            by[key] = by.get(key, 0) + 1
        out[nm] = by
    return out


R.step('audit_reports', audit_reports)
R.data['done'] = True
R.flush()
print('BUILD DONE', side, flush=True)
