"""Reproduce #1159 and its bounded downstream residual with synthetic data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.paths import countries_root, data_root

parser = argparse.ArgumentParser()
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
root = Path.cwd().resolve()
assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
assert countries_root().resolve() == root / 'lsms_library/countries'
assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve() != (Path.home() / '.local/share/lsms_library').resolve()
rows = [(c, '2000-01', 'h1', 'p1', item, 'Maize', 'Kg', q)
        for c, q in [('A', 1.), ('B', 2.)] for item in ['Seed', 'Urea']]
inputs = pd.DataFrame(rows, columns=[
    'country', 't', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity']).set_index(
        ['country', 't', 'i', 'plot_id', 'input', 'j', 'u'])
areas = pd.DataFrame([
    ('A', '2000-01', 'h1', 'p1', 1.), ('B', '2000-01', 'h1', 'p1', 4.)
], columns=['country', 't', 'i', 'plot_id', 'Area']).set_index(
    ['country', 't', 'i', 'plot_id'])

def describe(frame):
    result = {'rows': len(frame), 'index_names': list(frame.index.names),
              'column_totals': {c: float(frame[c].sum()) for c in frame}}
    if 'country' in frame.index.names:
        result['by_country'] = {str(c): float(part.iloc[:, 0].sum())
                                for c, part in frame.groupby(level='country')}
    return result

report = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
          'package': ll.__file__, 'countries_root': str(countries_root()),
          'data_root': str(data_root()),
          'transformations_sha256': hashlib.sha256(Path(T.__file__).read_bytes()).hexdigest(),
          'synthetic_data_only': True, 'reducers': {}, 'downstream': {}}
for axis, frame in [('index', inputs), ('column', inputs.reset_index('country'))]:
    report['reducers'][axis] = {fn.__name__: describe(fn(frame))
                                 for fn in [T.seed_kg, T.nitrogen_kg]}
report['downstream']['pooled_fertilizer_rate'] = describe(
    T.fertilizer_rate(inputs, areas, on='plot'))
for country in ['A', 'B']:
    report['downstream'][country] = describe(T.fertilizer_rate(
        inputs.xs(country, level='country'), areas.xs(country, level='country'), on='plot'))
args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
print(json.dumps(report, indent=2, sort_keys=True))
