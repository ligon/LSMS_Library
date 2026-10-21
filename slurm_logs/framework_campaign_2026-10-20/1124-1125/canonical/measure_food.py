"""Aggregate food API evidence; individual snapshots stay in private /local."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import lsms_library as ll
from lsms_library.local_tools import to_parquet
from lsms_library.transformations import food_kg_factors

country, phase, out_dir = sys.argv[1:]
top = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == top / 'lsms_library/countries'
root = Path(os.environ['LSMS_DATA_DIR']).resolve()
assert root.is_relative_to(Path('/local'))
private = root / 'comparison-snapshots'
private.mkdir(exist_ok=True)
c = ll.Country(country)
raw = c.food_acquired()
q = c.food_quantities(units='kgs')
p = c.food_prices()
factors = food_kg_factors(raw, unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())

def finite_sum(s):
    return math.fsum(float(v) for v in s.dropna() if np.isfinite(v))

def summary(frame, col, kilograms_only=False):
    if kilograms_only:
        frame = frame[frame.index.get_level_values('u') == 'kg']
    result = {}
    for wave in frame.index.get_level_values('t').unique():
        values = frame.xs(wave, level='t')[col]
        finite = values.dropna()
        finite = finite[np.isfinite(finite)]
        result[str(wave)] = {
            'rows': len(values), 'nonnull': int(values.notna().sum()),
            'finite_sum': finite_sum(values),
            'median': float(finite.median()) if len(finite) else None,
        }
    return result

labels = raw.index.get_level_values('u').astype(str)
comma_mask = labels.str.contains(r'\d,\d', regex=True)
comma_report = {}
for label in labels[comma_mask].unique():
    mask = labels == label
    selected = factors.loc[mask]
    comma_report[label] = {
        'rows': int(mask.sum()),
        'kg_factors': sorted(map(float, selected['kg_per_unit'].dropna().unique())),
        'sources': selected['KgFactorSource'].value_counts().to_dict(),
        'native_quantity_sum': finite_sum(raw.loc[mask, 'Quantity']),
    }
for name, frame in [('food_acquired', raw), ('quantities', q), ('prices', p)]:
    to_parquet(frame, str(private / f'{country}-{name}.parquet'), absolute_path=True)
report = {
    'country': country, 'phase': phase, 'package': ll.__file__,
    'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'private_root': str(root),
    'food_acquired_rows': len(raw), 'quantity_rows': len(q), 'price_rows': len(p),
    'kilograms': summary(q, 'Quantity', True), 'prices': summary(p, 'Price'),
    'comma_labels': comma_report,
    'factor_sources': factors['KgFactorSource'].value_counts().to_dict(),
}
dest = Path(out_dir) / f'{phase}-{country}.json'
dest.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
print(f'{phase} {country}: {len(raw)} source rows, {len(q)} quantity rows, {len(p)} price rows')
