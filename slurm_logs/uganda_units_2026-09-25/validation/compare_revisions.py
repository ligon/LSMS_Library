#!/usr/bin/env python3
"""Assert exact served-frame equivalence across two candidate source revisions."""
import argparse
import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--before-dir', type=Path, required=True)
p.add_argument('--after-dir', type=Path, required=True)
p.add_argument('--before-tag', required=True)
p.add_argument('--after-tag', required=True)
p.add_argument('--report', type=Path, required=True)
a = p.parse_args()
names = ['food_acquired', 'food_quantities', 'food_prices', 'food_expenditures',
         'food_kg_factors', 'fct', 'nutrition', 'food_prices_unitvalue',
         'food_prices_unitprice', 'food_prices_kgprice']
result = {'checks': {}, 'comparison': 'exact values, dtypes, columns and sorted canonical keys'}
for name in names:
    before = pd.read_pickle(a.before_dir / f'{a.before_tag}_{name}.pkl').sort_index().sort_index(axis=1)
    after = pd.read_pickle(a.after_dir / f'{a.after_tag}_{name}.pkl').sort_index().sort_index(axis=1)
    pdt.assert_frame_equal(before, after, check_exact=True)
    if name == 'food_kg_factors':
        assert before.attrs['kg_factor_sources'] == after.attrs['kg_factor_sources']
    result['checks'][name] = {'exact': True, 'shape': list(after.shape)}
a.report.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
