"""Aggregate paired audit-cache food API snapshots; no private rows emitted."""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from lsms_library.local_tools import get_dataframe

root = Path('/local/job40255479/canonical-audit-compare')
expected = {'Senegal': (783, 692), 'Benin': (126, 68), 'CotedIvoire': (36, 23)}
report = {}
for country, counts in expected.items():
    result = {}
    for name, column in [('food_acquired', None), ('quantities', 'Quantity'), ('prices', 'Price')]:
        frames = [get_dataframe(str(root / f'data-{label}' / 'comparison-snapshots' /
                                    f'{country}-{name}.parquet'))
                  for label in ['baseline', 'combined']]
        a, b = frames
        pd.testing.assert_index_equal(a.index, b.index, exact=True)
        pd.testing.assert_index_equal(a.columns, b.columns, exact=True)
        assert list(a.dtypes) == list(b.dtypes)
        if column is None:
            pd.testing.assert_frame_equal(a, b, check_exact=True)
            result[name] = {'rows': len(a), 'exactly_unchanged': True}
            continue
        av, bv = [frame[column].to_numpy(dtype=float, na_value=np.nan) for frame in frames]
        changed = ~np.isclose(av, bv, rtol=1e-12, atol=0, equal_nan=True)
        assert int(changed.sum()) == counts[0 if name == 'quantities' else 1]
        if name == 'quantities':
            native = a.index.get_level_values('u') != 'kg'
            assert not changed[native].any()
        waves = {}
        for wave in a.index.get_level_values('t').unique():
            mask = a.index.get_level_values('t') == wave
            if name == 'quantities':
                mask &= a.index.get_level_values('u') == 'kg'
            def total(values):
                return math.fsum(float(x) for x in values if np.isfinite(x))
            waves[str(wave)] = {
                'before_sum': total(av[mask]), 'after_sum': total(bv[mask]),
                'delta': total(bv[mask] - av[mask]),
                'changed_rows': int(changed[mask].sum()),
            }
        result[name] = {'rows': len(a), 'changed_rows': int(changed.sum()),
                        'by_wave': waves, 'index_columns_dtypes_unchanged': True}
    report[country] = result
report['totals'] = {'changed_quantity_rows': sum(r['quantities']['changed_rows'] for r in report.values()),
                    'changed_price_rows': sum(r['prices']['changed_rows'] for r in report.values())}
(root / 'food-comparison.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
print(json.dumps(report['totals'], sort_keys=True))
