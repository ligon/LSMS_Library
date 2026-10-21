"""Replay synthetic rate evidence with exact checkout/config/data identity."""
import json
import os
from pathlib import Path
import runpy
import subprocess

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.paths import countries_root, data_root

checkout = Path.cwd().resolve()
assert Path(ll.__file__).resolve().is_relative_to(checkout), ll.__file__
assert countries_root().resolve() == checkout / 'lsms_library/countries'
assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve().is_relative_to(Path('/local'))
frames = runpy.run_path(str(checkout / 'tests/test_gh1160_pooled_fertilizer_rate.py'))['_frames']
report = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
          'package': ll.__file__, 'countries_root': str(countries_root()),
          'data_root': str(data_root()), 'cases': {}}
for on in ['plot', 'parcel']:
    for nutrient, expected in [('N', [.46, .23]), ('product', [1., .5])]:
        for axis in ['index', 'column']:
            pi, pf = frames(axis, axis, on)
            got = T.fertilizer_rate(pi, pf, nutrient=nutrient, on=on)
            assert got.index.names == ['country', 't', 'i', 'parcel' if on == 'parcel' else 'plot_id']
            assert got.iloc[:, 0].tolist() == expected
            report['cases'][f'{nutrient}/{on}/{axis}'] = {
                'rows': len(got), 'index_names': got.index.names,
                'rates': dict(zip(got.index.get_level_values('country'), got.iloc[:, 0]))}
print(json.dumps(report, indent=2, sort_keys=True))
