"""Measure actual country/wave hashes, not only tagged build closures."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import lsms_library as ll
from lsms_library import _build_registry as registry
from lsms_library.paths import countries_root

top = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
assert countries_root().resolve() == top / 'lsms_library/countries'
assert Path(os.environ['LSMS_DATA_DIR']).resolve().is_relative_to(Path('/local'))
country_hashes, wave_hashes, errors, tables = {}, {}, {}, set()
for scheme in sorted(countries_root().glob('*/_/data_scheme.yml')):
    name = scheme.parent.parent.name
    c = ll.Country(name)
    for table in c.data_scheme:
        tables.add(table)
        key = f'{name}/{table}'
        try:
            country_hashes[key] = c._table_cache_hash(table, c.waves)
            for wave in c.waves:
                wave_hashes[f'{key}/{wave}'] = c[wave]._input_hash(table)
        except Exception as exc:
            errors[key] = f'{type(exc).__name__}: {exc}'
report = {
    'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'package': ll.__file__, 'country': country_hashes, 'wave': wave_hashes,
    'tagged': {str(t): registry.build_transforms_fingerprint(t) for t in sorted(tables)},
    'errors': errors,
}
Path(sys.argv[1]).write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
print(f'hashes: {len(country_hashes)} country, {len(wave_hashes)} wave; {len(errors)} errors')
