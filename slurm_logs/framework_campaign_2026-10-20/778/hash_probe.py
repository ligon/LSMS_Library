"""Footer/config-only fingerprint inventory; run in the checkout being measured."""
import json
import os
from pathlib import Path
import subprocess
import sys

import lsms_library as ll
from lsms_library._build_registry import build_transforms_fingerprint

top = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == top / 'lsms_library/countries'
assert Path(os.environ['LSMS_DATA_DIR']).resolve().is_relative_to(Path('/local/job40255479'))
out = {'country': {}, 'wave': {}, 'build_transforms': {}}
tables = set()
for name in ll.countries():
    country = ll.Country(name, preload_panel_ids=False)
    for table in country.data_scheme:
        tables.add(table)
        out['country'][f'{name}/{table}'] = country._table_cache_hash(table, country.waves)
        for wave in country.waves:
            out['wave'][f'{name}/{wave}/{table}'] = country[wave]._input_hash(table)
for table in sorted(tables):
    out['build_transforms'][table] = build_transforms_fingerprint(table)
Path(sys.argv[1]).write_text(json.dumps(out, indent=2, sort_keys=True) + '\n')
print({key: len(value) for key, value in out.items()})
