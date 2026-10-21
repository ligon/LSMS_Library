"""Reproduce roster order on unchanged baseline; private frames never printed."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import pandas as pd
import lsms_library as ll
from lsms_library.diagnostics import load_feature
from lsms_library.paths import countries_root
from lsms_library.local_tools import data_root
repo=Path('/local/job40255479/worktrees/baseline-976fc840d')
sha='976fc840d12ba40f1602b7dff58b81548632b2a9'
root=Path(os.environ['LSMS_DATA_DIR'])
assert Path.cwd().resolve()==repo
assert Path(ll.__file__).resolve()==repo/'lsms_library/__init__.py'
assert countries_root().resolve()==repo/'lsms_library/countries'
assert data_root().resolve()==root.resolve()
assert 'roster-review-seed-' in str(root)
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==sha
assert not list(root.rglob('*.parquet'))
co=ll.Country('Benin',preload_panel_ids=False)
wave=co['2018-19']
merge_on=list(set('t').union(wave.resources['household_roster']['merge_on']))
served=load_feature(co,'household_roster')
served.to_pickle(root/'served.pkl')
raw=pd.read_parquet(root/'Benin/var/household_roster.parquet')
raw.to_pickle(root/'raw-country.pkl')
raw_wave=pd.read_parquet(root/'Benin/2018-19/_/household_roster.parquet')
raw_wave.to_pickle(root/'raw-wave.pkl')
spec=importlib.util.spec_from_file_location('fingerprint',repo/'tests/generate_baseline.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
summary={'source_sha':sha,'seed':os.environ['PYTHONHASHSEED'],'merge_on_order':merge_on,
         'served':mod.fingerprint(served),'canonical_served':mod.fingerprint(served.sort_index().sort_index(axis=1)),
         'raw_country':mod.fingerprint(raw),'raw_wave':mod.fingerprint(raw_wave),
         'canonical_raw_country':mod.fingerprint(raw.sort_index().sort_index(axis=1)),
         'canonical_raw_wave':mod.fingerprint(raw_wave.sort_index().sort_index(axis=1))}
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,sort_keys=True))
