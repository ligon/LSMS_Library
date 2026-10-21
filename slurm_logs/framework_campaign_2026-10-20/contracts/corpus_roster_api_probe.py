"""Sanctioned roster load from an explicitly new physical cache copy.

Private frame pickle is written only below the selected private data root;
stdout contains a fingerprint and aggregate measurements, never households.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--repo', type=Path, required=True)
p.add_argument('--data', type=Path, required=True)
p.add_argument('--country', required=True)
p.add_argument('--source-sha', required=True)
a = p.parse_args()
import lsms_library as ll
from lsms_library.diagnostics import load_feature
from lsms_library.paths import countries_root
from lsms_library.local_tools import data_root
assert Path.cwd().resolve() == a.repo.resolve()
assert Path(ll.__file__).resolve() == a.repo / 'lsms_library/__init__.py'
assert countries_root().resolve() == a.repo / 'lsms_library/countries'
assert data_root().resolve() == a.data.resolve()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == a.source_sha
assert 'roster-review-' in str(a.data)
path = a.data / a.country / 'var/household_roster.parquet'
before = (path.stat().st_size, path.stat().st_mtime_ns)
co = ll.Country(a.country, preload_panel_ids=False)
df = load_feature(co, 'household_roster')
assert before == (path.stat().st_size, path.stat().st_mtime_ns)
spec = importlib.util.spec_from_file_location('fingerprint', a.repo / 'tests/generate_baseline.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
df.to_pickle(a.data / 'served.pkl')
print(json.dumps({'country': a.country, 'source_sha': a.source_sha, 'served': mod.fingerprint(df),
                  'canonical_served': mod.fingerprint(df.sort_index().sort_index(axis=1)),
                  'index_level_dtypes': [str(df.index.get_level_values(i).dtype) for i in range(df.index.nlevels)],
                  'cache_bytes_mtime_unchanged': True}, sort_keys=True))
