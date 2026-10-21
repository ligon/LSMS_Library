"""Run only the three #778 force-build controls in a NEW physical private L2.

Usage: python isolation_probe.py SOURCE_ROOT NEW_PRIVATE_ROOT REPORT.json [--cold]
SOURCE_ROOT is read-only. Only its L1 directory may be symlinked into the copy.
Aggregate report contains no survey rows. Run with matching cwd/PYTHONPATH.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import lsms_library as ll
from lsms_library.local_tools import data_root
import pytest

source, root, output = map(Path, sys.argv[1:4])
cold = sys.argv[4:] == ['--cold']
assert not sys.argv[4:] or cold
top = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == top / 'lsms_library/countries'
assert root.is_absolute() and root.is_relative_to(Path('/local/job40255479'))
assert root.resolve() != source.resolve() and not root.exists()
countries = ('Guyana', 'India', 'Togo')
root.mkdir()
if not cold:
    for country in countries:
        shutil.copytree(source / country, root / country)
blobs = source / 'dvc-cache'
if blobs.exists():
    (root / 'dvc-cache').symlink_to(blobs.resolve(), target_is_directory=True)


def snapshot(base):
    return {str(path.relative_to(base)): {
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'inode': path.stat().st_ino, 'mtime_ns': path.stat().st_mtime_ns,
    } for country in countries for path in (base / country).rglob('*') if path.is_file()}


original = snapshot(source)
before = snapshot(root)
assert cold or original.keys() == before.keys()
assert all(original[key]['sha256'] == before[key]['sha256'] for key in before)
assert all(original[key]['inode'] != before[key]['inode'] for key in before)
os.environ['LSMS_DATA_DIR'] = str(root)
data_root.cache_clear()
assert data_root().resolve() == root.resolve()
events = []
unlink = Path.unlink


def trace_unlink(path, *args, **kwargs):
    if path.suffix == '.parquet':
        events.append({'root': str(data_root()), 'country': path.parts[-3] if path.parent.name == 'var' else None,
                       'outer_root': path.is_relative_to(root), 'name': path.name})
    return unlink(path, *args, **kwargs)


class Probe:
    def pytest_runtest_call(self, item):
        events.append({'test': item.nodeid, 'root': str(data_root())})


Path.unlink = trace_unlink
try:
    tests = [
        '-q',
        'tests/test_guyana_index_uniqueness.py::test_no_silent_collapse_warning_on_cold_build',
        'tests/test_india_employment_activity_index.py::test_no_gh323_collapse_warning_on_a_COLD_build',
    ]
    if not cold:
        tests.append('tests/test_gh866_stale_country_parquet_not_laundered.py::test_a_wave_produced_table_is_never_evicted')
    code = pytest.main(tests, plugins=[Probe()])
finally:
    Path.unlink = unlink
after = snapshot(root)
source_after = snapshot(source)
keys = before.keys() | after.keys()
result = {
    'pytest_exit_code': int(code), 'copied_files': len(before), 'cold_outer_root': cold,
    'copy_sha_matches': True, 'copy_distinct_inodes': True,
    'source_bytes_and_stats_unchanged': original == source_after,
    'outer_byte_changes': [key for key in sorted(keys) if before.get(key, {}).get('sha256') != after.get(key, {}).get('sha256')],
    'outer_file_replacements': [key for key in sorted(keys) if before.get(key) != after.get(key)],
    'events': events,
    'root_restored': data_root().resolve() == root.resolve(),
}
output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
assert original == source_after, 'SOURCE_ROOT was changed'
assert result['root_restored'], 'fixture did not restore the outer private root'
raise SystemExit(int(code))
