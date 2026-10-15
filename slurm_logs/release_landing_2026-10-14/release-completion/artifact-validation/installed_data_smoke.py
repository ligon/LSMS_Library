"""Prepared installed-wheel data smoke; run only after coordinator verification.

Uses only the separately prepared Uganda cache. It is warm-data validation,
separate from the empty-cache credential-free metadata smoke. Fingerprinting
exactly follows tests/generate_baseline.py without importing checkout code.
"""
from pathlib import Path
from zipfile import ZipFile
import argparse
import collections
import hashlib
import importlib.metadata
import json
import os
import subprocess
import sysconfig
import time
import warnings

parser = argparse.ArgumentParser()
parser.add_argument('--expected-version', required=True)
parser.add_argument('--expected-sha', required=True)
parser.add_argument('--expected-wheel-sha256', required=True)
args = parser.parse_args()
assert args.expected_version == '0.15.0'
assert len(args.expected_sha) == 40 and all(c in '0123456789abcdef' for c in args.expected_sha)
assert len(args.expected_wheel_sha256) == 64 and all(c in '0123456789abcdef' for c in args.expected_wheel_sha256)

OUT = Path(__file__).resolve().parent
ARTIFACTS = OUT.parents[1] / 'final-release-docs'
DATA = Path('/local/job39919092/release-work/runtime/data-installed-release-smoke')
ENV = Path('/local/job39919092/release-work/published-install-env')
PREPARED = OUT.parent / 'final-promotion/installed-smoke-copy-manifest.json'
assert Path.cwd() == OUT
assert not (OUT / 'result.json').exists()
assert 'PYTHONPATH' not in os.environ and 'LSMS_COUNTRIES_ROOT' not in os.environ
assert not any(os.environ.get(k) for k in ('LSMS_NO_CACHE', 'LSMS_BUILD_BACKEND', 'PYTHONSAFEPATH'))
assert os.environ['LSMS_DATA_DIR'] == str(DATA)
assert os.sched_getaffinity(0) <= set(range(8))
for key in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'LSMS_BUILD_WORKERS', 'LSMS_MAKE_JOBS'):
    assert os.environ.get(key) == '1', key

import pandas as pd
import lsms_library as ll
from lsms_library import config
from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import read_parquet_cache_hash, read_parquet_cache_waves

pkg = Path(ll.__file__).resolve().parent
site = Path(sysconfig.get_path('purelib')).resolve()
assert site.is_relative_to(ENV) and pkg == site / 'lsms_library'
assert countries_root().resolve() == pkg / 'countries'
assert data_root().resolve() == DATA
assert importlib.metadata.version('LSMS_Library') == args.expected_version
assert subprocess.run(['git', '-C', str(pkg), 'rev-parse', '--is-inside-work-tree'], capture_output=True).returncode != 0
assert config.microdata_api_key() is None
assert not config.s3_creds_path().exists() and not config.s3_write_creds_path().exists()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(path):
    st = path.stat()
    return {'sha256': sha(path), 'bytes': st.st_size, 'mtime_ns': st.st_mtime_ns,
            'device': st.st_dev, 'inode': st.st_ino}


def cache_inventory():
    links = {str(p.relative_to(DATA)): str(p.readlink()) for p in DATA.rglob('*') if p.is_symlink()}
    assert links == {'dvc-cache': '/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache'}
    return {str(p.relative_to(DATA)): identity(p) for p in sorted(DATA.rglob('*.parquet'))}


def fingerprint(frame):
    # Exact existing corpus helper semantics: order-sensitive row/index hash,
    # sorted column list and dtype mapping; no sorting/coercion/rounding of rows.
    return {'shape': list(frame.shape), 'columns': sorted(frame.columns.tolist()),
            'index_names': list(frame.index.names),
            'dtypes': {str(k): str(v) for k, v in sorted(frame.dtypes.items(), key=lambda x: str(x[0]))},
            'content_hash': hashlib.sha256(pd.util.hash_pandas_object(frame, index=True).values.tobytes()).hexdigest()}


published = json.loads((ARTIFACTS / 'published-artifacts.json').read_text())
assert published['version'] == args.expected_version and published['tag_commit'] == args.expected_sha
assert published['all_package_members_match_tagged_git_blobs'] is True
assert published['missing_or_extra_package_members'] == []
wheel_record = next(r for r in published['artifacts'] if r['packagetype'] == 'bdist_wheel')
assert wheel_record['sha256'] == args.expected_wheel_sha256
assert Path(wheel_record['name']).name == wheel_record['name']
wheel = ARTIFACTS / 'dist' / wheel_record['name']
assert sha(wheel) == args.expected_wheel_sha256
with ZipFile(wheel) as archive:
    package_names = {n for n in archive.namelist() if n.startswith('lsms_library/') and not n.endswith('/')}
    installed_names = {'lsms_library/' + str(p.relative_to(pkg)) for p in pkg.rglob('*')
                       if p.is_file() and '__pycache__' not in p.parts}
    assert package_names == installed_names
    assert len(package_names) == published['package_member_count']
    for name in sorted(package_names):
        assert (site / name).read_bytes() == archive.read(name), name
package_before = {str(p.relative_to(pkg)): identity(p) for p in pkg.rglob('*')
                  if p.is_file() and '__pycache__' not in p.parts}

prepared = json.loads(PREPARED.read_text())
assert prepared['target'] == str(DATA) and prepared['files'] == 172
before = cache_inventory()
assert before == prepared['copy_files'], 'prepared smoke cache changed before use'
co = ll.Country('Uganda', preload_panel_ids=False)
expected_waves = ['2005-06', '2009-10', '2010-11', '2011-12', '2013-14', '2015-16', '2018-19', '2019-20']
assert list(co.waves) == expected_waves
cache = DATA / 'Uganda/var/assets.parquet'
expected_hash = co._table_cache_hash('assets', expected_waves)
assert expected_hash == 'b30d0fd4afbe78159cfceae8add20e94e912b9980c192c0bd93e0f9ed09cba1a'
assert read_parquet_cache_hash(cache) == expected_hash
assert set(read_parquet_cache_waves(cache)) == set(expected_waves)
started = time.perf_counter()
with warnings.catch_warnings(record=True) as captured:
    warnings.simplefilter('always')
    frame = co.assets()
elapsed = time.perf_counter() - started
actual = fingerprint(frame)
expected = {'shape': [116808, 1], 'columns': ['Value'], 'index_names': ['i', 't', 'j'],
            'dtypes': {'Value': 'float64'},
            'content_hash': 'ef853b421deca593f6e5286a7925317689561515a48bb629234f94202ebdf714'}
assert actual == expected, (actual, expected)
wave_rows = {str(k): int(v) for k, v in frame.index.get_level_values('t').value_counts().items()}
assert wave_rows == {'2005-06': 22994, '2009-10': 16306, '2010-11': 14394, '2011-12': 14846,
                     '2013-14': 14477, '2015-16': 15073, '2018-19': 9227, '2019-20': 9491}
assert int(frame.index.duplicated().sum()) == 4237
assert int(frame['Value'].isna().sum()) == 0
assert cache_inventory() == before, 'warm smoke changed its disposable cache'
package_after = {str(p.relative_to(pkg)): identity(p) for p in pkg.rglob('*')
                 if p.is_file() and '__pycache__' not in p.parts}
assert package_after == package_before, 'installed package changed excluding bytecode'
result = {'version': args.expected_version, 'tag_commit': args.expected_sha,
          'published_wheel_sha256': args.expected_wheel_sha256,
          'package': str(pkg), 'countries_root': str(countries_root()), 'data_root': str(DATA),
          'installed_members_equal_published_wheel': len(package_names),
          'prepared_copy_manifest_sha256': sha(PREPARED), 'private_parquets_unchanged': len(before),
          'served': actual, 'wave_rows': wave_rows, 'served_duplicate_keys': 4237,
          'source_cache_hash': expected_hash, 'seconds': elapsed,
          'warning_counts': dict(collections.Counter(w.category.__name__ for w in captured)),
          'package_unchanged_excluding_bytecode': True, 'table_api_calls': 1,
          'scope': 'Installed published-wheel ordinary warm assets call on a separate prepared physical copy. Existing immutable L1 is available, but fresh/full cache preflight requires no rebuild. This is separate from credential-free empty-cache smoke. Original/final/shared L2 APIs were not used.'}
(OUT / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
