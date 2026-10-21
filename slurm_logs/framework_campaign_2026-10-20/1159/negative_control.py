"""Run final reducer regressions against an untouched, explicitly pinned runtime."""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.paths import countries_root, data_root
import pytest

parser = argparse.ArgumentParser()
parser.add_argument('--runtime', type=Path, required=True)
parser.add_argument('--tests', type=Path, required=True)
parser.add_argument('--head', default='852ac2bfcd5f798533f56e7efa1f77db80c0138f')
args = parser.parse_args()
runtime = args.runtime.resolve()
assert Path.cwd().resolve() == runtime
assert Path(ll.__file__).resolve().is_relative_to(runtime), ll.__file__
assert countries_root().resolve() == runtime / 'lsms_library/countries'
assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve() != (Path.home() / '.local/share/lsms_library').resolve()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == args.head
expected = subprocess.check_output(['git', 'show', args.head + ':lsms_library/transformations.py'])
assert hashlib.sha256(Path(T.__file__).read_bytes()).digest() == hashlib.sha256(expected).digest()
print('Verified untouched runtime, exact transformation source, config and private data identity', flush=True)
raise SystemExit(pytest.main([
    '-q', '--import-mode=importlib', '-p', 'no:cacheprovider',
    str(args.tests / 'tests/test_gh1159_pooled_reducers.py'),
    str(args.tests / 'tests/test_gh1093_declared_unit_consumers.py'),
    '-k', 'pooled or country_finalize', '--tb=short']))
