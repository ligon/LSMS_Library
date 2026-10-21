"""Run final synthetic regressions against an explicitly pinned old runtime.

Run from the unchanged baseline checkout with its PYTHONPATH/countries root,
private LSMS_DATA_DIR, and --tests pointing to the candidate checkout.
"""
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
parser.add_argument('--tests', type=Path, required=True)
parser.add_argument('--head', default='4cab5f0db7d375ad543e652f61f43bd28fa94ecd')
args = parser.parse_args()
checkout = Path.cwd().resolve()
assert Path(ll.__file__).resolve().is_relative_to(checkout)
assert countries_root().resolve() == checkout / 'lsms_library/countries'
assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve().is_relative_to(Path('/local'))
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == args.head
expected = subprocess.check_output(['git', 'show', args.head + ':lsms_library/transformations.py'])
assert hashlib.sha256(Path(T.__file__).read_bytes()).digest() == hashlib.sha256(expected).digest()
raise SystemExit(pytest.main([
    '-q', '--import-mode=importlib', '-p', 'no:cacheprovider', '--tb=short',
    str(args.tests.resolve() / 'tests/test_gh1160_pooled_fertilizer_rate.py')]))
