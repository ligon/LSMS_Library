"""Independent actual-test acceptance using fixed private operands, no API/build."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.request

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_info
import lsms_library as ll
from lsms_library.paths import countries_root, data_root

root = Path('/local/job40255479/worktrees/uganda-nutrition-cpu-hash')
assert Path.cwd().resolve() == root
assert Path(ll.__file__).resolve() == root/'lsms_library/__init__.py'
assert countries_root().resolve() == root/'lsms_library/countries'
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip() == os.environ['REVIEW_FROZEN_HEAD']
private = Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve() == private
assert str(private).startswith('/local/job40255479/nutrition-repair-review-')

def forbidden(*args, **kwargs):
    raise AssertionError('Independent kernel control forbids network')
urllib.request.urlopen = forbidden
spec = importlib.util.spec_from_file_location('review_raw_invariance', root/'tests/test_uganda_invariance.py')
inv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inv)
inputs = Path('/local/job40255479/nutrition-ci-warm')
input_hashes = {name:hashlib.sha256((inputs/name).read_bytes()).hexdigest() for name in ['x.pkl','y.pkl','raw.pkl']}
x = pd.read_pickle(inputs/'x.pkl')
y = pd.read_pickle(inputs/'y.pkl')
raw = pd.read_pickle(inputs/'raw.pkl')
actual = x @ y.T
waves = sorted(raw.index.get_level_values('t').unique().astype(str))
country_root = private/'Uganda'
parquet = country_root/'var/nutrition.parquet'
parquet.parent.mkdir(parents=True,exist_ok=True)
actual.to_parquet(parquet)
inv.test_parquet_matches_baseline(country_root,waves,'var/nutrition.parquet')
changed = actual.copy()
changed.iloc[0,0] += 1.0
changed.to_parquet(parquet)
try:
    inv.test_parquet_matches_baseline(country_root,waves,'var/nutrition.parquet')
except AssertionError as error:
    assert 'content hash mismatch' in str(error)
else:
    raise AssertionError('Unmeasured changed value accepted')
actual.to_parquet(parquet)
difference = actual.to_numpy()-raw.to_numpy()
mask = difference != 0
bits = actual.to_numpy().view(np.uint64)
old_bits = raw.to_numpy().view(np.uint64)
for name,expected in input_hashes.items():
    assert hashlib.sha256((inputs/name).read_bytes()).hexdigest() == expected
result = {
 'source_head':os.environ['REVIEW_FROZEN_HEAD'],
 'package':str(Path(ll.__file__).resolve()),'countries_root':str(countries_root()),
 'private_root':str(private),'input_hashes':input_hashes,
 'requested_kernel':os.environ['OPENBLAS_CORETYPE'],
 'blas':[{k:v for k,v in item.items() if k!='filepath'} for item in threadpool_info()],
 'shape':list(actual.shape),'exact_hash':inv._fingerprint(actual)['content_hash'],
 'actual_invariance_passed':True,'unmeasured_plus_one_rejected':True,
 'same_index':actual.index.equals(raw.index),'same_columns':actual.columns.equals(raw.columns),
 'same_dtypes':actual.dtypes.equals(raw.dtypes),'changed_cells':int(mask.sum()),
 'changed_rows':int(mask.any(axis=1).sum()),'max_abs':float(np.abs(difference).max()),
 'max_ulps':int((np.maximum(bits,old_bits)-np.minimum(bits,old_bits)).max()),
 'versions':{'numpy':np.__version__,'pandas':pd.__version__},
 'no_country_api_or_build':True,
}
Path(sys.argv[1]).write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
