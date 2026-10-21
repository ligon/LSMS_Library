"""Reconstruct nutrition on a physical private Uganda cache copy, aggregate only.

Run with cwd/PYTHONPATH/LSMS_COUNTRIES_ROOT naming --expected-root and
LSMS_DATA_DIR naming --private-data. The source corpus is read-only.
Private pickle operands contain microdata and must never be committed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--expected-root', type=Path, required=True)
p.add_argument('--expected-sha', required=True)
p.add_argument('--source-data', type=Path, required=True)
p.add_argument('--private-data', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()

import numpy as np
import pandas as pd
import pyarrow as pa
import lsms_library as ll
from lsms_library.paths import countries_root, data_root

repo = a.expected_root.resolve()
private = a.private_data.resolve()
source = a.source_data.resolve()
assert Path.cwd().resolve() == repo
assert Path(ll.__file__).resolve().parent.parent == repo
assert countries_root().resolve() == repo / 'lsms_library/countries'
assert data_root().resolve() == private
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
assert head == a.expected_sha
assert private != source and not private.is_relative_to(source)
assert source != private and not source.is_relative_to(private)
assert not (private / 'Uganda').exists(), 'Use a fresh private data root'
private.mkdir(parents=True, exist_ok=True)
shutil.copytree(source / 'Uganda', private / 'Uganda')
(private / 'dvc-cache').symlink_to((source / 'dvc-cache').resolve(), target_is_directory=True)
for original in (source / 'Uganda').rglob('*.parquet'):
    copy = private / 'Uganda' / original.relative_to(source / 'Uganda')
    assert (original.stat().st_dev, original.stat().st_ino) != (copy.stat().st_dev, copy.stat().st_ino)
    assert hashlib.sha256(original.read_bytes()).digest() == hashlib.sha256(copy.read_bytes()).digest()


def digest(df):
    return hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values.tobytes()).hexdigest()


raw = pd.read_parquet(private / 'Uganda/var/nutrition.parquet')
fct = pd.read_parquet(private / 'Uganda/var/fct.parquet')
q = ll.Country('Uganda', assume_cache_fresh=True).food_quantities()
q = q[q.index.get_level_values('u').astype(str).str.lower() == 'kg']
q = q.droplevel('u').sum(axis=1).groupby(['i', 't', 'j']).sum()
common = q.index.unique(level='j').intersection(fct.columns)
x = q.unstack('j').loc[:, common].fillna(0)
y = fct.loc[:, common].fillna(0)
n = x @ y.T
out = {
    'source': head,
    'versions': {'numpy': np.__version__, 'pandas': pd.__version__, 'pyarrow': pa.__version__},
    'raw': {'shape': list(raw.shape), 'digest': digest(raw), 'index': raw.index.names},
    'reconstruction': {
        'digest': digest(n), 'shape': list(n.shape),
        'same_index': n.index.equals(raw.index), 'same_columns': n.columns.equals(raw.columns),
        'changed_cells': int((n != raw).to_numpy().sum()),
        'max_abs': float(np.abs(n.to_numpy() - raw.to_numpy()).max()),
    },
}
x.to_pickle(private / 'x.pkl')
y.to_pickle(private / 'y.pkl')
raw.to_pickle(private / 'raw.pkl')
a.output.write_text(json.dumps(out, indent=2, default=str) + '\n')
print(json.dumps(out, indent=2, default=str))
