"""Fingerprint a raw Uganda var/nutrition.parquet exactly as
tests/test_uganda_invariance.py::_fingerprint does (aggregates only).

usage: nuthash.py PARQUET OUT.json LABEL
"""
import hashlib
import json
import sys

import numpy as np
import pandas as pd

p, out, label = sys.argv[1:4]
df = pd.read_parquet(p, engine='pyarrow')
row_hashes = pd.util.hash_pandas_object(df, index=True)
rec = {'label': label, 'shape': list(df.shape),
       'columns': sorted(map(str, df.columns)), 'index_names': list(df.index.names),
       'dtypes': {str(k): str(v) for k, v in sorted(df.dtypes.items(), key=lambda x: str(x[0]))},
       'content_hash': hashlib.sha256(row_hashes.values.tobytes()).hexdigest(),
       'energy_sum': float(df['Energy'].sum()), 'protein_sum': float(df['Protein'].sum()),
       'numpy': np.__version__, 'pandas': pd.__version__}
try:
    from threadpoolctl import threadpool_info
    rec['blas'] = [{k: d.get(k) for k in ('internal_api', 'version', 'architecture', 'num_threads')}
                   for d in threadpool_info() if d.get('user_api') == 'blas']
except Exception as e:  # noqa: BLE001
    rec['blas'] = f'n/a: {e}'
with open(out, 'w') as fh:
    json.dump(rec, fh, indent=1, sort_keys=True)
print(json.dumps({k: rec[k] for k in ('label', 'shape', 'content_hash', 'energy_sum', 'blas')}))
