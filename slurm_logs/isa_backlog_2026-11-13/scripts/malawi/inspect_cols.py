"""Show what differs in a few plot_inputs columns between the two arms' saved frames.

usage: python inspect_cols.py <frames> <out.json> <col> [<col> ...]
"""
import json
import sys
from pathlib import Path

import pandas as pd

fr, outp, cols = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
a = pd.read_parquet(fr / 'base.plot_inputs.parquet').reset_index()
b = pd.read_parquet(fr / 'cand.plot_inputs.parquet').reset_index()
key = ['t', 'i', 'plot_id', 'input', 'u', 'input_obs']
for r in (a, b):
    for k in key:
        r[k] = r[k].astype('string')
m = a.merge(b, on=key, suffixes=('_a', '_b'))
res = {}
for c in cols:
    x, y = m[f'{c}_a'], m[f'{c}_b']
    diff = ~((x.isna() & y.isna()) | (x.astype('string') == y.astype('string')).fillna(False))
    sub = m[diff]
    res[c] = {'dtype': [str(a[c].dtype), str(b[c].dtype)], 'n_diff': int(diff.sum()),
              'pairs': {f'{p!r} -> {q!r}': int(n) for (p, q), n in
                        pd.Series(list(zip(sub[f'{c}_a'].tolist(), sub[f'{c}_b'].tolist()))).value_counts().head(12).items()},
              'base_types': {str(k): int(v) for k, v in a[c].map(lambda v: type(v).__name__).value_counts().items()},
              'cand_types': {str(k): int(v) for k, v in b[c].map(lambda v: type(v).__name__).value_counts().items()}}
    print(c, res[c], flush=True)
Path(outp).write_text(json.dumps(res, indent=1, default=str))
