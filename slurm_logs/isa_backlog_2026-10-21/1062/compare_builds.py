"""Compare private paired survey snapshots without disclosing their cells."""
import json
import os
from pathlib import Path

import pandas as pd

base = Path(os.environ['BASE_EVIDENCE'])
candidate = Path(os.environ['CANDIDATE_EVIDENCE'])
results = []
for path in sorted(base.glob('*.parquet')):
    other = candidate / path.name
    if not other.is_file():
        raise RuntimeError('Missing paired snapshot: ' + path.name)
    try:
        pd.testing.assert_frame_equal(pd.read_parquet(path), pd.read_parquet(other),
                                      check_exact=True)
    except AssertionError:
        raise RuntimeError('Paired frame differs: ' + path.name) from None
    results.append({'name': path.name, 'status': 'equal'})
print(json.dumps(results, indent=2))
