import json, sys
from pathlib import Path
import pandas as pd
o = Path(sys.argv[1]); countries = sys.argv[2:]
a, b = json.loads((o / 'base.json').read_text()), json.loads((o / 'cand.json').read_text())
for n in countries:
    ra, rb = a.get(n, {}), b.get(n, {})
    out = {'rows': [ra.get('rows'), rb.get('rows')], 'cols_equal': ra.get('columns') == rb.get('columns'),
           'warnings_equal': ra.get('warnings') == rb.get('warnings'), 'errors': [ra.get('error'), rb.get('error')]}
    pa, pb = o / f'base.{n}.parquet', o / f'cand.{n}.parquet'
    if pa.exists() and pb.exists():
        x, y = pd.read_parquet(pa), pd.read_parquet(pb)
        out['frame_equal'] = bool(x.equals(y))
        out['dtypes_equal'] = bool((x.dtypes == y.dtypes).all()) if list(x.columns) == list(y.columns) else False
    print(n, out, flush=True)
