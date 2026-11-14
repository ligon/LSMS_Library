import sys
from pathlib import Path
import pandas as pd
o = Path(sys.argv[1])
x, y = pd.read_parquet(o / 'base.pi.parquet'), pd.read_parquet(o / 'cand.pi.parquet')
print('rows', len(x), len(y), 'columns equal', list(x.columns) == list(y.columns),
      'frame_equal', x.equals(y), 'dtypes equal', bool((x.dtypes == y.dtypes).all()) if list(x.columns) == list(y.columns) else None,
      'warnings equal', (o / 'base.warn.txt').read_text() == (o / 'cand.warn.txt').read_text(), flush=True)
