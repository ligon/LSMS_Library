"""Build Malawi plot_inputs on one arm and save it (attrs dropped).  usage: <clone> <arm> <outdir>"""
import sys, warnings
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402
clone, arm, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
print('head', assert_env(clone), flush=True)
from lsms_library.country import Country  # noqa: E402
with warnings.catch_warnings(record=True) as ws:
    warnings.simplefilter('always')
    df = Country('Malawi').plot_inputs()
d = df.copy(); d.attrs = {}
d = d.reset_index()
for c in [c for c in d.columns if d[c].dtype == object]:
    d[c] = d[c].map(lambda x: x if pd.isna(x) else str(x)).astype('string')
d.to_parquet(outdir / f'{arm}.pi.parquet')
(outdir / f'{arm}.warn.txt').write_text('\n'.join(sorted({f'{type(w.message).__name__}: {str(w.message)[:300]}' for w in ws})))
print(arm, 'rows', len(df), 'derivation', df['Derivation'].notna().sum() if 'Derivation' in df else None, flush=True)
