"""Build plot_inputs for non-Malawi countries on one arm; save frames and warnings.
usage: python other_pi.py <clone> <arm> <outdir> <country>..."""
import json, sys, warnings
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402
clone, arm, outdir, countries = sys.argv[1], sys.argv[2], Path(sys.argv[3]), sys.argv[4:]
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)
from lsms_library.country import Country  # noqa: E402
res = {'head': head}
for name in countries:
    rec = {}
    try:
        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter('always')
            df = Country(name).plot_inputs()
        rec = {'rows': int(len(df)), 'columns': list(map(str, df.columns)), 'index': list(df.index.names),
               'derivation_present': 'Derivation' in df.columns,
               'warnings': sorted({f'{type(w.message).__name__}: {str(w.message)[:200]}' for w in ws})}
        d = df.copy(); d.attrs = {}
        d = d.reset_index()
        for c in [c for c in d.columns if d[c].dtype == object]:
            d[c] = d[c].map(lambda x: x if pd.isna(x) else str(x)).astype('string')
        d.to_parquet(outdir / f'{arm}.{name}.parquet')
    except Exception as e:  # noqa: BLE001
        rec = {'error': f'{type(e).__name__}: {e}'}
    res[name] = rec
    print(arm, name, {k: rec.get(k) for k in ('rows', 'derivation_present', 'error')}, flush=True)
    (outdir / f'{arm}.json').write_text(json.dumps(res, indent=1, default=str))
