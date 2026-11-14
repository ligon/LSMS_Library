"""Cold-build the touched tables of one arm into its private root; save frames.

usage: python build.py <clone> <arm> <framesdir> <country> [<country> ...]
Frames go to <framesdir> (job-local, never the reports directory).
"""
import json
import sys
import time
import traceback
import warnings
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, arm, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
countries = sys.argv[4:]
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)
import lsms_library as ll  # noqa: E402

TABLES = {
    'Mali': [('food_acquired', {}), ('food_expenditures', {}), ('food_prices', {}),
             ('food_quantities', {}), ('food_prices', {'units': 'unitvalue'})],
    'Senegal': [('food_acquired', {}), ('food_expenditures', {}), ('food_prices', {}),
                ('food_quantities', {}), ('food_prices', {'units': 'unitvalue'})],
    'Niger': [('food_acquired', {}), ('food_expenditures', {}), ('food_prices', {}),
              ('food_quantities', {})],
    'Uganda': [('sample', {}), ('cluster_features', {}), ('household_characteristics', {})],
}

for C in countries:
    log = {'arm': arm, 'head': head, 'country': C, 'tables': {}}
    c = ll.Country(C)
    log['waves'] = list(c.waves)
    for table, kw in TABLES[C]:
        name = table + ''.join(f'.{k}={v}' for k, v in kw.items())
        rec = {}
        t0 = time.time()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            try:
                df = getattr(c, table)(**kw)
                rec['rows'] = int(len(df))
                rec['index'] = list(map(str, df.index.names))
                rec['columns'] = list(map(str, df.columns))
                if 't' in df.index.names:
                    rec['rows_by_t'] = {str(k): int(v) for k, v in
                                        df.index.get_level_values('t').value_counts().sort_index().items()}
                if 'derivations' in df.attrs:
                    rec['derivations'] = json.loads(json.dumps(df.attrs['derivations'], default=str))
                for key in ('kg_factor_sources', 'kg_factor_baseline_gate'):
                    if key in df.attrs:
                        rec[key] = json.loads(json.dumps(df.attrs[key], default=str))
                d = df.copy()
                d.attrs = {}
                d.to_parquet(outdir / f'{arm}.{C}.{name}.parquet')
            except Exception as e:  # noqa: BLE001
                rec['error'] = f'{type(e).__name__}: {e}'
                rec['traceback'] = traceback.format_exc()[-4000:]
            rec['warnings'] = dict(Counter(x.category.__name__ for x in w))
            rec['warning_texts'] = sorted({f'{x.category.__name__}: {str(x.message)[:300]}' for x in w})[:30]
        rec['seconds'] = round(time.time() - t0, 1)
        log['tables'][name] = rec
        print(arm, C, name, {k: v for k, v in rec.items() if k not in ('traceback', 'warning_texts', 'derivations')}, flush=True)
        (outdir / f'{arm}.{C}.build.json').write_text(json.dumps(log, indent=1, default=str))
print('BUILD DONE', arm, countries, flush=True)
