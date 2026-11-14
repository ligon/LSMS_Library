"""Cold-build Malawi tables for one arm into its private root.

usage: python build.py <clone> <arm> <framesdir>
"""
import json
import sys
import time
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, arm, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)

import lsms_library as ll  # noqa: E402

log = {'arm': arm, 'head': head, 'tables': {}}


def dump():
    (outdir / f'{arm}.build.json').write_text(json.dumps(log, indent=1, default=str))


def run(name, fn):
    t0 = time.time()
    rec = {}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = fn()
            rec['rows'] = int(len(df))
            rec['index'] = list(map(str, df.index.names))
            if 't' in df.index.names:
                rec['rows_by_t'] = {str(k): int(v) for k, v in
                                    df.index.get_level_values('t').value_counts().sort_index().items()}
            d = df.copy()
            if 'kg_factor_sources' in df.attrs:
                rec['kg_factor_sources'] = json.loads(json.dumps(df.attrs['kg_factor_sources'], default=str))
            d.attrs = {}
            d.to_parquet(outdir / f'{arm}.{name}.parquet')
        except Exception as e:
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['traceback'] = traceback.format_exc()[-4000:]
        cats = {}
        for x in w:
            cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
        rec['warnings'] = cats
    rec['seconds'] = round(time.time() - t0, 1)
    log['tables'][name] = rec
    print(arm, name, {k: v for k, v in rec.items() if k != 'traceback'}, flush=True)
    dump()


c = ll.Country('Malawi')
log['waves'] = list(c.waves)
run('sample', lambda: c.sample())
run('household_roster', lambda: c.household_roster())
run('food_acquired', lambda: c.food_acquired())
run('food_quantities_kgs', lambda: c.food_quantities(units='kgs'))
run('food_prices_kgvalue', lambda: c.food_prices())
run('food_expenditures', lambda: c.food_expenditures())
try:
    from lsms_library.diagnostics import check_panel_consistency
    log['panel_consistency'] = str(check_panel_consistency(c))[:8000]
except Exception as e:
    log['panel_consistency'] = f'ERROR {type(e).__name__}: {e}'
print(arm, 'PANEL CONSISTENCY', log['panel_consistency'][:3000], flush=True)
dump()
print('BUILD DONE', arm, flush=True)
