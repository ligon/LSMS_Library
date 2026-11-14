"""Cold-build Malawi tables for one side (base or cand) into a private root.

usage: python build.py <clone> <side> <outdir>
"""
import json
import sys
import time
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, side, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)

import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

log = {'side': side, 'head': head, 'tables': {}}


def save(name, df):
    p = outdir / f'{side}.{name}.parquet'
    d = df.copy()
    d.attrs = {}
    d.to_parquet(p)
    return str(p)


def run(name, fn):
    t0 = time.time()
    rec = {}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = fn()
            rec['rows'] = int(len(df))
            rec['columns'] = list(map(str, df.columns))
            rec['index'] = list(map(str, df.index.names))
            if 't' in df.index.names:
                rec['rows_by_t'] = {str(k): int(v) for k, v in
                                    df.index.get_level_values('t').value_counts().sort_index().items()}
            rec['path'] = save(name, df)
            rec['attrs_keys'] = sorted(map(str, df.attrs))
            if 'derivations' in df.attrs:
                rec['derivations'] = json.loads(json.dumps(df.attrs['derivations'], default=str))
            if 'kg_factor_sources' in df.attrs:
                rec['kg_factor_sources'] = json.loads(json.dumps(df.attrs['kg_factor_sources'], default=str))
        except Exception as e:  # record and continue
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['traceback'] = traceback.format_exc()
        cats = {}
        for x in w:
            cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
        rec['warnings'] = cats
        rec['warning_msgs'] = sorted({str(x.message)[:300] for x in w})[:40]
    rec['seconds'] = round(time.time() - t0, 1)
    log['tables'][name] = rec
    print(name, {k: v for k, v in rec.items() if k not in ('traceback', 'warning_msgs', 'columns')}, flush=True)
    (outdir / f'{side}.build.json').write_text(json.dumps(log, indent=1, default=str))


c = ll.Country('Malawi')
log['waves'] = list(c.waves)
log['data_scheme'] = list(c.data_scheme)
run('sample', lambda: c.sample())
run('household_roster', lambda: c.household_roster())
run('food_acquired', lambda: c.food_acquired())
run('community_prices', lambda: c.community_prices())
run('food_quantities_kgs', lambda: c.food_quantities(units='kgs'))
run('food_prices_kgvalue', lambda: c.food_prices())
run('food_expenditures', lambda: c.food_expenditures())
try:
    from lsms_library.diagnostics import check_panel_consistency
    rep = check_panel_consistency(c)
    log['panel_consistency'] = str(rep)[:6000]
    print('PANEL CONSISTENCY', str(rep)[:3000], flush=True)
except Exception as e:
    log['panel_consistency'] = f'ERROR {type(e).__name__}: {e}'
# raw L2-country parquets (pre-finalize; weights at raw scale)
import lsms_library.paths as paths  # noqa: E402
var = Path(paths.data_root()) / 'Malawi' / 'var'
log['var_files'] = sorted(p.name for p in var.glob('*.parquet')) if var.exists() else []
(outdir / f'{side}.build.json').write_text(json.dumps(log, indent=1, default=str))
print('DONE', side, flush=True)
