"""Cold build of Malawi crop_production / plot_inputs (+ downstream) for one checkout.

Usage: python build.py <label> <checkout>
Writes row-level frames ONLY under /local/job40390255/data-private/637-mw/out/<label>/.
"""
import json, os, pickle, subprocess, sys, time, warnings
from pathlib import Path

label, checkout = sys.argv[1], Path(sys.argv[2]).resolve()
OUT = Path('/local/job40390255/data-private/637-mw/out') / label
OUT.mkdir(parents=True, exist_ok=True)

import lsms_library  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(str(checkout) + os.sep), lsms_library.__file__
assert os.environ['LSMS_DATA_DIR'] == f'/local/job40390255/data-private/637-mw/{label}', os.environ['LSMS_DATA_DIR']
from lsms_library.paths import countries_root  # noqa: E402
assert str(countries_root()).startswith(str(checkout)), countries_root()

import pandas as pd  # noqa: E402
from lsms_library.country import Country, grain_reports  # noqa: E402
from lsms_library.quantity_audit import quantity_reports  # noqa: E402
from lsms_library.null_read_audit import null_read_reports  # noqa: E402
from lsms_library.null_index_audit import null_index_reports  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

tables = sys.argv[3].split(',') if len(sys.argv) > 3 else ['crop_production', 'plot_inputs', 'plot_features']
c = Country('Malawi')
wlog = []
frames = {}
for tab in tables:
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        df = getattr(c, tab)()
    frames[tab] = df
    for x in w:
        wlog.append(dict(table=tab, category=x.category.__name__, message=str(x.message)))
    df.to_parquet(OUT / f'{tab}.parquet')
    with open(OUT / f'{tab}.attrs.pkl', 'wb') as fh:
        pickle.dump(dict(df.attrs), fh)
    print(f'{label} {tab}: {df.shape} in {time.time()-t0:.0f}s', flush=True)

# downstream
down = {}
if 'crop_production' in frames and 'plot_inputs' in frames:
    cp, pi = frames['crop_production'], frames['plot_inputs']
    import sys as _s
    _s.path.insert(0, str(countries_root() / 'Malawi' / '_'))
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        down['seed_kg'] = T.seed_kg(pi)
        down['nitrogen_kg'] = T.nitrogen_kg(pi)
        down['harvest_kg'] = T.harvest_kg(cp)
        try:
            import malawi
            sf = malawi.crop_conversion_factors()
            down['harvest_kg_shipped'] = T.harvest_kg(malawi.with_region(cp), shipped_factors=sf)
        except Exception as exc:
            print('shipped harvest_kg failed', repr(exc))
        if 'plot_features' in frames:
            down['yield_kg'] = T.yield_kg(cp, frames['plot_features'])
    for x in w:
        wlog.append(dict(table='downstream', category=x.category.__name__, message=str(x.message)))
    for k, v in down.items():
        v.to_parquet(OUT / f'down_{k}.parquet')
        with open(OUT / f'down_{k}.attrs.pkl', 'wb') as fh:
            pickle.dump(dict(v.attrs), fh)
        print(f'{label} {k}: {v.shape}', flush=True)

reports = dict(grain=grain_reports(country='Malawi'),
               quantity=quantity_reports(country='Malawi'),
               null_read=null_read_reports(country='Malawi'),
               null_index=null_index_reports(country='Malawi'))
with open(OUT / 'reports.pkl', 'wb') as fh:
    pickle.dump(reports, fh)
with open(OUT / 'warnings.json', 'w') as fh:
    json.dump(wlog, fh, indent=1, default=str)
print(label, 'warnings', len(wlog), {k: len(v) for k, v in reports.items()})
