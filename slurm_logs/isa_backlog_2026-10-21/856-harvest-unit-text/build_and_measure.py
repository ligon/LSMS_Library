"""GH #856 step 2 -- cold-build Malawi crop_production in ONE checkout and
record what the comparison needs.

Usage (one checkout at a time, each with its own private data root)::

    PYTHONPATH=<checkout> LSMS_DATA_DIR=<private root> \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 \
    python build_and_measure.py <checkout> <out_dir>

Writes to <out_dir> (outside the repo):
  cp.parquet            Country('Malawi').crop_production(), reset_index()
  kg_default.parquet    harvest_kg_factors(cp) (declared/metric/inferred layers)
  kg_shipped.parquet    harvest_kg_factors(with_region(cp), shipped_factors=
                        crop_conversion_factors())
  summary.json          attrs tallies, KgFactorSource counts, Harvest_kg by wave
"""
import importlib.util
import json
import os
import re
import sys
import time
import warnings
from pathlib import Path

checkout = os.path.realpath(sys.argv[1])
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)

import lsms_library  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(checkout + os.sep), \
    lsms_library.__file__

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from lsms_library.country import Country  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402
from lsms_library.transformations import harvest_kg_factors  # noqa: E402

assert os.path.realpath(countries_root()).startswith(checkout + os.sep)

MALAWI_ = countries_root() / 'Malawi' / '_'
sys.path.insert(0, str(MALAWI_))
spec = importlib.util.spec_from_file_location('malawi_856', MALAWI_ / 'malawi.py')
mw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mw)

t0 = time.time()
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    cp = Country('Malawi').crop_production()
build_s = time.time() - t0
def _public(msg):
    """A warning's text WITHOUT row-level detail: cut at the first per-row
    listing (``i=...``) and cap the length -- the repository is public."""
    msg = re.split(r':?\s*\bi=', msg)[0]
    return msg[:200]


build_warnings = sorted({f'{w.category.__name__}: {_public(str(w.message))}'
                         for w in caught if 'Malawi' in str(w.message)})

flat = cp.reset_index()
flat.to_parquet(out / 'cp.parquet')


def _jsonable(x):
    try:
        json.dumps(x)
        return x
    except TypeError:
        return json.loads(json.dumps(x, default=str))


summary = {
    'checkout': checkout,
    'build_seconds': round(build_s, 1),
    'rows': int(len(cp)),
    'index': list(cp.index.names),
    'columns': list(cp.columns),
    'rows_by_wave': {str(k): int(v) for k, v in flat.groupby('t').size().items()},
    'u_by_wave': {f'{t}|{u}': int(n) for (t, u), n in
                  flat.groupby(['t', 'u'], dropna=False).size().items()},
    'attrs_keys': sorted(cp.attrs),
    'attrs_sale': {k: _jsonable(cp.attrs.get(k)) for k in
                   ('sale_suppressed', 'sale_basis_mismatch', 'sale_basis_ladder',
                    'unit_text_recovered')
                   if k in cp.attrs},
    'malawi_warnings': build_warnings,
}

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    f0 = harvest_kg_factors(cp)
    shipped = mw.crop_conversion_factors()
    cpr = mw.with_region(cp)
    f1 = harvest_kg_factors(cpr, shipped_factors=shipped)

for name, f in (('kg_default', f0), ('kg_shipped', f1)):
    f.reset_index().to_parquet(out / f'{name}.parquet')
    kg = cp['Quantity'].to_numpy(dtype='float64') * f['kg_per_unit'].to_numpy(dtype='float64')
    by_wave = pd.Series(kg, index=flat['t'].to_numpy()).groupby(level=0).sum(min_count=1)
    summary[name] = {
        'source_counts': {str(k): int(v) for k, v in
                          f['KgFactorSource'].value_counts(dropna=False).items()},
        'harvest_kg_by_wave': {str(k): float(v) for k, v in by_wave.items()},
        'rows_with_factor': int(np.isfinite(f['kg_per_unit'].to_numpy(dtype='float64')).sum()),
    }

shipped_units = sorted({str(x) for x in (shipped.index.get_level_values('u')
                                          if 'u' in (shipped.index.names or [])
                                          else shipped['u'])})
summary['shipped_table_u_labels'] = shipped_units

(out / 'summary.json').write_text(json.dumps(summary, indent=1, default=str) + '\n')
print(json.dumps({k: summary[k] for k in ('build_seconds', 'rows', 'rows_by_wave')}, indent=1))
print('kg_default', summary['kg_default']['source_counts'])
print('kg_shipped', summary['kg_shipped']['source_counts'])
