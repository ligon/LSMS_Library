"""Cold-build Ethiopia crop_production through the Country API; save frame + warnings.

usage: build_cp.py <checkout> <outdir>
"""
import os, sys, json, warnings, subprocess, time
checkout, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
import pandas as pd
from lsms_library import Country
from lsms_library.country import grain_reports
from lsms_library.null_read_audit import null_read_reports
from lsms_library.null_index_audit import null_index_reports
from lsms_library.quantity_audit import quantity_reports
t0 = time.time()
with warnings.catch_warnings(record=True) as wl:
    warnings.simplefilter('always')
    cp = Country('Ethiopia').crop_production()
el = time.time() - t0
cp.to_parquet(os.path.join(outdir, 'crop_production_api.parquet'))
ws = [{'category': w.category.__name__, 'message': str(w.message)[:2000], 'filename': os.path.basename(w.filename)} for w in wl]
json.dump(ws, open(os.path.join(outdir, 'warnings.json'), 'w'), indent=1)
def jdump(name, obj):
    try:
        json.dump(obj, open(os.path.join(outdir, name), 'w'), indent=1, default=str)
    except Exception as e:
        print('could not dump', name, e)
jdump('grain_reports.json', grain_reports('Ethiopia', 'crop_production'))
jdump('null_read_reports.json', null_read_reports('Ethiopia', 'crop_production'))
jdump('null_index_reports.json', null_index_reports('Ethiopia', 'crop_production'))
jdump('quantity_reports.json', quantity_reports('Ethiopia', 'crop_production'))
print('elapsed', round(el, 1), 'shape', cp.shape, 'index', cp.index.names)
print('attrs keys', list(cp.attrs))
from collections import Counter
print(Counter(w['category'] for w in ws))
