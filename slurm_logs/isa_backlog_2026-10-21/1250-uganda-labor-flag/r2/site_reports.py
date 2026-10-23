"""Warm read of Country('Uganda').plot_labor() in a fresh process: Sites B (null
content), Q (quantity screen) and I (NaN index keys) run in _finalize_result on
EVERY read, so a warm read reports them.  Aggregates only.

Usage: CHECKOUT=<checkout> LSMS_DATA_DIR=<built dir> python site_reports.py <out.json>
"""
import json, os, sys, warnings
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
import lsms_library as ll
from lsms_library.null_read_audit import null_read_reports
from lsms_library.quantity_audit import quantity_reports
from lsms_library.null_index_audit import null_index_reports
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    df = ll.Country('Uganda').plot_labor()
rep = {'shape': list(df.shape),
       'site_B_null_read': null_read_reports(country='Uganda', table='plot_labor'),
       'site_Q_quantity': quantity_reports(country='Uganda', table='plot_labor'),
       'site_I_null_index': null_index_reports(country='Uganda', table='plot_labor'),
       'warning_classes': sorted({x.category.__name__ for x in w}),
       'warnings': sorted({f'{x.category.__name__}: {str(x.message)[:240]}' for x in w})}
json.dump(rep, open(sys.argv[1], 'w'), indent=1, default=str)
print(json.dumps({k: rep[k] for k in ('shape', 'warning_classes')}), 'B', len(rep['site_B_null_read']),
      'Q', len(rep['site_Q_quantity']), 'I', len(rep['site_I_null_index']))
