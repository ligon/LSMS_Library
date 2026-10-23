"""Site Q offenders on a warm read, without household ids.  Aggregates only."""
import os, sys, warnings
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
from lsms_library.quantity_audit import quantity_reports
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    cp = ll.Country('Mali').crop_production()
for r in quantity_reports(country='Mali', table='crop_production'):
    d = r if isinstance(r, dict) else vars(r)
    offs = d.get('offenders')
    print('wave', d.get('wave'), 'n offenders', len(offs) if offs is not None else None)
    import pandas as pd
    o = pd.DataFrame(offs) if offs is not None and not isinstance(offs, pd.DataFrame) else offs
    if o is not None and len(o):
        keep = [c for c in o.columns if c not in ('i', 'household', 'hh', 'v')]
        print(o[keep].to_string(index=False))
