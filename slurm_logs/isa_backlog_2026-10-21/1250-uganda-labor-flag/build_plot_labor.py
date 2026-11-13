"""Cold build of Country('Uganda').plot_labor() in the checkout named by CHECKOUT.
Writes the served frame (row-level; scratch only) and aggregate audit reports."""
import json, os, sys, time, warnings
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
import lsms_library as ll
from lsms_library.null_index_audit import null_index_reports
out = sys.argv[1]
t0 = time.time()
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    df = ll.Country('Uganda').plot_labor()
el = time.time() - t0
df.to_parquet(out + '.parquet')
rep = null_index_reports('Uganda', 'plot_labor')
json.dump({'elapsed_s': el, 'shape': list(df.shape), 'index': list(df.index.names),
           'null_index_reports': rep,
           'warnings': sorted({f'{x.category.__name__}: {str(x.message)[:300]}' for x in w})},
          open(out + '.json', 'w'), indent=1, default=str)
print('done', df.shape, round(el, 1))
