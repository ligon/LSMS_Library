"""Cold-build Malawi community_prices; dump the frame (private) and aggregates."""
import os, sys, subprocess, json, time
import pandas as pd
import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
print('lsms_library from', lsms_library.__file__, 'DATA', os.environ.get('LSMS_DATA_DIR'))
import lsms_library as ll
t0 = time.time()
df = ll.Country('Malawi').community_prices()
print('built in', round(time.time() - t0, 1), 's', df.shape, df.index.names, list(df.columns))
out = sys.argv[1]
df.to_parquet(out)
print('wrote', out)
