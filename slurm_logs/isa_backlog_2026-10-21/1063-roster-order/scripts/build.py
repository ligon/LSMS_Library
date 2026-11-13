"""Build Country(C).household_roster() and save to a pickle (scratch only).

Usage: python build.py <checkout> <country> <out.pkl> [table]
"""
import os
import subprocess
import sys
import time
import warnings

checkout, country, out = sys.argv[1], sys.argv[2], sys.argv[3]
table = sys.argv[4] if len(sys.argv) > 4 else 'household_roster'
import lsms_library as ll  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
warnings.simplefilter('ignore')
t0 = time.time()
df = getattr(ll.Country(country), table)()
df.to_pickle(out)
print(country, table, df.shape, f'{time.time()-t0:.1f}s', os.environ.get('LSMS_DATA_DIR'))
