"""Pre-existing hazard: absent_subset_frame is evaluated twice on the DVC descent
(top of load_dataframe_with_dvc, then again inside load_from_waves).  If
_has_country_builder answers True then False (its make -n probe returns True on
OSError/timeout), the constructed empty frame is WRITTEN to var/<t>.parquet with
the union coverage.  Simulated with a counter.  Usage: race.py <ck> <country> <table> <empty_wave> <pop_wave>"""
import os, sys, warnings
from pathlib import Path
ck, country, table, ew, pw = sys.argv[1:6]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(ck) + os.sep), ll.__file__
from lsms_library.country import Country
from lsms_library.local_tools import read_parquet_cache_waves
import pandas as pd
warnings.simplefilter('ignore')
root = Path(os.environ['LSMS_DATA_DIR'])
var = root / country / 'var' / f'{table}.parquet'
n = getattr(ll.Country(country), table)(waves=[pw]).shape[0]
print('step1 populated', pw, 'rows', n, 'var manifest', read_parquet_cache_waves(var))
real = Country._has_country_builder
calls = {'n': 0}
def flaky(self, m):
    calls['n'] += 1
    return True if calls['n'] == 1 else real(self, m)
Country._has_country_builder = flaky
df = getattr(ll.Country(country), table)(waves=[ew])
Country._has_country_builder = real
print('step2 empty request rows', len(df), 'builder calls', calls['n'])
print('var manifest now', read_parquet_cache_waves(var), 'parquet rows', len(pd.read_parquet(var)))
again = getattr(ll.Country(country), table)(waves=[pw])
print('step3 populated', pw, 'rows now', len(again), '(was', n, ')')
