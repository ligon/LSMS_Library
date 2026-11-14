"""Build and save the served all-waves food_acquired frame (fa_agg) for one country.

usage: fa_only.py CLONE SIDE OUTDIR COUNTRY
"""
import os, sys, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env

clone, side, outdir, country = sys.argv[1:5]
assert_env(clone)
warnings.simplefilter('ignore')
import lsms_library as ll

d = os.path.join(outdir, side, country)
os.makedirs(d, exist_ok=True)
fa = ll.Country(country)._aggregate_wave_data(None, 'food_acquired')
fa = fa.copy(); fa.attrs = {}
fa.to_parquet(os.path.join(d, 'fa_agg.parquet'))
print(side, country, 'fa_agg rows', len(fa), flush=True)
