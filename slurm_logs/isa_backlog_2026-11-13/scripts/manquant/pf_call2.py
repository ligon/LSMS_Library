"""Healthy Country('Senegal').plot_features() with INFO logging; save the frame.  Usage: pf_call2.py <out.parquet>"""
import logging, os, sys, warnings
logging.basicConfig(level=logging.INFO, format='LOG %(name)s %(levelname)s %(message)s')
import lsms_library as ll
from lsms_library import paths
r = os.path.realpath(str(paths.data_root()))
assert r.startswith('/local/job') or '/isa-sm-20261113/private/' in r, r
print('backend', os.environ.get('LSMS_BUILD_BACKEND', 'default'), 'root', r, 'lib', ll.__file__)
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    df = ll.Country('Senegal').plot_features()
print('rows_by_wave', df.index.get_level_values('t').value_counts().to_dict())
df = df.copy(); df.attrs = {}
df.to_parquet(sys.argv[1])
