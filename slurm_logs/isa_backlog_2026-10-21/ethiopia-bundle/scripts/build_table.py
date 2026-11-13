"""Build one Ethiopia table through the Country API; save the frame.
usage: build_table.py <checkout> <table> <out.parquet>"""
import os, sys, warnings
checkout, table, out = sys.argv[1:4]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
from lsms_library import Country
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    df = getattr(Country('Ethiopia'), table)()
df.to_parquet(out); print(table, df.shape, list(df.index.names))
