"""Build Country('Mali').plot_inputs() (all waves) and pickle it.

Usage: python build_1118.py <checkout> <out.pkl>  (PYTHONPATH=<checkout>, private LSMS_DATA_DIR)
"""
import os, pickle, sys, warnings
ck, out = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(ck) + os.sep), ll.__file__
warnings.simplefilter("ignore")
df = ll.Country("Mali").plot_inputs()
pickle.dump(df, open(out, "wb"))
print(len(df), list(df.index.names), dict(df.dtypes.astype(str)))
