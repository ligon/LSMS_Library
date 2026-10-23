import os, sys, warnings, pickle
ck, out = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(ck) + os.sep), ll.__file__
warnings.simplefilter("ignore")
df = ll.Country("Burkina_Faso").plot_inputs(waves=["2018-19"])
pickle.dump(df, open(out, "wb"))
print(len(df), dict(df.dtypes.astype(str)))
