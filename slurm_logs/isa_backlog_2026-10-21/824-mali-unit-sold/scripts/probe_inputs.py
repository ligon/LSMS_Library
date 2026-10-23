import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd
m = ll.Country('Mali')
for k in ['Mali::crop_production::3b-per-harvest-month', 'Mali::crop_production::11f-per-harvest-month']:
    x = m.derivation_inputs(k)
    print(k, x.shape, list(x.index.names), list(x.columns)[:9], 'dup (t,i,crop)', int(x.index.duplicated(keep=False).sum()))
    d = x[x.index.duplicated(keep=False)]
    if len(d):
        print(d.reset_index(drop=False).drop(columns=['i']).to_string())
