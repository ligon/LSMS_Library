"""Round 4: cluster_features on one path vs the cold branch build.  COUNTS ONLY.
Usage: r4_paths.py <label> <cold cluster_features.parquet>"""
import os, sys, subprocess, warnings
import pandas as pd
import lsms_library as ll
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
warnings.simplefilter('ignore')
cold = pd.read_parquet(sys.argv[2])
cf = ll.Country('Malawi').cluster_features()
print(f'{sys.argv[1]}: shape {cf.shape} | DataFrame.equals cold {cf.equals(cold)} | 2013-14 equal {cf.xs("2013-14", level="t").equals(cold.xs("2013-14", level="t"))}')
