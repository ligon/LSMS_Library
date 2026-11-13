import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
C = f'{checkout}/lsms_library/countries/Mali'
specs = {'2014-15': {'EACIS3A_p2.dta': ['s3aq08a', 's3aq23a', 's3aq24'], 'EACIS3B_p2.dta': ['s3bq09', 's3bq10a', 's3bq13a', 's3bq14']},
         '2017-18': {'eaci17_s7fp2.dta': ['s7fq13a'], 'eaci17_s7gp2.dta': ['s7gq21a', 's7gq22'], 'eaci17_s11fp1.dta': ['s11fq11a', 's11fq14a', 's11fq15']}}
for w, files in specs.items():
    os.chdir(f'{C}/{w}/_')
    for f, cols in files.items():
        d = get_dataframe(f'../Data/{f}')
        for c in cols:
            x = pd.to_numeric(d[c], errors='coerce')
            vc = x[(x >= 98) & ((x % 1 != 0) | x.isin([99, 999, 9999, 99999, 999999, 9999999]) | (x >= 9990))].round(3).value_counts().head(8).to_dict()
            print(w, f, c, 'n', int(x.notna().sum()), 'sentinel-like', vc)
    if w == '2014-15':
        b = get_dataframe('../Data/EACIS3B_p2.dta')
        q = pd.to_numeric(b['s3bq10a'], errors='coerce')
        r = b[(q > 9990)]
        print(' s3bq10a>9990 rows: s3bq03', r['s3bq03'].astype(str).tolist(), 's3bq09', r['s3bq09'].tolist(), 's3bq10b', r['s3bq10b'].astype(str).tolist(), 's3bq10c', r['s3bq10c'].tolist())
