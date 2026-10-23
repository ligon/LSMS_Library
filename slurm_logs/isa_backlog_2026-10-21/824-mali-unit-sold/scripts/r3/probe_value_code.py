"""Is 999999 a value code in the EAC-I sale-value variables?  Aggregates only."""
import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import numpy as np, pandas as pd, pyreadstat
from lsms_library.data_access import get_data_file
for rel, var, qvar, mvar in [('Mali/2014-15/Data/EACIS3A_p2.dta', 's3aq24', 's3aq23a', None),
                             ('Mali/2014-15/Data/EACIS3B_p2.dta', 's3bq14', 's3bq13a', 's3bq09'),
                             ('Mali/2017-18/Data/eaci17_s7gp2.dta', 's7gq22', 's7gq21a', None),
                             ('Mali/2017-18/Data/eaci17_s11fp1.dta', 's11fq15', 's11fq14a', 's11fq10')]:
    p = get_data_file(rel)
    df, meta = pyreadstat.read_dta(str(p), encoding='latin1', apply_value_formats=False)
    vl = meta.variable_to_label.get(var)
    v = pd.to_numeric(df[var], errors='coerce'); q = pd.to_numeric(df[qvar], errors='coerce')
    top = v.value_counts().head(4).to_dict()
    print(f'{rel.split("/")[-1]} {var}: value labels {meta.value_labels.get(vl) if vl else None}; n>0 {int((v>0).sum())}; '
          f'==999999 {int((v==999999).sum())} (qty there: 0 {int(((v==999999)&(q==0)).sum())}, NA {int(((v==999999)&q.isna()).sum())}, >0 {int(((v==999999)&(q>0)).sum())}); '
          f'>=999999 {int((v>=999999).sum())}; ==99999 {int((v==99999).sum())}; ==9999 {int((v==9999).sum())}; max {v.max()}')
    if mvar:
        m = df[mvar]
        print(f'   tree rows with value 999999: month count values {m[v==999999].tolist()}')
