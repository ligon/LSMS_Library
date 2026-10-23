import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
C = f'{checkout}/lsms_library/countries/Mali'
os.chdir(f'{C}/2017-18/_')
g = get_dataframe('../Data/eaci17_s7gp2.dta')
fl = g['s7gq20'].astype('string'); q = pd.to_numeric(g['s7gq21a'], errors='coerce'); v = pd.to_numeric(g['s7gq22'], errors='coerce')
print('s7g: flag NA rows', int(fl.isna().sum()), '; Oui with qty NA', int((fl.eq('Oui') & q.isna()).sum()), '; Oui with value NA', int((fl.eq('Oui') & v.isna()).sum()))
print('  -> rows the pre-#824 sum (min_count=0) turns from NA into 0: qty', int((~fl.eq('Non').fillna(False) & q.isna()).sum()), 'value', int((~fl.eq('Non').fillna(False) & v.isna()).sum()))
# How many of those reach a served single-plot row?  Count served 2017-18 field rows with Quantity_sold==0 & Value_sold==0 vs raw Non rows attached
n = pd.read_parquet(f'/local/job40390255/data-private/824-ml/out/{label}_served.parquet').reset_index()
f17 = n[(n.t == '2017-18') & ~n.perennial.fillna(False)]
print('served 2017-18 field rows with Quantity_sold==0', int((f17.Quantity_sold == 0).sum()), '; Quantity_sold NA', int(f17.Quantity_sold.isna().sum()), '; non-null', int(f17.Quantity_sold.notna().sum()))
# KgFactor_sold coverage on priced rows (raw, pre-collapse), and per-unit vs total behaviour on Kg rows
def cov(name, qa, ub, kc, va, d):
    qq = pd.to_numeric(d[qa], errors='coerce'); vv = pd.to_numeric(d[va], errors='coerce'); kk = pd.to_numeric(d[kc], errors='coerce')
    pr = (qq > 0) & (vv > 0)
    lab = d[ub].astype('string')
    kg = pr & lab.isin(['Kg', 'Kilogramme'])
    print(f'{name}: priced {int(pr.sum())}; sold kg factor >0 on {int((pr & (kk > 0)).sum())}; on sold-in-Kg priced rows factor==1 {int((kg & (kk == 1)).sum())} / ==qty {int((kg & np.isclose(kk, qq)).sum())} / n {int(kg.sum())}')
os.chdir(f'{C}/2014-15/_')
s = get_dataframe('../Data/EACIS3A_p2.dta'); cov('2014-15 s3a (s3aq23c)', 's3aq23a', 's3aq23b', 's3aq23c', 's3aq24', s)
b = get_dataframe('../Data/EACIS3B_p2.dta'); b = b[b['s3bq01'].notna() & b['s3bq03'].astype('string').eq('Oui')]; cov('2014-15 s3b (s3bq13c)', 's3bq13a', 's3bq13b', 's3bq13c', 's3bq14', b)
os.chdir(f'{C}/2017-18/_')
cov('2017-18 s7g (s7gq21d)', 's7gq21a', 's7gq21b', 's7gq21d', 's7gq22', g)
t = get_dataframe('../Data/eaci17_s11fp1.dta'); t = t[t['s11fq01'].notna() & t['s11fq03'].astype('string').eq('Oui')]; cov('2017-18 s11f (s11fq14c)', 's11fq14a', 's11fq14b', 's11fq14c', 's11fq15', t)
print('2017-18 s11f producing rows with a plot location (s11fq05c & s11fq05d)', int((t['s11fq05c'].notna() & t['s11fq05d'].notna()).sum()), 'of', len(t))
