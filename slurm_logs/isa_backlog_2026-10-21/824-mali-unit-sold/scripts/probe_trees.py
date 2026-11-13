import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
C = f'{checkout}/lsms_library/countries/Mali'
os.chdir(f'{C}/2014-15/_')
b = get_dataframe('../Data/EACIS3B_p2.dta')
b = b[b['s3bq01'].notna() & b['s3bq03'].astype('string').str.strip().eq('Oui')]
n = lambda c: pd.to_numeric(b[c], errors='coerce')
print('2014-15 s3b producing rows', len(b))
print(' s3bq09 months: value_counts', n('s3bq09').value_counts(dropna=False).sort_index().to_dict())
for c in ['s3bq10a', 's3bq10c', 's3bq13a', 's3bq13c', 's3bq14']:
    x = n(c); print(f' {c}: nonnull {x.notna().sum()} pos {(x>0).sum()} zero {(x==0).sum()} sentinel99/999/9999 {x.isin([99,999,9999]).sum()} median {x[x>0].median()}')
print(' s3bq10b', b['s3bq10b'].astype('string').value_counts(dropna=False).to_dict())
print(' s3bq13b', b['s3bq13b'].astype('string').value_counts(dropna=False).to_dict())
kg = b['s3bq10b'].astype('string') == 'Kilogramme'
print(' harvest Kilogramme rows: s3bq10c==1 share', (n('s3bq10c')[kg] == 1).mean(), ' s3bq10c==s3bq10a share', (np.isclose(n('s3bq10c')[kg], n('s3bq10a')[kg])).mean(), 'n', kg.sum())
kg = b['s3bq13b'].astype('string') == 'Kilogramme'
print(' sold Kilogramme rows: s3bq13c==1 share', (n('s3bq13c')[kg] == 1).mean(), ' ==s3bq13a share', (np.isclose(n('s3bq13c')[kg], n('s3bq13a')[kg])).mean(), 'n', kg.sum())
print(' s3bq10a NA but s3bq09 present', (n('s3bq10a').isna() & n('s3bq09').notna()).sum())
print(' sold qty>0 but value NA/0', ((n('s3bq13a')>0) & ~(n('s3bq14')>0)).sum(), '; value>0 but qty NA/0', ((n('s3bq14')>0) & ~(n('s3bq13a')>0)).sum())
print(' months==0 rows', (n('s3bq09')==0).sum(), '; s3bq13a > s3bq10a (same unit) rows', ((n('s3bq13a') > n('s3bq10a')) & (b['s3bq13b'].astype('string')==b['s3bq10b'].astype('string'))).sum())
# 2017-18
os.chdir(f'{C}/2017-18/_')
f = get_dataframe('../Data/eaci17_s11fp1.dta')
print('2017-18 s11f columns sample', [c for c in f.columns][:3])
f = f[f['s11fq01'].notna() & f['s11fq03'].astype('string').str.strip().eq('Oui')]
n = lambda c: pd.to_numeric(f[c], errors='coerce')
print('2017-18 s11f producing rows', len(f))
print(' s11fq10 raw', f['s11fq10'].astype('string').value_counts(dropna=False).to_dict())
for c in ['s11fq11a', 's11fq11c', 's11fq14a', 's11fq14c', 's11fq15']:
    x = n(c); print(f' {c}: nonnull {x.notna().sum()} pos {(x>0).sum()} zero {(x==0).sum()} sent {x.isin([99,999,9999]).sum()} median {x[x>0].median()}')
print(' s11fq11b', f['s11fq11b'].astype('string').value_counts(dropna=False).to_dict())
print(' s11fq14b', f['s11fq14b'].astype('string').value_counts(dropna=False).to_dict())
kg = f['s11fq11b'].astype('string') == 'Kg'
print(' harvest Kg rows: s11fq11c==1 share', (n('s11fq11c')[kg]==1).mean(), ' ==s11fq11a share', np.isclose(n('s11fq11c')[kg], n('s11fq11a')[kg]).mean(), 'n', kg.sum())
kg = f['s11fq14b'].astype('string') == 'Kg'
print(' sold Kg rows: s11fq14c==1 share', (n('s11fq14c')[kg]==1).mean(), ' ==s11fq14a share', np.isclose(n('s11fq14c')[kg], n('s11fq14a')[kg]).mean(), 'n', kg.sum())
print(' plot location s11fq05c/d nonnull', f['s11fq05c'].notna().sum(), f['s11fq05d'].notna().sum())
# s3a: Oui/Non vs s3aq23b
os.chdir(f'{C}/2014-15/_')
s = get_dataframe('../Data/EACIS3A_p2.dta')
fl = s['s3aq22'].astype('string')
print('2014-15 s3a rows', len(s), ' s3aq22', fl.value_counts(dropna=False).to_dict())
print(' s3aq23b on Non rows', s.loc[fl.eq('Non'), 's3aq23b'].astype('string').value_counts(dropna=False).to_dict())
print(' s3aq23b on Oui rows', s.loc[fl.eq('Oui'), 's3aq23b'].astype('string').value_counts(dropna=False).to_dict())
print(' s3aq23b on NA/Manquant flag rows', s.loc[~fl.isin(['Oui','Non']), 's3aq23b'].astype('string').value_counts(dropna=False).to_dict())
os.chdir(f'{C}/2017-18/_')
g = get_dataframe('../Data/eaci17_s7gp2.dta')
fl = g['s7gq20'].astype('string')
print('2017-18 s7g rows', len(g), ' s7gq20', fl.value_counts(dropna=False).to_dict())
print(' s7gq21b on Non rows', g.loc[fl.eq('Non'), 's7gq21b'].astype('string').value_counts(dropna=False).to_dict())
print(' s7gq21b on Oui rows', g.loc[fl.eq('Oui'), 's7gq21b'].astype('string').value_counts(dropna=False).to_dict())
print(' s7g dup (grappe, exploitation, s7gq01)', g.duplicated(['grappe','exploitation','s7gq01']).sum())
