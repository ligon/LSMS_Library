import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
C = f'{checkout}/lsms_library/countries/Mali'
sys.path.insert(0, f'{C}/_')
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe
from mali import i as mali_i, _crop_labels, _unit_labels
os.chdir(f'{C}/2014-15/_')
s = get_dataframe('../Data/EACIS3A_p2.dta')
d = pd.DataFrame({'i': s.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1),
                  'plot': s['s3aq01'].astype('Int64').astype('string') + '_' + s['s3aq02'].astype('Int64').astype('string'),
                  'crop': _crop_labels(s['s3aq03b']).values, 'u': _unit_labels(s['s3aq08b']).values, 'su': _unit_labels(s['s3aq23b']).values,
                  'q': pd.to_numeric(s['s3aq23a'], errors='coerce'), 'v': pd.to_numeric(s['s3aq24'], errors='coerce')})
d = d.dropna(subset=['crop'])
g = d.groupby(['i','plot','crop'], dropna=False)
sz = g.size(); nu = g['u'].nunique(); nsu = g['su'].nunique()
sold_rows = g['q'].apply(lambda x: (x > 0).sum())
print('2014-15 s3a groups', len(sz), ' >1 row', int((sz>1).sum()), ' >1 harvest u', int((nu>1).sum()), ' >1 sold unit', int((nsu>1).sum()), ' >1 positive-sale row', int((sold_rows>1).sum()))
print(' sold-unit labels unmapped:', sorted(set(s['s3aq23b'].dropna().astype(str)) - set(d.su.dropna().astype(str))))
multi = sz[sz>1].index
dm = d.set_index(['i','plot','crop']).loc[multi]
print(' rows in multi groups', len(dm), ' with su notna', int(dm.su.notna().sum()), ' with q>0', int((dm.q>0).sum()))
os.chdir(f'{C}/2017-18/_')
f = get_dataframe('../Data/eaci17_s7fp2.dta')
f['i'] = f.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])), axis=1)
f['plot'] = f['s7fq01'].astype('Int64').astype('string') + '_' + f['s7fq02'].astype('Int64').astype('string')
f['crop'] = _crop_labels(f['s7fq03']).values
f = f.dropna(subset=['crop'])
g = f.groupby(['i','plot','crop'], dropna=False).size()
print('2017-18 s7f groups', len(g), ' >1 row', int((g>1).sum()))
g7 = get_dataframe('../Data/eaci17_s7gp2.dta')
print(' s7gq21b unmapped:', sorted(set(g7['s7gq21b'].dropna().astype(str)) - set(_unit_labels(g7['s7gq21b']).dropna().index.map(lambda k: str(g7['s7gq21b'].loc[k])))))
lab = g7['s7gq21b'].dropna().astype(str)
mp = _unit_labels(lab)
print(' s7gq21b label->canonical', dict(sorted(set(zip(lab, mp.astype(object).where(mp.notna(), None))))))
t = get_dataframe('../Data/eaci17_s11fp1.dta')
for c in ['s11fq11b', 's11fq14b']:
    lab = t[c].dropna().astype(str); mp = _unit_labels(lab)
    print(f' {c} label->canonical', dict(sorted(set(zip(lab, mp.astype(object).where(mp.notna(), None))))))
os.chdir(f'{C}/2014-15/_')
b = get_dataframe('../Data/EACIS3B_p2.dta')
for c in ['s3bq10b', 's3bq13b']:
    lab = b[c].dropna().astype(str); mp = _unit_labels(lab)
    print(f' {c} label->canonical', dict(sorted(set(zip(lab, mp.astype(object).where(mp.notna(), None))))))
lab = s['s3aq23b'].dropna().astype(str); mp = _unit_labels(lab)
print(' s3aq23b label->canonical', dict(sorted(set(zip(lab, mp.astype(object).where(mp.notna(), None))))))
lab = s['s3aq08b'].dropna().astype(str); mp = _unit_labels(lab)
print(' s3aq08b label->canonical', dict(sorted(set(zip(lab, mp.astype(object).where(mp.notna(), None))))))
lab = f['s7fq13c'].dropna().astype(str); mp = _unit_labels(lab)
print(' s7fq13c label->canonical', dict(sorted(set(zip(lab, mp.astype(object).where(mp.notna(), None))))))
