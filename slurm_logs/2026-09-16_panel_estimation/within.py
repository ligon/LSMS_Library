import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, lsms_library as ll
from lsms_library.transformations import harvest_kg

def hh(s, name):
    s = s.groupby(['i','t']).sum(min_count=1)
    return s.rename(name)

def within_share(df, col):
    """share of total variance of log(col) that is WITHIN household"""
    d = df[[col]].dropna()
    d = d[d[col] > 0]
    d = d.assign(y=np.log(d[col]))
    n = d.groupby(level='i')['y'].transform('size')
    d = d[n >= 2]                      # only households with >=2 obs identify within
    if len(d) < 50: return None, 0, 0
    mu = d.groupby(level='i')['y'].transform('mean')
    wv = ((d['y']-mu)**2).sum()/max(len(d)-d.index.get_level_values('i').nunique(),1)
    tv = d['y'].var()
    return wv/tv, len(d), d.index.get_level_values('i').nunique()

for c in ('Ethiopia','GhanaSPS'):
    print("="*66); print(c, flush=True)
    C=ll.Country(c)
    cp=C.crop_production(); pf=C.plot_features()
    print("  AreaUnit:", pf['AreaUnit'].astype(str).value_counts().head(4).to_dict(), flush=True)
    hk=harvest_kg(cp)
    hk=hk if isinstance(hk,pd.Series) else hk.iloc[:,0]
    out=hh(hk,'output_kg')
    area=hh(pf['Area'],'area')
    try:
        pl=C.plot_labor(); lab=hh(pl['PersonDays'],'labor')
    except Exception as e:
        lab=None; print("  plot_labor FAIL",e)
    parts=[out,area]+([lab] if lab is not None else [])
    df=pd.concat(parts,axis=1)
    print(f"  hh-year rows={len(df):,}", flush=True)
    for col in ['output_kg','area','labor']:
        if col not in df: continue
        w,n,nh=within_share(df,col)
        print(f"  within-share log({col:<9}) = {('%.3f'%w) if w else 'n/a':>6}   (n={n:,} hh-yrs, {nh:,} hh with >=2)", flush=True)
