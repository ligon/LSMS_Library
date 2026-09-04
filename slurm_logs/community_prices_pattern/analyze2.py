import pandas as pd, numpy as np, os
S=os.environ['SCRATCH']; EX=S+'/extracts'
RECIPE = {'Malawi':('Price','NumberOfUnits'),'Nigeria':('Price',None),'Ethiopia':('Price','Quantity'),
 'EthiopiaRHS':('Price',None),'Mali':('Price','Quantity'),'Niger':('Price','Quantity'),
 'Tanzania':('Price',None),'GhanaLSS':('Price','NumberOfUnits')}
def load(c,t):
    p=f"{EX}/{c}__{t}.parquet"; return pd.read_parquet(p) if os.path.exists(p) else None
rows=[]
for c,(num,den) in RECIPE.items():
    cp=load(c,'community_prices'); fa=load(c,'food_acquired')
    if cp is None or fa is None: continue
    pr=cp[num].astype(float)
    if den and den in cp.columns:
        d=cp[den].astype(float); pr=pr/d.where(d>0)
    cpu=pr.groupby(level=['t','v','j','u'],observed=True).median().rename('cp')
    p=fa[fa.index.get_level_values('s').astype(str)=='purchased'] if 's' in fa.index.names else fa
    uv=(p['Expenditure']/p['Quantity'].where(p['Quantity']>0)).replace([np.inf,-np.inf],np.nan)
    uv=uv[uv>0]
    hh=uv.groupby(level=['t','v','j','u'],observed=True).median().rename('hh')
    j=pd.concat([hh,cpu],axis=1,join='inner').dropna()
    j=j[(j.hh>0)&(j.cp>0)]
    if not len(j): continue
    j['r']=j.hh/j.cp
    for t,sub in j.groupby(level='t',observed=True):
        r=sub['r']
        rows.append(dict(country=c,wave=t,n=len(r),
            exact_tie=float((np.isclose(r,1.0,rtol=1e-9)).mean()),
            within10=float(((r>=1/1.1)&(r<=1.1)).mean()),
            within25=float(((r>=0.8)&(r<=1.25)).mean()),
            med=float(r.median()), geomean=float(np.exp(np.log(r).mean())),
            iqr_log=float(np.log(r).quantile(.75)-np.log(r).quantile(.25))))
    # per-unit breakdown for the biggest wave
    big=j.groupby(level='t',observed=True).size().idxmax()
    w=j.xs(big,level='t')
    bu=w.groupby(level='u',observed=True)['r'].agg(['size','median']).sort_values('size',ascending=False).head(6)
    print(f"\n--- {c} ({big}) ratio by u:"); print(bu.to_string())
out=pd.DataFrame(rows); out.to_csv(S+'/ratio_detail.csv',index=False)
print("\n=== RATIO DETAIL (t,v,j,u) ==="); print(out.to_string(index=False))
