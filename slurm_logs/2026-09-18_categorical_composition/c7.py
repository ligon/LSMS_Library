import sys, pickle, collections, math
from pathlib import Path
sys.path.insert(0,'/local/job39002769/mirrors/LSMS_Library')
import lsms_library
assert '/local/job39002769/mirrors/LSMS_Library' in lsms_library.__file__
import pandas as pd
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
umap=collections.defaultdict(dict)
for r in rows:
    if r['table']!='u' or 'Preferred Label' not in r['cols']: continue
    k=[c for c in r['cols'] if c!='Preferred Label'][0]
    for a,b in zip(r['df'][k],r['df']['Preferred Label']):
        a=str(a).strip()
        umap[r['country']][a]=str(b).strip()
        if a.endswith('.0'):
            umap[r['country']][a[:-2]]=str(b).strip()
        try:
            f=float(a)
            if f==int(f): umap[r['country']][str(int(f))]=str(b).strip(); umap[r['country']][f"{int(f)}.0"]=str(b).strip()
        except: pass
D=Path.home()/'.local/share/lsms_library'
print(f"{'country':16s} {'rows':>9s} {'u_vals':>7s} {'unmapped':>8s} {'code-like_after':>15s}")
for c in sorted(p.name for p in D.iterdir() if p.is_dir() and p.name!='dvc-cache'):
    f=D/c/'var/food_acquired.parquet'
    if not f.exists(): continue
    try: df=pd.read_parquet(f)
    except Exception as e: print(c,'ERR',e); continue
    idx=df.index
    if 'u' not in (idx.names or []): 
        if 'u' in df.columns: u=df['u']
        else: print(f"{c:16s} no u"); continue
    else: u=idx.get_level_values('u')
    vals=pd.Series(u).astype(str)
    uniq=vals.unique()
    m=umap.get(c,{})
    mapped=[m.get(v,v) for v in uniq]
    unmapped=sum(1 for v in uniq if v not in m)
    codelike=sum(1 for v in mapped if str(v)[:1].isdigit())
    print(f"{c:16s} {len(df):9d} {len(uniq):7d} {unmapped:8d} {codelike:15d}"
          + ("   e.g. "+", ".join(repr(v) for v in [x for x in mapped if str(x)[:1].isdigit()][:5]) if codelike else ""))

print("\n=== row weight of code-like u, countries that DO declare #+name:u ===")
have={r['country'] for r in rows if r['table']=='u' and 'Preferred Label' in r['cols']}
for c in sorted(have):
    f=D/c/'var/food_acquired.parquet'
    if not f.exists(): continue
    df=pd.read_parquet(f)
    u=pd.Series(df.index.get_level_values('u') if 'u' in (df.index.names or []) else df['u']).astype(str)
    m=umap[c]
    mp=u.map(lambda v: m.get(v,v)).astype(str)
    bad=mp.str[:1].str.isdigit().fillna(False)
    print(f"  {c:16s} rows={len(df):8d}  code-like rows={int(bad.sum()):7d} ({100*bad.mean():.2f}%)  distinct={mp[bad].nunique()}")
