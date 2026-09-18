import warnings; warnings.filterwarnings('ignore')
import lsms_library
assert '/local/job38958677/' in lsms_library.__file__
import pandas as pd, yaml, json
from pathlib import Path
from lsms_library.local_tools import df_from_orgfile
root=Path('lsms_library/countries/GhanaLSS')
out=Path('slurm_logs/ghanalss_aggregate_labels')
waves=['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
t=df_from_orgfile(root/'_/categorical_mapping.org',name='harmonize_food')
t.columns=[str(x).strip() for x in t.columns]
for c in t.columns: t[c]=t[c].astype(str).str.strip()

rows=[]
for w in waves:
    wt=df_from_orgfile(root/w/'_/categorical_mapping.org',name='harmonize_food')
    wt.columns=[str(x).strip() for x in wt.columns]
    if 'Preferred Label' not in wt.columns:
        wt=wt.reset_index(); wt.columns=[str(x).strip() for x in wt.columns]
    m=dict(zip(wt['Preferred Label'].astype(str).str.strip(),
               wt['Aggregate Label'].astype(str).str.strip()))
    for _,r in t.iterrows():
        wl=r[w]
        if wl in ('','nan'): continue
        a=m.get(wl)
        if a not in (None,'','nan'):
            rows.append({'PL':r['Preferred Label'],'wave':w,'wave_label':wl,'agg':a})
cw=pd.DataFrame(rows)
g=cw.groupby('PL')['agg'].agg(lambda s: sorted(set(s)))
clean={k:v[0] for k,v in g.items() if len(v)==1}
conf={k:v for k,v in g.items() if len(v)>1}

with open(out/'aggregate_label_candidate.yml','w') as f:
    f.write("# GhanaLSS candidate 'Aggregate Label', keyed on country harmonize_food\n")
    f.write("# 'Preferred Label'.  Derived by the CROSSWALK route:\n")
    f.write("#   country PL -> that row's per-wave label column -> wave harmonize_food\n")
    f.write("#   'Preferred Label' -> wave 'Aggregate Label'.\n")
    f.write(f"# {len(clean)} unambiguous; {len(conf)} conflicted (NOT included -- see conflicts.org).\n")
    f.write("# Feed to: python -m lsms_library.util.orgtbl ... --column 'Aggregate Label'\n")
    yaml.safe_dump(clean, f, default_flow_style=False, allow_unicode=True, sort_keys=True)

det={}
for k,v in conf.items():
    sub=cw[cw['PL']==k]
    det[k]={a:sorted(set(sub[sub['agg']==a]['wave'])) for a in v}
with open(out/'conflicts.json','w') as f: json.dump(det,f,indent=2,sort_keys=True)
print('unambiguous:',len(clean),' conflicted:',len(conf))
print('distinct aggregate values among unambiguous:',len(set(clean.values())))
# how coarse is the result?
allv=set(clean.values())|{a for v in conf.values() for a in v}
print('distinct aggregate values overall:',len(allv),'from',len(g),'Preferred Labels')
# bucket size distribution on the unambiguous part
s=pd.Series(clean).value_counts()
print('singleton buckets:',(s==1).sum(),' multi-PL buckets:',(s>1).sum(),' largest:',s.head(8).to_dict())
