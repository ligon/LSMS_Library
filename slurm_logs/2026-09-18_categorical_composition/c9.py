import sys,pickle,collections
sys.path.insert(0,'/local/job39002769/mirrors/LSMS_Library')
import pandas as pd; from pathlib import Path
rows=pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def umap(c):
    m={}
    for r in rows:
        if r['table']=='u' and r['country']==c and 'Preferred Label' in r['cols']:
            k=[x for x in r['cols'] if x!='Preferred Label'][0]
            for a,b in zip(r['df'][k],r['df']['Preferred Label']):
                a=str(a).strip(); m[a]=str(b).strip()
                try:
                    f=float(a)
                    if f==int(f): m[str(int(f))]=str(b).strip(); m[f"{int(f)}.0"]=str(b).strip()
                except: pass
    return m
GLOB={}
for r in rows:
    if r['level']=='global' and r['table']=='u':
        GLOB=dict(zip(r['df']['Original Label'].astype(str),r['df']['Preferred Label'].astype(str)))
D=Path.home()/'.local/share/lsms_library'
for c in ['Malawi','Uganda','Nigeria','Mali']:
    df=pd.read_parquet(D/c/'var/food_acquired.parquet')
    u=pd.Series(df.index.get_level_values('u')).astype(str)
    m=umap(c)
    served=u.map(lambda v:m.get(v,v))
    kg=[x for x in served.unique() if str(x).lower().replace(' ','') in ('kg','kgs','kilogram','kilogramme','kilo')]
    cnt={k:int((served==k).sum()) for k in kg}
    after={GLOB.get(k,k) for k in kg}
    print(f"{c:10s} kilogram spellings served TODAY: {cnt}   -> after global fold: {after}")
