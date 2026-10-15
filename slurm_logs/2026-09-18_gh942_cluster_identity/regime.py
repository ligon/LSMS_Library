import os, glob, warnings
import pandas as pd
warnings.filterwarnings('ignore')
root = os.path.expanduser('~/.local/share/lsms_library')
rows=[]
for c in sorted(os.listdir(root)):
    p = os.path.join(root,c,'var','sample.parquet')
    if not os.path.exists(p): continue
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        rows.append((c,'ERR',str(e)[:60])); continue
    d = df.reset_index()
    if not {'i','t','v'} <= set(d.columns):
        rows.append((c,'NOCOLS',str(list(d.columns)[:6]))); continue
    d = d[['t','i','v']].astype(str).drop_duplicates()
    waves = sorted(d['t'].unique())
    if len(waves)<2:
        rows.append((c,'1WAVE',waves)); continue
    out=[]
    for a,b in zip(waves,waves[1:]):
        A=d[d.t==a].drop_duplicates('i').set_index('i')['v']
        B=d[d.t==b].drop_duplicates('i').set_index('i')['v']
        common=A.index.intersection(B.index)
        n=len(common)
        same=int((A[common]==B[common]).sum()) if n else 0
        # bijection check on cluster pairs
        if n:
            pr=pd.DataFrame({'va':A[common].values,'vb':B[common].values}).drop_duplicates()
            npair=len(pr); one2one = int(pr.groupby('va')['vb'].nunique().gt(1).sum())+int(pr.groupby('vb')['va'].nunique().gt(1).sum())
        else:
            npair=0; one2one=-1
        out.append((a,b,n,same,round(100*same/n,1) if n else None,npair,one2one))
    rows.append((c,'OK',out))
for r in rows:
    print(r[0], r[1])
    if r[1]=='OK':
        for o in r[2]: print('   ',o)
    else:
        print('   ',r[2])
