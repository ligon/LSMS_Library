import warnings, sys, pandas as pd
warnings.filterwarnings('ignore')
import lsms_library as ll
assert '/local/job39002769/mirrors/LSMS_Library' in ll.__file__, ll.__file__
for c in sys.argv[1:]:
    try:
        d = ll.Country(c).sample().reset_index()[['t','i','v']].astype(str)
    except Exception as e:
        print(c,'ERR',str(e)[:80]); continue
    d=d.drop_duplicates()
    enc=sum(1 for a,b in zip(d.v,d.i) if a in b)/len(d)
    waves=sorted(d.t.unique())
    print(f'== {c}  i-contains-v={enc:.3f}')
    for a,b in zip(waves,waves[1:]):
        A=d[d.t==a].drop_duplicates('i').set_index('i')['v']; B=d[d.t==b].drop_duplicates('i').set_index('i')['v']
        k=A.index.intersection(B.index); n=len(k)
        same=int((A[k]==B[k]).sum()) if n else 0
        if n:
            pr=pd.DataFrame({'a':A[k].values,'b':B[k].values}).drop_duplicates()
            viol=int(pr.groupby('a')['b'].nunique().gt(1).sum())+int(pr.groupby('b')['a'].nunique().gt(1).sum())
            print(f'   {a} -> {b}: hh={n} same_v={same} ({100*same/n:.1f}%) pairs={len(pr)} nonbijective={viol} nA={A.nunique()} nB={B.nunique()}')
        else:
            print(f'   {a} -> {b}: hh=0  (no household linkage)')
