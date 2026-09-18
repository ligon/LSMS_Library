import warnings, sys, os, pandas as pd
warnings.filterwarnings('ignore')
import lsms_library as ll
assert '/local/job39002769/mirrors/LSMS_Library' in ll.__file__, ll.__file__
root=os.path.expanduser('~/.local/share/lsms_library')
cs = sys.argv[1:] or sorted(d for d in os.listdir(root) if os.path.exists(f'{root}/{d}/var/sample.parquet'))
for c in cs:
    try:
        d = ll.Country(c).sample().reset_index()[['t','i','v']]
    except Exception as e:
        print(f'{c}: ERR {str(e)[:70]}'); continue
    d = d.dropna().astype(str).drop_duplicates()
    waves=sorted(d.t.unique())
    if len(waves)<2: print(f'{c}: single wave'); continue
    print(f'== {c}')
    for a,b in zip(waves,waves[1:]):
        A=d[d.t==a].drop_duplicates('i').set_index('i')['v']; B=d[d.t==b].drop_duplicates('i').set_index('i')['v']
        k=A.index.intersection(B.index); n=len(k)
        if not n:
            print(f'   {a:9s}->{b:9s} hh=0   NO-LINKAGE'); continue
        same=int((A[k]==B[k]).sum())
        # tautology check: does i embed v in BOTH waves for the matched hh?
        taut=sum(1 for x in k if (A[x] in x) and (B[x] in x))/n
        pr=pd.DataFrame({'a':A[k].values,'b':B[k].values}).drop_duplicates()
        viol=int(pr.groupby('a')['b'].nunique().gt(1).sum())+int(pr.groupby('b')['a'].nunique().gt(1).sum())
        print(f'   {a:9s}->{b:9s} hh={n:6d} same_v={100*same/n:5.1f}% pairs={len(pr):5d} nonbij={viol:4d} nA={A.nunique():5d} nB={B.nunique():5d} i-embeds-v={taut:.2f}')
