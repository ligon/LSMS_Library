import pickle, collections, math, pandas as pd
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def key(r):
    k=[c for c in r['cols'] if c!='Preferred Label']
    return k[0] if k else None
def isnum(x):
    try: float(x); return True
    except: return False
code=[];lab=[];mixed=[]
for r in rows:
    k=key(r)
    if k is None: continue
    s=r['df'][k]
    vals=[str(x).strip() for x in s if not (isinstance(x,float) and math.isnan(x))]
    vals=[v for v in vals if v not in ('','nan')]
    if not vals: continue
    n=sum(isnum(v) for v in vals)
    (code if n==len(vals) else lab if n==0 else mixed).append((r,n,len(vals)))
print(f"CODE-KEYED: {len(code)}   LABEL-KEYED: {len(lab)}   MIXED: {len(mixed)}  (of {len(rows)} tables)")
for nm,grp in (('CODE',code),('MIXED',mixed)):
    print(f"\n--- {nm}-keyed by level ---")
    print(collections.Counter(r['level'] for r,_,_ in grp))
print("\n=== CODE-KEYED tables: does every code map to a LABEL? ===")
out=[]
for r,n,tot in code:
    df=r['df']; k=key(r)
    if 'Preferred Label' in df.columns:
        tgt='Preferred Label'
    else:
        others=[c for c in df.columns if c!=k]
        tgt=others[0] if others else None
    if tgt is None: out.append((r,'NO TARGET',0,0)); continue
    v=df[tgt]
    blank=sum(1 for x in v if (isinstance(x,float) and math.isnan(x)) or str(x).strip() in ('','nan'))
    numeric=sum(1 for x in v if str(x).strip() not in ('','nan') and isnum(x))
    out.append((r,tgt,blank,numeric))
tot_blank=sum(b for _,_,b,_ in out); tot_num=sum(nn for _,_,_,nn in out)
print(f"code-keyed tables={len(out)}  rows with BLANK target={tot_blank}  rows whose target is itself NUMERIC={tot_num}")
for r,tgt,b,nn in out:
    if b or nn: print(f"  {r['level']:7s} {r['country']:14s} {r['wave']:9s} {r['table']:24s} tgt={tgt:16s} blank={b} numeric_target={nn} n={r['nrows']}")
print("\n=== MIXED-key tables (a code and a label in ONE key column) ===")
for r,n,tot in mixed:
    print(f"  {r['level']:7s} {r['country']:14s} {r['wave']:9s} {r['table']:24s} key={key(r)!r} numeric={n}/{tot}")
