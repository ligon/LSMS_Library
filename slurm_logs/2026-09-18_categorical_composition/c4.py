import pickle, collections, math, re, unicodedata
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def key(r):
    k=[c for c in r['cols'] if c!='Preferred Label']
    return k[0] if k else None
def norm(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode()
    s=re.sub(r'[^a-z0-9]','',s.lower())
    s=re.sub(r'(s|es|mme|me)$','',s)
    return s
def isnum(x):
    try: float(x); return True
    except: return False
AXIS={'u':'u','u_kg':'u','harmonize_food':'j','harmonize_crop':'j','harmonize_price_item':'j','harmonize_unit':'u','harmonize_price_unit':'u','AreaUnit':'u','units':'u','unit_9b':'u','agric_unit':'u','time_unit':'u'}
tot=collections.Counter()
detail=[]
for r in rows:
    t=r['table']
    if t not in AXIS: continue
    if 'Preferred Label' not in r['cols']: continue
    k=key(r); df=r['df']
    g=collections.defaultdict(list)
    for a,b in zip(df[k],df['Preferred Label']):
        b=str(b).strip()
        if b in ('','nan'): continue
        g[b].append(str(a).strip())
    for pl,srcs in g.items():
        if len(srcs)<2: continue
        tot[(AXIS[t],'folds')]+=1
        # commensurable-by-spelling?  all sources normalise to the PL or to each other
        ns={norm(s) for s in srcs}
        spelling = (len(ns)==1)
        numeric  = all(isnum(s) for s in srcs)
        kind = 'spelling' if spelling else ('code' if numeric else 'distinct-labels')
        tot[(AXIS[t],kind)]+=1
        if kind=='distinct-labels':
            detail.append((AXIS[t],r['level'],r['country'],r['wave'],t,pl,srcs))
print("FOLDS by axis/kind:")
for k2,v in sorted(tot.items()): print(f"  {k2}: {v}")
print(f"\nDISTINCT-LABEL folds (the suspicious class): {len(detail)}")
byc=collections.Counter((d[0],d[2],d[4]) for d in detail)
for k2,v in byc.most_common(30): print(f"  {k2}: {v}")
print("\n--- sample u-axis distinct-label folds ---")
n=0
for d in detail:
    if d[0]!='u': continue
    n+=1
    if n>25: break
    print(f"  {d[2]:14s} {d[3]:8s} {d[4]:18s} {d[5]!r} <- {d[6][:7]}")
