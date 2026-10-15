import pickle, collections
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def key(r):
    k=[c for c in r['cols'] if c!='Preferred Label']
    return k[0] if k else None
def keytype(r):
    df=r['df']; k=key(r)
    if k is None: return 'nokey'
    s=df[k].dropna().astype(str).str.strip()
    s=s[s!='']
    if len(s)==0: return 'empty'
    def num(x):
        try: float(x); return True
        except: return False
    n=sum(num(x) for x in s)
    if n==len(s): return 'code'
    if n==0: return 'label'
    return f'mixed({n}/{len(s)})'
by=collections.defaultdict(list)
for r in rows: by[r['table']].append(r)

print("distinct table names:", len(by))
multi = {n:v for n,v in by.items() if len({(x['level'],x['country']) for x in v})>1 or len(v)>1}
# names appearing at >1 LEVEL
lvl_multi = {n:v for n,v in by.items() if len({x['level'] for x in v})>1}
print("names at >1 LEVEL:", len(lvl_multi))
print("names in >1 FILE:", len([n for n,v in by.items() if len(v)>1]))
print()
for n,v in sorted(lvl_multi.items()):
    print(f"### {n}  ({len(v)} instances, levels={sorted({x['level'] for x in v})})")
    kts=collections.Counter()
    for x in sorted(v,key=lambda z:(z['level'],z['country'],z['wave'])):
        kt=keytype(x); kts[(key(x),kt.split('(')[0])]+=1
        print(f"   {x['level']:8s} {x['country']:16s} {x['wave']:10s} key={key(x)!r:22s} type={kt:12s} n={x['nrows']}")
    print(f"   -> key namespaces: {dict(kts)}")
    print()
