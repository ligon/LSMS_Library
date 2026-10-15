import pickle, collections, re
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def key(r):
    k=[c for c in r['cols'] if c!='Preferred Label']
    return k[0] if k else None
MAG=re.compile(r'(\d+(?:[.,]\d+)?)\s*(kgs?|kilo\w*|lts?|litres?|liters?|l\b|grammes?|grams?|g\b|ml|cl)',re.I)
SCALE={'kg':1,'kilo':1,'lt':1,'litre':1,'liter':1,'l':1,'gramme':.001,'gram':.001,'g':.001,'ml':.001,'cl':.01}
def mags(s):
    out=set()
    for v,unit in MAG.findall(str(s)):
        u=unit.lower().rstrip('s')
        for k2,f in SCALE.items():
            if u.startswith(k2): out.add(round(float(v.replace(',','.'))*f,6)); break
    return out
bad=[]
for r in rows:
    if r['table'] not in ('u','harmonize_unit','harmonize_price_unit','AreaUnit','units','unit_9b','agric_unit'): continue
    if 'Preferred Label' not in r['cols']: continue
    k=key(r); g=collections.defaultdict(list)
    for a,b in zip(r['df'][k], r['df']['Preferred Label']):
        b=str(b).strip()
        if b in ('','nan'): continue
        g[b].append(str(a).strip())
    for pl,srcs in g.items():
        if len(srcs)<2: continue
        ms=set()
        for s in srcs: ms|=mags(s)
        if len(ms)>1:
            bad.append((r['country'],r['wave'],r['table'],pl,sorted(ms),srcs))
print(f"u-axis folds with CONFLICTING explicit magnitudes: {len(bad)}")
for b in bad: print(f"  {b[0]:14s} {b[2]:18s} {b[3]!r:22s} kg/l={b[4]}  <- {b[5]}")
print()
# u_kg coverage
for r in rows:
    if r['table']=='u_kg':
        print("u_kg table:", r['level'], r['country'], r['cols'], len(r['df']))
        print(r['df'].to_string())
