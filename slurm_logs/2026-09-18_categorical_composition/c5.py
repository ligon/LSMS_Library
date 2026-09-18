import pickle, collections, math, re
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def isnum(x):
    try: float(x); return True
    except: return False
def ctype(series):
    vals=[str(x).strip() for x in series]
    vals=[v for v in vals if v not in ('','nan','---')]
    if not vals: return 'empty'
    n=sum(isnum(v) for v in vals)
    return 'code' if n==len(vals) else ('label' if n==0 else 'mixed')
def key(r):
    k=[c for c in r['cols'] if c!='Preferred Label']
    return k[0] if k else None

# (a) key heading contradicts content
mis=[]
for r in rows:
    k=key(r)
    if k is None: continue
    t=ctype(r['df'][k])
    codeish = k.lower().startswith('code') or k.lower() in ('proizvod',)
    labelish= 'label' in k.lower() or k.lower() in ('alternate spelling','text','unit')
    if codeish and t in ('label',): mis.append(('CODE-heading holds LABELS',r,k,t))
    if labelish and t in ('code',): mis.append(('LABEL-heading holds CODES',r,k,t))
    if t=='mixed' and (codeish or labelish): mis.append(('heading holds BOTH',r,k,t))
print(f"=== (a) key heading contradicts content: {len(mis)} tables ===")
for m,r,k,t in mis: print(f"  {m:26s} {r['level']:7s} {r['country']:14s} {r['wave']:9s} {r['table']:22s} key={k!r} content={t}")

# (b) heading role classification
WAVE=re.compile(r'^(19|20)\d\d([-Q][\dA-Za-z-]*)?$')
KEYNAMES={'Code','Original Label','Alternate Spelling','Text','proizvod','Unit','Aggregate','GD Category','Wave'}
OUT={'Preferred Label','Aggregate Label','Comparable Label','Nutrition Label','Aggregate (short)','Settlement Label','Label'}
hd=collections.Counter(); role=collections.defaultdict(set); files=collections.defaultdict(set)
for r in rows:
    for c in r['cols']:
        hd[c]+=1; files[c].add(r['path'])
def cls(c):
    if c=='Preferred Label': return 'OUTPUT(canonical)'
    if WAVE.match(c) or c.startswith(('Code_','Label_')) or c in ('Wave','File'): return 'PRESENCE/wave-scoped'
    if c in OUT: return 'OUTPUT(alt)'
    if c in KEYNAMES: return 'KEY'
    return 'EXTRA'
cnt=collections.Counter(); hcnt=collections.Counter()
for c,n in hd.items():
    cnt[cls(c)]+=n; hcnt[cls(c)]+=1
print("\n=== (b) heading roles ===")
for k2 in sorted(cnt): print(f"  {k2:22s} distinct_headings={hcnt[k2]:3d}  (table,heading) pairs={cnt[k2]:4d}")
print("\n  EXTRA headings:")
for c,n in sorted(((c,hd[c]) for c in hd if cls(c)=='EXTRA'), key=lambda z:-z[1]):
    print(f"    {n:3d}  {c!r}  files={len(files[c])}")
print("\n  PRESENCE headings (distinct):", hcnt['PRESENCE/wave-scoped'])
print("\n=== (c) files ===")
print("  total org files parsed:", len({r['path'] for r in rows}))
print("  files containing >=1 table:", len({r['path'] for r in rows}))
print("  files with a mis-named key heading:", len({r['path'] for _,r,_,_ in mis}))
print("  tables total:", len(rows))
