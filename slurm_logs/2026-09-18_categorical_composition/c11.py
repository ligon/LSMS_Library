import pickle, collections, re, math
rows=pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
WAVE=re.compile(r'^(19|20)\d\d([-Q][\dA-Za-z-]*)?$')
def ispres(c): return WAVE.match(c) or c.startswith(('Code_','Label_')) or c in ('Wave','File')
MARK={'x','X','yes','Yes','✓','y','1','*'}
pres_t=0;key_t=0; pres_pairs=0; key_pairs=0
det=collections.Counter(); ktabs=collections.Counter()
for r in rows:
    for c in r['cols']:
        if not ispres(c): continue
        vals=[str(v).strip() for v in r['df'][c]]
        vals=[v for v in vals if v not in ('','nan','---')]
        if not vals: kind='empty'
        elif all(v in MARK for v in vals): kind='presence-mark'
        else: kind='per-wave-KEY(free strings)'
        det[kind]+=1
        if kind.startswith('per-wave'): ktabs[(r['country'],r['table'])]+=1
print("(table,heading) pairs among 'presence'-classed headings:")
for k,v in det.most_common(): print(f"  {v:4d}  {k}")
print("\ntables with >=1 per-wave KEY column:")
for k,v in ktabs.most_common(): print(f"  {k}: {v} wave columns")
print("\ndistinct tables:",len(ktabs))

print("\n--- presence-MARK columns (the genuine ones) ---")
for r in rows:
    for c in r['cols']:
        if not ispres(c): continue
        vals=[str(v).strip() for v in r['df'][c]]; vals=[v for v in vals if v not in ('','nan','---')]
        if vals and all(v in MARK for v in vals):
            print(f"  {r['country']}/{r['table']}  col={c!r}  vals={sorted(set(vals))}")
print("\n--- Uganda u sample ---")
for r in rows:
    if r['country']=='Uganda' and r['table']=='u':
        print(r['cols']); print(r['df'].head(4).to_string())
