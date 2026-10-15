import pickle, collections
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def key(r):
    k=[c for c in r['cols'] if c!='Preferred Label']
    return k[0] if k else None
by=collections.defaultdict(list)
for r in rows: by[(r['country'],r['table'])].append(r)
print("=== same COUNTRY, >1 level ===")
for (c,t),v in sorted(by.items()):
    if len({x['level'] for x in v})>1:
        print(f"{c} / {t}: " + ", ".join(f"{x['level']}:{x['wave'] or '-'}:key={key(x)!r}:n={x['nrows']}" for x in v))
print()
print("=== Mali u head ===")
for r in rows:
    if r['country']=='Mali' and r['table']=='u': print(r['df'].head(8).to_string())
print("=== GLOBAL u ===")
for r in rows:
    if r['level']=='global' and r['table']=='u': print(r['df'].to_string())
print("=== tables WITHOUT Preferred Label ===")
n=0
for r in rows:
    if 'Preferred Label' not in r['cols']:
        n+=1; print(f"  {r['level']:8s} {r['country']:14s} {r['wave']:9s} {r['table']:26s} cols={r['cols']}")
print("count:",n)
