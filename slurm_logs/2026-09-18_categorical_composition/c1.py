import pickle, collections, sys
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
hd = collections.Counter(); hd_lvl = collections.defaultdict(collections.Counter)
first = collections.Counter()  # positional key column
for r in rows:
    for c in r['cols']:
        hd[c]+=1; hd_lvl[c][r['level']]+=1
    k = [c for c in r['cols'] if c!='Preferred Label']
    first[k[0] if k else None]+=1
print("DISTINCT HEADINGS:", len(hd))
print("TOTAL (table,heading) pairs:", sum(hd.values()))
for c,n in hd.most_common():
    print(f"{n:5d}  {dict(hd_lvl[c])}  {c!r}")
print("\n=== POSITIONAL KEY COLUMN ===")
for c,n in first.most_common(): print(f"{n:5d} {c!r}")
