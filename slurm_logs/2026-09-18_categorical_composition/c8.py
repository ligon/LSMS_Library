import pickle
rows=pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
for r in rows:
    if r['country']=='GhanaLSS' and r['table']=='harmonize_food':
        print(r['level'],r['wave'],r['cols']); print(r['df'].head(3).to_string()); break
print("\n== 5: Preferred Labels for kilogram in the 4 Code-keyed u countries ==")
for r in rows:
    if r['table']!='u': continue
    if r['country'] not in ('Malawi','Mali','Nigeria','Uganda'): continue
    k=[c for c in r['cols'] if c!='Preferred Label'][0]
    df=r['df']
    m=df[df['Preferred Label'].astype(str).str.lower().str.contains('kilo|^kg',regex=True,na=False)]
    print(f"-- {r['country']} key={k!r}: {sorted(set(m['Preferred Label'].astype(str)))}")
    print("   rows:", list(zip(m[k].astype(str),m['Preferred Label'].astype(str)))[:12])
