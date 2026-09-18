import pickle, collections
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def isnum(x):
    try: float(x); return True
    except: return False
tot=0; ident=0; det=collections.Counter()
for r in rows:
    if 'Preferred Label' not in r['cols']: continue
    k=[c for c in r['cols'] if c!='Preferred Label'][0]
    for a,b in zip(r['df'][k],r['df']['Preferred Label']):
        b=str(b).strip()
        if b in ('','nan','---'): continue
        if isnum(b):
            tot+=1; det[(r['level'],r['country'],r['table'])]+=1
            if str(a).strip()==b: ident+=1
print(f"rows whose Preferred Label is NUMERIC (a code propagating upward): {tot}  (identity-maps: {ident})")
for k2,v in det.most_common(): print(f"  {k2}: {v}")
