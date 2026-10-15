import pickle
rows = pickle.load(open('/local/job39002769/tmp/cm/rows.pkl','rb'))
def isnum(x):
    try: float(x); return True
    except: return False
for r in rows:
    if (r['country'],r['table']) in [('Senegal','harmonize_input'),('Burkina_Faso','harmonize_food'),('Niger','harmonize_assets'),('Uganda','harmonize_education'),('Senegal','u'),('GhanaSPS','AreaUnit')]:
        k=[c for c in r['cols'] if c!='Preferred Label'][0]
        bad=[(a,b) for a,b in zip(r['df'][k],r['df']['Preferred Label']) if isnum(str(a).strip()) and str(a).strip() not in ('','nan')]
        print(f"{r['country']}/{r['table']} key={k!r} numeric rows: {bad[:6]}")
