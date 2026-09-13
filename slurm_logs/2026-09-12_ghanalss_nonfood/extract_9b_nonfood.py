import warnings; warnings.filterwarnings('ignore')
import pandas as pd, json
from lsms_library.local_tools import df_from_orgfile
def real(x):
    return str(x).strip() not in ('','---','nan','None','<NA>')
out={}
for w in ('2005-06','2012-13','2016-17'):
    try:
        tbl = df_from_orgfile(f'lsms_library/countries/GhanaLSS/{w}/_/categorical_mapping.org',
                              name='harmonize_food')
    except Exception as e:
        print(f'{w}: no wave harmonize_food ({e})'); continue
    cols=[c for c in tbl.columns if 'Code_9b' in c or 'Label_9b' in c]
    if 'Code_9b' not in tbl.columns:
        print(f'{w}: columns {list(tbl.columns)}'); continue
    t=tbl[tbl['Code_9b'].notna() & (tbl['Code_9b'].astype(str).str.strip()!='')].copy()
    t['food']=t['Preferred Label'].map(real)
    nf=t[~t['food']]
    print(f'{w}: 9B codes {len(t)} | food {int(t.food.sum())} | NON-FOOD (blank label) {len(nf)}')
    lab='Label_9b' if 'Label_9b' in t.columns else None
    out[w]={int(float(c)):(str(l).strip() if lab else '') for c,l in
            zip(nf['Code_9b'], nf[lab] if lab else nf['Code_9b'])}
    for k in sorted(out[w])[:8]: print(f'      {k:4d}  {out[w][k]}')
json.dump(out, open('/tmp/nf9b.json','w'), indent=1)
print('wrote /tmp/nf9b.json')
