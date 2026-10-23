"""Same-type inorganic slot pairs in raw Module D, 2010-11 and 2019-20 (aggregates)."""
import json, sys
import pandas as pd, pyreadstat
from lsms_library.data_access import get_data_file
TYPES = {1, 2, 3, 4, 5, 6}
ULAB = {1: 'Gram', 2: 'Kilogram', 3: '2 kg Bag', 4: '3 kg Bag', 5: '5 kg Bag', 6: '10 kg Bag', 7: '50 kg Bag', 13: 'Other (Specify)'}
SPEC = {'2010-11': [('Data/Full_Sample/Agriculture/ag_mod_d.dta', 'ag_d39a', 'ag_d39f', 'ag_d39d', 'ag_d39i', 'ag_d39e', 'ag_d39j')],
        '2019-20': [('Data/Cross_Sectional/ag_mod_d.dta', 'ag_d39a', 'ag_d39g', 'ag_d39d', 'ag_d39j', 'ag_d39e', None),
                    ('Data/Panel/ag_mod_d_19.dta', 'ag_d39a', 'ag_d39g', 'ag_d39d', 'ag_d39j', 'ag_d39e', None)]}
def dec(s):
    c = pd.to_numeric(s, errors='coerce').round()
    return c.map(lambda x: ULAB.get(int(x)) if pd.notna(x) else None)
res = {}
for w, files in SPEC.items():
    tot = dict(pairs=0, both_kg_positive=0, use_yes=0, use_yes_both_positive=0,
               equal_kg=0, shared_key_before_1193=0, newly_coalesced_by_1193=0,
               newly_coalesced_use_yes_both_positive=0)
    for rel, t1, t2, q1, q2, u1, u2 in files:
        d, _ = pyreadstat.read_dta(str(get_data_file(f'Malawi/{w}/{rel}')), apply_value_formats=False,
                                   usecols=None if '2016' not in rel else None)
        a, b = pd.to_numeric(d[t1], errors='coerce').round(), pd.to_numeric(d[t2], errors='coerce').round()
        m = a.isin(TYPES) & b.isin(TYPES) & (a == b)
        x1, x2 = pd.to_numeric(d[q1], errors='coerce'), pd.to_numeric(d[q2], errors='coerce')
        pos = (x1 > 0) & (x2 > 0)
        use = pd.to_numeric(d['ag_d38'], errors='coerce') == 1
        k1 = dec(d[u1])
        k2 = dec(d[u2]) if u2 else pd.Series(None, index=d.index, dtype=object)
        same_before = (k1.isna() & k2.isna()) | (k1 == k2).fillna(False)
        tot['pairs'] += int(m.sum())
        tot['both_kg_positive'] += int((m & pos).sum())
        tot['use_yes'] += int((m & use).sum())
        tot['use_yes_both_positive'] += int((m & use & pos).sum())
        tot['equal_kg'] += int((m & (x1 == x2)).sum())
        tot['shared_key_before_1193'] += int((m & same_before).sum())
        tot['newly_coalesced_by_1193'] += int((m & ~same_before).sum())
        tot['newly_coalesced_use_yes_both_positive'] += int((m & ~same_before & use & pos).sum())
    res[w] = tot
    print(w, tot, flush=True)
json.dump(res, open(sys.argv[1], 'w'), indent=1)
