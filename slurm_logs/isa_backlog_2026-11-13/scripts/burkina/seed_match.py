"""2021-22 (and 2018-19) seed rows of plot_inputs that find a crop_production row at
(t, i, crop), by crop -- the #1119 seed-to-harvest check.  Counts only.  Usage: OUT_JSON"""
import json, sys, warnings
warnings.simplefilter('ignore')
import lsms_library as ll
c = ll.Country('Burkina_Faso')
pi = c.plot_inputs().reset_index()
cp = c.crop_production().reset_index()
out = {}
for w in ('2018-19', '2021-22'):
    s = pi[(pi['t'].astype(str) == w) & (pi['input'].astype(str) == 'Seed')]
    h = set(zip(cp.loc[cp['t'].astype(str) == w, 'i'].astype(str), cp.loc[cp['t'].astype(str) == w, 'crop'].astype(str)))
    rec = {}
    for crop, g in s.groupby(s['crop'].astype(str)):
        hit = sum((i, crop) in h for i in g['i'].astype(str))
        rec[crop] = [int(hit), int(len(g))]
    out[w] = dict(sorted(rec.items(), key=lambda kv: -kv[1][1]))
    out[w + '_harvest_crop_labels'] = sorted(set(cp.loc[cp['t'].astype(str) == w, 'crop'].astype(str)))
json.dump(out, open(sys.argv[1], 'w'), indent=1, ensure_ascii=False)
print(out)
