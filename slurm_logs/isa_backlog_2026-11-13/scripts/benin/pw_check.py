"""2021-22 panel_weight, base against branch.  usage: pw_check.py <base sample.pkl> <cand sample.pkl> <updated_ids.json>"""
import json, pickle, sys
import pandas as pd
b, c = (pickle.load(open(p, 'rb')) for p in sys.argv[1:3])
upd = json.load(open(sys.argv[3]))['2021-22']
for name, df in (('base', b), ('cand', c)):
    d = df.reset_index()
    d['t'] = d['t'].astype(str)
    for w in ('2018-19', '2021-22'):
        x = d[d['t'] == w]
        print(name, w, 'rows', len(x), 'panel_weight non-NA', int(x['panel_weight'].notna().sum()),
              'mean', round(float(x['panel_weight'].mean()), 6), 'weight non-NA', int(x['weight'].notna().sum()),
              'weight mean', round(float(x['weight'].mean()), 6))
bd = b.reset_index(); bd['t'] = bd['t'].astype(str)
cd = c.reset_index(); cd['t'] = cd['t'].astype(str)
bn = bd[bd['t'] == '2021-22'].copy(); bn['i'] = bn['i'].map(lambda x: upd.get(x, x))
cn = cd[cd['t'] == '2021-22']
m = bn.set_index('i')[['weight', 'panel_weight']].join(cn.set_index('i')[['weight', 'panel_weight']], lsuffix='_b', rsuffix='_c')
linked = m.index.isin(set(upd.values()))
print('weight equal on all 2021-22:', bool(((m['weight_b'] - m['weight_c']).abs() < 1e-12).all()))
r = (m.loc[linked, 'panel_weight_c'] / m.loc[linked, 'panel_weight_b'])
print('linked', int(linked.sum()), 'cand/base panel_weight ratio min/max', float(r.min()), float(r.max()),
      '(= base mean over the 8,032 / base mean over the linked when constant)')
print('unlinked', int((~linked).sum()), 'cand panel_weight NA:', bool(m.loc[~linked, 'panel_weight_c'].isna().all()))
print('2018-19 panel_weight equal:', bool(((bd[bd.t == '2018-19'].set_index('i')['panel_weight'] - cd[cd.t == '2018-19'].set_index('i')['panel_weight']).abs() < 1e-12).all()))
print('DONE')
