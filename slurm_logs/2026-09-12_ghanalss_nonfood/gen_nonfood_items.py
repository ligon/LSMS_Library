"""Generate the `nonfood_items` org table for GhanaLSS Section 9A.

Each wave supplies its OWN code -> label from its OWN source (GLSS5/GLSS6
Stata value labels on `lfreqcd`; GLSS7 the `s9aname` column, which that wave
ships instead of value labels).  Labels are NOT self-identifying -- GLSS7 code
28 is literally "men" -- so a label shared by several codes inside one wave is
disambiguated by its RANK in code order, which is stable across GLSS5/GLSS6
(GLSS6 is GLSS5 shifted by one from code 4 on, so the ordering is preserved).
"""
import os, sys, re, collections
sys.path.insert(0, os.environ['REPO'])
from nf_labels import value_labels, glss7_names

R = os.environ['REPO']
WAVES = ['2005-06', '2012-13', '2016-17']


def titled(s):
    out = s[:1].upper() + s[1:] if s else s
    return out


def vocab(d):
    """{code -> Preferred Label}; rank-disambiguate labels shared by >1 code."""
    by_label = collections.defaultdict(list)
    for c in sorted(d):
        by_label[d[c]].append(c)
    out = {}
    for lab, codes in by_label.items():
        for n, c in enumerate(codes, 1):
            out[c] = titled(lab) if len(codes) == 1 else f'{titled(lab)} ({n})'
    return out


src = {'2005-06': value_labels('2005-06', 'partb/sec9a.dta', 'lfreqcd'),
       '2012-13': value_labels('2012-13', 'PARTB/sec9a.dta', 'lfreqcd'),
       '2016-17': glss7_names(R)}
voc = {w: vocab(src[w]) for w in WAVES}

rows = {}           # Preferred Label -> {wave: (code, raw label)}
for w in WAVES:
    for c, pl in voc[w].items():
        rows.setdefault(pl, {})[w] = (c, src[w][c])

# injectivity, the GH #323 / #783 invariant: one label, one item per wave
for w in WAVES:
    assert len(set(voc[w].values())) == len(voc[w]), f'{w}: labels not injective'

hdr = (['Preferred Label'] + [f'Code_{w}' for w in WAVES]
       + [f'Label_{w}' for w in WAVES])
lines = ['#+name: nonfood_items', '| ' + ' | '.join(hdr) + ' |',
         '|' + '|'.join(['-' * (len(h) + 2) for h in hdr]) + '|']
for pl in sorted(rows):
    r = rows[pl]
    cells = [pl]
    cells += [str(r[w][0]) if w in r else '' for w in WAVES]
    cells += [r[w][1] if w in r else '' for w in WAVES]
    lines.append('| ' + ' | '.join(c.replace('|', '/') for c in cells) + ' |')

open(f'{R}/.coder/nonfood_items.org.fragment', 'w').write('\n'.join(lines) + '\n')

n_all = sum(1 for r in rows.values() if len(r) == 3)
n56 = sum(1 for r in rows.values() if '2005-06' in r and '2012-13' in r)
print(f'rows: {len(rows)}')
for w in WAVES:
    print(f'  {w}: {len(voc[w])} codes')
print(f'  GLSS5 & GLSS6 share a row: {n56} (of {len(voc["2005-06"])} GLSS5 codes)')
print(f'  all three waves share a row: {n_all}')
print(f'  GLSS7-only rows: {sum(1 for r in rows.values() if list(r) == ["2016-17"])}')
