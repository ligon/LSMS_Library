"""Generate `nonfood_items_9b`: the Section 9B NON-food vocabulary.

Harmonised across waves by EXACT normalised label match only.  No fuzzy
matching: GLSS7 lists BRANDS where GLSS5/6 listed categories, and a fuzzy
join would pool distinct items (the same reason a GLSS6->GLSS7 crosswalk was
refused for 9A -- see CONTENTS.org).
"""
import json, re, unicodedata
from collections import defaultdict
nf = json.load(open('/tmp/nf9b.json'))
WAVES = ['2005-06','2012-13','2016-17']

# Three GLSS7 brand names occur TWICE, in two different product blocks.  Each
# block is terminated by an "Other <category>" item, which is what names it --
# read off the codes, not guessed.  Without this the two codes pool into one
# `j` and two distinct instrument items are silently merged (GH #323).
DISAMBIGUATE = {
    ('2016-17', 667): 'Yazz (washing powder)',
    ('2016-17', 986): 'Yazz (sanitary pad)',
    ('2016-17', 688): 'Rexona (bathing soap)',
    ('2016-17', 962): 'Rexona (deodorant)',
    ('2016-17', 897): 'Bic (pen)',
    ('2016-17', 968): 'Bic (razor)',
}

def norm(s):
    s = unicodedata.normalize('NFKD', str(s)).strip().casefold()
    s = re.sub(r'^\d+[\.\)]\s*', '', s)
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'\s*[\.,;:]+$', '', s)
    return s.replace(' /', '/').replace('/ ', '/')

items = {}
for w in WAVES:
    for code, label in nf.get(w, {}).items():
        code = int(code)
        disamb = DISAMBIGUATE.get((w, code))
        k = norm(disamb) if disamb else norm(label)
        if not k: continue
        assert w not in items.get(k, {}), (
            f'{w}: codes {items[k][w][0]} and {code} both normalise to {k!r} -- '
            'two distinct items would pool; add them to DISAMBIGUATE')
        items.setdefault(k, {})[w] = (code, str(label).strip(), disamb)

rows = []
for k, per in items.items():
    pref = next((per[w][2] or per[w][1]) for w in reversed(WAVES) if w in per)
    rows.append((pref, per))
rows.sort(key=lambda r: r[0].casefold())

labels = [r[0] for r in rows]
dupes = sorted({x for x in labels if labels.count(x) > 1})
assert not dupes, f'Preferred Label not injective: {dupes}'

hdr = ['Preferred Label'] + [f'Code_{w}' for w in WAVES] + [f'Label_{w}' for w in WAVES]
widths = [max(len(hdr[i]),
              max((len(str(_cell(r, i))) for r in rows), default=0))
          for i in range(len(hdr))] if False else None

def cells(pref, per):
    return ([pref] + [str(per[w][0]) if w in per else '' for w in WAVES]
            + [per[w][1].replace('|', '/') if w in per else '' for w in WAVES])

body = [cells(p, q) for p, q in rows]
W = [max(len(hdr[i]), max((len(r[i]) for r in body), default=0)) for i in range(len(hdr))]
def line(c): return '| ' + ' | '.join(c[i].ljust(W[i]) for i in range(len(hdr))) + ' |'
out = ['#+name: nonfood_items_9b', line(hdr),
       '|' + '+'.join('-'*(W[i]+2) for i in range(len(hdr))) + '|']
out += [line(r) for r in body]
open('/tmp/nonfood_items_9b.org','w').write('\n'.join(out) + '\n')

n = {i: sum(1 for _,p in rows if len(p)==i) for i in (1,2,3)}
print(f'rows {len(rows)} | all three waves {n[3]} | two {n[2]} | one only {n[1]}')
for w in WAVES:
    got = sum(1 for _,p in rows if w in p)
    print(f'   {w}: {got} codes carried (source had {len(nf.get(w,{}))})')
    assert got == len(nf.get(w, {})), 'lost a code'
print('OK: every source code is carried exactly once')
