"""Scratch-only: regenerate a country's crosswalk with its occupied/chain guard
disabled, then classify the entries the guard drops under single-pass."""
import sys, os, json
from pathlib import Path
from collections import Counter
country, scr = sys.argv[1], Path(sys.argv[2])
here = Path(os.getcwd())
src = (here / 'panel_ids.py').read_text()
reps = {
 'Mali': [("    if prev in cur_set and cur != prev:", "    if False:")],
 'Niger': [("    if prev_i in cur_ids_14:", "    if False:")],
 'GhanaLSS': [("    if prev in cur_set and cur != prev:", "    if False:"),
              ("if collisions:\n    raise ValueError(", "if False:\n    raise ValueError(")],
}[country]
for a, b in reps:
    assert src.count(a) == 1, (country, a)
    src = src.replace(a, b)
scr.mkdir(parents=True, exist_ok=True)
fake = scr / 'panel_ids.py'; fake.write_text(src)
sys.path.insert(0, str(here))
g = {'__file__': str(fake), '__name__': '__main__'}
exec(compile(src, str(fake), 'exec'), g)
new = json.load(open(scr / 'updated_ids.json'))
old = json.load(open(here / 'updated_ids.json'))
cur_all = set(g.get('cur_set') or g.get('cur_ids_14') or [])
res = {}
for w in new:
    o, n = old.get(w) or {}, new.get(w) or {}
    added = {k: v for k, v in n.items() if k not in o}
    if not added and o == n:
        continue
    moving = {k for k, v in n.items() if k != v}
    chained = {k for k, v in added.items() if v in moving}
    collide = {k for k, v in added.items() if v != k and v in cur_all and v not in moving}
    served = {x: n.get(x, x) for x in (cur_all | set(n))}
    dups = sum(c - 1 for c in Counter(served.values()).values() if c > 1)
    res[w] = dict(committed=len(o), unguarded=len(n), added=len(added), changed=sum(o[k] != n[k] for k in o if k in n),
                  added_onto_a_moving_occupant=len(chained), added_onto_a_staying_occupant=len(collide),
                  single_pass_duplicate_ids=dups, current_ids=len(cur_all))
print(country, json.dumps(res))
json.dump(res, open(scr / 'summary.json', 'w'), indent=1)
