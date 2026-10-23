"""Step 2a: per (country, wave), does the committed updated_ids map contain
chains (a value that is itself a non-self key)?  Compare single-pass rename
m.get(x, x) against the closure _close_id_map(m)."""
import json, sys, warnings, os
warnings.simplefilter("ignore")
import lsms_library as ll
from lsms_library.paths import countries_root


def _close_id_map(mapping, wave=None):
    """Copy of the closure removed by GH #1245 (local_tools at d353b7ea7),
    kept here so this script runs against either checkout."""
    closed = {}
    for k in mapping:
        seen = [k]
        v = mapping[k]
        while v in mapping and mapping[v] != v:
            if v in seen:
                raise ValueError(f"Cycle detected in updated_ids for wave {wave!r}")
            seen.append(v)
            v = mapping[v]
        closed[k] = v
    return closed

PANEL = ['Burkina_Faso','Ethiopia','EthiopiaRHS','GhanaLSS','Malawi','Mali','Niger','Nigeria','Senegal','Tanzania','Uganda']
out = {}
for c in PANEL:
    p = countries_root()/c/'_'/'updated_ids.json'
    ui = json.load(open(p))
    rows = []
    for w, m in ui.items():
        m = m or {}
        nonid = {k: v for k, v in m.items() if k != v}
        chain_keys = [k for k, v in m.items() if v in m and m[v] != v and k != v]
        closed = _close_id_map(m, wave=w) if m else {}
        differ = [k for k in m if closed[k] != m[k]]
        # non-injectivity under each semantics (over keys only)
        def noninj(f):
            from collections import Counter
            cnt = Counter(f.values()); return sum(n-1 for n in cnt.values() if n > 1)
        rows.append(dict(wave=w, entries=len(m), nonidentity=len(nonid),
                         chain_keys=len(chain_keys), closure_differs=len(differ),
                         noninjective_single=noninj(m), noninjective_closed=noninj(closed),
                         examples=differ[:3]))
    out[c] = rows
    for r in rows:
        print(c, r)
json.dump(out, open(sys.argv[1], 'w'), indent=1)
