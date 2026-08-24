"""Probe Country._table_cache_hash for a spread of countries x tables."""
import sys, json, warnings
warnings.filterwarnings('ignore')
import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
from lsms_library.country import Country

COUNTRIES = ['Uganda', 'Malawi', 'Tanzania', 'Nigeria', 'Ethiopia',
             'GhanaLSS', 'CotedIvoire', 'Niger', 'Guatemala', 'Albania']
TABLES = ['sample', 'household_roster', 'food_acquired', 'cluster_features', 'housing']

out = {}
for c in COUNTRIES:
    try:
        C = Country(c)
        waves = list(C.waves)
    except Exception as e:
        out[c] = {'ERROR': repr(e)[:200]}
        continue
    d = {}
    for t in TABLES:
        try:
            d[t] = C._table_cache_hash(t, waves)
        except Exception as e:
            d[t] = 'ERR:' + repr(e)[:120]
    out[c] = d

# also the raw build-transform fingerprints, which is where the trap lives
from lsms_library._build_registry import build_transforms_fingerprint
out['_btf'] = {t: build_transforms_fingerprint(t) for t in TABLES}
out['_btf']['<all>'] = build_transforms_fingerprint(None)

with open(sys.argv[1], 'w') as f:
    json.dump(out, f, indent=1, sort_keys=True)
print(json.dumps(out, indent=1, sort_keys=True))
