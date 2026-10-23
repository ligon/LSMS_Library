"""Cache-hash census: every (country, table) L2-country hash, every framework
fingerprint, and every declared (country, wave, table) L2-wave input hash.

Usage: python hash_census.py <checkout> <out.json>
Config/source reads only -- no data builds.
"""
import json
import os
import sys
import warnings

checkout, out = sys.argv[1], sys.argv[2]
import lsms_library as ll  # noqa: E402
from lsms_library import _build_registry as br  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
warnings.simplefilter("ignore")

res = {"country": {}, "btf": {}, "wave": {}, "errors": {}}
tables = set()
for cdir in sorted(p for p in countries_root().iterdir() if (p / "_" / "data_scheme.yml").exists()):
    name = cdir.name
    try:
        c = ll.Country(name, preload_panel_ids=False)
        scheme = list(c.data_scheme)
        waves = list(c.waves)
    except Exception as e:  # noqa: BLE001
        res["errors"][name] = repr(e)[:200]
        continue
    for t in scheme:
        tables.add(t)
        try:
            res["country"][f"{name}/{t}"] = c._table_cache_hash(t, waves)
        except Exception as e:  # noqa: BLE001
            res["errors"][f"{name}/{t}"] = repr(e)[:200]
        for w in waves:
            try:
                wv = c[w]
                if t in getattr(wv, "data_scheme", []) or True:
                    h = wv._input_hash(t)
                    if h is not None:
                        res["wave"][f"{name}/{w}/{t}"] = h
            except Exception as e:  # noqa: BLE001
                res["errors"][f"{name}/{w}/{t}"] = repr(e)[:200]
for t in sorted(tables) + [None]:
    try:
        res["btf"][str(t)] = br.build_transforms_fingerprint(t)
    except Exception as e:  # noqa: BLE001
        res["errors"][f"btf/{t}"] = repr(e)[:200]
with open(out, "w") as f:
    json.dump(res, f, indent=0, sort_keys=True)
print({k: len(v) for k, v in res.items()})
