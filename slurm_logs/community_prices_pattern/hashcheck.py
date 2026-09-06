import sys, os
sys.path.insert(0, os.environ['WT'])
import lsms_library
assert 'wt-gsps-inputs' in lsms_library.__file__, lsms_library.__file__
from lsms_library.paths import countries_root, data_root
assert str(countries_root()).startswith(os.environ['WT'])
assert str(data_root()).startswith(os.environ['SCRATCH'])
import pyarrow.parquet as pq
from lsms_library.country import Country
COUNTRIES = ['Malawi','Nigeria','Ethiopia','EthiopiaRHS','Mali','Niger','Tanzania','GhanaLSS']
for c in COUNTRIES:
    C = Country(c)
    for t in ('community_prices','food_acquired','sample'):
        p = data_root()/c/'var'/f'{t}.parquet'
        if not p.exists():
            print(f"{c:14s} {t:18s} NO PARQUET"); continue
        md = pq.read_schema(p).metadata or {}
        emb = md.get(b'lsms_cache_hash')
        emb = emb.decode() if emb else None
        try:
            want = C._table_cache_hash(t, list(C.waves))
        except Exception as e:
            want = f"ERR {type(e).__name__}: {e}"
        print(f"{c:14s} {t:18s} emb={str(emb)[:12]} want={str(want)[:12]} match={emb==want}")
