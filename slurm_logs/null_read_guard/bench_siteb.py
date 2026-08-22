import time, warnings, json, glob
warnings.simplefilter('ignore')
import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
from lsms_library.country import Country, _required_scheme_columns
from lsms_library.null_read_audit import audit_declared_columns

SP = '/tmp/claude-43292/-global-scratch-fsa-fc-jevons-ligon-mirrors-LSMS-Library/25ce0c52-31ea-4572-beb6-4e970ec341f6/scratchpad/'
recs = [json.loads(l) for p in sorted(glob.glob(SP + 'build_?.jsonl')) for l in open(p)]
ok = [r for r in recs if r.get('status') == 'ok']
big = sorted(ok, key=lambda r: -r['nrow'])[:3]
for r in big:
    C = Country(r['country'])
    entry = (C.resources or {}).get('Data Scheme', {}).get(r['table'])
    req = _required_scheme_columns(entry)
    t0 = time.time(); df = getattr(C, r['table'])(); warm = time.time() - t0
    ts = []
    for _ in range(3):
        t = time.time()
        audit_declared_columns(df, req, country=r['country'], table=r['table'])
        ts.append(time.time() - t)
    print(f"{r['country']}/{r['table']}: {len(df):,} rows x {len(df.columns)} cols "
          f"| warm read {warm:.2f}s | Site B {min(ts)*1000:.0f} ms "
          f"({100*min(ts)/max(warm,1e-9):.1f}% of the read)")
    del df
