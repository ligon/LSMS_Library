import warnings, time, os
import pandas as pd, numpy as np
import lsms_library
print('lsms_library:', lsms_library.__file__)
from lsms_library.paths import data_root
from lsms_library.quantity_audit import audit_quantities, QUANTITY_OUTLIER_K
root = data_root()
tot = fired = 0
rows_ratio = []
for c in ['Tanzania','Uganda','Malawi','Nigeria','Ethiopia']:
    p = root / c / 'var' / 'crop_production.parquet'
    if not p.exists():
        print(f'--- {c}: absent at {p}'); continue
    df = pd.read_parquet(p)
    ts=[]
    for _ in range(3):
        t0=time.perf_counter(); reps = audit_quantities(df,'Quantity',country=c,table='crop_production'); ts.append((time.perf_counter()-t0)*1000)
    n = sum(r['n_implausible'] for r in reps); tot += len(df); fired += n
    print(f'{c}: rows={len(df):,} fired={n} reports={len(reps)} time={min(ts):.0f}ms')
    for r in reps:
        top = r['offenders'][0]
        print(f"   {r['wave']}: {r['n_implausible']}/{r['rows']:,} top {top['j']} {top['value']:,.0f} {top['u']} i={top['i']} x{top['ratio']}")
print(f'TOTAL rows={tot:,} fired={fired} ({100*fired/tot:.4f}%)')
