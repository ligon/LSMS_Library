import warnings; warnings.filterwarnings("ignore")
import pandas as pd, lsms_library as ll
C=ll.Country('Ethiopia'); cp=C.crop_production()
f=cp.index.to_frame(index=False)[['i','t']].drop_duplicates()
pat=f.groupby('i')['t'].apply(lambda s:'+'.join(sorted(s)))
vc=pat.value_counts()
print("Ethiopia crop_production: household wave-patterns (top 12)")
print(vc.head(12).to_string())
print("\nhh per wave:", f.groupby('t')['i'].nunique().to_dict())
# population universes
try:
    pop=C.population
    for w,r in sorted(pop.items()):
        print(f"  {w:<10} {r.get('universe_tag')}  ({r.get('source_type')}/{r.get('confidence')})")
except Exception as e: print("population:",e)
