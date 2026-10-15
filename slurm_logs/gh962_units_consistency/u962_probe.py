import warnings; warnings.filterwarnings("ignore")
import pandas as pd, numpy as np
from lsms_library.transformations import (food_kg_factors, food_quantities_from_acquired,
                                          _unit_sentinel_mask, FOOD_KG_FACTOR_LAYERS)
F='/global/scratch/fsa/fc_jevons/ligon/venv_validate/data_39131795/Uganda/var/food_acquired.parquet'
fa=pd.read_parquet(F)
print("index:", fa.index.names, "cols:", list(fa.columns), "rows:", f"{len(fa):,}")
u=pd.Series(fa.index.get_level_values('u'),index=fa.index).astype(str)
dash=(u=='---').to_numpy()
print("u=='---':", dash.sum(), " mask catches:", _unit_sentinel_mask(fa).sum(), " u=='Unknown':", (u=='Unknown').sum(), " u=='Value':", (u=='Value').sum())
q=pd.to_numeric(fa['Quantity'],errors='coerce'); e=pd.to_numeric(fa['Expenditure'],errors='coerce')
hq=(q.notna()&(q>0)).to_numpy(); he=(e.notna()&(e>0)).to_numpy()
print("--- '---' rows partition: qty>0:", (dash&hq).sum(), " exp-only:", (dash&~hq&he).sum(), " neither:", (dash&~hq&~he).sum())
d=fa[dash].copy(); d['hq']=hq[dash]
print("--- by source s:"); print(d.groupby([d.index.get_level_values('s'),'hq']).size().unstack(fill_value=0))
print("--- by wave t:"); print(d.groupby([d.index.get_level_values('t'),'hq']).size().unstack(fill_value=0))
print("--- top items among '---' rows:"); print(d.groupby([d.index.get_level_values('j'),'hq']).size().unstack(fill_value=0).assign(n=lambda x:x.sum(axis=1)).sort_values('n',ascending=False).head(12))
print("--- Quantity stats on '---' qty>0 rows:"); print(q[dash&hq].describe()[['count','50%','mean','max']])
# what does each relabelling do in the ladder?
def relabel(df, mask, label):
    fr=df.reset_index(); fr.loc[mask,'u']=label; return fr.set_index(df.index.names)
def rung_report(df, mask, tag):
    fq=food_quantities_from_acquired(df, units='kgs')
    src=fq.attrs.get('kg_factor_sources'); print(f"[{tag}] kg_factor_sources:", src)
    kgf=food_kg_factors(df)
    det=kgf if isinstance(kgf,pd.DataFrame) else None
    if isinstance(kgf,dict): print(f"[{tag}] kgf keys:", list(kgf.keys())[:8]); det=kgf.get('detail')
    if det is not None and 'KgFactorSource' in getattr(det,'columns',[]):
        sub=det[mask] if len(det)==len(df) else None
        if sub is not None: print(f"[{tag}] rung of the relabelled rows:\n", sub['KgFactorSource'].value_counts().to_string(), "\n  kg_per_unit median:", sub['kg_per_unit'].median())
    return fq
U_COUNT='Number of Units (General)'
print("\n=== A: as served today ('---')"); rung_report(fa, dash, "dash")
m2=dash.copy(); df_draft=relabel(fa, dash&hq, U_COUNT); df_draft=relabel(df_draft, dash&~hq, 'Value')
print("\n=== B: the draft (qty->'Number of Units (General)', exp-only->'Value', Quantity left NaN)"); fqB=rung_report(df_draft, dash, "draft")
df_conv=df_draft.copy(); qq=df_conv['Quantity'].to_numpy(copy=True); qq[dash&~hq]=e.to_numpy()[dash&~hq]; df_conv['Quantity']=qq
print("\n=== C: draft + corpus convention (Value rows get Quantity=Expenditure)"); fqC=rung_report(df_conv, dash, "draft+conv")
df_unk=relabel(fa, dash, 'Unknown')
print("\n=== D: first attempt (all '---' -> 'Unknown')"); fqD=rung_report(df_unk, dash, "unknown")
for tag,fq in (("B",fqB),("C",fqC),("D",fqD)):
    uu=fq.index.get_level_values('u')
    print(tag, "food_quantities rows:", f"{len(fq):,}", " rows u=Value:", (uu=='Value').sum(), " u=Unknown:", (uu=='Unknown').sum(), " u=NumUnits:", (uu==U_COUNT).sum(), " kg total:", f"{fq.loc[uu=='kg','Quantity'].sum():,.0f}")
