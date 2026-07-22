#!/usr/bin/env python
"""Verify EthiopiaRHS community_prices (#438 / #275)."""
import pandas as pd
import lsms_library as ll
from lsms_library.diagnostics import is_this_feature_sane

c = ll.Country('EthiopiaRHS', preload_panel_ids=False)
df = c.community_prices()
print("=== community_prices ===")
print("shape:", df.shape, "| index:", df.index.names, "| columns:", list(df.columns))
print("waves:", sorted(df.index.get_level_values('t').unique().tolist()))
print("dups:", int(df.index.duplicated().sum()))
print("--- per-wave counts ---"); print(df.groupby(level='t').size().to_string())
jl = 'j' if 'j' in df.index.names else ('i' if 'i' in df.index.names else None)
if jl: print("--- items (j) ---"); print(df.index.get_level_values(jl).value_counts().head(12).to_string())
s = pd.to_numeric(df['Price'], errors='coerce').dropna()
print(f"Price: [{s.min():.2f}, {s.max():.2f}] median={s.median():.2f} n={len(s)} neg={int((s<0).sum())}")
print("u values:", sorted(df.index.get_level_values('u').unique().tolist()) if 'u' in df.index.names else df.get('u'))

print("\n=== v matches cluster_features v? ===")
cf = c.cluster_features().reset_index()
cp_v = set(df.index.get_level_values('v').astype(str))
cf_v = set(cf['v'].astype(str))
print(f"community_prices v: {len(cp_v)} | cluster_features v: {len(cf_v)} | overlap: {len(cp_v & cf_v)} = {len(cp_v & cf_v)/max(1,len(cp_v)):.3f}")

print("\n=== is_this_feature_sane ===")
print(is_this_feature_sane(df, 'EthiopiaRHS', 'community_prices'))

print("\n=== composes with crop_production / food on j? ===")
cp_j = set(df.index.get_level_values(jl).astype(str)) if jl else set()
crop_j = set(c.crop_production().index.get_level_values('j').astype(str))
print("shared j with crop_production:", sorted(cp_j & crop_j))
