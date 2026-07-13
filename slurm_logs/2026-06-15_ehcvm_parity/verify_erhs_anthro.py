#!/usr/bin/env python
"""Verify EthiopiaRHS anthropometry (#438 Tier-2 pilot)."""
import lsms_library as ll
from lsms_library.diagnostics import is_this_feature_sane

c = ll.Country('EthiopiaRHS', preload_panel_ids=False)
df = c.anthropometry()
print("=== anthropometry ===")
print("shape:", df.shape)
print("index names:", df.index.names)
print("columns:", list(df.columns))
print("waves (t):", sorted(df.index.get_level_values('t').unique().tolist()))
if 'i' in df.index.names:
    print("i-key:", f"{df.index.get_level_values('i').notna().mean():.3f}")
if 'v' in df.index.names:
    print("v present in index: YES  v-null frac:",
          f"{df.index.get_level_values('v').isna().mean():.3f}")
else:
    print("v present in index: NO")
print("--- per-wave counts ---")
print(df.groupby(level='t').size().to_string())
print("--- measure ranges ---")
for col in ('Height', 'Weight', 'Age_months'):
    if col in df.columns:
        s = df[col].dropna()
        print(f"  {col}: [{s.min():.1f}, {s.max():.1f}] n={len(s)} median={s.median():.1f}")
print("--- Sex distribution ---")
if 'Sex' in df.columns:
    print(df['Sex'].value_counts(dropna=False).to_string())
print("--- duplicate index tuples:", df.index.duplicated().sum())

print("\n=== is_this_feature_sane ===")
rep = is_this_feature_sane(df, 'EthiopiaRHS', 'anthropometry')
print(rep)

print("\n=== roster (i,pid) join coverage ===")
ros = c.household_roster()
ant_ip = set(map(tuple, df.reset_index()[['i', 'pid']].drop_duplicates().values.tolist()))
ros_ip = set(map(tuple, ros.reset_index()[['i', 'pid']].drop_duplicates().values.tolist()))
inter = ant_ip & ros_ip
print(f"anthro (i,pid): {len(ant_ip)}  roster (i,pid): {len(ros_ip)}  "
      f"overlap: {len(inter)} = {len(inter)/max(1,len(ant_ip)):.3f} of anthro")
