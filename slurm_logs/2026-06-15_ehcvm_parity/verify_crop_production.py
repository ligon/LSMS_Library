#!/usr/bin/env python
"""Verify EthiopiaRHS crop_production (#438) + area_output deprecation shim."""
import warnings
import pandas as pd
import lsms_library as ll
from lsms_library.diagnostics import is_this_feature_sane

c = ll.Country('EthiopiaRHS', preload_panel_ids=False)
df = c.crop_production()
print("=== crop_production ===")
print("shape:", df.shape, "| index:", df.index.names, "| columns:", list(df.columns))
print("waves:", sorted(df.index.get_level_values('t').unique().tolist()))
print("i-key:", f"{df.index.get_level_values('i').notna().mean():.3f}")
print("v in index:", 'v' in df.index.names,
      "| v-null:", f"{df.index.get_level_values('v').isna().mean():.3f}" if 'v' in df.index.names else "n/a")
print("dups:", int(df.index.duplicated().sum()))
print("--- per-wave counts ---"); print(df.groupby(level='t').size().to_string())
print("--- crop (j) distribution ---"); print(df.index.get_level_values('j').value_counts().to_string())
for col in ('Quantity', 'Area_ha'):
    s = pd.to_numeric(df[col], errors='coerce').dropna()
    print(f"  {col}: [{s.min():.2f}, {s.max():.2f}] n={len(s)} neg={int((s<0).sum())}")
print("  u values:", sorted(df['u'].dropna().unique().tolist()))

print("\n=== is_this_feature_sane ===")
print(is_this_feature_sane(df, 'EthiopiaRHS', 'crop_production'))

print("\n=== area_output retired ===")
print("'area_output' in data_scheme:", 'area_output' in c.data_scheme)
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    ao = c.area_output()
    deps = [x for x in w if issubclass(x.category, DeprecationWarning)]
    print("DeprecationWarning emitted:", len(deps) > 0)
print("legacy_area_output shape:", ao.shape, "| index:", ao.index.names)
print("legacy cols (sample):", list(ao.columns)[:6])

print("\n=== adversarial: re-widen round-trips one cell ===")
# crop_production Maize for a 1994a HH vs the legacy wide column
cp = df.reset_index()
row = cp[(cp['t']=='1994a') & (cp['j']=='Maize') & cp['Quantity'].notna()].iloc[0]
i_val = row['i']
legacy_val = ao.loc[('1994a', i_val), 'Maize_kg'] if ('1994a', i_val) in ao.index else 'MISSING'
print(f"i={i_val} Maize 1994a: crop_production.Quantity={row['Quantity']} | legacy Maize_kg={legacy_val}")
