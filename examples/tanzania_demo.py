#!/usr/bin/env python3
"""
Quick tour of lsms_library using Tanzania NPS data.

Run from the repo root (or anywhere with the package installed):

    python examples/tanzania_demo.py

Prerequisites
-------------
- ``pip install lsms_library`` (or ``poetry install`` from the repo root)
- DVC credentials set up (``import lsms_library as ll; ll.authenticate()``)
  OR existing cached parquets under ``~/.local/share/lsms_library/``
"""

import lsms_library as ll

# ── 1. What countries are available? ─────────────────────────────────

# Feature.countries lists every country that declares a given table.
# Using household_roster as the most universal table:
roster = ll.Feature('household_roster')
print("Countries with household_roster data:")
for c in roster.countries:
    print(f"  {c}")

# ── 2. Pick a country and explore its structure ──────────────────────

tz = ll.Country('Tanzania')

print(f"\nCountry : {tz.name}")
print(f"Waves   : {tz.waves}")
print(f"Tables  : {tz.data_scheme}")

# ── 3. Cluster features: where are the households? ──────────────────

# cluster_features gives the geographic context for each household:
# Region, District, Rural/Urban, and (when available) GPS coordinates.
cf = tz.cluster_features()

print("\n── cluster_features ──")
print(f"Shape : {cf.shape}")
print(f"Index : {cf.index.names}")
print(f"Columns: {cf.columns.tolist()}")
print(cf.groupby('t')['Region'].nunique().rename('Regions per wave'))

# ── 4. Household characteristics (derived from household_roster) ─────

# household_characteristics is auto-derived from household_roster
# at API time---no separate data files needed.  Passing market='Region'
# joins the Region from cluster_features as an 'm' index level,
# which is the market dimension used in demand estimation.

hc = tz.household_characteristics(market='Region')

print("\n── household_characteristics (market='Region') ──")
print(f"Shape : {hc.shape}")
print(f"Index : {hc.index.names}")
print(f"Columns: {hc.columns.tolist()[:10]}{'...' if len(hc.columns) > 10 else ''}")

# Count households per wave and market
print("\nHouseholds per (t, m):")
print(hc.groupby(['t', 'm']).size().unstack(fill_value=0))

# ── 5. Food expenditures (derived from food_acquired) ───────────────

# food_expenditures is derived from food_acquired via
# transformations.food_expenditures_from_acquired().  Again we pass
# market='Region' so that the returned DataFrame has the index
# structure (i, t, m, j) expected by cfe.Regression.

x = tz.food_expenditures(market='Region')

print("\n── food_expenditures (market='Region') ──")
print(f"Shape : {x.shape}")
print(f"Index : {x.index.names}")
print(f"Columns: {x.columns.tolist()}")
print(f"\nDistinct food items (j): {x.index.get_level_values('j').nunique()}")
print(f"Waves: {sorted(x.index.get_level_values('t').unique())}")
print(f"\nMean expenditure by wave:")
print(x.groupby('t')['Expenditure'].mean().round(1))

# ── 6. Food quantities ──────────────────────────────────────────────

# food_quantities converts raw quantities into kilograms using
# country-specific unit conversion factors.  Same market= interface.

q = tz.food_quantities(market='Region')

print("\n── food_quantities (market='Region') ──")
print(f"Shape : {q.shape}")
print(f"Index : {q.index.names}")
print(f"Columns: {q.columns.tolist()}")
print(f"\nMean quantity (kg) by wave:")
print(q.groupby('t')['Quantity'].mean().round(2))

# ── 7. Peek at the data ─────────────────────────────────────────────

print("\n── Sample rows: food_expenditures ──")
print(x.head(8).to_string())
