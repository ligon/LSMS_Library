#!/usr/bin/env python
"""GhanaLSS 1998-99 food_acquired -> canonical long form.

Emits one row per (t, i, j, u, s, visit) stacking the two acquisition
modules into the `s` (source) index level:

- s='purchased'  : Section 9B (SEC9B.DTA).  GLSS4 records purchase VALUE
  only (s9bq1..s9bq6, visits 1-6) -- no physical quantity and no unit.
  Per the food-acquired LCU-only convention we emit u='Value' with
  Expenditure=value and Quantity=Expenditure (we do NOT fabricate a
  physical quantity; price-based imputation is a downstream Phase-3
  concern, out of scope here).
- s='produced'   : Section 8H (SEC8H.DTA).  Records produced QUANTITY
  (s8hq3..s8hq8 = the same 2nd..7th visits) in a native unit (s8hq9) plus a unit price
  (s8hq10).  We emit Quantity, the native u, and Price; Expenditure is
  left NaN (no produced value is recorded).

The household id `i` is built with the SAME construction sample() and
household_roster() use (the wave mapping.py `i()` helper:
format_id(clust)+format_id(nh, zeropadding=2), NO separator) so the keys
match and the framework's _join_v_from_sample() finds them.  `v` is NOT
emitted -- the framework joins it at API time.
"""
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile, format_id
from lsms_library.paths import countries_root
import numpy as np
import pandas as pd

t = '1998-99'

# ---------------------------------------------------------------------------
# Categorical mappings (food-code harmonization + production unit decode)
# ---------------------------------------------------------------------------
labels = df_from_orgfile('./categorical_mapping.org', name='harmonize_food',
                         encoding='ISO-8859-1')
food_9b = labels[['Preferred Label', 'Code_9b']].dropna(subset=['Code_9b'])
food_9b = food_9b.set_index('Code_9b')['Preferred Label'].to_dict()
food_8h = labels[['Preferred Label', 'Code_8h']].dropna(subset=['Code_8h'])
food_8h = food_8h.set_index('Code_8h')['Preferred Label'].to_dict()

units = df_from_orgfile('./categorical_mapping.org', name='s8hq9',
                        encoding='ISO-8859-1')
unitsd = units.set_index('Code')['Label'].to_dict()


def _hhid(df):
    """Canonical household id: format_id(clust)+format_id(nh, zeropadding=2).

    Identical construction to the wave mapping.py `i()` helper used by
    sample() / household_roster(), e.g. clust=4002, nh=1 -> '400201'.
    """
    return df.apply(
        lambda r: (format_id(r['clust'], zeropadding=0) or '')
        + (format_id(r['nh'], zeropadding=2) or ''),
        axis=1)


# ---------------------------------------------------------------------------
# PURCHASES (SEC9B): value-only, visits 1-6 -> s='purchased'
# ---------------------------------------------------------------------------
pur = get_dataframe('../Data/SEC9B.DTA', convert_categoricals=True)
pur['i'] = _hhid(pur)
pur['j'] = pur['fdexpcd'].replace(food_9b)
# Keep only rows that decoded to a harmonized food label (drop the
# non-food expenditure codes recorded in section 9B, and any unmapped
# codes), so no '' / NA `j` leaks into the canonical output.
pur = pur[pur['j'].isin(set(food_9b.values()))]

# Source question numbers are NOT visit numbers.  This wave's own Stata
# variable labels give the mapping:
#   s9bq1 '2nd visit amount spent' .. s9bq6 '7th visit amount spent'
visit_cols_pur = {f's9bq{v}': v + 1 for v in range(1, 7)}  # -> visits 2-7
pur = pur.rename(columns=visit_cols_pur)
pur_long = pur.melt(id_vars=['i', 'j'], value_vars=list(visit_cols_pur.values()),
                    var_name='visit', value_name='Expenditure')
pur_long['Expenditure'] = pur_long['Expenditure'].replace({'': np.nan, 0: np.nan})
pur_long = pur_long.dropna(subset=['Expenditure'])
# Sum any duplicate (i, j, visit) records (a HH can have several lines
# per food code per visit).
pur_long = pur_long.groupby(['i', 'j', 'visit'], as_index=False)['Expenditure'].sum()
pur_long['u'] = 'Value'
pur_long['s'] = 'purchased'
# LCU-only convention: carry the value as Quantity too (no physical qty).
pur_long['Quantity'] = pur_long['Expenditure']
pur_long['Price'] = np.nan

# ---------------------------------------------------------------------------
# PRODUCTION (SEC8H): quantity + native unit + price, visits 3-8 -> 'produced'
# ---------------------------------------------------------------------------
prod = get_dataframe('../Data/SEC8H.DTA', convert_categoricals=True)
prod = prod[prod['s8hq1'] == 1]  # only HHs that consumed own produce in past 12mo
prod['i'] = _hhid(prod)
prod['j'] = prod['homagrcd'].replace(food_8h)
prod = prod[prod['j'].isin(set(food_8h.values()))]
prod['u'] = prod['s8hq9'].replace(unitsd)
prod['Price'] = prod['s8hq10'].replace({'': np.nan, 0: np.nan})

#   s8hq3 '2nd visit units consumed' .. s8hq8 '7th visit units consumed'
#   (s8hq1/s8hq2 are screeners: 'HH consume any home produce', 'No. of months')
visit_cols_pro = {f's8hq{v}': v - 1 for v in range(3, 9)}  # -> visits 2-7
prod = prod.rename(columns=visit_cols_pro)
prod_long = prod.melt(id_vars=['i', 'j', 'u', 'Price'],
                      value_vars=list(visit_cols_pro.values()),
                      var_name='visit', value_name='Quantity')
prod_long['Quantity'] = prod_long['Quantity'].replace({'': np.nan, 0: np.nan})
prod_long = prod_long.dropna(subset=['Quantity'])
# Sum duplicate (i, j, u, visit) quantity records; carry the price (first).
prod_long = prod_long.groupby(['i', 'j', 'u', 'visit'], as_index=False).agg(
    Quantity=('Quantity', 'sum'), Price=('Price', 'first'))
prod_long['s'] = 'produced'
prod_long['Expenditure'] = np.nan

# ---------------------------------------------------------------------------
# Stack the two modules into rows under the `s` level, build canonical index.
# ---------------------------------------------------------------------------
cols = ['t', 'i', 'j', 'u', 's', 'visit', 'Quantity', 'Expenditure', 'Price']
pur_long['t'] = t
prod_long['t'] = t
fa = pd.concat([pur_long[cols], prod_long[cols]], ignore_index=True)

# `u` decodes to strings.  `visit` stays an INTEGER: it is a count, and until
# 2026-09-09 this script finished the cast with `.astype(str)`, which made this
# the one multi-visit wave whose `visit` level arrived as '2'..'7' while every
# other wave's arrived as 2..7.  Measured on the mixed level that produced:
# `food_acquired().xs(2, level='visit')` returned 752,970 rows and silently
# omitted this wave's 104,351 (a 12.2% undercount of visit-2 rows), and a left
# merge on an Int64 `visit` key left all 542,105 of this wave's rows unmatched
# with no error.  The `.astype(int)` is kept -- the melt can leave the column
# object-typed -- and the trailing `.astype(str)` is the bug.
fa['u'] = fa['u'].astype(str)
fa['visit'] = fa['visit'].astype(int)

fa = fa.set_index(['t', 'i', 'j', 'u', 's', 'visit']).sort_index()

# --- canonical `u`: the country's `_/unit_labels.org` Preferred-Label axis ----
# RESTORED 2026-09-08.  The country-level `_/food_acquired.py` applied
# `df1['u'].replace(ulabelsd['u']['Preferred Label'])` until c345d317; GH #109
# Phase 2 (6de0ce37) rewrote that script without it and only 2016-17's wave
# script re-implemented it, so this wave has shipped the raw survey spellings
# ('american tin', 'bowl', 'litre', 'Maxi bag') as `u` ever since -- off the
# axis `community_prices` and every other consumer of `u` share.
#
# Two disciplines this country's CONTENTS.org requires:
#   * resolve the table through countries_root(), never a package-relative
#     path, so LSMS_COUNTRIES_ROOT is honoured (Trap 6 / GH #753);
#   * ASSERT the index stays unique.  `food_acquired` is in
#     `_ADDITIVE_MEASURE_COLUMNS`, so a duplicate makes core SUM
#     Quantity/Expenditure and re-derive `Price = Expenditure/Quantity` on the
#     WHOLE frame -- destroying every recorded farmgate price (Trap 9).
#     Measured before this landed: 0 rows in duplicate groups on the delivered
#     5,259,320-row table under this exact map.
_ul = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'unit_labels.org',
                      name='unit_label').dropna()
_umap = dict(zip(_ul['u'].astype(str).str.strip(),
                 _ul['Preferred Label'].astype(str).str.strip()))
assert _umap, 'unit_labels.org: unit_label decoded to an EMPTY dict'
fa = fa.rename(index=lambda x: _umap.get(x, x), level='u')
assert not fa.index.duplicated().any(), (
    'unit canonicalisation collided on (t, i, j, u, s, visit) -- two raw unit '
    'spellings share a Preferred Label for one (household, item, visit); '
    'resolve it here, not in core (CONTENTS.org Trap 9)')

to_parquet(fa, 'food_acquired.parquet')
