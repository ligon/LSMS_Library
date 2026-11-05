"""Build crop_production for Niger ECVMA 2014-15 (GAP 1, item-level).

Two source files, as the instrument splits harvest from sale:
  - ECVMA2_AS2E1P2.dta : plot-crop harvest — qty + unit (AS02EQ07A /
    AS02EQ07B), the survey's own kilograms for the line (AS02EQ07C -> the
    ``Quantity_kg`` column, GH #1436) plus the harvest start/end month
    (AS02EQ06A / AS02EQ06B).  One row per reported (field, parcel, crop) line.
  - ECVMA2_AS2E2P2.dta : CROP level (no plot) — sold qty + unit
    (AS02EQ12A / AS02EQ12B -> the ``Unit_sold`` column, GH #824) and sale
    value (AS02EQ13).

i is built from (GRAPPE, MENAGE, EXTENSION) via niger.i -- the full
ECVMA2 household key, matching this wave's sample / roster idxvars
(GH #679; EXTENSION 2 is a split-off household).  As in 2021-22 a crop-level
sale (a positive AS02EQ12A or AS02EQ13; 1,081 records) goes on the crop's single
SERVED harvest line (583), keyed Niger::crop_production::hh-crop-sale-single-row, and
otherwise on ONE row with plot_id = 'Farm-level' (498: 479 on two or more served lines,
19 with no harvest row), its index the sentinels of a row with no harvest (GH
#1457).  A line with no quantity or month is removed at the API, so it does not count
unless it is the crop's only line.  No split, no sum, nothing dropped.  ``harvest_month`` = AS02EQ06A (month-of-harvest-start, 1-12);
``planting_month`` is not in this module (NaN).  The modules' unlabelled
"don't know" codes (9999 on the quantities, 9999999 on the sale value) are
mapped to NA, exact value only (GH #865).

plot = "{AS02EQ01}_{AS02EQ03}".  Index = (t, i, plot_id, crop, u, condition,
production_obs); the index is unique without a line coordinate, so
``production_obs`` is the ``unresolved_observation`` sentinel (GH #1360).  The
module asks no product-state question (2E.7a/7b/7c/7d are quantity, unit,
kilograms, harvest finished), so ``condition`` is the ``unknown_condition``
sentinel (GH #1436).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _finish_crop_production, _na_exact,
                   derive_hh_crop_sale_attachment, served_harvest_lines, stamp_hh_crop_sale)

# "Don't know" codes of these modules, unlabelled in the .dta (GH #865) --
# a DIFFERENT width from 2011-12's 999999.  Exact values only.  Raw counts:
# AS02EQ07A == 9999 on 23 rows of ECVMA2_AS2E1P2 (all with unit code 99
# 'Manquant'; Mil 7, Niebe 7, Sorgho 4, Voandzou 2, Oseille 1, Sesame 1,
# Autre 1); AS02EQ12A == 9999 on 7 rows and AS02EQ13 == 9999999 on 7 rows of
# ECVMA2_AS2E2P2.
QTY_SENTINELS = (9999,)
VALUE_SENTINELS = (9999999,)
# The kilogram column's own codes (GH #1436).  AS02EQ07C == 999999 on the 23 rows
# with AS02EQ07A == 9999.  999998 is one row, and is TREATED AS a code: the questionnaire
# documents only 9999 and 99999.  The next largest total here is 24,000; the crop-level
# sibling AS02EQ110E holds 9998 once beside 9999 ten times; this wave's files use a
# "...98" family (AS01Q20 holds 99999998 on 1,275 rows, 9999998 on 620, 999998 on 70);
# and the line's own sale would be 0.42 FCFA per kg.  One value to remove to reverse it.
# See CONTENTS.org, "the survey harvest kilograms are served".
KG_SENTINELS = (999999, 999998)


crop_map, unit_map = _crop_maps()

base = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/'
src = get_dataframe(base + 'ECVMA2_AS2E1P2.dta', convert_categoricals=True)
srcn = get_dataframe(base + 'ECVMA2_AS2E1P2.dta', convert_categoricals=False)

hh = src.apply(lambda r: niger_i(pd.Series([r['GRAPPE'], r['MENAGE'], r['EXTENSION']],
                                           index=['GRAPPE', 'MENAGE', 'EXTENSION'])), axis=1)
field = srcn['AS02EQ01'].apply(format_id)
parcel = srcn['AS02EQ03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

# harvest start month: integer 1-12; 0 / >12 -> NaN
hmonth = pd.to_numeric(srcn['AS02EQ06A'], errors='coerce')
hmonth = hmonth.where((hmonth >= 1) & (hmonth <= 12))

df = pd.DataFrame({
    'i':             hh.values,
    'plot_id':          plot.values,
    'crop':          _crop_labels(srcn['CULTURE'], src['CULTURE'], crop_map).values,
    'u':             _unit_labels(src['AS02EQ07B'], unit_map).values,
    'Quantity':      _na_exact(srcn['AS02EQ07A'], QTY_SENTINELS).values,
    # A total for the line, not a factor per unit (GH #1436).
    'Quantity_kg':   _na_exact(srcn['AS02EQ07C'], KG_SENTINELS).values,
    'harvest_month': hmonth.values,
})

# --- crop-level sale block (AS2E2), single-plot attribution --------------
sold = get_dataframe(base + 'ECVMA2_AS2E2P2.dta', convert_categoricals=True)
soldn = get_dataframe(base + 'ECVMA2_AS2E2P2.dta', convert_categoricals=False)
sold_i = sold.apply(lambda r: niger_i(pd.Series([r['GRAPPE'], r['MENAGE'], r['EXTENSION']],
                                                index=['GRAPPE', 'MENAGE', 'EXTENSION'])), axis=1)
sold_crop = _crop_labels(soldn['AS02EQ110B'], sold['AS02EQ110B'], crop_map)
sold_df = pd.DataFrame({
    'i':             sold_i.values,
    'crop':          sold_crop.values,
    'Quantity_sold': _na_exact(sold['AS02EQ12A'], QTY_SENTINELS).values,
    'Value_sold':    _na_exact(sold['AS02EQ13'], VALUE_SENTINELS).values,
    # The SOLD unit (AS02EQ12B; same 12-code label set as the harvest unit
    # AS02EQ07B, same `u` table) -- GH #824.  Part of the KEY below, so a
    # sale is never summed across units.
    'Unit_sold':     _unit_labels(sold['AS02EQ12B'], unit_map).values,
}).dropna(subset=['i', 'crop'])
# One sale record per (household, crop, SOLD UNIT).  Measured on
# ECVMA2_AS2E2P2 (5,226 rows): every (household, crop) is one row, so the
# sum is a no-op here; the key still names the grain the sum is taken over.
sold_one = (sold_df.groupby(['i', 'crop', 'Unit_sold'], as_index=False, dropna=False)
            [['Quantity_sold', 'Value_sold']].sum(min_count=1))
records = sold_one

# Attribute ONLY to (i, crop) pairs with a single harvest LINE -- one plot,
# one line, so the sale lands on exactly one served row (no fabricated split
# across plots or lines; GH #1360) -- AND sold in a single unit (no
# fabricated conversion across units -- a second unit would duplicate the
# harvest row).
serving = served_harvest_lines(df)     # a line the API would remove does not count
lines_per = df[serving].dropna(subset=['i', 'crop']).groupby(['i', 'crop']).size()
single = lines_per[lines_per == 1].index
units_per = sold_one.groupby(['i', 'crop']).size()
one_unit = units_per[units_per == 1].index
sold_one = sold_one[sold_one.set_index(['i', 'crop']).index.isin(single)
                    & sold_one.set_index(['i', 'crop']).index.isin(one_unit)]

df = df.merge(sold_one, on=['i', 'crop'], how='left')
sale_columns = [c for c in sold_one.columns if c not in ('i', 'crop')]
df.loc[~serving, sale_columns] = pd.NA

# Any other sale: ONE Farm-level row (GH #1457).
df = derive_hh_crop_sale_attachment(df, records)

df = _finish_crop_production(df, '2014-15')
df['Derivation'] = stamp_hh_crop_sale(df)

assert df.index.is_unique, 'crop_production 2014-15 repeats a native harvest line'
# Conservation: the records' sums, on one row each.
assert df['Value_sold'].sum() == records['Value_sold'].sum(), 'a sale value moved'
assert df['Quantity_sold'].sum() == records['Quantity_sold'].sum(), 'a sold quantity moved'
assert len(df) > 0, 'crop_production 2014-15 produced no rows'
to_parquet(df, 'crop_production.parquet')
