"""Build crop_production for Niger ECVMA 2014-15 (GAP 1, item-level).

Two source files, as the instrument splits harvest from sale:
  - ECVMA2_AS2E1P2.dta : plot-crop harvest — qty + unit (AS02EQ07A /
    AS02EQ07B) plus the harvest start/end month (AS02EQ06A / AS02EQ06B).
    One row per reported (field, parcel, crop) line.
  - ECVMA2_AS2E2P2.dta : CROP level (no plot) — sold qty + unit
    (AS02EQ12A / AS02EQ12B -> the ``Unit_sold`` column, GH #824) and sale
    value (AS02EQ13).

i is built from (GRAPPE, MENAGE, EXTENSION) via niger.i -- the full
ECVMA2 household key, matching this wave's sample / roster idxvars
(GH #679; EXTENSION 2 is a split-off household).  As in 2021-22 the
crop-level sale is attributed only to (i, crop) pairs grown on a single
plot; multi-plot crops keep Quantity_sold / Value_sold NaN (no fabricated
split).  ``harvest_month`` = AS02EQ06A (month-of-harvest-start, 1-12);
``planting_month`` is not in this module (NaN).  The modules' unlabelled
"don't know" codes (9999 on the quantities, 9999999 on the sale value) are
mapped to NA, exact value only (GH #865).

plot = "{AS02EQ01}_{AS02EQ03}".  Index = (t, i, plot, crop, u).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _finish_crop_production, _na_exact)

# "Don't know" codes of these modules, unlabelled in the .dta (GH #865) --
# a DIFFERENT width from 2011-12's 999999.  Exact values only.  Raw counts:
# AS02EQ07A == 9999 on 23 rows of ECVMA2_AS2E1P2 (all with unit code 99
# 'Manquant'; Mil 7, Niebe 7, Sorgho 4, Voandzou 2, Oseille 1, Sesame 1,
# Autre 1); AS02EQ12A == 9999 on 7 rows and AS02EQ13 == 9999999 on 7 rows of
# ECVMA2_AS2E2P2.
QTY_SENTINELS = (9999,)
VALUE_SENTINELS = (9999999,)


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

# Attribute ONLY to (i, crop) pairs grown on a single plot (no fabricated
# split across plots) AND sold in a single unit (no fabricated conversion
# across units -- a second unit would duplicate the harvest row).
plots_per = df.dropna(subset=['i', 'crop']).groupby(['i', 'crop'])['plot_id'].nunique()
single = plots_per[plots_per == 1].index
units_per = sold_one.groupby(['i', 'crop']).size()
one_unit = units_per[units_per == 1].index
sold_one = sold_one[sold_one.set_index(['i', 'crop']).index.isin(single)
                    & sold_one.set_index(['i', 'crop']).index.isin(one_unit)]

df = df.merge(sold_one, on=['i', 'crop'], how='left')

df = _finish_crop_production(df, '2014-15')

assert len(df) > 0, 'crop_production 2014-15 produced no rows'
to_parquet(df, 'crop_production.parquet')
