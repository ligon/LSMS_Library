"""Build crop_production for Niger EHCVM 2021-22 (GAP 1, item-level).

The 2021-22 instrument SPLITS the crop module that 2018-19 kept together:
  - s16c_me_ner2021.dta : plot-crop level — harvest qty + unit (s16cq16a /
    s16cq16b) and the intercrop flag (s16cq07).  One row per reported
    (field, parcel, crop) harvest line.
  - s16d_me_ner2021.dta : CROP level (no plot) — sold qty + unit (s16dq05a /
    s16dq05b -> the ``Unit_sold`` column, GH #824) and sale value (s16dq06).
    Its crop-level lines are REPLICATED once per plot; exact duplicates are
    dropped before the sale is attributed (see the comment in the sale block).

Because the sale block has no plot dimension, its qty/value can only be
attributed to a plot-crop row when the household grows that crop on a
SINGLE plot (a 1:1 (i, crop) -> plot).  Where the crop spans several plots
the sale cannot be split without inventing an allocation, so Quantity_sold
/ Value_sold are left NaN for those rows (reported-only discipline — no
fabricated split).

Index = (t, i, plot, crop, u); plot = "{s16cq02}_{s16cq03}".
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _finish_crop_production)


crop_map, unit_map = _crop_maps()

# --- plot-crop harvest (s16c) -------------------------------------------
src = get_dataframe('../Data/s16c_me_ner2021.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16c_me_ner2021.dta', convert_categoricals=False)

hh = src.apply(lambda r: niger_i(pd.Series([r['grappe'], r['menage']],
                                           index=['grappe', 'menage'])), axis=1)
field = srcn['s16cq02'].apply(format_id)
parcel = srcn['s16cq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)
intercropped = src['s16cq07'].map({'Association de cultures': True, 'Pure': False})

df = pd.DataFrame({
    'i':            hh.values,
    'plot_id':         plot.values,
    'crop':         _crop_labels(srcn['s16cq04'], src['s16cq04'], crop_map).values,
    'u':            _unit_labels(src['s16cq16b'], unit_map).values,
    'Quantity':     src['s16cq16a'].values,
    'intercropped': intercropped.values,
})

# --- crop-level sale block (s16d), attributed only when crop is on one plot
sold = get_dataframe('../Data/s16d_me_ner2021.dta', convert_categoricals=True)
soldn = get_dataframe('../Data/s16d_me_ner2021.dta', convert_categoricals=False)

# THE s16d FILE REPEATS ITS CROP-LEVEL LINES (GH #824, measured 2026-09-24).
# s16d has no plot column, yet 12,232 rows for 8,691 (household, crop)
# pairs: 2,633 pairs carry 2-5 rows, and the multiplicity equals the number
# of PLOTS the crop is grown on in s16c for 89.6% of them -- the export
# replicated each crop-level record once per plot.  The repeated lines are
# byte-identical on all 47 columns (5,786 of the 6,174 multi-pair rows are
# exact duplicates, and EVERY one of the 1,320 multi-pair rows that carries
# a sale is).  Summing them, as this script did, served a single sale 2-4x:
# 144 single-plot crops, Value_sold 78,934,620 -> 73,206,995 FCFA on the
# single-plot pairs.  DECISION: a line identical to another on every column
# is the same record, not a second sale -- drop exact duplicates before any
# sum.  Distinct lines within a pair (388 rows, none with a sale) are kept.
_dup = soldn.duplicated(keep='first').values
sold = sold[~_dup]
soldn = soldn[~_dup]

sold_i = sold.apply(lambda r: niger_i(pd.Series([r['grappe'], r['menage']],
                                                index=['grappe', 'menage'])), axis=1)
sold_crop = _crop_labels(soldn['s16dq01'], sold['s16dq01'], crop_map)
sold_df = pd.DataFrame({
    'i':             sold_i.values,
    'crop':          sold_crop.values,
    'Quantity_sold': pd.to_numeric(sold['s16dq05a'], errors='coerce').values,
    'Value_sold':    pd.to_numeric(sold['s16dq06'], errors='coerce').values,
    # The SOLD unit (s16dq05b), whose 8-label set is spelled DIFFERENTLY from
    # the harvest unit s16cq16b ('tiya', 'sac de 100kg', 'panier', ...); the
    # `u` table carries both spellings -- GH #824.  Part of the KEY below.
    'Unit_sold':     _unit_labels(sold['s16dq05b'], unit_map).values,
})
# One sale record per (household, crop, SOLD UNIT) -- never a sum across
# units.  After the de-duplication above every sale-bearing pair is one
# row, so the sum only merges a pair's unsold placeholder lines.
sold_df = sold_df.dropna(subset=['i', 'crop'])
sold_one = (sold_df.groupby(['i', 'crop', 'Unit_sold'], as_index=False, dropna=False)
            [['Quantity_sold', 'Value_sold']].sum(min_count=1))

# Attribute ONLY to (i, crop) pairs grown on exactly one plot in the harvest
# block (no fabricated split across plots) AND sold in a single unit (no
# fabricated conversion across units -- a second unit would duplicate the
# harvest row).  Measured: no pair sells in two units.
plots_per = df.dropna(subset=['i', 'crop']).groupby(['i', 'crop'])['plot_id'].nunique()
single = plots_per[plots_per == 1].index
units_per = sold_one.groupby(['i', 'crop']).size()
one_unit = units_per[units_per == 1].index
sold_one = sold_one[sold_one.set_index(['i', 'crop']).index.isin(single)
                    & sold_one.set_index(['i', 'crop']).index.isin(one_unit)]

df = df.merge(sold_one, on=['i', 'crop'], how='left')

df = _finish_crop_production(df, '2021-22')

assert len(df) > 0, 'crop_production 2021-22 produced no rows'
to_parquet(df, 'crop_production.parquet')
