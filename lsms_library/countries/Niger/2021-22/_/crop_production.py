"""Build crop_production for Niger EHCVM 2021-22 (GAP 1, item-level).

The 2021-22 instrument SPLITS the crop module that 2018-19 kept together:
  - s16c_me_ner2021.dta : plot-crop level — harvest qty + unit (s16cq16a /
    s16cq16b), the survey's estimate of the line in kg (s16cq16c -> the
    ``Quantity_kg`` column, GH #1436, keyed survey-kg-estimate) and the intercrop
    flag (s16cq07).  One row per reported (field, parcel, crop) harvest line.
  - s16d_me_ner2021.dta : CROP level (no plot) — sold qty + unit (s16dq05a /
    s16dq05b -> the ``Unit_sold`` column, GH #824), sold state (s16dq05d -> the
    ``Condition_sold`` column, GH #1462) and sale value (s16dq06).
    Its crop-level lines are REPLICATED once per plot; exact duplicates are
    dropped before the sale is attributed (see the comment in the sale block).

Because the sale block has no plot dimension, its qty/value can only be
attributed to a plot-crop row when the household reports that crop on a
SINGLE harvest line (a 1:1 (i, crop) -> served row), which then carries the
key Niger::crop_production::hh-crop-sale-single-row.  Where the crop spans
several plots, or several harvest lines of one plot, the sale cannot be split
without inventing an allocation, so it goes on ONE row with plot_id = 'Farm-level',
no harvest Quantity and the sentinels of a row with no harvest on the index (GH
#1457: 553 of the 1,818 sales, 36,330,475 of 104,959,545 FCFA, of which 144 sales,
4,585,225 FCFA, are two lines of one crop on ONE plot).  Until
GH #1360 the rule asked for a single PLOT; the extra lines of a single plot
were then either collapsed away by the framework or served carrying the same
sale twice.

Index = (t, i, plot_id, crop, u, condition, production_obs); plot =
"{s16cq02}_{s16cq03}".  ``condition`` is the harvest's own "Etat du produit"
(s16cq16d, GH #1436).  s16c records no harvest-line number and one (plot, crop,
unit) can carry several lines (846 key groups, 1,856 rows), and the state
separates only a minority of them (1,856 -> 1,817), so ``production_obs`` is the
CONSTRUCTED ``row:<source_fingerprint>:<n>`` -- the zero-based row of the held
s16c release, taken before any filter (GH #1360).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _crop_conditions, _crop_sold_conditions,
                   _finish_crop_production, _crop_row_observations,
                   derive_survey_kg_estimate, stamp_survey_kg_estimate,
                   derive_hh_crop_sale_attachment, stamp_hh_crop_sale)


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
    # Zero-based row of the held release, before any filter (GH #1360).
    'production_obs': _crop_row_observations(
        len(srcn), '../Data/s16c_me_ner2021.dta', script_path=__file__).values,
    'crop':         _crop_labels(srcn['s16cq04'], src['s16cq04'], crop_map).values,
    'u':            _unit_labels(src['s16cq16b'], unit_map).values,
    # By code: the 2018-19 code list is different, see categorical_mapping.org.
    'condition':    _crop_conditions(srcn['s16cq16d'],
                                     'harmonize_crop_condition_ehcvm2').values,
    'Quantity':     src['s16cq16a'].values,
    # A total for the line, not a factor per unit (GH #1436).
    'Quantity_kg':  derive_survey_kg_estimate(srcn['s16cq16c']).values,
    'intercropped': intercropped.values,
})

# --- crop-level sale block (s16d), attributed only when crop is on one plot
sold = get_dataframe('../Data/s16d_me_ner2021.dta', convert_categoricals=True)
soldn = get_dataframe('../Data/s16d_me_ner2021.dta', convert_categoricals=False)

# THE s16d FILE REPEATS ITS CROP-LEVEL LINES (GH #824, measured 2026-09-24).
# s16d has no plot column, yet 12,232 rows for 8,691 (household, crop)
# pairs: 2,633 pairs carry 2-8 rows, and joined on (grappe, menage, crop)
# the multiplicity equals the number of s16c HARVEST LINES for 100% of the
# 8,691 pairs (re-measured 2026-10-14; it equals the number of distinct
# (field, parcel) plots for only 65.7% of the multi-row pairs) -- the
# export replicated each crop-level record once per harvest line, so the
# repeats are a join artefact.  The repeated lines are
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
    # The SOLD state (s16dq05d).  Its labels are not the harvest's
    # ('epis/coques/panicules' against 'epis/coques,'), its codes are (GH #1462).
    'Condition_sold': _crop_sold_conditions(soldn['s16dq05d'],
                                            'harmonize_crop_condition_ehcvm2').values,
})
# One sale record per (household, crop, SOLD UNIT, SOLD STATE) -- never a sum
# across units or states.  After the de-duplication above every sale-bearing
# pair is one row, so the sum only merges a pair's unsold placeholder lines.
sold_df = sold_df.dropna(subset=['i', 'crop'])
sold_one = (sold_df.groupby(['i', 'crop', 'Unit_sold', 'Condition_sold'],
                            as_index=False, dropna=False)
            [['Quantity_sold', 'Value_sold']].sum(min_count=1))
records = sold_one

# Attribute ONLY to (i, crop) pairs with exactly one harvest LINE in the
# harvest block -- one plot, one line, so the sale lands on one served row
# (no fabricated split across plots or lines; GH #1360) -- AND sold in a
# single unit and state (no fabricated conversion -- a second one would
# duplicate the harvest row).  Measured: no pair sells in two units or states.
lines_per = df.dropna(subset=['i', 'crop']).groupby(['i', 'crop']).size()
single = lines_per[lines_per == 1].index
units_per = sold_one.groupby(['i', 'crop']).size()
one_unit = units_per[units_per == 1].index
sold_one = sold_one[sold_one.set_index(['i', 'crop']).index.isin(single)
                    & sold_one.set_index(['i', 'crop']).index.isin(one_unit)]

df = df.merge(sold_one, on=['i', 'crop'], how='left')

# Any other sale: ONE Farm-level row (GH #1457).  After the s16d de-duplication
# above, so a line repeated once per harvest line is one sale here.
df = derive_hh_crop_sale_attachment(df, records)

df = _finish_crop_production(df, '2021-22')
# After the tail, so the stamps ride on served rows (CLAUDE.md "Derived Values").
df['Derivation'] = stamp_survey_kg_estimate(df)
df['Derivation'] = stamp_hh_crop_sale(df)

assert df.index.is_unique, 'crop_production 2021-22 repeats a native harvest line'
# Conservation: the records' sums, on one row each.
assert df['Value_sold'].sum() == records['Value_sold'].sum(), 'a sale value moved'
assert df['Quantity_sold'].sum() == records['Quantity_sold'].sum(), 'a sold quantity moved'
assert len(df) > 0, 'crop_production 2021-22 produced no rows'
to_parquet(df, 'crop_production.parquet')
