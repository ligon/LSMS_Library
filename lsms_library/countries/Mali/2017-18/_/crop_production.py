"""Build crop_production (item-level harvest) for Mali EACI 2017-18.

Sources (post-harvest passage 2; cultivation passage 1):
  - eaci17_s7fp2.dta   seasonal-crop harvest roster (one row per plot-crop)
  - eaci17_s7gp2.dta   crop-sales roster, keyed (grappe, exploitation, crop)
                       — HOUSEHOLD-crop grain, NOT plot-crop
  - eaci17_s11fp1.dta  perennial-tree roster (one row per HH-tree; no field grid)
  - eaci17_s11cp1.dta  cultivation roster -> planting month

Grain: (t, i, plot, crop).  plot = "{field}_{parcel}" (s7fq01_s7fq02) for
seasonal crops; <NA> for perennial trees.  i = (grappe, exploitation) —
the 2017-18 household key (cf. sample/cluster_features data_info.yml).

Reported, item-level columns only.  IMPORTANT: the 2017-18 sales module
(s7g) records sold quantity / value at the (household, crop) grain, NOT
plot-crop.  To avoid fabricating a plot allocation, Quantity_sold /
Value_sold are joined onto the plot-crop row ONLY when the (i, crop) maps
to a single plot (68% of crop-hh combos); where a crop spans >1 plot
(32%), sold stays NaN at the plot grain — the survey simply did not record
sales at plot granularity.  (The household-crop sold totals remain
recoverable from the raw module; a transformations.py rollup can sum
harvest to (i, crop) and join the full sold series there.)

Variable map traced from MLI_EACI2.do (WB harmonised crop section):
  field=s7fq01 parcel=s7fq02 crop=s7fq03 harvest_qty=s7fq13a unit=s7fq13c
  fully_harvested=s7fq06 harvest_month=s7fq12b
  sold: crop=s7gq01 sold_y/n=s7gq20 sold_qty=s7gq21a sold_value=s7gq22
  perennial: crop=s11fq01 still_prod=s11fq03 qty=s11fq10 unit=s11fq11c-?
             sold_qty=s11fq14a sold_value=s11fq15
  cultivation: field=s11cq01 parcel=s11cq02 crop=s11cq03 plant_month=s11cq14b
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, crop_production_finalize

WAVE = '2017-18'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])),
                    axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


# --- seasonal harvest (s7f) ---
s7f = get_dataframe('../Data/eaci17_s7fp2.dta').copy()
s7f['i'] = _hhid(s7f)
seasonal = pd.DataFrame({
    't': WAVE,
    'i': s7f['i'],
    'plot': _plot(s7f, 's7fq01', 's7fq02'),
    'crop': s7f['s7fq03'],
    'u': s7f['s7fq13c'],
    'Quantity': s7f['s7fq13a'],
    'Quantity_sold': pd.NA,
    'Value_sold': pd.NA,
    'harvest_month': s7f['s7fq12b'],
    'planting_month': pd.NA,
    'intercropped': pd.NA,   # 2017-18 s11cq05 has no documented monoculture/
                             # intercrop binary; left NaN (honest missing).
    'perennial': False,
})

# --- sold (s7g): household-crop grain; attach only to single-plot crops ---
s7g = get_dataframe('../Data/eaci17_s7gp2.dta').copy()
s7g['i'] = _hhid(s7g)
sold = pd.DataFrame({
    'i': s7g['i'],
    'crop': s7g['s7gq01'],
    'Quantity_sold': pd.to_numeric(s7g['s7gq21a'], errors='coerce'),
    'Value_sold': pd.to_numeric(s7g['s7gq22'], errors='coerce'),
})
not_sold = s7g['s7gq20'].astype('string').str.strip().eq('Non')
sold.loc[not_sold, ['Quantity_sold', 'Value_sold']] = 0
sold = sold.groupby(['i', 'crop'], as_index=False).agg(
    {'Quantity_sold': 'sum', 'Value_sold': 'sum'})

# (i, crop) -> number of distinct plots in the harvest roster
nplots = (seasonal.dropna(subset=['plot'])
          .groupby(['i', 'crop'])['plot'].nunique().rename('nplots'))
seasonal = seasonal.merge(nplots, on=['i', 'crop'], how='left')
seasonal = seasonal.drop(columns=['Quantity_sold', 'Value_sold']).merge(
    sold, on=['i', 'crop'], how='left')
# blank out sold where the crop spans >1 plot (cannot place at plot grain)
multi = seasonal['nplots'].fillna(0) > 1
seasonal.loc[multi, ['Quantity_sold', 'Value_sold']] = pd.NA
seasonal = seasonal.drop(columns=['nplots'])

# --- planting month from cultivation roster (s11c) ---
s11c = get_dataframe('../Data/eaci17_s11cp1.dta').copy()
s11c['i'] = _hhid(s11c)
cult = pd.DataFrame({
    'i': s11c['i'],
    'plot': _plot(s11c, 's11cq01', 's11cq02'),
    'crop': s11c['s11cq03'],
    'planting_month': s11c['s11cq14b'],
}).dropna(subset=['plot', 'crop']).drop_duplicates(
    subset=['i', 'plot', 'crop'], keep='first')
seasonal = seasonal.drop(columns=['planting_month']).merge(
    cult, on=['i', 'plot', 'crop'], how='left')

# --- perennial trees (s11f): no field grid -> plot = <NA>, perennial=True ---
# Like 2014-15, s11f is a FIXED tree roster (every HH gets a row per species,
# s11fq03 == 'Non' for trees it does not have).  Keep only s11fq03 == 'Oui'
# (currently producing) — the ~270 rows with an actual harvest; matching WB
# `drop if s11fq03==2`.
s11f = get_dataframe('../Data/eaci17_s11fp1.dta').copy()
s11f = s11f[s11f['s11fq01'].notna()]
s11f = s11f[s11f['s11fq03'].astype('string').str.strip().eq('Oui')]
# perennial roster uses 'ménage' as the within-cluster household key
key2 = 'exploitation' if 'exploitation' in s11f.columns else 'ménage'
s11f['exploitation'] = s11f[key2]
s11f['i'] = _hhid(s11f)
perennial = pd.DataFrame({
    't': WAVE,
    'i': s11f['i'],
    'plot': pd.NA,
    'crop': s11f['s11fq01'],
    # s11fq11a = harvested quantity (numeric); s11fq11b = its unit.
    # (s11fq10 is a free-text "months producing" field, NOT a quantity.)
    'u': s11f['s11fq11b'] if 's11fq11b' in s11f.columns else pd.NA,
    'Quantity': pd.to_numeric(s11f['s11fq11a'], errors='coerce'),
    'Quantity_sold': pd.to_numeric(s11f['s11fq14a'], errors='coerce')
                       if 's11fq14a' in s11f.columns else pd.NA,
    'Value_sold': pd.to_numeric(s11f['s11fq15'], errors='coerce')
                       if 's11fq15' in s11f.columns else pd.NA,
    'harvest_month': pd.NA,
    'planting_month': pd.NA,
    'intercropped': pd.NA,
    'perennial': True,
})

df = pd.concat([seasonal, perennial], ignore_index=True)
df = crop_production_finalize(df)

assert len(df) > 0, "crop_production 2017-18 produced no rows"
assert df.index.is_unique, "Non-unique (t, i, plot, crop) in crop_production 2017-18"

to_parquet(df, 'crop_production.parquet')
