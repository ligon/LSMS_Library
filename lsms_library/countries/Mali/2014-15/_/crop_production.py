"""Build crop_production (item-level harvest) for Mali EACI 2014-15.

Sources (post-harvest, passage 2 / cultivation, passage 1):
  - EACIS3A_p2.dta   seasonal-crop harvest roster (one row per plot-crop)
  - EACIS3B_p2.dta   perennial-tree roster (one row per HH-tree; no field grid)
  - EACICULTURE_p1.dta (s1c) cultivation roster -> planting month + intercrop

Grain: (t, i, plot, crop).  plot = "{field}_{parcel}" (s3aq01_s3aq02) for
seasonal crops; <NA> for perennial trees.  Reported, item-level columns
only — Quantity / u / Quantity_sold / Value_sold / planting_month /
harvest_month / intercropped / perennial.  No harvest_kg / yield / share /
main_crop (those are transformations over these rows).

Variable map traced from MLI_EACI1.do (WB LSMS-ISA harmonised crop section):
  field=s3aq01 parcel=s3aq02 crop=s3aq03b harvest_qty=s3aq08a unit=s3aq08b
  sold_y/n=s3aq22 sold_qty=s3aq23a sold_value=s3aq24 harvest_month=s3aq07b
  perennial: crop=s3bq01 still_prod=s3bq03 qty=s3bq09 unit=s3bq10b
             sold_qty=s3bq13a sold_value=s3bq14
  cultivation: field=s1cq01 parcel=s1cq02 crop=s1cq03 plant_month=s1cq11b
             intercrop=s1cq05 (Pure / Association de cultures)
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, crop_production_finalize

WAVE = '2014-15'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


# --- seasonal harvest (s3a) ---
s3a = get_dataframe('../Data/EACIS3A_p2.dta').copy()
s3a['i'] = _hhid(s3a)
seasonal = pd.DataFrame({
    't': WAVE,
    'i': s3a['i'],
    'plot': _plot(s3a, 's3aq01', 's3aq02'),
    'crop': s3a['s3aq03b'],
    'u': s3a['s3aq08b'],
    'Quantity': s3a['s3aq08a'],
    'Quantity_sold': s3a['s3aq23a'],
    'Value_sold': s3a['s3aq24'],
    'harvest_month': s3a['s3aq07b'],
    'planting_month': pd.NA,
    'intercropped': pd.NA,
    'perennial': False,
})
# s3aq22 == 'Non' -> household reports not having sold this crop: 0 sold.
not_sold = s3a['s3aq22'].astype('string').str.strip().eq('Non')
seasonal.loc[not_sold, ['Quantity_sold', 'Value_sold']] = 0

# --- planting month + intercrop from cultivation roster (s1c) ---
s1c = get_dataframe('../Data/EACICULTURE_p1.dta').copy()
s1c['i'] = _hhid(s1c)
cult = pd.DataFrame({
    'i': s1c['i'],
    'plot': _plot(s1c, 's1cq01', 's1cq02'),
    'crop': s1c['s1cq03'],
    'planting_month': s1c['s1cq11b'],
    'intercropped': s1c['s1cq05'].astype('string').str.strip()
                       .map({'Pure': False, 'Association de cultures': True}),
})
cult = cult.dropna(subset=['plot', 'crop'])
cult = cult.drop_duplicates(subset=['i', 'plot', 'crop'], keep='first')

seasonal = seasonal.drop(columns=['planting_month', 'intercropped']).merge(
    cult, on=['i', 'plot', 'crop'], how='left')

# --- perennial trees (s3b): no field grid -> plot = <NA>, perennial=True ---
# s3b is a FIXED tree roster: every HH gets a row per possible species, with
# s3bq03 == 'Non' for trees it does not have/produce (3416 placeholder rows
# with no harvest).  Keep only s3bq03 == 'Oui' (currently producing) — the
# 358 rows that carry an actual harvest, matching WB `drop if s3bq03==2`.
s3b = get_dataframe('../Data/EACIS3B_p2.dta').copy()
s3b = s3b[s3b['s3bq01'].notna()]  # rows that actually name a tree
s3b = s3b[s3b['s3bq03'].astype('string').str.strip().eq('Oui')]
s3b['i'] = _hhid(s3b)
perennial = pd.DataFrame({
    't': WAVE,
    'i': s3b['i'],
    'plot': pd.NA,
    'crop': s3b['s3bq01'],
    'u': s3b['s3bq10b'],
    'Quantity': pd.to_numeric(s3b['s3bq09'], errors='coerce'),
    'Quantity_sold': s3b['s3bq13a'],
    'Value_sold': s3b['s3bq14'],
    'harvest_month': pd.NA,
    'planting_month': pd.NA,
    'intercropped': pd.NA,
    'perennial': True,
})

df = pd.concat([seasonal, perennial], ignore_index=True)
df = crop_production_finalize(df)

assert len(df) > 0, "crop_production 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t, i, plot, crop) in crop_production 2014-15"

to_parquet(df, 'crop_production.parquet')
