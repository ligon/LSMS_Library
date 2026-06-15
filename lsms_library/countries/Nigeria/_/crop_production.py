#!/usr/bin/env python
"""Build item-level crop_production for Nigeria GHS-Panel (GAP 1).

Natural grain (t, i, plot, crop): one row per crop grown on a plot in the
post-harvest crop module (secta3*).  Stores REPORTED item-level fields
only -- no kg conversion, no yield, no main_crop, no value shares (those
are transformations).

Per-wave source structure (post-harvest round):

  W1 2010-11  secta3_harvestw1.dta      single plot-crop file; sold +
              (Post Harvest Wave 1/Agriculture)   value at plot-crop grain.
  W2 2012-13  secta3_harvestw2.dta      single plot-crop file; sold +
              (Post Harvest Wave 2/Agriculture)   value at plot-crop grain.
  W3 2015-16  secta3i_harvestw3.dta     annual plot-crop harvest qty/unit.
              (sold/value live in secta3ii at hh-crop grain -- no plot
               linkage, so NOT attributed to a plot here.)
  W4 2018-19  secta3i_harvestw4.dta     annual plot-crop harvest qty/unit;
              secta3iii_harvestw4.dta   perennial/tree plot-crop harvest.
  W5 2023-24  secta3i_harvestw5.dta     annual plot-crop harvest qty/unit;
              secta3iii_harvestw5.dta   perennial/tree plot-crop harvest.
              (Post Harvest Wave 5/Agriculture)

crop labels (j): cropcode -> Preferred Label via harmonize_food (shared
with food_acquired; reused food labels where the crop is a consumed food).
units (u): native production-unit label normalized to a base Preferred
Label registered in the `u` table.  `v` is added at API time by
_join_v_from_sample.  plot_id aligns with plot_features (format_id).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import (PH_QUARTER, crop_production_for_wave, _crop_labels,
                     format_id)

crop_labels = _crop_labels()


def _intercropped_from_planting(planting_fn):
    """Derive a per-(i, plot, cropcode) intercropped flag from the
    post-planting crop roster (sect11f): cropping-type (s11fq2) mono ->
    False, mixed/inter/alley/relay/strip -> True.  Returns a Series keyed
    by (i, plot, cropcode) or None if unavailable."""
    try:
        df = get_dataframe(planting_fn, convert_categoricals=False)
    except Exception:
        return None
    cols = {c.lower(): c for c in df.columns}
    cc = cols.get('cropcode')
    plot = cols.get('plotid')
    hh = cols.get('hhid')
    q2 = cols.get('s11fq2')
    if not all([cc, plot, hh, q2]):
        return None
    code = pd.to_numeric(df[q2], errors='coerce')
    # 1 = mono-cropping -> not intercropped; 2..7 mixed/inter/etc -> True.
    inter = pd.Series(pd.NA, index=df.index, dtype='boolean')
    inter = inter.where(~(code == 1), False)
    inter = inter.where(~(code.between(2, 7)), True)
    key = pd.DataFrame({
        'i': df[hh].apply(format_id).values,
        'plot': df[plot].apply(format_id).values,
        'crop_code': pd.to_numeric(df[cc], errors='coerce').astype('Int64').values,
        'intercropped': inter.values,
    })
    key = key.dropna(subset=['intercropped']).drop_duplicates(['i', 'plot', 'crop_code'])
    return key


def _align_intercropped(raw, hhid, plot, cropcol, planting_fn):
    """Build an intercropped Series aligned to `raw` rows by joining the
    planting-roster flag on (i, plot, cropcode)."""
    key = _intercropped_from_planting(planting_fn)
    if key is None:
        return None
    j = pd.DataFrame({
        'i': raw[hhid].apply(format_id).values,
        'plot': raw[plot].apply(format_id).values,
        'crop_code': pd.to_numeric(raw[cropcol], errors='coerce').astype('Int64').values,
    }, index=raw.index)
    merged = j.merge(key, on=['i', 'plot', 'crop_code'], how='left')
    return pd.Series(merged['intercropped'].values, index=raw.index, dtype='boolean')


pieces = []

# ----------------------------- W1 2010-11 -----------------------------
t = PH_QUARTER['2010-11']
f = '../2010-11/Data/Post Harvest Wave 1/Agriculture/secta3_harvestw1.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
inter = _align_intercropped(
    raw, 'hhid', 'plotid', 'sa3q2',
    '../2010-11/Data/Post Planting Wave 1/Agriculture/sect11f_plantingw1.dta')
pieces.append(crop_production_for_wave(t, [dict(
    df=raw, dec=dec, hhid='hhid', plot='plotid', crop='sa3q2',
    qty='sa3q6a', unit='sa3q6b',
    qty_sold='sa3q11a', value_sold='sa3q12', sold_on='plot',
    intercropped=inter, perennial=False)], crop_labels))

# ----------------------------- W2 2012-13 -----------------------------
t = PH_QUARTER['2012-13']
f = '../2012-13/Data/Post Harvest Wave 2/Agriculture/secta3_harvestw2.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
inter = _align_intercropped(
    raw, 'hhid', 'plotid', 'cropcode',
    '../2012-13/Data/Post Planting Wave 2/Agriculture/sect11f_plantingw2.dta')
pieces.append(crop_production_for_wave(t, [dict(
    df=raw, dec=dec, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3q6a1', unit='sa3q6a2',
    qty_sold='sa3q11a', value_sold='sa3q12', sold_on='plot',
    intercropped=inter, perennial=False)], crop_labels))

# ----------------------------- W3 2015-16 -----------------------------
# secta3i: annual plot-crop harvest.  Sold/value are hh-crop grain
# (secta3ii) -> not attributable to a plot, left NaN.
t = PH_QUARTER['2015-16']
f = '../2015-16/Data/secta3i_harvestw3.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
inter = _align_intercropped(
    raw, 'hhid', 'plotid', 'cropcode',
    '../2015-16/Data/sect11f_plantingw3.dta')
pieces.append(crop_production_for_wave(t, [dict(
    df=raw, dec=dec, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iq6i', unit='sa3iq6ii',
    harv_m='sa3iq6c1', harv_y='sa3iq6c2',
    intercropped=inter, perennial=False)], crop_labels))

# ----------------------------- W4 2018-19 -----------------------------
# secta3i annual + secta3iii perennial, both plot-crop.
t = PH_QUARTER['2018-19']
frames = []
fa = '../2018-19/Data/secta3i_harvestw4.dta'
raw_a = get_dataframe(fa, convert_categoricals=False)
dec_a = get_dataframe(fa, convert_categoricals=True)
inter_a = _align_intercropped(
    raw_a, 'hhid', 'plotid', 'cropcode',
    '../2018-19/Data/sect11f_plantingw4.dta')
frames.append(dict(
    df=raw_a, dec=dec_a, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iq6i', unit='sa3iq6ii',
    plant_m='sa3iq4a1', plant_y='sa3iq4a2',
    harv_m='sa3iq6c1', harv_y='sa3iq6c2',
    intercropped=inter_a, perennial=False))
fp = '../2018-19/Data/secta3iii_harvestw4.dta'
raw_p = get_dataframe(fp, convert_categoricals=False)
dec_p = get_dataframe(fp, convert_categoricals=True)
frames.append(dict(
    df=raw_p, dec=dec_p, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iiiq13a', unit='sa3iiiq13c',
    plant_m='sa3iiiq8a', plant_y='sa3iiiq8b',
    harv_m='sa3iiiq12a', harv_y='sa3iiiq12b',
    perennial=True))
pieces.append(crop_production_for_wave(t, frames, crop_labels))

# ----------------------------- W5 2023-24 -----------------------------
t = PH_QUARTER['2023-24']
frames = []
fa = '../2023-24/Data/Post Harvest Wave 5/Agriculture/secta3i_harvestw5.dta'
raw_a = get_dataframe(fa, convert_categoricals=False)
dec_a = get_dataframe(fa, convert_categoricals=True)
frames.append(dict(
    df=raw_a, dec=dec_a, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iq9a', unit='sa3iq9b',
    plant_m='sa3iq5a', plant_y='sa3iq5b',
    harv_m='sa3iq14a', harv_y='sa3iq14b',
    perennial=False))
fp = '../2023-24/Data/Post Harvest Wave 5/Agriculture/secta3iii_harvestw5.dta'
raw_p = get_dataframe(fp, convert_categoricals=False)
dec_p = get_dataframe(fp, convert_categoricals=True)
frames.append(dict(
    df=raw_p, dec=dec_p, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iiiq23a', unit='sa3iiiq23b',
    plant_m='sa3iiiq18a', plant_y='sa3iiiq18b',
    harv_m='sa3iiiq22a', harv_y='sa3iiiq22b',
    perennial=True))
pieces.append(crop_production_for_wave(t, frames, crop_labels))

# ----------------------------- combine -------------------------------
df = pd.concat(pieces, axis=0)
df = df.sort_index()

to_parquet(df, '../var/crop_production.parquet')
