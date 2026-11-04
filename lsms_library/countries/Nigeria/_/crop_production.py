#!/usr/bin/env python
"""Build item-level crop_production for Nigeria GHS-Panel (GAP 1).

Natural grain (t, i, plot, crop): one row per crop grown on a plot in the
post-harvest crop module (secta3*).  Stores REPORTED item-level fields
only -- no kg conversion, no yield, no main_crop, no value shares (those
are transformations).

Per-wave source structure (post-harvest round):

  W1 2010-11  secta3_harvestw1.dta      single plot-crop file; sold +
              (Post Harvest Wave 1/Agriculture)   value at plot-crop grain,
                                        with the sale's OWN unit (sa3q11b).
  W2 2012-13  secta3_harvestw2.dta      single plot-crop file; sold +
              (Post Harvest Wave 2/Agriculture)   value at plot-crop grain,
                                        with the sale's OWN unit (sa3q11b).
  W3 2015-16  secta3i_harvestw3.dta     annual plot-crop harvest qty/unit.
  W4 2018-19  secta3i_harvestw4.dta     annual plot-crop harvest qty/unit;
              secta3iii_harvestw4.dta   perennial/tree plot-crop harvest.
  W5 2023-24  secta3i_harvestw5.dta     annual plot-crop harvest qty/unit;
              secta3iii_harvestw5.dta   perennial/tree plot-crop harvest.
              (Post Harvest Wave 5/Agriculture)
  W3-W5       secta3ii_harvestw{3,4,5}  the SALE, at hh-crop grain: no plot.
              Placed on the household's single served row for that crop, else
              on ONE plot_id='Farm-level' row with no harvest Quantity
              (GH #843; nigeria._attach_hh_crop_sales).

KgFactor (GH #859): the survey's own "Kg/L conversion factor" (`*_conv`),
asked BESIDE the harvest quantity and unit, carried per row.  Only W4 and W5
ship one at all; W1/W2/W3 have no `*_conv` column in any harvest file, so
KgFactor is legitimately all-NaN there (hence `optional: true`).  Wired, per
section, to the `*_conv` that pairs with the quantity/unit this build stores:

  W4 secta3i    sa3iq6_conv     (pairs with sa3iq6i  / sa3iq6ii)
  W4 secta3iii  sa3iiiq13_conv  (pairs with sa3iiiq13a / sa3iiiq13c)
  W5 secta3i    sa3iq9_conv     (pairs with sa3iq9a  / sa3iq9b)
  W5 secta3iii  sa3iiiq23_conv  (pairs with sa3iiiq23a / sa3iiiq23b)

Deliberately NOT wired: sa3iq6d_conv (W4) and sa3iq15_conv (W5) belong to the
"How much MORE [CROP] does HH expect to harvest" block, which has its OWN
unit/size/condition columns this build never reads -- their factor describes a
different container; and sa3iiq1_conv (secta3ii, W4/W5) rates the household's
TOTAL harvest, at hh-crop grain with no plot linkage, not this row's quantity.

Unit_sold (GH #824): the A3 sale block asks q11 "How much did you sell to
or through this [BUYER]?" with its OWN quantity and PROD. UNIT CODE
(sa3q11b), distinct from the harvest unit q6b that `u` carries.  W1/W2 pass
it as `unit_sold`; it is canonicalised through the same `_canon_unit` as
`u`.  Identified by the survey's value-label vocabulary -- sa3q11b carries
the identical code->label set as the harvest unit column in both waves (W1
36 labels, W2 48) -- not by the variable label, whose W1 text reads
"(11a)...(unit)".  No sold-CONDITION column exists in W1/W2 A3.  W3 asks the
sale its own unit (sa3iiq5b); W4/W5 ask only a quantity "Of the [QTY] [UNIT]
[CONDITION] of [CROP] harvested", so the sale is in the unit of the household's
total harvest (sa3iiq1c / sa3iiq3b).  On a W3-W5 row `Unit_sold` can therefore
differ from `u`, and on a Farm-level row `u` is 'Unknown' beside a real
`Unit_sold`: that row has no harvest for `u` to describe (GH #843).

Value_harvested (GH #844): the household's own Naira VALUATION of the
harvest the row's Quantity measures, asked on the same plot-crop row in every
wave -- never a transaction, so distinct from Value_sold.  The wording differs:

  W1/W2  sa3q18     "If you had sold all [CROP] harvested since the last
                    visit, what would be the total value?" (sale block)
  W3     sa3iq6a    "What is the estimated value of the [CROP] harvested
                    during this agricultural (rainy) season?"
  W4     sa3iq6a    (secta3i) / sa3iiiq14 (secta3iii) "What is the estimated
                    value of the [CROP] harvested from [PLOT] ...?"
  W5     sa3iq10    (secta3i) / sa3iiiq24 (secta3iii), same wording as W4.

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
                     format_id, hh_crop_sale_records)

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
        'plot_id': df[plot].apply(format_id).values,
        'crop_code': pd.to_numeric(df[cc], errors='coerce').astype('Int64').values,
        'intercropped': inter.values,
    })
    key = key.dropna(subset=['intercropped']).drop_duplicates(['i', 'plot_id', 'crop_code'])
    return key


def _align_intercropped(raw, hhid, plot, cropcol, planting_fn):
    """Build an intercropped Series aligned to `raw` rows by joining the
    planting-roster flag on (i, plot, cropcode)."""
    key = _intercropped_from_planting(planting_fn)
    if key is None:
        return None
    j = pd.DataFrame({
        'i': raw[hhid].apply(format_id).values,
        'plot_id': raw[plot].apply(format_id).values,
        'crop_code': pd.to_numeric(raw[cropcol], errors='coerce').astype('Int64').values,
    }, index=raw.index)
    merged = j.merge(key, on=['i', 'plot_id', 'crop_code'], how='left')
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
    qty_sold='sa3q11a', unit_sold='sa3q11b', value_sold='sa3q12', sold_on='plot',
    value_harvested='sa3q18',
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
    qty_sold='sa3q11a', unit_sold='sa3q11b', value_sold='sa3q12', sold_on='plot',
    value_harvested='sa3q18',
    intercropped=inter, perennial=False)], crop_labels))

# ----------------------------- W3 2015-16 -----------------------------
# secta3i: annual plot-crop harvest.  Sold/value are hh-crop grain (secta3ii);
# hh_sales places them per the GH #843 ruling.
t = PH_QUARTER['2015-16']
f = '../2015-16/Data/secta3i_harvestw3.dta'
raw = get_dataframe(f, convert_categoricals=False)
dec = get_dataframe(f, convert_categoricals=True)
inter = _align_intercropped(
    raw, 'hhid', 'plotid', 'cropcode',
    '../2015-16/Data/sect11f_plantingw3.dta')
pieces.append(crop_production_for_wave(t, [dict(
    df=raw, dec=dec, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iq6i', unit='sa3iq6ii', value_harvested='sa3iq6a',
    harv_m='sa3iq6c1', harv_y='sa3iq6c2',
    intercropped=inter, perennial=False)], crop_labels,
    hh_sales=hh_crop_sale_records('2015-16', crop_labels)))

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
    qty='sa3iq6i', unit='sa3iq6ii', kg_factor='sa3iq6_conv',
    value_harvested='sa3iq6a',
    plant_m='sa3iq4a1', plant_y='sa3iq4a2',
    harv_m='sa3iq6c1', harv_y='sa3iq6c2',
    intercropped=inter_a, perennial=False))
fp = '../2018-19/Data/secta3iii_harvestw4.dta'
raw_p = get_dataframe(fp, convert_categoricals=False)
dec_p = get_dataframe(fp, convert_categoricals=True)
frames.append(dict(
    df=raw_p, dec=dec_p, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iiiq13a', unit='sa3iiiq13c', kg_factor='sa3iiiq13_conv',
    value_harvested='sa3iiiq14',
    plant_m='sa3iiiq8a', plant_y='sa3iiiq8b',
    harv_m='sa3iiiq12a', harv_y='sa3iiiq12b',
    perennial=True))
pieces.append(crop_production_for_wave(
    t, frames, crop_labels, hh_sales=hh_crop_sale_records('2018-19', crop_labels)))

# ----------------------------- W5 2023-24 -----------------------------
t = PH_QUARTER['2023-24']
frames = []
fa = '../2023-24/Data/Post Harvest Wave 5/Agriculture/secta3i_harvestw5.dta'
raw_a = get_dataframe(fa, convert_categoricals=False)
dec_a = get_dataframe(fa, convert_categoricals=True)
frames.append(dict(
    df=raw_a, dec=dec_a, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iq9a', unit='sa3iq9b', kg_factor='sa3iq9_conv',
    value_harvested='sa3iq10',
    plant_m='sa3iq5a', plant_y='sa3iq5b',
    harv_m='sa3iq14a', harv_y='sa3iq14b',
    perennial=False))
fp = '../2023-24/Data/Post Harvest Wave 5/Agriculture/secta3iii_harvestw5.dta'
raw_p = get_dataframe(fp, convert_categoricals=False)
dec_p = get_dataframe(fp, convert_categoricals=True)
frames.append(dict(
    df=raw_p, dec=dec_p, hhid='hhid', plot='plotid', crop='cropcode',
    qty='sa3iiiq23a', unit='sa3iiiq23b', kg_factor='sa3iiiq23_conv',
    value_harvested='sa3iiiq24',
    plant_m='sa3iiiq18a', plant_y='sa3iiiq18b',
    harv_m='sa3iiiq22a', harv_y='sa3iiiq22b',
    perennial=True))
pieces.append(crop_production_for_wave(
    t, frames, crop_labels, hh_sales=hh_crop_sale_records('2023-24', crop_labels)))

# ----------------------------- combine -------------------------------
df = pd.concat(pieces, axis=0)
df = df.sort_index()
# Concatenating all-NaN waves (W1-W3, no `*_conv` anywhere) with float waves
# can degrade the dtype; pin it so the parquet stores a float column.
df['KgFactor'] = pd.to_numeric(df['KgFactor'], errors='coerce').astype('float64')
# Same pin for the sale columns (W1/W2 read off the row, W3-W5 hh_sales) and
# the row's provenance; the parquet must store these types, not object.
df['Quantity_sold'] = pd.to_numeric(df['Quantity_sold'],
                                    errors='coerce').astype('float64')
df['Value_sold'] = pd.to_numeric(df['Value_sold'],
                                 errors='coerce').astype('float64')
df['Unit_sold'] = df['Unit_sold'].astype('string')
df['Derivation'] = df['Derivation'].astype('string')
# A Farm-level row has no annual/perennial answer (GH #843).
df['perennial'] = df['perennial'].astype('boolean')
# And for Value_harvested (GH #844), present in every wave but pinned anyway.
df['Value_harvested'] = pd.to_numeric(df['Value_harvested'],
                                      errors='coerce').astype('float64')

to_parquet(df, '../var/crop_production.parquet')
