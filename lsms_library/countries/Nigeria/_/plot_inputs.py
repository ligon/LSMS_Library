#!/usr/bin/env python
"""Build item-level plot_inputs for Nigeria GHS-Panel (GAP 2).

Natural grain (t, i, plot, input, crop): one row per agricultural input
applied to a plot in the post-planting / post-harvest input modules.
Stores REPORTED item-level fields ONLY -- no seed_kg / nitrogen_kg
conversion, no inorganic/organic any-use flags, no fertilizer totals
(those are transformations over these rows).

`input` (index) is a harmonized input-type label registered in the
harmonize_input categorical table:
  Seed / NPK / Urea / Other Inorganic Fertilizer / Organic Fertilizer /
  Pesticide / Herbicide / Animal Traction
`crop` (index) is the seed's crop (harmonize_food labels) for seed rows;
non-seed (crop-agnostic) inputs carry crop = 'n/a'.

Every emitted row carries a REPORTED quantity (a recorded item), never a
bare any-use flag: chemicals carry the reported quantity+unit; animal
traction carries reported person/animal-days (own + rented).  The plain
"did you use a tractor/machine?" question (W1-W5) records no per-input
quantity at this grain and is left to a transformation (used_machinery),
not stored as a flag-only row.

Columns (reported): Quantity + u (native unit), Purchased (bool) +
Quantity_purchased, Improved (bool, seed rows).

Per-wave source structure
-------------------------
The Nigeria GHS input modules organise each input by ACQUISITION CHANNEL
(left-over / free / commercial-1 / commercial-2).  Fertilizer / pesticide
move from the post-planting round (W1/W2: sect11d/sect11c2) to the
post-harvest round (W3-W5: secta11d / secta11c2), and the fertilizer
recording switches from one-type-code-per-channel (W1-W3) to
one-column-per-type (W4/W5: NPK / Urea / other / organic).  Seeds stay in
post-planting throughout (W1-W3 sect11e per-channel; W4/W5 sect11f
plot-crop planting roster with a single quantity + improved/certified
flags).  Each wave maps to a single t = PP_QUARTER[wave] (matching
plot_features; plot_id aligns via format_id with crop_production too).

  W1 2010-11  seeds  sect11e_plantingw1   ferts sect11d  pest sect11c
  W2 2012-13  seeds  sect11e_plantingw2   ferts sect11d  pest sect11c2
  W3 2015-16  seeds  sect11e_plantingw3   ferts secta11d_harvest pest secta11c2_harvest
  W4 2018-19  seeds  sect11f_plantingw4   ferts secta11c2_harvest (wide-typed)
  W5 2023-24  seeds  sect11f_plantingw5   ferts secta11c2_harvest (wide-typed)
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import (PP_QUARTER, _crop_labels, seed_rows_for_wave,
                     fert_rows_long_typed, fert_rows_wide_typed, chem_rows,
                     assemble_plot_inputs, INPUT_NPK, INPUT_UREA,
                     INPUT_FERT_OTHER, INPUT_ORGANIC, INPUT_PESTICIDE,
                     INPUT_HERBICIDE, INPUT_ANIMAL)

crop_labels = _crop_labels()

# Fertilizer TYPE-code -> harmonized input label (W1-W3 one-type-per-channel).
# Source value labels: 1=NPK, 2=UREA, 3=Composite Manure (organic),
# 4=Other.  Composite manure is an organic fertilizer.
FERT_TYPE_MAP = {1: INPUT_NPK, 2: INPUT_UREA, 3: INPUT_ORGANIC,
                 4: INPUT_FERT_OTHER}


def _read(f):
    raw = get_dataframe(f, convert_categoricals=False)
    dec = get_dataframe(f, convert_categoricals=True)
    return raw, dec


pieces = []

# ============================ W1 2010-11 =============================
t = PP_QUARTER['2010-11']
parts = []

# -- seeds (sect11e): leftover/free/commercial-1/commercial-2 channels.
sraw, sdec = _read('../2010-11/Data/Post Planting Wave 1/Agriculture/'
                   'sect11e_plantingw1.dta')
parts.append(seed_rows_for_wave(
    sraw, sdec, 'hhid', 'plotid', 's11eq2',
    channels=[
        dict(qty='s11eq6a', unit='s11eq6b', purchased=False),    # leftover
        dict(qty='s11eq10a', unit='s11eq10b', purchased=False),  # free
        dict(qty='s11eq17a', unit='s11eq17b', purchased=True),   # commercial 1
        dict(qty='s11eq28a', unit='s11eq28b', purchased=True),   # commercial 2
    ], crop_labels=crop_labels, improved=None))

# -- fertilizer (sect11d): leftover/free/comm-1/comm-2, type-per-channel.
fraw, fdec = _read('../2010-11/Data/Post Planting Wave 1/Agriculture/'
                   'sect11d_plantingw1.dta')
parts.append(fert_rows_long_typed(
    fraw, fdec, 'hhid', 'plotid',
    channels=[
        dict(type='s11dq3', qty='s11dq4', unit=None, purchased=False),
        dict(type='s11dq7', qty='s11dq8', unit=None, purchased=False),
        dict(type='s11dq14', qty='s11dq15', unit=None, purchased=True),
        dict(type='s11dq25', qty='s11dq26', unit=None, purchased=True),
    ], type_map=FERT_TYPE_MAP))

# -- pesticide / herbicide / traction (sect11c).
praw, pdec = _read('../2010-11/Data/Post Planting Wave 1/Agriculture/'
                   'sect11c_plantingw1.dta')
parts.append(chem_rows(praw, pdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_PESTICIDE, qty='s11cq2a', unit='s11cq2b'),
    dict(input=INPUT_HERBICIDE, qty='s11cq11a', unit='s11cq11b'),
    dict(input=INPUT_ANIMAL, qty_cols=['s11cq20', 's11cq21'], u_label='days'),
]))
pieces.append(assemble_plot_inputs(t, parts))

# ============================ W2 2012-13 =============================
t = PP_QUARTER['2012-13']
parts = []
sraw, sdec = _read('../2012-13/Data/Post Planting Wave 2/Agriculture/'
                   'sect11e_plantingw2.dta')
parts.append(seed_rows_for_wave(
    sraw, sdec, 'hhid', 'plotid', 'cropcode',
    channels=[
        dict(qty='s11eq6a', unit='s11eq6b', purchased=False),
        dict(qty='s11eq10a', unit='s11eq10b', purchased=False),
        dict(qty='s11eq18a', unit='s11eq18b', purchased=True),
        dict(qty='s11eq30a', unit='s11eq30b', purchased=True),
    ], crop_labels=crop_labels, improved=None))

fraw, fdec = _read('../2012-13/Data/Post Planting Wave 2/Agriculture/'
                   'sect11d_plantingw2.dta')
parts.append(fert_rows_long_typed(
    fraw, fdec, 'hhid', 'plotid',
    channels=[
        dict(type='s11dq3', qty='s11dq4', unit=None, purchased=False),
        dict(type='s11dq7', qty='s11dq8', unit=None, purchased=False),
        dict(type='s11dq14', qty='s11dq15', unit=None, purchased=True),
        dict(type='s11dq25a', qty='s11dq26', unit=None, purchased=True),
    ], type_map=FERT_TYPE_MAP))

praw, pdec = _read('../2012-13/Data/Post Planting Wave 2/Agriculture/'
                   'sect11c2_plantingw2.dta')
parts.append(chem_rows(praw, pdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_PESTICIDE, qty='s11c2q2a', unit='s11c2q2b'),
    dict(input=INPUT_HERBICIDE, qty='s11c2q11a', unit='s11c2q11b'),
    dict(input=INPUT_ANIMAL, qty_cols=['s11c2q20', 's11c2q21'], u_label='days'),
]))
pieces.append(assemble_plot_inputs(t, parts))

# ============================ W3 2015-16 =============================
t = PP_QUARTER['2015-16']
parts = []
# Seeds: PP round.  Improved from s11eq3b (1 HYBRID/2 IMPROVED -> True;
# 3 TRADITIONAL/4 LOCAL -> False).
sraw, sdec = _read('../2015-16/Data/sect11e_plantingw3.dta')
imp = pd.to_numeric(sraw['s11eq3b'], errors='coerce')
improved = pd.Series(pd.NA, index=sraw.index, dtype='boolean')
improved = improved.where(~imp.isin([1, 2]), True)
improved = improved.where(~imp.isin([3, 4]), False)
parts.append(seed_rows_for_wave(
    sraw, sdec, 'hhid', 'plotid', 'cropcode',
    channels=[
        dict(qty='s11eq6a', unit='s11eq6b', purchased=False),
        dict(qty='s11eq10a', unit='s11eq10b', purchased=False),
        dict(qty='s11eq18a', unit='s11eq18b', purchased=True),
        dict(qty='s11eq30a', unit='s11eq30b', purchased=True),
    ], crop_labels=crop_labels, improved=improved))

# Fertilizer: post-harvest secta11d.  leftover/free/purch-1/purch-2 +
# organic.  Organic recorded with its own qty/unit (s11dq37a/b).
fraw, fdec = _read('../2015-16/Data/secta11d_harvestw3.dta')
parts.append(fert_rows_long_typed(
    fraw, fdec, 'hhid', 'plotid',
    channels=[
        dict(type='s11dq3', qty='s11dq4a', unit='s11dq4b', purchased=False),
        dict(type='sect11dq7', qty='sect11dq8a', unit='sect11dq8b', purchased=False),
        dict(type='s11dq15', qty='s11dq16a', unit='s11dq16b', purchased=True),
        dict(type='s11dq27', qty='s11dq28a', unit='s11dq28b', purchased=True),
    ], type_map=FERT_TYPE_MAP))
# Organic fertilizer (separate qty/unit block, no type code).
parts.append(fert_rows_wide_typed(
    fraw, fdec, 'hhid', 'plotid',
    specs=[dict(input=INPUT_ORGANIC, qty='s11dq37a', unit='s11dq37b')]))

# Pesticide / herbicide / traction: post-harvest secta11c2.
praw, pdec = _read('../2015-16/Data/secta11c2_harvestw3.dta')
parts.append(chem_rows(praw, pdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_PESTICIDE, qty='s11c2q2a', unit='s11c2q2b'),
    dict(input=INPUT_HERBICIDE, qty='s11c2q11a', unit='s11c2q11b'),
    dict(input=INPUT_ANIMAL, qty_cols=['s11c2q20', 's11c2q21'], u_label='days'),
]))
pieces.append(assemble_plot_inputs(t, parts))

# ============================ W4 2018-19 =============================
t = PP_QUARTER['2018-19']
parts = []
# Seeds: sect11f planting roster (one row per plot-crop).  Quantity
# planted s11fq3d_1/_2; improved s11fq3b (1 Improved -> True, 2 Local ->
# False); certified s11fq3aa.  No purchase channel here (the W4/W5 seed
# acquisition/cost detail lives in sect11e1/e2 at a different grain).
sraw, sdec = _read('../2018-19/Data/sect11f_plantingw4.dta')
imp = pd.to_numeric(sraw['s11fq3b'], errors='coerce')
improved = pd.Series(pd.NA, index=sraw.index, dtype='boolean')
improved = improved.where(~(imp == 1), True)
improved = improved.where(~(imp == 2), False)
# certified strengthens improved=True (a certified seed is improved).
cert = pd.to_numeric(sraw['s11fq3aa'], errors='coerce')
improved = improved.where(~(cert == 1), True)
parts.append(seed_rows_for_wave(
    sraw, sdec, 'hhid', 'plotid', 'cropcode',
    channels=[dict(qty='s11fq3d_1', unit='s11fq3d_2', purchased=False)],
    crop_labels=crop_labels, improved=improved))

# Fertilizer: secta11c2 wide-typed (NPK / Urea / other inorganic /
# organic each its own qty+unit).
fraw, fdec = _read('../2018-19/Data/secta11c2_harvestw4.dta')
parts.append(fert_rows_wide_typed(fraw, fdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_NPK, qty='s11c2q37a', unit='s11c2q37b'),
    dict(input=INPUT_UREA, qty='s11c2q38a', unit='s11c2q38b'),
    dict(input=INPUT_FERT_OTHER, qty='s11c2q39a', unit='s11c2q39b'),
    dict(input=INPUT_ORGANIC, qty='s11dq37a', unit='s11dq37b'),
]))
# Pesticide / herbicide / traction in the same secta11c2 file.
parts.append(chem_rows(fraw, fdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_PESTICIDE, qty='s11c2q2a', unit='s11c2q2b'),
    dict(input=INPUT_HERBICIDE, qty='s11c2q11a', unit='s11c2q11b'),
    dict(input=INPUT_ANIMAL, qty_cols=['s11c2q20', 's11c2q21'], u_label='days'),
]))
pieces.append(assemble_plot_inputs(t, parts))

# ============================ W5 2023-24 =============================
t = PP_QUARTER['2023-24']
parts = []
sraw, sdec = _read('../2023-24/Data/Post Planting Wave 5/Agriculture/'
                   'sect11f_plantingw5.dta')
imp = pd.to_numeric(sraw['s11fq7'], errors='coerce')
improved = pd.Series(pd.NA, index=sraw.index, dtype='boolean')
improved = improved.where(~(imp == 1), True)
improved = improved.where(~(imp == 2), False)
cert = pd.to_numeric(sraw['s11fq6'], errors='coerce')
improved = improved.where(~(cert == 1), True)
parts.append(seed_rows_for_wave(
    sraw, sdec, 'hhid', 'plotid', 'cropcode',
    channels=[dict(qty='s11fq5a', unit='s11fq5b', purchased=False)],
    crop_labels=crop_labels, improved=improved))

# Fertilizer: secta11c2 wide-typed (NPK s11c2q7a/b, Urea s11c2q11a/b,
# other s11c2q9a/b, organic s11c2q16a/b).
fraw, fdec = _read('../2023-24/Data/Post Harvest Wave 5/Agriculture/'
                   'secta11c2_harvestw5.dta')
parts.append(fert_rows_wide_typed(fraw, fdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_NPK, qty='s11c2q7a', unit='s11c2q7b'),
    dict(input=INPUT_UREA, qty='s11c2q11a', unit='s11c2q11b'),
    dict(input=INPUT_FERT_OTHER, qty='s11c2q9a', unit='s11c2q9b'),
    dict(input=INPUT_ORGANIC, qty='s11c2q16a', unit='s11c2q16b'),
]))
# Pesticide / herbicide / traction.  W5 swapped the question order:
# herbicide s11c2q1/2a/2b, pesticide s11c2q3/4a/4b.
parts.append(chem_rows(fraw, fdec, 'hhid', 'plotid', specs=[
    dict(input=INPUT_HERBICIDE, qty='s11c2q2a', unit='s11c2q2b'),
    dict(input=INPUT_PESTICIDE, qty='s11c2q4a', unit='s11c2q4b'),
    dict(input=INPUT_ANIMAL, qty_cols=['s11c2q15', 's11c2q16'], u_label='days'),
]))
pieces.append(assemble_plot_inputs(t, parts))

# ============================== combine =============================
df = pd.concat(pieces, axis=0)
df = df.sort_index()

to_parquet(df, '../var/plot_inputs.parquet')
