# Formatting Functions for Nigeria GHS-Panel 2010-11 (W1)
#
# GH #1035 (rulings @ligon 2026-10-18, amended 2026-10-19): the W1
# post-harvest food module's sized unit codes 11-95 come from the agriculture
# production-unit card, and the card states each container's size
# ('Sack/Bag: Small (20 kg)').  The stated weight is served -- through the
# `survey_kg` rung, as `Quantity_kg` -- ONLY where the row records a FRACTION
# of the container (0 < Quantity < 1).  A quantity of one or more containers
# reads as a count of a local unit (red team r1: those rows price at a median
# 0.032x the item's own Kg-row price at the card weight), so it keeps the
# price-ratio inference it always had, under its weightless `u` label.
#
# Registered as `Nigeria::food_acquired::w1-card-weight` in
# Nigeria/_/derivations.yml against THIS module.  Placed at wave scope rather
# than in Nigeria/_/nigeria.py for locality only: `Wave._input_hash` hashes
# every `.py` in a wave's `_/` dir into every table that wave builds, and the
# country hash folds every wave, so an edit here re-warms all of Nigeria
# either way (CLAUDE.md "Derived Values"; the 2018-19 mapping.py measured it).
#
# This module is also loaded by `Wave.formatting_functions` for every
# 2010-11 YAML build, so it must stay import-cheap (no data reads at import)
# and must not define a callable named after a 2010-11 table or column.
import contextlib
import os
import sys

import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_HERE, '..', '..', '_'))

CARD_WEIGHT_DERIVATION = 'Nigeria::food_acquired::w1-card-weight'

#: The post-harvest (2011Q1) food module, exactly as `food_acquired.py` reads
#: it: each acquisition source's own quantity and unit (GH #591).
W1_PH_ROUND = '2011Q1'
W1_PH_FOOD_FILE = '../Data/sect10b_harvestw1.csv'
W1_PH_SOURCES = {
    'purchased': {'quantity': 's10bq3a', 'unit': 's10bq3b'},
    'produced':  {'quantity': 's10bq5a', 'unit': 's10bq5b'},
    'inkind':    {'quantity': 's10bq6a', 'unit': 's10bq6b'},
}
W1_PH_EXPENDITURE = 's10bq4'

#: Survey-stated MASS of each sized container code, kilograms.  Transcribed
#: from sect10b_harvestw1.dta, value-label set SP10BQ2B (shared by
#: s10bq2b/3b/5b/6b): '11 Sack/Bag: Small (20 kg)' ... '83 Pick-up Van: Big
#: (2,500 kg)'.  The three Bundle labels are truncated at Stata's 60
#: characters ('...: Small (1', '...: Medium (', '...: Big (40'); their 15 / 25
#: / 40 kg are from the survey's own 2010-11/Data/w1agnsconversion.csv
#: (nscode 61/62/63, one value for every crop), which carries the label
#: weight for every other code here too.  Every weight is at least 3 kg, so
#: the `0 < Quantity < 1` rule can never select a row whose quantity equals
#: its card weight (a kg figure keyed into the quantity field; 165 rows).
W1_PH_CARD_KG = {
    11: 20.0, 12: 50.0, 13: 100.0, 14: 120.0,          # Sack/Bag
    21: 15.0, 22: 30.0, 23: 50.0, 24: 75.0,            # Basket
    31: 10.0, 32: 25.0, 33: 40.0, 34: 75.0,            # Basin
    41: 5.0, 42: 8.0, 43: 15.0,                        # Bunch (plantain / FFB)
    51: 3.0, 52: 5.0, 53: 8.0,                         # Tuber of yam
    61: 15.0, 62: 25.0, 63: 40.0,                      # Bundle
    71: 60.0, 72: 85.0, 73: 110.0, 74: 150.0,          # Wheelbarrow
    81: 1500.0, 82: 2000.0, 83: 2500.0,                # Pick-up van
}

#: The jerry can / keg / rubber of palm oil codes state a VOLUME ('Small(10
#: Lt.)' ... 'Drum (200 Lt.)').  Recorded, and deliberately NOT served as
#: kilograms: a `Quantity_kg` built from litres would assert 1 l = 1 kg as a
#: SURVEY figure, which `volume_as_mass=False` cannot decline and which would
#: carry the density assumption into the price-per-kg baseline that
#: `transformations._BASELINE_UNIT_CONVERSION` keeps volumes out of.
W1_PH_CARD_LITRES = {91: 10.0, 92: 20.0, 93: 25.0, 94: 50.0, 95: 200.0}


def derive_w1_card_weight_kg(quantity, card_kg):
    """Kilograms of a W1-PH row recorded as a FRACTION of a sized container.

    ``Quantity_kg = Quantity x card_kg`` where ``0 < Quantity < 1`` and the
    row's unit is a sized container with a stated mass; NaN otherwise --
    including every row of one or more containers, which keeps the
    price-ratio inference (GH #1035 amended ruling).  Vectorised over the
    served rows (``Quantity`` after the build's duplicate-key collapse).
    """
    q = pd.to_numeric(pd.Series(quantity), errors='coerce').to_numpy(dtype=float)
    k = pd.to_numeric(pd.Series(card_kg), errors='coerce').to_numpy(dtype=float)
    take = (q > 0) & (q < 1) & np.isfinite(k) & (k > 0)
    return np.where(take, q * k, np.nan)


def _card_kg_by_label():
    """``{served 2011Q1 u label: card kg}`` -- through the same per-round
    decode the build uses (`nigeria.food_unit_labels`), so a relabelled
    ``2010-11`` column is followed rather than silently missed."""
    from nigeria import food_unit_labels
    labels = food_unit_labels(W1_PH_ROUND)
    return {labels[c]: kg for c, kg in W1_PH_CARD_KG.items()}


def stamp_w1_card_weight(harvest):
    """``Quantity_kg`` and ``Derivation`` on the built 2011Q1 frame.

    Applied to the frame `nigeria.food_acquired_for_wave` returns, i.e.
    AFTER its duplicate-key collapse, so the rule reads the served
    ``Quantity`` and the stamp cannot ride a row a collapse later discards.
    Rows the rule does not select keep ``Quantity_kg`` NA and ``Derivation``
    NA.
    """
    if 'Quantity_kg' in harvest.columns:
        raise ValueError('W1-PH food_acquired already carries Quantity_kg')
    out = harvest.copy()
    t = np.asarray(out.index.get_level_values('t').astype(str))
    u = pd.Series(out.index.get_level_values('u').astype(str))
    card = u.map(_card_kg_by_label()).to_numpy(dtype=float)
    card = np.where(t == W1_PH_ROUND, card, np.nan)
    kg = derive_w1_card_weight_kg(out['Quantity'], card)
    out['Quantity_kg'] = kg
    derivation = pd.Series(pd.NA, index=out.index, dtype='string')
    derivation[np.isfinite(kg)] = CARD_WEIGHT_DERIVATION
    out['Derivation'] = derivation
    return out


def inputs_food_acquired_w1_card_weight(wave='2010-11'):
    """The registry `inputs` callable: raw sect10b_harvestw1 answers.

    One row per DERIVED served row, indexed ``(t, i, j, u, s)`` exactly as
    the served table is -- ``i`` and ``j`` decoded as the build decodes
    them (``j`` through ``harmonized_food_labels(W1_PH_ROUND)``, the same
    quarter-specific food decode as the served row, GH #1144), ``u``
    through the per-round unit decode -- with the source row's ORIGINAL
    variables as columns: ``hhid``, ``item_cd``, ``s10bq3a`` / ``s10bq3b`` /
    ``s10bq4`` (purchases), ``s10bq5a`` / ``s10bq5b`` (own production),
    ``s10bq6a`` / ``s10bq6b`` (gifts).  The source row repeats once per
    derived source.  The card weight itself is ``W1_PH_CARD_KG`` above.
    """
    if wave != '2010-11':
        raise ValueError(f'{CARD_WEIGHT_DERIVATION} is a 2010-11 derivation, not {wave!r}')
    import nigeria
    from lsms_library.local_tools import format_id, get_dataframe

    raw_vars = ['hhid', 'item_cd', 's10bq3a', 's10bq3b', W1_PH_EXPENDITURE,
                's10bq5a', 's10bq5b', 's10bq6a', 's10bq6b']
    # Match the build's cwd (make) for its relative raw-source path.
    # Both served rows and raw input keys use the same food quarter.
    with contextlib.chdir(_HERE):
        served = stamp_w1_card_weight(nigeria.food_acquired_for_wave(
            W1_PH_FOOD_FILE, W1_PH_ROUND, sources=W1_PH_SOURCES,
            expenditure=W1_PH_EXPENDITURE))
        food_labels = nigeria.harmonized_food_labels(W1_PH_ROUND)
        df = get_dataframe(W1_PH_FOOD_FILE)
    derived = served.index[served['Derivation'].notna()]
    unit_codes = nigeria.food_unit_labels(W1_PH_ROUND)

    i = df['hhid'].astype(int).astype(str)
    j = df['item_cd'].replace(food_labels)
    pieces = []
    for s, spec in W1_PH_SOURCES.items():
        u = df[spec['unit']].map(unit_codes)
        u = u.where(u.notna(), df[spec['unit']].map(format_id))
        idx = pd.MultiIndex.from_arrays(
            [np.full(len(df), W1_PH_ROUND), i.values, j.values, u.values,
             np.full(len(df), s)], names=['t', 'i', 'j', 'u', 's'])
        piece = df[raw_vars].copy()
        piece.index = idx
        pieces.append(piece[idx.isin(derived)])
    return pd.concat(pieces).sort_index()
