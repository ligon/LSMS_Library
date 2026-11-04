# Formatting Functions for Nigeria GHS-Panel 2018-19 (W4)
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             '..', '..', '_'))
from nigeria import _C8_DERIVATION_KEY, _C8_DROP_DERIVATION_KEY, _c8_raw_inputs

# GH #834 derivation/deletion callables, registered in
# Nigeria/_/derivations.yml against THIS module (a wave-scoped
# `lsms_library.countries.Nigeria.<wave>._.mapping` path, not the country
# module).  The shared logic stays in Nigeria/_/nigeria.py; these wrappers
# only resolve the wave and call it.  NOTE (measured 2026-09-13): this does
# NOT shrink the cache blast radius to `community_prices` -- `Wave._input_hash`
# hashes every `.py` in a wave's `_/` dir (the `binp:` loop) into EVERY table
# that wave builds, and the country `_table_cache_hash` folds every wave in,
# so editing any of these files still re-warms all 24 Nigeria tables.  The
# wave-scope placement keeps the registry path off the country module on
# principle (AGENTS.md), but the per-table saving the Togo precedent implies
# is NOT realised by the current hasher.
_DERIVATION_KEY = _C8_DERIVATION_KEY
_DROP_KEY = _C8_DROP_DERIVATION_KEY


def _folder(wave):
    """Resolve a folder ('2015-16') or PH-quarter ('2016Q1') spelling to the
    wave folder."""
    from nigeria import Waves, wave_folder_map
    return wave if wave in Waves else wave_folder_map[wave]


def derive_c8_metric_package_unit_price(price, size_quantity):
    """Per-unit price of a C8 row whose recorded price is of a PACKAGE.

    Section C8 prices "a quantity of ONE (1) [ITEM]" in a stated unit and
    size; where the coded size (c8q2c: '34. 250 GRAMS') states a metric
    quantity the reported Naira figure is the price of that package, so the
    per-unit price is ``price / size_quantity`` with ``size_quantity`` in
    the base metric unit (g or cl).  Vectorised; NaN where the size quantity
    is missing or not positive (those rows are dropped by the caller, never
    served).
    """
    p = pd.to_numeric(pd.Series(price), errors='coerce')
    q = pd.to_numeric(pd.Series(size_quantity), errors='coerce')
    return (p / q.where(q > 0)).to_numpy()


def inputs_community_prices(wave='2018-19'):
    """The registry `inputs` callable: raw C8 rows for the derived cells.

    Raw surveyed fields -- cluster id, item, unit label, size label, size
    quantity, reported price -- one row per W4 C8 record whose coded size
    (c8q2c) names a metric quantity (exactly the rows whose served Price
    this derivation divides).
    """
    return _c8_raw_inputs(_folder(wave), keep='derived')


def inputs_c8_unrecorded_size_drop(wave):
    """The deletion entry's `inputs` callable: the DROPPED W4 rows -- none.

    W4 drops nothing under this rule.  The 1,191 rows on raw unit code 1
    ('1. KILOGRAMS (KG)') carry a blank coded size c8q2c, and the pre-#834
    prose described them as a dropped 'Grams(g)' unit -- but that spelling
    is not what this read of sectc8_harvestw4.dta yields, and the rows are
    in fact SERVED under `Kg` (a blank size beside KILOGRAMS reads as one
    kilogram, and every derived g row on the W4 side comes from a *coded*
    size, never from a blank one).  Returns an empty frame with the input
    columns.
    """
    return _c8_raw_inputs(_folder(wave), keep='dropped')


# ---------------------------------------------------------------------------
# GH #881 (ruling @ligon 2026-11-03, option A): the purchased row's WINDOW.
# ---------------------------------------------------------------------------
#
# W4 asks no 7-day purchase value.  Its purchase block (q8 "purchased in the
# past 30 days?", q9a/q9b/q9_cvn "quantity of the MOST RECENT purchase",
# q10 "amount paid for it") describes ONE purchase within the last 30 days,
# and `food_acquired_for_wave` serves it as the s='purchased' row -- beside
# produced / in-kind rows of the same (t, i, j) that are 7-day consumption
# (q6a / q7a), and beside every other wave's 7-day purchased row.  The
# numbers are the survey's own and are served unchanged; what is registered
# is the WINDOW (CLAUDE.md "Derived Values": "a value served under a name
# implying a different window -- the function is the identity and the
# registry is the point"), as Ethiopia does for its section-12 sales.  Option
# B -- a constructed 7-day Expenditure = q5a x (q10 / q9a) on the #876 EHCVM
# pattern -- was not taken.

W4_LAST_PURCHASE_KEY = 'Nigeria::food_acquired::w4-last-purchase-30d'

#: The two W4 food modules, as `food_acquired.py` reads them (cwd = this
#: directory), and the purchase-block variables the derived rows come from.
W4_FOOD = {
    '2018Q3': ('../Data/sect7b_plantingw4.csv', 's7bq'),
    '2019Q1': ('../Data/sect10b_harvestw4.csv', 's10bq'),
}
_W4_RAW = ('8', '9a', '9b', '9c', '9_cvn', '10', '5a', '2b', '2c', '2_cvn')


def w4_food_sources(prefix):
    """``food_acquired_for_wave`` sources for one W4 round (``s7bq`` /
    ``s10bq``): purchased from the 30-day purchase block, produced / in-kind
    from the 7-day consumption block in the consumption unit, each with the
    size column beside its unit (GH #1033)."""
    return {
        'purchased': {'quantity': f'{prefix}9a', 'unit': f'{prefix}9b',
                      'kg_factor': f'{prefix}9_cvn', 'size': f'{prefix}9c'},
        'produced':  {'quantity': f'{prefix}6a', 'unit': f'{prefix}2b',
                      'kg_factor': f'{prefix}2_cvn', 'size': f'{prefix}2c'},
        'inkind':    {'quantity': f'{prefix}7a', 'unit': f'{prefix}2b',
                      'kg_factor': f'{prefix}2_cvn', 'size': f'{prefix}2c'},
    }


def derive_w4_last_purchase_30d(quantity, expenditure, quantity_kg):
    """W4 purchased rows -> the ``Quantity`` / ``Expenditure`` /
    ``Quantity_kg`` they serve (GH #881): THE IDENTITY.

    ``Quantity`` = q9a, ``Expenditure`` = q10 and ``Quantity_kg`` = q9a x
    q9_cvn, exactly as recorded.  What the registered derivation records is
    the window: one most-recent purchase within the last 30 days, not 7-day
    consumption.  No options, by rule; the build does not call it, because
    there is nothing to compute.
    """
    return pd.DataFrame({'Quantity': quantity, 'Expenditure': expenditure,
                         'Quantity_kg': quantity_kg})


def stamp_w4_last_purchase_window(frame):
    """``Derivation`` = :data:`W4_LAST_PURCHASE_KEY` on every W4
    ``s='purchased'`` row of a built W4 frame; NA elsewhere.

    Applied to what ``food_acquired_for_wave`` returns -- AFTER its
    duplicate-key collapse -- so the stamp cannot ride a row a collapse later
    discards.  Values are not touched.
    """
    if 'Derivation' in frame.columns:
        raise ValueError('W4 food_acquired already carries a Derivation column')
    out = frame.copy()
    t = out.index.get_level_values('t').astype(str)
    s = out.index.get_level_values('s').astype(str)
    take = np.asarray(t.isin(list(W4_FOOD)) & (s == 'purchased'))
    derivation = pd.Series(pd.NA, index=out.index, dtype='string')
    derivation[take] = W4_LAST_PURCHASE_KEY
    out['Derivation'] = derivation
    return out


def inputs_food_acquired_w4_last_purchase(wave='2018-19'):
    """The registry ``inputs`` callable: the raw purchase-block answers.

    One row per DERIVED served row (both W4 rounds), indexed
    ``(t, i, j, u, s)`` exactly as the served table is -- the build itself
    is re-run to name the derived keys, and ``i`` / ``j`` / ``u`` decoded as
    it decodes them -- with the source row's ORIGINAL variables as columns:
    ``hhid``, ``item_cd`` and, per round (``s7bq*`` post-planting,
    ``s10bq*`` post-harvest; the other round's columns are NA), q8 (bought in
    the past 30 days?), q9a / q9b / q9c / q9_cvn (most recent purchase:
    quantity, unit, size, kg factor), q10 (amount paid), and q5a / q2b / q2c /
    q2_cvn (7-day consumption out of purchases, in the consumption unit and
    size) -- the last four being what a user needs to re-derive a 7-day value
    under option B.
    """
    if _folder(wave) != '2018-19':
        raise ValueError(f'{W4_LAST_PURCHASE_KEY} is a 2018-19 derivation, not {wave!r}')
    import contextlib
    import nigeria
    from lsms_library.local_tools import format_id, get_dataframe
    from lsms_library.transformations import U_UNKNOWN

    pieces = []
    with contextlib.chdir(os.path.dirname(os.path.abspath(__file__))):
        for t, (fn, p) in W4_FOOD.items():
            served = stamp_w4_last_purchase_window(nigeria.food_acquired_for_wave(
                fn, t, sources=w4_food_sources(p), expenditure=f'{p}10'))
            derived = served.index[served['Derivation'].notna()]
            df = get_dataframe(fn)
            raw_vars = ['hhid', 'item_cd'] + [p + v for v in _W4_RAW]
            labels = nigeria.harmonized_food_labels(t)
            unit_codes = nigeria.food_unit_labels(t)
            u = nigeria.sized_food_unit(df[f'{p}9b'], df[f'{p}9c'], unit_codes)
            u = u.where(u.notna(), df[f'{p}9b'].map(format_id)).fillna(U_UNKNOWN)
            idx = pd.MultiIndex.from_arrays(
                [np.full(len(df), t), df['hhid'].astype(int).astype(str).values,
                 df['item_cd'].replace(labels).values, u.values,
                 np.full(len(df), 'purchased')], names=['t', 'i', 'j', 'u', 's'])
            piece = df[raw_vars].copy()
            piece.index = idx
            pieces.append(piece[idx.isin(derived)])
    return pd.concat(pieces).sort_index()
