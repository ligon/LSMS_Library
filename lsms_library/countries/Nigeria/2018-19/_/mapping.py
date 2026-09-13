# Formatting Functions for Nigeria GHS-Panel 2018-19 (W4)
import os
import sys

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
