# Formatting Functions for Nigeria GHS-Panel 2015-16 (W3)
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

    W3's size is the FREE-TEXT c8q2b, blank on 13,767 of the 13,768
    metric-unit rows; the handful stating a quantity ('300gram', 'one 75cl
    bottle', 'one 450g tin') parse to a package size, so the reported Naira
    figure is the price of that package and the per-unit price is
    ``price / size_quantity`` in the base metric unit (g or cl).  The rest
    are DROPPED (see `Nigeria::community_prices::c8-unrecorded-size-drop`).
    Vectorised; NaN where the size quantity is missing or not positive
    (those rows are dropped by the caller, never served).
    """
    p = pd.to_numeric(pd.Series(price), errors='coerce')
    q = pd.to_numeric(pd.Series(size_quantity), errors='coerce')
    return (p / q.where(q > 0)).to_numpy()


def inputs_community_prices(wave='2015-16'):
    """The registry `inputs` callable: raw C8 rows for the derived cells.

    Raw surveyed fields -- cluster id, item, unit label, size text, parsed
    size quantity, reported price -- one row per W3 C8 record whose free-text
    size stated a metric quantity (exactly the rows whose served Price this
    derivation divides).
    """
    return _c8_raw_inputs(_folder(wave), keep='derived')


def inputs_c8_unrecorded_size_drop(wave):
    """The deletion entry's `inputs` callable: the DROPPED W3 rows.

    One row per W3 C8 record whose unit is metric (g / cl / Kg / l) but
    whose free-text size c8q2b records no quantity: 13,767 of the 13,768
    g/cl rows (the 13,768th parses '50cl' and is derived, not dropped),
    plus all 2,597 Kg and 836 l rows -- the same blank-c8q2b rule,
    previously uncounted anywhere.  A price of an unrecorded package is not
    a per-unit price, so the build drops these rather than serving them
    mis-scaled (~189x/~42x measured); a dropped row has nowhere to carry a
    Derivation key, so the registry entry carries `rows: 0` and these are
    its inputs.
    """
    return _c8_raw_inputs(_folder(wave), keep='dropped')
