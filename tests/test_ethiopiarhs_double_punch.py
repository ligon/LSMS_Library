"""EthiopiaRHS food_acquired -- the data-entry double-punch (GH #323).

`food_acquired` is in ``feature.py``'s ``_ADDITIVE_MEASURE_COLUMNS``, so when
the canonical ``(t, i, j, u, s)`` index is non-unique core ALREADY SUMS Quantity
and Expenditure.  That is right for the 127 rows (1994a-1997) that are a genuine
second acquisition of the same item in the same unit from the same source, with
DIFFERENT measurements (1997 hh 10_93 produced Berbere 1.0 kg AND 30.0 kg).

It is wrong for a row punched twice.  The ERHS q36 module is a
one-row-per-food-item roster with NO line / transaction / occasion identifier
(all 27 source columns were enumerated; q36_1a is a section flag, not a line
number), so two rows equal on the canonical grain AND on both measures are one
measurement recorded twice -- and the sum DOUBLE-COUNTS them.  Across the five
roster waves: 90 rows, 514.9 Quantity and 343.1 Birr of pure overstatement.

``ethiopiarhs._drop_double_punched()`` removes them BEFORE core sums.  These
tests exercise that country-level helper directly -- they assert a property of
the EthiopiaRHS data and its hook, not of any core collapse mechanism.
"""
import importlib.util

import pandas as pd

from lsms_library.paths import countries_root


def _erhs():
    spec = importlib.util.spec_from_file_location(
        'erhs_mod', countries_root() / 'EthiopiaRHS' / '_' / 'ethiopiarhs.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_drops_double_punched_rows():
    """The 1989 smoking gun: food89.dta rows 865/866 are identical in EVERY
    source column (hh 20120, foodcode 34, qty 2.0, unit 9, value 0.5).  They are
    one measurement punched twice.  Core's sum reports 4.0 / 1.0 -- a pure
    overstatement.  The hook must collapse them to one row.
    """
    erhs = _erhs()
    punched = pd.DataFrame({
        'i': ['20120', '20120'],
        'j': ['Fenugreek (Abish)', 'Fenugreek (Abish)'],
        'u': ['9', '9'],
        's': ['purchased', 'purchased'],
        'Quantity': [2.0, 2.0],
        'Expenditure': [0.5, 0.5],
    })
    out = erhs._drop_double_punched(punched)
    assert len(out) == 1, 'byte-identical double-punch was not dropped'
    assert out['Quantity'].iloc[0] == 2.0
    assert out['Expenditure'].iloc[0] == 0.5


def test_keeps_rows_that_differ_in_measurement():
    """The other half: rows that DIFFER are real repeat acquisitions (1997 hh
    10_93 produced Berbere 1.0 kg AND 30.0 kg).  The dedup must NOT touch them
    -- core's sum correctly folds them together."""
    erhs = _erhs()
    real = pd.DataFrame({
        'i': ['10_93', '10_93'],
        'j': ['Berbere', 'Berbere'],
        'u': ['kg', 'kg'],
        's': ['produced', 'produced'],
        'Quantity': [1.0, 30.0],
        'Expenditure': [float('nan'), float('nan')],
    })
    out = erhs._drop_double_punched(real)
    assert len(out) == 2, 'real repeat measurements were destroyed'


def test_dedup_is_keyed_on_the_canonical_grain_plus_both_measures():
    """A row differing on ANY of (i, j, u, s, Quantity, Expenditure) survives.

    Guards the subset: dropping a measure from the key would silently discard
    real repeat acquisitions; dropping a grain level would merge distinct items.
    """
    erhs = _erhs()
    base = dict(i='1', j='Teff', u='kg', s='purchased',
                Quantity=1.0, Expenditure=2.0)
    for field, other in [('i', '2'), ('j', 'Maize'), ('u', 'g'),
                         ('s', 'produced'), ('Quantity', 9.0),
                         ('Expenditure', 9.0)]:
        rows = pd.DataFrame([base, {**base, field: other}])
        out = erhs._drop_double_punched(rows)
        assert len(out) == 2, f'rows differing only in {field!r} were merged'
