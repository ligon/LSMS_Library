"""Unit tests for the ``units=`` kwarg on
:func:`lsms_library.transformations.food_prices_from_acquired` and
:func:`lsms_library.transformations.food_quantities_from_acquired`.

Design doc: ``slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org``.
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    food_prices_from_acquired,
    food_quantities_from_acquired,
)


@pytest.fixture
def synthetic_food_acquired():
    """Five-row food_acquired covering kg-convertible, inferred-factor,
    LCU-only (``u='Value'``), unconvertible (``u='tin'``), and
    survey-reported-Price (``s='produced'``) cases."""
    idx = pd.MultiIndex.from_tuples(
        [
            ('2020', 'C1', 'H1', 'rice',  'kg',        'purchased'),
            ('2020', 'C1', 'H1', 'rice',  '50kg sack', 'purchased'),
            ('2020', 'C1', 'H2', 'meals', 'Value',     'purchased'),
            ('2020', 'C1', 'H1', 'maize', 'tin',       'purchased'),
            ('2020', 'C1', 'H1', 'maize', 'kg',        'produced'),
        ],
        names=['t', 'v', 'i', 'j', 'u', 's'],
    )
    return pd.DataFrame(
        {
            'Quantity':    [10.0, 1.0, 50.0, 5.0, 8.0],
            'Expenditure': [200.0, 800.0, 50.0, 100.0, np.nan],
            'Price':       [np.nan, np.nan, np.nan, np.nan, 30.0],
        },
        index=idx,
    )


# ----- food_prices: invalid units ------------------------------------------

def test_food_prices_invalid_units_raises(synthetic_food_acquired):
    with pytest.raises(ValueError, match="units="):
        food_prices_from_acquired(synthetic_food_acquired, units='bogus')


# ----- food_prices: kgvalue (default, backward-compat) ---------------------

def test_food_prices_kgvalue_default_is_kgvalue(synthetic_food_acquired):
    out_default = food_prices_from_acquired(synthetic_food_acquired)
    out_explicit = food_prices_from_acquired(synthetic_food_acquired, units='kgvalue')
    pd.testing.assert_frame_equal(out_default, out_explicit)


def test_food_prices_kgvalue_drops_lcu_rows(synthetic_food_acquired):
    """u='Value' rows have no kg factor, so kgvalue NaNs them out."""
    out = food_prices_from_acquired(synthetic_food_acquired, units='kgvalue')
    assert 'Value' not in out.index.get_level_values('u')


# ----- food_prices: unitvalue ----------------------------------------------

def test_food_prices_unitvalue_native_per_unit(synthetic_food_acquired):
    out = food_prices_from_acquired(synthetic_food_acquired, units='unitvalue')
    # rice/kg: 200/10 = 20
    rice_kg = out.xs(('rice', 'kg'), level=['j', 'u'])['Price'].iloc[0]
    assert rice_kg == pytest.approx(20.0)
    # rice/50kg sack: 800/1 = 800 (per sack)
    rice_sack = out.xs(('rice', '50kg sack'), level=['j', 'u'])['Price'].iloc[0]
    assert rice_sack == pytest.approx(800.0)


def test_food_prices_unitvalue_kwacha_per_kwacha(synthetic_food_acquired):
    """Designed sentinel: u='Value' rows give Price=1 (Kwacha per Kwacha)."""
    out = food_prices_from_acquired(synthetic_food_acquired, units='unitvalue')
    val_rows = out[out.index.get_level_values('u') == 'Value']
    assert len(val_rows) == 1
    assert val_rows['Price'].iloc[0] == pytest.approx(1.0)


# ----- food_prices: unitprice ----------------------------------------------

def test_food_prices_unitprice_returns_reported_only(synthetic_food_acquired):
    """unitprice keeps only rows where the survey recorded Price."""
    out = food_prices_from_acquired(synthetic_food_acquired, units='unitprice')
    # Only one row (maize/kg/produced, Price=30) was reported
    assert len(out) == 1
    assert out['Price'].iloc[0] == pytest.approx(30.0)


def test_food_prices_unitprice_no_price_column_returns_empty(synthetic_food_acquired):
    df = synthetic_food_acquired.drop(columns=['Price'])
    out = food_prices_from_acquired(df, units='unitprice')
    assert out.empty
    assert list(out.columns) == ['Price']


def test_food_prices_unitprice_does_not_fall_back_to_unitvalue(synthetic_food_acquired):
    """Explicitly: missing Price → NaN, not silent fallback to E/Q."""
    out_unitprice = food_prices_from_acquired(synthetic_food_acquired, units='unitprice')
    out_unitvalue = food_prices_from_acquired(synthetic_food_acquired, units='unitvalue')
    assert len(out_unitprice) < len(out_unitvalue)


# ----- food_prices: kgprice -------------------------------------------------

def test_food_prices_kgprice_converts_reported_price(synthetic_food_acquired):
    """kgprice = reported Price / kg_per_unit.  Reported maize/kg/produced
    Price=30, kg_per_unit=1 → kgprice=30."""
    out = food_prices_from_acquired(synthetic_food_acquired, units='kgprice')
    assert len(out) == 1
    assert out['Price'].iloc[0] == pytest.approx(30.0)


# ----- food_quantities: invalid units --------------------------------------

def test_food_quantities_invalid_units_raises(synthetic_food_acquired):
    with pytest.raises(ValueError, match="units="):
        food_quantities_from_acquired(synthetic_food_acquired, units='bogus')


# ----- food_quantities: kgs (default, with carry rule) ---------------------

def test_food_quantities_kgs_default_is_kgs(synthetic_food_acquired):
    out_default = food_quantities_from_acquired(synthetic_food_acquired)
    out_explicit = food_quantities_from_acquired(synthetic_food_acquired, units='kgs')
    pd.testing.assert_frame_equal(out_default, out_explicit)


def test_food_quantities_kgs_carries_lcu_rows(synthetic_food_acquired):
    """Carry rule: u='Value' rows are NOT dropped from kgs mode."""
    out = food_quantities_from_acquired(synthetic_food_acquired, units='kgs')
    val_rows = out[out.index.get_level_values('u') == 'Value']
    assert len(val_rows) == 1
    assert val_rows['Quantity'].iloc[0] == pytest.approx(50.0)


def test_food_quantities_kgs_carries_unconvertible_units(synthetic_food_acquired):
    """Carry rule: any u lacking a kg factor (not just 'Value') survives."""
    # Pure-tin synthetic: no rice rows to anchor inference.
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'C1', 'H1', 'maize', 'tin', 'purchased')],
        names=['t', 'v', 'i', 'j', 'u', 's'],
    )
    df = pd.DataFrame(
        {'Quantity': [5.0], 'Expenditure': [100.0]},
        index=idx,
    )
    out = food_quantities_from_acquired(df, units='kgs')
    assert len(out) == 1
    assert out.index.get_level_values('u')[0] == 'tin'
    assert out['Quantity'].iloc[0] == pytest.approx(5.0)


def test_food_quantities_kgs_tags_converted_rows_as_kg(synthetic_food_acquired):
    out = food_quantities_from_acquired(synthetic_food_acquired, units='kgs')
    # rice should land in 'kg' bucket (via inferred factor for sack)
    rice_kg = out.xs(('rice', 'kg'), level=['j', 'u'])
    assert len(rice_kg) >= 1


# ----- food_quantities: units mode -----------------------------------------

def test_food_quantities_units_preserves_native_u(synthetic_food_acquired):
    out = food_quantities_from_acquired(synthetic_food_acquired, units='units')
    units_seen = set(out.index.get_level_values('u'))
    # Should include all original u values (none converted to 'kg')
    expected = {'kg', '50kg sack', 'Value', 'tin'}
    assert expected.issubset(units_seen)


def test_food_quantities_units_no_kg_conversion(synthetic_food_acquired):
    """In units mode, '50kg sack' is NOT collapsed into 'kg'."""
    out = food_quantities_from_acquired(synthetic_food_acquired, units='units')
    sack_rows = out[out.index.get_level_values('u') == '50kg sack']
    assert len(sack_rows) == 1
    assert sack_rows['Quantity'].iloc[0] == pytest.approx(1.0)


# ----- backward compatibility: kgvalue/kgs still work without u index ------

def test_food_prices_kgvalue_without_u_handles_gracefully():
    """Pre-Phase-4 callers may pass a frame without a 'u' index level."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'C1', 'H1', 'rice'), ('2020', 'C1', 'H1', 'maize')],
        names=['t', 'v', 'i', 'j'],
    )
    df = pd.DataFrame(
        {'Quantity': [10.0, 5.0], 'Expenditure': [200.0, 100.0]},
        index=idx,
    )
    # Without 'u', _apply_kg_conversion is a no-op → Quantity_kg absent.
    # Code should either degrade gracefully or surface a clear error.
    with pytest.raises((KeyError, ValueError)):
        food_prices_from_acquired(df, units='kgvalue')


def test_food_quantities_units_without_u_index():
    """When u is absent from the index, fall back to a single-bucket
    aggregation rather than crashing."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'C1', 'H1', 'rice'), ('2020', 'C1', 'H1', 'maize')],
        names=['t', 'v', 'i', 'j'],
    )
    df = pd.DataFrame(
        {'Quantity': [10.0, 5.0], 'Expenditure': [200.0, 100.0]},
        index=idx,
    )
    out = food_quantities_from_acquired(df)
    assert 'Quantity' in out.columns
    assert len(out) == 2
