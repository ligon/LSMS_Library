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


@pytest.mark.parametrize('units', ['unitprice', 'kgprice'])
def test_food_prices_price_mode_without_price_column_raises(
        synthetic_food_acquired, units):
    """GH #828: no ``Price`` column means the instrument records no unit
    price.  That used to come back as an empty ``(0, 1)`` frame -- the absence
    of the column dressed as the absence of rows -- and is now a typed error
    naming the basis and the missing column (a ``ValueError`` subclass)."""
    from lsms_library.errors import UnitsUnavailableError
    df = synthetic_food_acquired.drop(columns=['Price'])
    with pytest.raises(UnitsUnavailableError, match="Price") as info:
        food_prices_from_acquired(df, units=units)
    assert isinstance(info.value, ValueError)
    assert info.value.units == units
    assert info.value.missing == ['Price']
    assert info.value.country is None
    named = info.value.for_country('Someland')
    assert named.country == 'Someland' and 'Someland' in str(named)


@pytest.mark.parametrize('units', ['kgvalue', 'unitvalue'])
def test_food_prices_value_mode_without_expenditure_raises_typed(
        synthetic_food_acquired, units):
    """The ``*value`` modes already raised ``ValueError`` for a missing
    ``Expenditure``; it is now the same typed error (still a ValueError)."""
    from lsms_library.errors import UnitsUnavailableError
    df = synthetic_food_acquired.drop(columns=['Expenditure'])
    with pytest.raises(UnitsUnavailableError, match="Expenditure"):
        food_prices_from_acquired(df, units=units)


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


# ----- Country-level re-raise / fall-through (GH #828) ---------------------
#
# The transform-level tests above stop at food_prices_from_acquired.  These
# exercise the `except UnitsUnavailableError` branch in the generated
# Country.food_prices method: it must NAME the country and re-raise whenever
# the caller asked for something the legacy path cannot honour (units=, a
# labels= dict), and fall through to the legacy aggregation only on a bare
# call.  _aggregate_wave_data is monkeypatched, so no data is read.

_LEGACY_SENTINEL = pd.DataFrame({'Price': [1.0]},
                                index=pd.Index(['legacy'], name='i'))


@pytest.fixture
def country_without(monkeypatch, synthetic_food_acquired):
    """Return ``make(drop)``: a ``Country('Malawi')`` whose food_acquired lacks
    the columns in *drop*; any other table (the legacy food_prices
    aggregation) comes back as ``_LEGACY_SENTINEL``."""
    import lsms_library as ll
    from lsms_library.country import Country

    def make(drop):
        fa = synthetic_food_acquired.drop(columns=drop)

        def fake_aggregate(self, waves, table, *args, **kwargs):
            if table == 'food_acquired':
                return fa.copy()
            return _LEGACY_SENTINEL.copy()

        monkeypatch.setattr(Country, '_aggregate_wave_data', fake_aggregate)
        monkeypatch.setattr(Country, '_unit_kg_factors', lambda self: {})
        c = ll.Country('Malawi')
        assert 'food_acquired' in c.data_scheme
        return c

    return make


@pytest.mark.parametrize('units', ['unitprice', 'kgprice'])
def test_country_food_prices_names_the_country(country_without, units):
    from lsms_library.errors import UnitsUnavailableError
    c = country_without(['Price'])
    with pytest.raises(UnitsUnavailableError) as info:
        c.food_prices(units=units)
    assert info.value.country == 'Malawi'
    assert info.value.units == units
    assert info.value.missing == ['Price']
    assert str(info.value).startswith('Malawi: ')


def test_country_food_prices_bare_call_falls_through(country_without):
    """units=None (default kgvalue) on a food_acquired with no Expenditure:
    the pre-#828 legacy fall-through is kept for a bare call."""
    c = country_without(['Expenditure'])
    out = c.food_prices()
    pd.testing.assert_frame_equal(out, _LEGACY_SENTINEL)


def test_country_food_prices_labels_dict_does_not_fall_through(country_without):
    """A labels= dict cannot be honoured by the legacy path, so the refusal
    re-raises even with units=None -- the strictness the generic
    ``except ValueError`` handler already had before #828."""
    from lsms_library.errors import UnitsUnavailableError
    c = country_without(['Expenditure'])
    with pytest.raises(UnitsUnavailableError) as info:
        c.food_prices(labels={'Rural': 'Settlement'})
    assert info.value.country == 'Malawi'
    assert info.value.missing == ['Expenditure']
