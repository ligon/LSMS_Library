"""GH #1452: a wave a survey's unit-weight table did not reach keeps the estimate it had.

Synthetic, no data.  The table's own rows are tested in ``test_ehcvm_unit_weights.py``; the
cold comparison against development is in the PR.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lsms_library import ehcvm, transformations as T

OLD, NEW = '2014', '2018-19'
KEY = 'Mali::food_acquired::' + ehcvm.NSU_DERIVATION_NAME


def _frame(key=KEY):
    """Two waves of one item at 100 a kilo.  A `Tas` prices at 2 kg in the older wave and
    5 kg in the newer, where a table weighs it at 3 kg."""
    rows = []
    for t, tas_price in ((OLD, 200.0), (NEW, 500.0)):
        for h in range(12):
            rows.append((t, f'h{h}', 'Riz', 'Kg', 'purchased', 1.0, 100.0, np.nan, None))
            kg = 3.0 if t == NEW else np.nan
            rows.append((t, f'h{h}', 'Riz', 'Tas', 'purchased', 1.0, tas_price, kg,
                         key if t == NEW else None))
    df = pd.DataFrame(rows, columns=['t', 'i', 'j', 'u', 's', 'Quantity', 'Expenditure',
                                     'Quantity_kg', 'Derivation'])
    df['Derivation'] = df['Derivation'].astype('string')
    return df.set_index(['t', 'i', 'j', 'u', 's'])


def _wave(out, t):
    return out[out.index.get_level_values('t') == t]


def _never_wired():
    return _frame().assign(Quantity_kg=np.nan, Derivation=pd.NA)


def test_the_name_is_the_one_the_hook_stamps():
    assert ehcvm.NSU_DERIVATION_NAME in T._KG_WAVE_SCOPED_TABLES


def test_an_older_row_reads_what_it_read_with_the_table_never_wired():
    got = _wave(T.food_kg_factors(_frame()), OLD)
    want = _wave(T.food_kg_factors(_never_wired()), OLD)
    pd.testing.assert_frame_equal(got, want)
    tas = got[got.index.get_level_values('u') == 'Tas']
    # (2 + 5) / 2: both waves' cells, which is what taking the table's rows out restores.
    assert tas['kg_per_unit'].iloc[0] == pytest.approx(3.5)


def test_without_the_scope_the_older_row_estimates_from_its_own_wave_alone(monkeypatch):
    """What this exists to stop, so a later change that drops the scope is seen."""
    monkeypatch.setattr(T, '_KG_WAVE_SCOPED_TABLES', ())
    tas = _wave(T.food_kg_factors(_frame()), OLD)
    tas = tas[tas.index.get_level_values('u') == 'Tas']
    assert tas['kg_per_unit'].iloc[0] == pytest.approx(2.0)


def test_a_table_wave_is_as_built(monkeypatch):
    got = _wave(T.food_kg_factors(_frame()), NEW)
    monkeypatch.setattr(T, '_KG_WAVE_SCOPED_TABLES', ())
    want = _wave(T.food_kg_factors(_frame()), NEW)
    pd.testing.assert_frame_equal(got, want)
    assert (got[got.index.get_level_values('u') == 'Tas']['KgFactorSource'] == 'survey_kg').all()


def test_another_survey_kilogram_keeps_its_vote():
    """Malawi's and Nigeria's own kilograms are not this table: nothing moves for them."""
    frame = _frame(key='Malawi::food_acquired::cfactor')
    assert T._rows_older_than_their_table(frame) == (None, None)
    tas = _wave(T.food_kg_factors(frame), OLD)
    tas = tas[tas.index.get_level_values('u') == 'Tas']
    assert tas['kg_per_unit'].iloc[0] == pytest.approx(2.0)


def test_a_country_with_no_older_wave_gets_one_estimate():
    frame = _wave(_frame(), NEW)
    assert T._rows_older_than_their_table(frame) == (None, None)


def test_a_frame_without_the_columns_is_left_alone():
    assert T._rows_older_than_their_table(_frame().drop(columns='Derivation')) == (None, None)
    assert T._rows_older_than_their_table(_frame().drop(columns='Quantity_kg')) == (None, None)


def test_a_subset_is_a_slice_of_the_whole():
    whole = _wave(T.food_kg_factors(_frame()), OLD)
    lone = T.food_kg_factors(_frame(), serve_waves=[OLD])
    pd.testing.assert_series_equal(lone['kg_per_unit'], whole['kg_per_unit'])


@pytest.mark.parametrize('key', [
    KEY + '-other',
    'Mali::other_table::' + ehcvm.NSU_DERIVATION_NAME,
    ehcvm.NSU_DERIVATION_NAME + '::food_acquired::another-rule',
    'not-a-registry-key-' + ehcvm.NSU_DERIVATION_NAME,
])
def test_only_the_exact_food_derivation_is_scoped(key):
    assert T._rows_older_than_their_table(_frame(key=key)) == (None, None)


@pytest.mark.parametrize('keys', [
    KEY + '+Mali::food_acquired::last-purchase',
    'Mali::food_acquired::last-purchase+' + KEY,
])
def test_a_union_keeps_the_exact_scoped_derivation(keys):
    got = T.food_kg_factors(_frame(key=keys))
    want = T.food_kg_factors(_frame())
    pd.testing.assert_frame_equal(got, want)


@pytest.mark.requires_s3
@pytest.mark.parametrize('country,wave', [('Burkina_Faso', '2014'), ('Mali', '2014-15')])
def test_source_older_factors_equal_the_no_nsu_estimate(country, wave):
    import lsms_library as ll
    c = ll.Country(country)
    frame = c._aggregate_wave_data(None, 'food_acquired')
    keys = frame['Derivation'].fillna('').str.split('+', regex=False)
    table_rows = keys.map(lambda values: ehcvm.nsu_derivation_key(country) in values)
    assert table_rows.any()
    tableless = frame.assign(Quantity_kg=frame['Quantity_kg'].mask(table_rows))
    kwargs = dict(unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
    got = T.food_kg_factors(frame, serve_waves=[wave], **kwargs)
    expected = T.food_kg_factors(tableless, serve_waves=[wave], **kwargs)
    assert len(got) > 10000
    # Keep all audit columns, indexes and dtypes, not only the winning factor.
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    for transform, units in [(T.food_quantities_from_acquired, 'kgs'),
                             (T.food_prices_from_acquired, 'kgvalue')]:
        actual = transform(frame, units=units, serve_waves=[wave], **kwargs)
        prior = transform(tableless, units=units, serve_waves=[wave], **kwargs)
        pd.testing.assert_frame_equal(actual, prior, check_exact=True)


@pytest.mark.parametrize('country,wave,dependency', [
    (country, wave, dependency)
    for country in ehcvm.NSU_COUNTRIES
    for wave in ('2018-19', '2021-22')
    for dependency in (('table', 'labels') if wave == '2018-19' else ('table',))
])
def test_declared_dependency_moves_food_hash_but_not_shocks(monkeypatch, country, wave, dependency):
    import importlib
    import lsms_library as ll
    from lsms_library.paths import countries_root
    module = importlib.import_module('lsms_library.country')
    c = ll.Country(country)
    w = c[wave]
    before_food, before_shocks = w._input_hash('food_acquired'), w._input_hash('shocks')
    filename = (ehcvm.NSU_TABLES[country]['table'] if dependency == 'table'
                else ehcvm.NSU_TABLES[country]['sources']['2021-22'])
    source = (countries_root() / country / '2021-22' / 'Data' / filename).resolve()
    fingerprint = module.source_fingerprint
    seen = []

    def changed(path):
        result = fingerprint(path)
        if path.resolve() == source:
            seen.append(path)
            return result + ':1452-source-change-control'
        return result

    monkeypatch.setattr(module, 'source_fingerprint', changed)
    assert w._input_hash('food_acquired') != before_food
    assert seen
    assert w._input_hash('shocks') == before_shocks
