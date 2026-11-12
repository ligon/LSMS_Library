"""Missing country qualification is disclosed without changing explicit factors."""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import transformations as T


def crop(labels=('Alpha', 'Beta'), *, country='index'):
    n = len(labels)
    frame = pd.DataFrame({'country': list(labels), 't': ['w'] * n,
                          'i': ['household'] * n, 'plot_id': ['plot'] * n,
                          'j': ['Maize'] * n, 'u': ['Basket'] * n,
                          'Quantity': [2.] * n})
    keys = ['t', 'i', 'plot_id', 'j', 'u']
    if country == 'index':
        keys.insert(0, 'country')
    elif country == 'absent':
        frame = frame.drop(columns='country')
    frame = frame.set_index(keys)
    frame.attrs['country'] = 'StaleScalarCountry'
    return frame


def table(*, qualified=False, placement='columns', empty=False):
    frame = pd.DataFrame({'country': ['Alpha', 'Beta'],
                          'u': ['Basket', 'Basket'], 'KgFactor': [5., 20.],
                          'Source': ['Alpha table', 'Beta table']})
    keys = ['country', 'u'] if qualified else ['u']
    if not qualified:
        frame = frame.iloc[:1].drop(columns='country')
    if empty:
        frame = frame.iloc[:0]
    frame.attrs['note'] = 'caller-owned'
    return frame.set_index(keys) if placement == 'index' else frame


def caught_call(frame, factors):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        result = T.harvest_kg_factors(frame, shipped_factors=factors)
    shipped = [w for w in caught if issubclass(w.category, T.ShippedFactorWarning)]
    return result, shipped


@pytest.mark.parametrize('country', ['index', 'column'])
@pytest.mark.parametrize('placement', ['index', 'columns'])
@pytest.mark.parametrize('qualified', [False, True])
def test_actual_caller_preserves_explicit_values_sources_and_counts(country,
                                                                   placement,
                                                                   qualified):
    frame, factors = crop(country=country), table(qualified=qualified,
                                                placement=placement)
    before, original = frame.copy(deep=True), factors.copy(deep=True)
    result, caught = caught_call(frame, factors)
    expected = [5., 20.] if qualified else [5., 5.]
    np.testing.assert_array_equal(result.kg_per_unit, expected)
    np.testing.assert_array_equal(result.kg_shipped, expected)
    assert result.KgFactorSource.tolist() == ['shipped', 'shipped']
    assert result.kg_shipped_source.tolist() == (
        ['Alpha table', 'Beta table'] if qualified else ['Alpha table'] * 2)
    assert result.attrs['kg_factor_sources']['shipped_matched'] == 2
    assert result.attrs['kg_factor_sources']['shipped'] == 2
    assert len(caught) == (0 if qualified else 1)
    if not qualified:
        assert 'pooled' in str(caught[0].message)
        assert "no 'country' key" in str(caught[0].message)
    pd.testing.assert_frame_equal(frame, before)
    pd.testing.assert_frame_equal(factors, original)
    assert frame.attrs == before.attrs and factors.attrs == original.attrs


@pytest.mark.parametrize('country', ['index', 'column', 'absent'])
@pytest.mark.parametrize('placement', ['index', 'columns'])
def test_unpooled_and_countryless_frames_add_no_warning(country, placement):
    frame = crop(('Alpha', 'Alpha'), country=country)
    result, caught = caught_call(frame, table(placement=placement))
    assert not caught
    np.testing.assert_array_equal(result.kg_per_unit, [5., 5.])


@pytest.mark.parametrize('country', ['index', 'column'])
@pytest.mark.parametrize('placement', ['index', 'columns'])
@pytest.mark.parametrize('qualified', [False, True])
def test_empty_tables_do_not_add_pooled_warning(country, placement, qualified):
    result, caught = caught_call(crop(country=country),
                                table(qualified=qualified,
                                      placement=placement, empty=True))
    assert not caught
    assert result.kg_per_unit.isna().all()
    assert result.attrs['kg_factor_sources']['shipped_matched'] == 0


@pytest.mark.parametrize('index_labels,column_labels,pooled', [
    (['Alpha', 'Beta'], ['Alpha', 'Alpha'], True),
    (['Alpha', 'Alpha'], ['Alpha', 'Beta'], False),
])
@pytest.mark.parametrize('qualified', [False, True])
def test_index_identity_outranks_redundant_column_and_stale_attrs(index_labels,
                                                               column_labels,
                                                               pooled,
                                                               qualified):
    frame = crop(index_labels)
    frame['country'] = column_labels
    result, caught = caught_call(frame, table(qualified=qualified))
    assert len(caught) == int(pooled and not qualified)
    expected = [5., 20.] if pooled and qualified else [5., 5.]
    np.testing.assert_array_equal(result.kg_per_unit, expected)


@pytest.mark.parametrize('country', ['index', 'column'])
def test_warning_describes_offer_even_when_reported_outranks_it(country):
    frame = crop(country=country)
    frame['KgFactor'] = [2., 3.]
    result, caught = caught_call(frame, table())
    assert len(caught) == 1 and 'pooled' in str(caught[0].message)
    np.testing.assert_array_equal(result.kg_per_unit, [2., 3.])
    np.testing.assert_array_equal(result.kg_shipped, [5., 5.])
    assert result.KgFactorSource.tolist() == ['reported', 'reported']
    assert result.attrs['kg_factor_sources']['shipped'] == 0
    assert result.attrs['kg_factor_sources']['shipped_matched'] == 2


@pytest.mark.parametrize('duplicate', [5., 20.])
def test_ambiguity_refusal_precedes_warning_promoted_to_error(duplicate):
    factors = pd.DataFrame({'u': ['Basket', 'Basket'],
                            'KgFactor': [5., duplicate]})
    with warnings.catch_warnings():
        warnings.simplefilter('error', T.ShippedFactorWarning)
        with pytest.raises(ValueError, match='ambiguous'):
            T.harvest_kg_factors(crop(), shipped_factors=factors)


@pytest.mark.parametrize('country', ['index', 'column'])
def test_existing_dropped_key_diagnostic_and_new_scope_both_emit(country):
    factors = table()
    factors['region'] = 'one explicit region'
    result, caught = caught_call(crop(country=country), factors)
    assert len(caught) == 2
    assert 'DROPPED' in str(caught[0].message) and 'region' in str(caught[0].message)
    assert 'pooled' in str(caught[1].message)
    np.testing.assert_array_equal(result.kg_per_unit, [5., 5.])


@pytest.mark.parametrize('country', ['index', 'column'])
def test_existing_zero_match_diagnostic_and_new_scope_both_emit(country):
    factors = table()
    factors['u'] = 'Different basket'
    result, caught = caught_call(crop(country=country), factors)
    assert len(caught) == 2
    assert 'pooled' in str(caught[0].message)
    assert 'matched 0 of 2 rows' in str(caught[1].message)
    assert result.kg_per_unit.isna().all()
    assert result.attrs['kg_factor_sources']['shipped_matched'] == 0


def test_unusable_offer_preserves_usable_factor_diagnostic():
    factors = table()
    factors['KgFactor'] = 0.
    result, caught = caught_call(crop(), factors)
    assert len(caught) == 2
    assert 'pooled' in str(caught[0].message)
    assert 'usable' in str(caught[1].message)
    assert result.kg_per_unit.isna().all()


@pytest.mark.parametrize('labels,pooled', [
    (['Alpha', pd.NA], False), (['Alpha', 'Beta', pd.NA], True),
])
def test_lookup_counts_only_explicit_nonmissing_country_labels(labels, pooled):
    # Exercise the lookup directly; no new public missing-identity policy.
    frame = crop(labels)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        values, sources, matched = T._shipped_factor_lookup(frame, table())
    assert len(caught) == int(pooled)
    np.testing.assert_array_equal(values, [5.] * len(labels))
    assert sources.tolist() == ['Alpha table'] * len(labels)
    assert matched == len(labels)


def test_empty_input_adds_no_pooled_warning_and_preserves_zero_match_notice():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        values, sources, matched = T._shipped_factor_lookup(crop(()), table())
    assert len(caught) == 1 and 'matched 0 of 0 rows' in str(caught[0].message)
    assert len(values) == len(sources) == matched == 0
