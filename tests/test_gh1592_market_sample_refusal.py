"""GH1592: do not promise a cluster fallback without an existing v relation.

All providers are invented; borrow actual methods without constructing Country.
"""
from copy import deepcopy
import warnings

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from dvc.exceptions import DvcException
from lsms_library.country import Country, SampleUnavailableWarning
from lsms_library.null_index_audit import (
    NullIndexKeyError, NullIndexKeyWarning, _clear_null_index_reports,
)


class MarketFixture:
    name = 'InventedMarket1592'
    data_scheme = {'sample': {}, 'cluster_features': {}}
    _add_market_index = Country._add_market_index
    _join_v_from_sample = Country._join_v_from_sample
    _report_unlabelled_markets = Country._report_unlabelled_markets

    def __init__(self, sample):
        self.sample_value = sample
        self.sample_calls = self.lookup_calls = 0

    def sample(self):
        self.sample_calls += 1
        if isinstance(self.sample_value, Exception):
            raise self.sample_value
        return self.sample_value.copy(deep=True)

    def _market_lookup(self, column='Region'):
        self.lookup_calls += 1
        return pd.DataFrame({'t': ['roundA', 'roundB'],
                             'v': ['cluster5', 'cluster5'],
                             'm': ['ClusterA', 'ClusterB']})


def rows(v=None):
    result = pd.DataFrame({'Quantity': pd.array([7., 8., 9., 10.], dtype='Float64')},
        index=pd.MultiIndex.from_tuples(
            [('roundA', 'house1', 'item1'), ('roundA', 'house1', 'item2'),
             ('roundB', 'house1', 'item1'), ('roundB', 'house1', 'item2')],
            names=['t', 'i', 'j']))
    if v is not None:
        result['v'] = pd.array(['cluster5'] * 4, dtype='string')
        if v == 'index':
            result = result.set_index('v', append=True)
    result.attrs = {'id_converted': True, 'example': {'preserve': [1, 'typed']}}
    return result


def sample(*, region=True, cluster=True):
    result = pd.DataFrame(index=pd.MultiIndex.from_tuples(
        [('house1', 'roundA'), ('house1', 'roundB')], names=['i', 't']))
    if region:
        result['Region'] = pd.array(['HouseholdA', 'HouseholdB'], dtype='string')
    if cluster:
        result['v'] = pd.array(['cluster5', 'cluster5'], dtype='string')
    return result


def availability(kind):
    return {'file': FileNotFoundError, 'os': OSError, 'dvc': DvcException}[kind](
        'invented missing sample cause')


@pytest.fixture(autouse=True)
def clear_reports(monkeypatch):
    monkeypatch.delenv('LSMS_READ_STRICT', raising=False)
    _clear_null_index_reports()
    yield
    _clear_null_index_reports()


@pytest.mark.parametrize('kind', ['file', 'os', 'dvc'])
def test_missing_sample_without_v_preserves_cause_before_false_warning(kind):
    error = availability(kind)
    fixture = MarketFixture(error)
    frame = rows()
    before, attrs = frame.copy(deep=True), deepcopy(frame.attrs)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        with pytest.raises(type(error)) as raised:
            fixture._add_market_index(frame, column='Region', table='invented')
    assert raised.value is error
    assert not caught
    assert fixture.sample_calls == 1
    assert fixture.lookup_calls == 0
    assert_frame_equal(frame, before, check_exact=True)
    assert frame.attrs == attrs


@pytest.mark.parametrize('kind', ['file', 'os', 'dvc'])
@pytest.mark.parametrize('placement', ['index', 'column'])
def test_existing_v_has_honest_full_time_cluster_fallback(kind, placement):
    fixture = MarketFixture(availability(kind))
    frame = rows(placement)
    before, attrs = frame.copy(deep=True), deepcopy(frame.attrs)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = fixture._add_market_index(frame, column='Region', table='invented')
    assert out.index.get_level_values('m').tolist() == ['ClusterA']*2 + ['ClusterB']*2
    assert len(out) == len(frame) and out.index.is_unique
    assert out.Quantity.tolist() == frame.Quantity.tolist() and out.attrs == attrs
    assert fixture.sample_calls == fixture.lookup_calls == 1
    source_warnings = [w for w in caught if issubclass(w.category, SampleUnavailableWarning)]
    assert len(source_warnings) == 1
    assert 'attempting' in str(source_warnings[0].message)
    assert 'invented missing sample cause' in str(source_warnings[0].message)
    assert_frame_equal(frame, before, check_exact=True)
    assert frame.attrs == attrs


@pytest.mark.parametrize('cluster', [False, True])
def test_healthy_household_labels_win_without_cluster_lookup(cluster):
    fixture = MarketFixture(sample(cluster=cluster))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = fixture._add_market_index(rows(), table='invented')
    assert out.index.get_level_values('m').tolist() == ['HouseholdA']*2 + ['HouseholdB']*2
    assert fixture.sample_calls == 1 and fixture.lookup_calls == 0 and not caught
    assert out.attrs == rows().attrs


def test_sample_v_tall_rows_use_same_time_keys_without_multiplication():
    fixture = MarketFixture(sample(region=False))
    out = fixture._add_market_index(rows(), table='invented')
    assert len(out) == 4 and out.index.is_unique
    assert out.index.get_level_values('m').tolist() == ['ClusterA']*2 + ['ClusterB']*2
    assert out.Quantity.tolist() == rows().Quantity.tolist()


@pytest.mark.parametrize('error', [ValueError('guard'), KeyError('guard'),
                                   AttributeError('guard'), RuntimeError('guard')])
@pytest.mark.parametrize('placement', [None, 'index'])
def test_sample_refusal_identity_still_propagates_without_fallback(error, placement):
    fixture = MarketFixture(error)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        with pytest.raises(type(error)) as raised:
            fixture._add_market_index(rows(placement), table='invented')
    assert raised.value is error and not caught
    assert fixture.sample_calls == 1 and fixture.lookup_calls == 0


def test_partial_household_labels_keep_priority_over_cluster_labels():
    own = sample()
    own.loc[('house1', 'roundB'), 'Region'] = pd.NA
    fixture = MarketFixture(own)
    out = fixture._add_market_index(rows(), table='invented')
    assert out.index.get_level_values('m').tolist() == ['HouseholdA']*2 + ['ClusterB']*2
    assert len(out) == 4 and out.index.is_unique


@pytest.mark.parametrize('strict', [False, True])
def test_existing_missing_cluster_labels_keep_current_na_market_contract(monkeypatch, strict):
    fixture = MarketFixture(availability('file'))
    frame = rows('index').reset_index('v')
    frame.loc[frame.index.get_level_values('t') == 'roundB', 'v'] = pd.NA
    if strict:
        monkeypatch.setenv('LSMS_READ_STRICT', '1')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        if strict:
            with pytest.raises(NullIndexKeyError):
                fixture._add_market_index(frame, table='invented')
        else:
            out = fixture._add_market_index(frame, table='invented')
            assert len(out) == 4 and out.index.get_level_values('m').isna().sum() == 2
            assert any(issubclass(w.category, NullIndexKeyWarning) for w in caught)
    assert fixture.sample_calls == fixture.lookup_calls == 1


def test_returned_sample_without_v_and_label_remains_separate_shape_boundary():
    fixture = MarketFixture(sample(region=False, cluster=False))
    with pytest.warns(SampleUnavailableWarning, match='no `v`'):
        with pytest.raises(KeyError, match='v.*not in index'):
            fixture._add_market_index(rows(), table='invented')
    assert fixture.sample_calls == 2 and fixture.lookup_calls == 0
