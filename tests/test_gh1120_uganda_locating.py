"""Approved own-cover weights, explicit membership and epistemic identities."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def ug():
    path = Path(__file__).parents[1] / 'lsms_library/countries/Uganda/_/uganda.py'
    spec = importlib.util.spec_from_file_location('uganda1120', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def covers():
    return pd.DataFrame({
        't': ['2015-16'] * 4, 'i': ['primary', 'primary-null', 'ag', 'ag-zero'],
        '_cover': ['primary', 'primary-null', None, None],
        '_ag_member': ['primary', 'primary-null', 'ag', 'ag-zero'],
        'v': ['11', None, None, None], '_ag_v': [11., 22., 33., 44.],
        'weight': pd.array([10., None, None, None], dtype='Float64'),
        'panel_weight': pd.array([2., None, None, None], dtype='Float64'),
        '_ag_panel_weight': pd.array([9., 8., 7., 0.], dtype='Float64'),
        'Region': ['North', None, None, None],
        '_ag_Region': ['East', 'West', 'Central', 'South'],
    }).set_index(['t', 'i'])


def test_primary_nulls_and_agriculture_panel_only(ug):
    got = ug.sample(covers())
    assert got['v'].tolist() == ['11', 'unlocated-primary-null', '33', '44']
    pd.testing.assert_extension_array_equal(
        got.weight.array, pd.array([10., None, None, None], dtype='Float64'))
    pd.testing.assert_extension_array_equal(
        got.panel_weight.array, pd.array([2., None, 7., 0.], dtype='Float64'))
    np.testing.assert_array_equal(got.panel_weight.dropna().to_numpy(float), [2., 7., 0.])
    assert got.Region.dropna().tolist() == ['North', 'Central', 'South']
    assert pd.isna(got.Region.iloc[1])
    assert not any(c.startswith('_') for c in got)


def test_cover_disagreement_is_diagnosed_not_used(ug):
    raw = covers()
    raw.loc[('2015-16', 'primary'), '_ag_v'] = 99
    with pytest.warns(UserWarning, match='covers disagree'):
        got = ug.sample(raw)
    assert got.loc[('2015-16', 'primary'), 'v'] == '11'


@pytest.mark.parametrize('column', ['v', 'panel_weight', 'Region'])
def test_conflicting_source_duplicate_refused_before_precedence(ug, column):
    frame = covers().iloc[[0]]
    duplicate = frame.copy()
    duplicate[column] = {'v': '99', 'panel_weight': 50., 'Region': 'Other'}[column]
    with pytest.raises(ValueError, match='conflicting duplicate'):
        ug.locating_cover(pd.concat([frame, duplicate]))
    pd.testing.assert_frame_equal(ug.locating_cover(pd.concat([frame, frame])), frame)


def test_same_membership_drives_sample_and_clusters(ug):
    frame = covers()
    sample = ug.sample(frame)
    cluster = ug.cluster_features(frame.drop(columns=['weight', 'panel_weight', '_ag_panel_weight']))
    assert set(sample.v) == set(cluster.index.get_level_values('v'))
    assert pd.isna(cluster.loc[('2015-16', 'unlocated-primary-null'), 'Region'])


def test_singletons_not_movers_and_no_geo_only_membership(ug):
    frame = pd.DataFrame({'t': ['2009-10'] * 4, 'i': ['a', 'b', 'c', 'geo'],
                          'v': [None, None, 'moved-c', None],
                          '_cover': ['a', None, 'c', None],
                          '_frame': [None, 'b', None, None],
                          'Region': [None] * 4}).set_index(['t', 'i'])
    with pytest.warns(UserWarning, match='dropping 1'):
        got = ug.cluster_features(frame)
    assert got.index.get_level_values('v').tolist() == ['unlocated-a', 'unlocated-b', 'moved-c']
    assert ug.singleton_v(None) is None


def test_plot_key_keeps_known_and_structural_suffix(ug):
    hh = pd.Series(['h'] * 4)
    got = ug._plot_key(hh, pd.Series(['1', None, '2', '3']),
                       pd.Series(['1', '2', None, '']))
    assert got.tolist() == ['h-1-1', 'unknown_plot', 'unknown_plot', 'h-3-']


def test_all_null_singleton_has_declared_identity_provenance(ug):
    frame = pd.DataFrame({'t': ['2011-12'], 'i': ['h'], 'v': [None],
                          '_frame': ['h'], 'Region': [None]}).set_index(['t', 'i'])
    got = ug.cluster_features(frame)
    assert got.Derivation.iloc[0] == 'Uganda::cluster_features::unlocated-identity'
    assert pd.isna(got.Region.iloc[0])
    assert ug.derive_unlocated_identity(frame).tolist() == ['unlocated-h']
