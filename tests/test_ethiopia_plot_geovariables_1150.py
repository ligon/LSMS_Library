"""WB plot covariates: native field identity, optionality and disclosed units."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library import Country, Feature
from lsms_library.country import _enforce_canonical_dtypes
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


@pytest.fixture(scope='module')
def eth():
    path = countries_root() / 'Ethiopia/_/ethiopia.py'
    spec = importlib.util.spec_from_file_location('ethiopia1150', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def exact_frame(left, right):
    pd.testing.assert_frame_equal(left, right, check_exact=True)
    assert left.attrs == right.attrs
    for col in left:
        pd.testing.assert_extension_array_equal(left[col].convert_dtypes().array,
                                               right[col].convert_dtypes().array)
        if pd.api.types.is_numeric_dtype(left[col]):
            mask = left[col].notna()
            np.testing.assert_array_equal(left.loc[mask, col].to_numpy(),
                                          right.loc[mask, col].to_numpy())


def sources(wave):
    # Hold household/parcel/field fixed but change holder; then hold holder
    # and plot fixed but change household. Neither key component is redundant.
    field = pd.DataFrame({
        'household_id': ['h', 'h', 'different', 'h', 'h'],
        'household_id2': ['output', 'output', 'other-output', 'output', 'output'],
        'holder_id': ['a', 'b', 'a', 'a', 'a'],
        'parcel_id': [1, 1, 1, 2, 1], 'field_id': [1, 1, 1, 1, 2],
        'gps': [10000., 0., 12500., np.nan, 1e-12], 'unit': [1] * 5,
        'irr': [1, 2, np.nan, 1, 2],
    }, index=[9, 3, 7, 2, 5])
    parcel = field[['holder_id', 'household_id', 'parcel_id']].drop_duplicates()
    parcel = parcel.assign(acquire=1)
    geo = field.iloc[[4, 0, 1, 2]][['holder_id', 'household_id', 'parcel_id', 'field_id']].copy()
    geo['plot_srtmslp'] = [np.nan, 100., 0., 37.]
    geo['plot_srtm'] = [2., 3., 4., 5.]
    geo['plot_twi'] = [9, 8, 7, 6]
    geo['dist_household'] = ['NA', '503.3', '', ' 0.1 ']
    orphan = geo.iloc[[0]].assign(holder_id='geo-only', plot_srtmslp=75.)
    geo = pd.concat([orphan, geo], ignore_index=True)
    if wave == '2021-22':
        geo = geo.rename(columns={'parcel_id': 'parcelroster_id', 'field_id': 'fieldroster_id'})
    colmap = dict(hhid='household_id2' if wave in ('2013-14', '2015-16') else 'household_id',
                  join_hhid='household_id', holder_id='holder_id', parcel_id='parcel_id',
                  field_id='field_id', area_gps='gps', area_unit='unit', acquire='acquire',
                  irrigated='irr')
    return parcel, field, geo, colmap


@pytest.mark.parametrize('wave', ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22'])
def test_left_enrichment_preserves_native_constructor(eth, wave):
    parcel, field, geo, c = sources(wave)
    old = eth.plot_features_for_wave(wave, parcel, field, c)
    new = eth.plot_features_for_wave(wave, parcel, field, c, geovar=geo)
    exact_frame(new[old.columns], old)
    assert len(new) == len(field)
    assert list(new.index.get_level_values('i')) == field[c['hhid']].tolist()
    assert new.PlotElevation.tolist()[:3] == [3., 4., 5.]
    assert new.PlotElevation.isna().tolist() == [False, False, False, True, False]
    assert new.PlotDistanceToHousehold.iloc[0] == 503.3
    assert new.PlotDistanceToHousehold.isna().tolist() == [False, True, False, True, True]
    assert new.PlotSlope.iloc[0] == 100.
    assert new.PlotSlope.iloc[1] == 0.
    assert new.PlotSlope.iloc[2] == 37.
    assert 'Derivation' not in new
    shuffled = eth.plot_features_for_wave(wave, parcel, field, c, geovar=geo.iloc[::-1])
    exact_frame(new, shuffled)
    assert 'PlotSlope' not in old  # optional absence is not an all-NA column


@pytest.mark.parametrize('conflict', [False, True])
def test_duplicate_source_keys_refuse_even_identical_values(eth, conflict):
    parcel, field, geo, c = sources('2018-19')
    extra = geo.iloc[[1]].copy()
    if conflict:
        extra['plot_srtmslp'] = 20.
    with pytest.raises(ValueError, match='duplicate/conflicting native keys'):
        eth.plot_features_for_wave('2018-19', parcel, field, c, pd.concat([geo, extra]))


@pytest.mark.parametrize('delta', ['nextafter', 'tiny'])
def test_exact_comparison_negative_calibration(delta):
    a = pd.DataFrame({'value': pd.array([1., 0., pd.NA], dtype='Float64')})
    b = a.copy()
    if delta == 'nextafter':
        b.loc[0, 'value'] = np.nextafter(1., 2.)
    else:
        b.loc[1, 'value'] = -1e-15
    with pytest.raises(AssertionError):
        exact_frame(a, b)


def test_optional_declarations_do_not_fabricate_unaffected_columns():
    path = Path(__file__).parents[1] / 'lsms_library/data_info.yml'
    schema = load_yaml(path)['Columns']['plot_features']
    for col in ('PlotSlope', 'PlotElevation', 'PlotWetnessIndex', 'PlotDistanceToHousehold'):
        assert schema[col]['optional'] is True
    frame = pd.DataFrame({'Area': pd.array([0., pd.NA, 1.25], dtype='Float64')})
    original = frame.copy()
    _enforce_canonical_dtypes(frame, 'plot_features')
    got = frame
    exact_frame(got, original)
    assert 'PlotSlope' not in got
    assert Country('Uganda')._materialization_entry('plot_features').get('PlotSlope') is None


def test_country_feature_composition_and_native_percent(eth, monkeypatch):
    """Production composition control, not an independent source oracle."""
    parcel, field, geo, c = sources('2021-22')
    native = eth.plot_features_for_wave('2021-22', parcel, field, c, geo)
    country = Country('Ethiopia')
    # Keep real dtype/provenance finalization; replace data-bearing spine only.
    monkeypatch.setattr(country, '_join_v_from_sample', lambda frame, *a, **k: frame)
    served = country._finalize_result(native.copy(), country._materialization_entry('plot_features'), 'plot_features')
    assert 'Derivation' not in served
    feature = Feature('plot_features')
    # Feature instantiates Country internally: substitute the selected method
    # with the already finalized synthetic frame, preserving real assembly.
    monkeypatch.setattr(country, 'plot_features', lambda **kw: served.copy())
    import lsms_library as cm
    monkeypatch.setattr(cm, 'Country', lambda name, **kw: country)
    result = feature(['Ethiopia'])
    assert len(result) == len(served)
    assert result.index.names[0] == 'country'
    assert 'Derivation' not in result
    assert result.PlotSlope.iloc[0] == 100.


def test_numeric_distance_is_not_round_tripped_through_text(eth):
    parcel, field, geo, c = sources('2018-19')
    values = np.array([1.25, np.nextafter(1., 2.), -1e-15, 0., 8338.9])
    geo['dist_household'] = values
    got = eth.plot_features_for_wave('2018-19', parcel, field, c, geo)
    np.testing.assert_array_equal(got.PlotDistanceToHousehold.iloc[[0, 1, 2, 4]].to_numpy(),
                                  values[[2, 3, 4, 1]])


def test_duplicate_normalized_keys_are_diagnosed(eth):
    _, field, geo, c = sources('2018-19')
    geo['parcel_id'] = geo['parcel_id'].astype(object)
    extra = geo.iloc[[1]].copy()
    extra['parcel_id'] = '1.0'
    with pytest.raises(ValueError, match='duplicate/conflicting'):
        eth._plot_geovariables_for_fields(field, pd.concat([geo, extra]), c)


def test_unaffected_country_and_feature_keep_optional_columns_absent(monkeypatch):
    country = Country('Uganda')
    native = pd.DataFrame({'Area': pd.array([0., np.nextafter(1., 2.), pd.NA], dtype='Float64'),
                           'Tenure': pd.array(['owned', 'rented_in', 'owned'], dtype='string')},
                          index=pd.MultiIndex.from_tuples(
                              [('2019-20', 'invented-v', 'invented-h', str(k)) for k in range(3)],
                              names=['t', 'v', 'i', 'plot_id']))
    monkeypatch.setattr(country, '_join_v_from_sample', lambda frame, *a, **kw: frame)
    served = country._finalize_result(native.copy(), country._materialization_entry('plot_features'), 'plot_features')
    additions = {'PlotSlope', 'PlotElevation', 'PlotWetnessIndex', 'PlotDistanceToHousehold'}
    assert not additions.intersection(served.columns)
    np.testing.assert_array_equal(served.Area.dropna().to_numpy(), native.Area.dropna().to_numpy())
    monkeypatch.setattr(country, 'plot_features', lambda **kw: served.copy())
    import lsms_library as ll
    monkeypatch.setattr(ll, 'Country', lambda name, **kw: country)
    composed = Feature('plot_features')(['Uganda'])
    assert len(composed) == len(native)
    assert not additions.intersection(composed.columns)
    np.testing.assert_array_equal(composed.Area.dropna().to_numpy(), native.Area.dropna().to_numpy())


def test_nigeria_slope_declared_as_native_percent():
    schema = load_yaml(Path(__file__).parents[1] / 'lsms_library/data_info.yml')['Columns']['plot_features']
    assert schema['PlotSlope']['optional'] is True
    assert 'percent' in schema['PlotSlope']['note']
    nigeria = Country('Nigeria')._materialization_entry('plot_features')
    assert nigeria['PlotSlope'] == {'type': 'float', 'optional': True}


def test_attachment_preserves_existing_attrs_and_negative_tiny_percent(eth):
    parcel, field, geo, c = sources('2018-19')
    old = eth.plot_features_for_wave('2018-19', parcel, field, c)
    old.attrs = {'existing': {'id_converted': True, 'witness': 'invented'}}
    geo['plot_srtmslp'] = [1., np.nextafter(1., 2.), -1e-15, 0., 100.]
    new = eth._attach_plot_geovariables(old.copy(), field, geo, c)
    exact_frame(new[old.columns], old)
    np.testing.assert_array_equal(new.PlotSlope.iloc[[0, 1, 2, 4]].to_numpy(),
                                  np.array([-1e-15, 0., 100., np.nextafter(1., 2.)]))
    assert 'Derivation' not in new
