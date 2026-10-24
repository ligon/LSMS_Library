"""Source-faithful optional Burkina 2021 parcel coordinates (GH #1058).

2021-22 reads s16a from BOTH components of the round (GH #1276): the panel
(Data/Panel/, WB 6224) ships s16aq46 coordinates; the cross-section
(Data/Cross_Section/, WB 6277) ships none, so its parcels are served with
Latitude / Longitude NA.
"""
import ast
import importlib.util
import runpy
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import local_tools as tools
from lsms_library.paths import countries_root, data_root


ROOT = countries_root() / 'Burkina_Faso'
POSITIONS = {'latitude': 's16aq46__latitude',
             'longitude': 's16aq46__longitude'}
OLD_COLUMNS = ['Area', 'AreaUnit', 'Tenure', 'TenureSystem',
               'SoilType', 'Irrigated']


def _colmap(wave):
    script = ROOT / wave / '_' / 'plot_features.py'
    assignment = next(node for node in ast.parse(script.read_text()).body
                      if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name)
                              and target.id == 'colmap'
                              for target in node.targets))
    return {item.arg: ast.literal_eval(item.value)
            for item in assignment.value.keywords}


@pytest.fixture
def helper(monkeypatch):
    path = ROOT / '_' / 'burkina_faso.py'
    spec = importlib.util.spec_from_file_location('bf_plot_gps_helper', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(ROOT / '2021-22' / '_')
    return module


@pytest.fixture
def source():
    # The placeholder is interspersed, and retained labels are noncontiguous.
    return pd.DataFrame({
        'grappe': [4] * 6, 'menage': [120, 2, 3, 4, 5, 6],
        's16aq02': [1, np.nan, 2, 3, 4, 5],
        's16aq03': [1, np.nan, 1, 1, 1, 1],
        's16aq47': [41., 99., 42., 43., 44., 45.],
        's16aq45': [2., 1., 1., 1., np.nan, 1.],
        's16aq09a': [1.] * 6, 's16aq09b': [1.] * 6,
        's16aq10': [1.] * 6, 's16aq13': [1.] * 6,
        's16aq18': [1.] * 6, 's16aq17': [1.] * 6,
        's16aq46__latitude': [12.25, 99., np.nan, 0., -3.5, np.nan],
        's16aq46__longitude': [-1.25, 99., 2.75, np.nan, 0., np.nan],
    }, index=[2, 5, 9, 13, 21, 34])


@pytest.fixture
def cross_source(source):
    """A cross-section s16a: same instrument, its own grappes (586-1185), and
    no s16aq46 columns -- as WB 6277 ships it."""
    cross = source.drop(columns=list(POSITIONS.values())).copy()
    cross['grappe'] = 600
    return cross


def _id(grappe, menage, wave, component='panel'):
    if wave == '2021-22':
        return tools.format_id(grappe) + tools.format_id(menage, zeropadding=3)
    return (tools.format_id(grappe) + '0'
            + tools.format_id(menage, zeropadding=2))


def _expected(source, wave, component='panel'):
    rows = source.loc[source.s16aq02.notna() & source.s16aq03.notna()]
    if component == 'cross':
        lat = lon = pd.array([pd.NA] * len(rows), dtype='Float64')
    else:
        lat = pd.array(rows.s16aq46__latitude, dtype='Float64')
        lon = pd.array(rows.s16aq46__longitude, dtype='Float64')
    expected = pd.DataFrame({
        't': wave,
        'i': [_id(g, m, wave, component) for g, m in zip(rows.grappe, rows.menage)],
        'plot_id': [tools.format_id(f) + '_' + tools.format_id(p)
                    for f, p in zip(rows.s16aq02, rows.s16aq03)],
        'Latitude': lat,
        'Longitude': lon,
    })
    return expected.set_index(['t', 'i', 'plot_id'])


def _expected_2021(panel, cross):
    """Both components of 2021-22, panel first (the script's concat order)."""
    return pd.concat([_expected(panel, '2021-22'),
                      _expected(cross, '2021-22', 'cross')])


def _assert_positions(frame, source, wave, expected=None):
    if expected is None:
        expected = _expected(source, wave)
    pd.testing.assert_frame_equal(frame[['Latitude', 'Longitude']],
                                  expected, check_exact=True)


def _build(helper, source, colmap):
    return helper.plot_features_for_wave(
        '2021-22', source, colmap,
        id_fn=lambda g, m: helper.i(pd.Series([g, m])))


def test_helper_retains_positions_independent_of_flag_area_and_index(helper, source):
    colmap = dict(_colmap('2021-22'), **POSITIONS)
    result = _build(helper, source, colmap)
    _assert_positions(result, source, '2021-22')
    old = _build(helper, source,
                 {key: value for key, value in colmap.items()
                  if key not in POSITIONS})
    pd.testing.assert_frame_equal(result[OLD_COLUMNS], old, check_exact=True)
    assert result.attrs == old.attrs


@pytest.mark.parametrize('axis', ['latitude', 'longitude'])
def test_optional_axes_are_independent(helper, source, axis):
    colmap = {key: value for key, value in _colmap('2021-22').items()
              if key not in POSITIONS}
    colmap[axis] = POSITIONS[axis]
    result = _build(helper, source, colmap)
    expected = _expected(source, '2021-22')
    name = axis.title()
    pd.testing.assert_series_equal(result[name], expected[name], check_exact=True)
    assert ({'Latitude', 'Longitude'} - {name}).isdisjoint(result.columns)


@pytest.mark.parametrize('axis', ['latitude', 'longitude'])
def test_missing_declared_coordinate_source_raises(helper, source, axis):
    colmap = dict(_colmap('2021-22'), **POSITIONS)
    with pytest.raises(KeyError, match=POSITIONS[axis]):
        _build(helper, source.drop(columns=POSITIONS[axis]), colmap)


def test_absent_optional_keys_leave_existing_wave_columns(helper, source):
    result = _build(helper, source, _colmap('2018-19'))
    assert list(result.columns) == OLD_COLUMNS


@pytest.mark.parametrize('wave', ['2018-19', '2021-22'])
def test_actual_wave_script_binds_its_source_and_formatter(monkeypatch, source,
                                                          cross_source, wave):
    captured = {}
    reads = []

    def read(path, convert_categoricals=True):
        assert not convert_categoricals
        assert Path(path).name == ('s16a_me_bfa2018.dta' if wave == '2018-19'
                                   else 's16a_me_bfa2021.dta')
        reads.append(Path(path).parent.name)
        if Path(path).parent.name == 'Cross_Section':
            return cross_source.copy()
        return source.copy()

    def write(frame, path):
        assert path == 'plot_features.parquet'
        captured['frame'] = frame

    monkeypatch.chdir(ROOT / wave / '_')
    monkeypatch.syspath_prepend(str(ROOT / '_'))
    monkeypatch.setattr(tools, 'get_dataframe', read)
    monkeypatch.setattr(tools, 'to_parquet', write)
    runpy.run_path(str(ROOT / wave / '_' / 'plot_features.py'))
    result = captured['frame']
    if wave == '2021-22':
        assert reads == ['Panel', 'Cross_Section'], reads
        expected = _expected_2021(source, cross_source)
        _assert_positions(result, source, wave, expected)
    else:
        assert list(result.columns) == OLD_COLUMNS
        expected = _expected(source, wave)
    pd.testing.assert_index_equal(result.index, expected.index)


@pytest.mark.parametrize('fault', ['q47_as_latitude', 'swapped_axes', 'wrong_key'])
def test_source_oracle_rejects_wrong_projection(helper, source, fault):
    result = _build(helper, source, dict(_colmap('2021-22'), **POSITIONS))
    if fault == 'q47_as_latitude':
        rows = source.loc[source.s16aq02.notna() & source.s16aq03.notna()]
        result['Latitude'] = pd.array(rows.s16aq47, dtype='Float64')
    elif fault == 'swapped_axes':
        result[['Latitude', 'Longitude']] = result[['Longitude', 'Latitude']].to_numpy()
    else:
        result = result.rename(index=lambda value: str(value) + '-wrong', level='i')
    with pytest.raises(AssertionError):
        _assert_positions(result, source, '2021-22')


def test_country_declares_existing_coordinates_as_optional_float():
    entry = ll.Country('Burkina_Faso')._materialization_entry('plot_features')
    for name in ['Latitude', 'Longitude']:
        assert entry[name] == {'type': 'float', 'optional': True}


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', ['2018-19', '2021-22'])
def test_held_wave_output_matches_native_source_coordinates(wave):
    # Use the normal Country build, then inspect its actual script output.
    ll.Country('Burkina_Faso').plot_features()
    frame = tools.get_dataframe(data_root('Burkina_Faso') / wave / '_' /
                                'plot_features.parquet')
    if wave == '2018-19':
        assert list(frame.columns) == OLD_COLUMNS
    else:
        panel, cross = (tools.get_dataframe(ROOT / wave / 'Data' / sub /
                                            's16a_me_bfa2021.dta',
                                            convert_categoricals=False)
                        for sub in ('Panel', 'Cross_Section'))
        _assert_positions(frame, panel, wave, _expected_2021(panel, cross))


@pytest.mark.requires_s3
def test_public_coordinates_follow_existing_panel_key_mapping():
    country = ll.Country('Burkina_Faso')
    actual = country.plot_features(waves=['2021-22'])
    panel, cross = (tools.get_dataframe(ROOT / '2021-22' / 'Data' / sub /
                                        's16a_me_bfa2021.dta',
                                        convert_categoricals=False)
                    for sub in ('Panel', 'Cross_Section'))
    expected = tools.id_walk(_expected_2021(panel, cross), country.updated_ids)
    actual = actual[['Latitude', 'Longitude']].droplevel('v')
    actual = actual.reorder_levels(expected.index.names)
    pd.testing.assert_frame_equal(actual.sort_index(), expected.sort_index(),
                                  check_exact=True)
