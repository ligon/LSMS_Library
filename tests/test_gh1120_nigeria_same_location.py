"""Settled GH1120 PH movement refinement through actual Nigeria helpers.

Only sanctioned source reads are replaced with synthetic frames. Cover/roster
extraction, routing, weights, identities and derivation logic remain real.
"""
import importlib.util

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


@pytest.fixture(scope='module')
def ng():
    path = countries_root() / 'Nigeria' / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_same_location', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(params=['2018-19', '2023-24'])
def sources(request, ng, monkeypatch):
    import lsms_library.local_tools as lt
    folder = request.param
    spec = ng.ROUND_COVERS[folder]
    movement = 'hhsmlc' if folder == '2018-19' else 'hh_moved'
    same = 1 if folder == '2018-19' else 2
    same_label = '1. YES' if folder == '2018-19' else '2. NO, IN ORIGINAL DWELLING'
    other_label = '2. MOVED WITHIN LOCALITY' if folder == '2018-19' else '1. YES, MOVED CLOSE TO ORIGINAL DWELLING'

    def raw(ids, eas):
        n = len(ids)
        frame = pd.DataFrame({'hhid': ids, 'ea': eas, 'state': [32]*n,
                              'lga': [3201]*n, 'zone': [5]*n,
                              'sector': [2]*n, 'strata': [5]*n})
        frame[spec['weight']] = [0., np.nan] + [2.]*(n-2)
        frame[spec['panel_weight']] = [4., 0.] + [np.nan]*(n-2)
        return frame

    pp = raw([1, 2, 3, 4, 5, 7, 8], [10, 20, 30, 40, 0, 70, 80])
    ph = raw([1, 2, 3, 4, 5, 6, 7, 8], [0, 0, 0, 0, 0, 0, 81, 0])
    ph[movement] = same
    ph.loc[1, movement] = 3 if folder == '2023-24' else 2
    ph.loc[7, ['state', 'lga']] = [99, 9901]  # must not make a hybrid ID

    def decoded(frame):
        return frame.assign(state='rivers', zone='south south', sector='2. RURAL')

    pp_dec, ph_dec = decoded(pp), decoded(ph)
    ph_dec[movement] = [same_label, other_label, same, 'WRONG LABEL',
                        same_label, same_label, same_label, same_label]
    frames = {}
    for rnd, data, dec in [('pp', pp, pp_dec), ('ph', ph, ph_dec)]:
        frames[(ng._data_path(folder, spec[rnd]), False)] = data
        frames[(ng._data_path(folder, spec[rnd]), True)] = dec
        frames[(ng._data_path(folder, spec[rnd+'_roster']), False)] = data.copy()
        frames[(ng._data_path(folder, spec[rnd+'_roster']), True)] = dec.copy()
    # Real cluster helper's own left geo join, with a geo-only outsider.
    if spec['geo'] is not None:
        fn, lat, lon = spec['geo']
        frames[(ng._data_path(folder, fn), True)] = pd.DataFrame({
            'hhid': [1, 8, 99], lat: [1., 8., 99.], lon: [2., 9., 99.]})

    def get(fn, convert_categoricals=True, **kwargs):
        return frames[(str(fn), convert_categoricals)].copy(deep=True)

    monkeypatch.setattr(lt, 'get_dataframe', get)
    return folder, frames, movement, same, same_label


def test_approved_label_uses_complete_own_pp_identity(ng, sources):
    folder, frames, _, _, _ = sources
    original = {k: v.copy(deep=True) for k, v in frames.items()}
    sample = ng.sample_for_wave(folder)
    ph = sample.xs(ng.PH_QUARTER[folder], level='t')
    assert ph['v'].to_dict() == {
        '1': '32/3201/10', '2': 'moved-2', '3': 'moved-3',
        '4': 'moved-4', '5': 'moved-5', '6': 'moved-6',
        '7': '32/3201/81', '8': '32/3201/80'}
    assert set(ph.index[ph['Derivation'].notna()]) == {'1', '8'}
    assert set(ph['Derivation'].dropna()) == {'Nigeria::sample::ph-same-location'}
    assert sample.xs(ng.PP_QUARTER[folder], level='t')['Derivation'].isna().all()
    for key, frame in original.items():
        pd.testing.assert_frame_equal(frames[key], frame, check_exact=True)


def test_native_weights_rows_order_and_attributes_do_not_follow_pp(ng, sources):
    folder, frames, _, _, _ = sources
    spec = ng.ROUND_COVERS[folder]
    expected = ng._cover_frame(ng._data_path(folder, spec['ph']), spec)
    got = ng.round_covers(folder)[ng.PH_QUARTER[folder]]
    unchanged = [c for c in expected.columns if c != 'v']
    pd.testing.assert_frame_equal(got[unchanged], expected[unchanged], check_exact=True)
    assert got.index.equals(expected.index)
    assert got.loc['1', 'weight'] == 0 and pd.isna(got.loc['2', 'weight'])
    assert got.loc['2', 'panel_weight'] == 0


def test_wrong_code_with_same_label_does_not_authorize_reuse(ng, sources):
    folder, frames, movement, same, _ = sources
    fn = ng._data_path(folder, ng.ROUND_COVERS[folder]['ph'])
    frames[(fn, False)].loc[0, movement] = 2 if same == 1 else 1
    ph = ng.sample_for_wave(folder).xs(ng.PH_QUARTER[folder], level='t')
    assert ph.loc['1', 'v'] == 'moved-1'
    assert 'Derivation' not in ph or pd.isna(ph.loc['1', 'Derivation'])


def test_cluster_rows_share_repaired_key_and_keep_existing_geo_join(ng, sources):
    folder, _, _, _, _ = sources
    cf = ng.cluster_features_for_wave(folder).reset_index().set_index(['t', 'i'])
    ph = cf.xs(ng.PH_QUARTER[folder], level='t')
    assert ph.loc['1', 'v'] == '32/3201/10'
    assert ph.loc['8', 'v'] == '32/3201/80'
    assert set(ph.index[ph['Derivation'].notna()]) == {'1', '8'}
    assert set(ph['Derivation'].dropna()) == {'Nigeria::cluster_features::ph-same-location'}
    assert '99' not in ph.index
    if folder == '2018-19':
        assert ph.loc['1', 'Latitude'] == 1.
    else:
        assert ph[['Latitude', 'Longitude']].isna().all().all()


def test_missing_label_does_not_authorize_reuse(ng, sources):
    folder, frames, movement, _, _ = sources
    fn = ng._data_path(folder, ng.ROUND_COVERS[folder]['ph'])
    frames[(fn, True)].loc[0, movement] = pd.NA
    ph = ng.sample_for_wave(folder).xs(ng.PH_QUARTER[folder], level='t')
    assert ph.loc['1', 'v'] == 'moved-1'
    assert 'Derivation' not in ph or pd.isna(ph.loc['1', 'Derivation'])


def test_cover_priority_and_unstamped_roster_only_row(ng, sources):
    folder, frames, _, _, _ = sources
    fn = ng._data_path(folder, ng.ROUND_COVERS[folder]['ph_roster'])
    for labels in [False, True]:
        frame = frames[(fn, labels)].copy()
        frame.loc[0, 'ea'] = 91  # roster must not override household cover
        extra = frame.iloc[[0]].copy().assign(hhid=9, ea=90)
        frames[(fn, labels)] = pd.concat([frame, extra], ignore_index=True)
    ph = ng.sample_for_wave(folder).xs(ng.PH_QUARTER[folder], level='t')
    assert ph.loc['1', 'v'] == '32/3201/10' and ph.loc['1', 'weight'] == 0
    assert ph.loc['9', 'v'] == '32/3201/90'
    assert ph.loc['9', ['weight', 'panel_weight']].isna().all()
    assert pd.isna(ph.loc['9', 'Derivation'])


def test_registry_and_original_named_answers_reproduce_repaired_rows(ng, sources):
    folder, frames, movement, same, same_label = sources
    registry = load_yaml(countries_root() / 'Nigeria' / '_' / 'derivations.yml')
    for table in ['sample', 'cluster_features']:
        record = registry[f'Nigeria::{table}::ph-same-location']
        assert record['columns'] == ['v']
        assert record['function'].endswith(':derive_ph_same_location_v')
        assert record['inputs'].endswith(':inputs_ph_same_location')
        assert {'hhid', 'ea', 'hhsmlc', 'hh_moved'} <= set(record['raw_variables'])
    inputs = ng.inputs_ph_same_location(ng.PH_QUARTER[folder])
    assert list(inputs.index.names) == ['t', 'i']
    assert set(inputs.index.get_level_values('i')) == {'1', '8'}
    assert inputs[movement].eq(same).all()
    assert inputs[movement+'_label'].eq(same_label).all()
    assert inputs.loc[(ng.PH_QUARTER[folder], '8'), 'state'] == 99
    assert inputs.loc[(ng.PH_QUARTER[folder], '8'), 'pp_state'] == 32
    expected = ng.sample_for_wave(folder).loc[inputs.index, 'v']
    pd.testing.assert_series_equal(ng.derive_ph_same_location_v(inputs), expected)


@pytest.mark.parametrize('missing', [('state',), ('lga',), ('state', 'lga')])
def test_roster_missing_component_keeps_recorded_mover(ng, monkeypatch, missing):
    import lsms_library.local_tools as lt
    roster = pd.DataFrame({'hhid': [17, 18], 'ea': [0, 10],
                          'state': [32, 32], 'lga': [3201, 3201],
                          'zone': [5, 5], 'sector': [2, 2]}).drop(columns=list(missing))
    monkeypatch.setattr(lt, 'get_dataframe', lambda *a, **k: roster.copy())
    out = ng._roster_locator('roster', ng.ROUND_COVERS['2015-16'], 'cover')
    assert out.loc['17', 'v'] == 'moved-17'
    assert out.loc['18', 'v'] == 'unlocated-18'
    assert out[['weight', 'panel_weight']].isna().all().all()


def test_w3_remains_unlabelled_sentinel(ng, monkeypatch):
    import lsms_library.local_tools as lt
    spec = ng.ROUND_COVERS['2015-16']
    pp = pd.DataFrame({'hhid': [17], 'ea': [10], 'state': [32], 'lga': [3201],
                       'zone': [5], 'sector': [2], 'wt_wave3': [2.], 'wt_w1_w2_w3': [3.]})
    ph = pp.assign(ea=0, hhsmlc=1)
    def get(fn, convert_categoricals=True, **kwargs):
        return (ph if 'harvest' in str(fn) else pp).copy()
    monkeypatch.setattr(lt, 'get_dataframe', get)
    ph_sample = ng.sample_for_wave('2015-16').xs('2016Q1', level='t')
    assert ph_sample.loc['17', 'v'] == 'moved-17'
    assert 'Derivation' not in ph_sample


def test_cover_missing_location_still_refuses(ng, sources):
    folder, frames, _, _, _ = sources
    fn = ng._data_path(folder, ng.ROUND_COVERS[folder]['ph'])
    frames[(fn, False)]['state'] = frames[(fn, False)]['state'].astype('Int64')
    frames[(fn, False)].loc[6, 'state'] = pd.NA  # ea=81 is not a mover
    with pytest.raises(AssertionError, match='cover row with no cluster'):
        ng.round_covers(folder)


def _assert_exact_numeric(left, right):
    """GH #1206: frame check_exact alone can miss nullable-float changes."""
    left = pd.Series(left).reset_index(drop=True).astype('Float64')
    right = pd.Series(right).reset_index(drop=True).astype('Float64')
    assert left.isna().equals(right.isna())
    assert left.array.equals(right.array)
    np.testing.assert_array_equal(left.dropna().to_numpy(dtype=float),
                                  right.dropna().to_numpy(dtype=float))


@pytest.mark.parametrize('a,b', [(1., np.nextafter(1., 2.)),
                                 (0., np.finfo(float).tiny)])
def test_exact_numeric_check_rejects_small_differences(a, b):
    with pytest.raises(AssertionError):
        _assert_exact_numeric([a, pd.NA], [b, pd.NA])


def test_ph_native_weight_bits_are_kept_on_identity_repair(ng, sources):
    folder, frames, _, _, _ = sources
    spec = ng.ROUND_COVERS[folder]
    fn = ng._data_path(folder, spec['ph'])
    for decoded in [False, True]:
        frames[(fn, decoded)].loc[0, spec['weight']] = np.nextafter(1., 2.)
        frames[(fn, decoded)].loc[0, spec['panel_weight']] = np.finfo(float).tiny
    got = ng.sample_for_wave(folder).xs(ng.PH_QUARTER[folder], level='t')
    assert got.loc['1', 'v'] == '32/3201/10'
    raw = frames[(fn, False)].copy()
    raw.index = raw.hhid.astype(str)
    for col, source in [('weight', spec['weight']),
                        ('panel_weight', spec['panel_weight'])]:
        _assert_exact_numeric(got[col], raw.loc[got.index, source])


def test_duplicate_cover_household_refused(ng, sources):
    folder, frames, _, _, _ = sources
    fn = ng._data_path(folder, ng.ROUND_COVERS[folder]['ph'])
    for decoded in [False, True]:
        frames[(fn, decoded)] = pd.concat(
            [frames[(fn, decoded)], frames[(fn, decoded)].iloc[[0]]],
            ignore_index=True)
    with pytest.raises(AssertionError, match='lists a household twice'):
        ng.sample_for_wave(folder)


def test_duplicate_geo_household_refused(ng, sources):
    folder, frames, _, _, _ = sources
    spec = ng.ROUND_COVERS[folder]
    if spec['geo'] is None:
        return  # W5 intentionally has no wired geo source.
    fn = ng._data_path(folder, spec['geo'][0])
    frames[(fn, True)] = pd.concat([frames[(fn, True)],
                                    frames[(fn, True)].iloc[[0]]], ignore_index=True)
    with pytest.raises(AssertionError, match='household has two fixes'):
        ng.cluster_features_for_wave(folder)
