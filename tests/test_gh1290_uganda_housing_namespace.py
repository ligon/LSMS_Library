"""Synthetic actual-YAML routing for the 2015 housing hh -> HHID bridge.

Only the source reader is replaced; no real Country/Feature build or provider
read. All identifiers and payloads below are invented. The historical nine
unmatched housing identities require a separate, authorized source oracle.
"""
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.country import Wave
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


@pytest.fixture
def route(monkeypatch):
    # Keep the runtime's package/config/data bindings. LSMS_NO_CACHE skips
    # both wave cache reads and writes; only in-memory synthetic frames enter.
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    scheme = load_yaml(countries_root() / 'Uganda/_/data_scheme.yml')
    country = SimpleNamespace(name='Uganda', resources=scheme,
                              formatting_functions={})
    calls = []

    def build(housing, cover, agriculture=None):
        if agriculture is None:
            agriculture = _cover(('ag-only',), ('native-ag',))

        def reader(path, **kwargs):
            name = Path(path).name
            assert name in {'gsec9.dta', 'gsec1.dta', 'AGSEC1.dta'}, 'unexpected source request'
            calls.append(name)
            if name == 'gsec1.dta' and isinstance(cover, Exception):
                raise cover
            frame = {'gsec9.dta': housing, 'gsec1.dta': cover,
                     'AGSEC1.dta': agriculture}[name]
            if isinstance(frame, Exception):
                raise frame
            return frame.copy(deep=True)

        monkeypatch.setattr(local_tools, 'get_dataframe', reader)
        return Wave('2015-16', '2015-16', country).grab_data('housing')

    build.calls = calls
    return build


def _housing(ids=('old-a', 'old-b')):
    return pd.DataFrame({'hhid': list(ids),
                         'h9q4': ['Grass', 'Iron sheets'][:len(ids)],
                         'h9q6': ['Mud', None][:len(ids)]})


def _cover(ids=('old-a', 'old-b'), targets=('new-a', 'new-b')):
    return pd.DataFrame({'hh': list(ids), 'HHID': list(targets)})


def test_shipped_route_rekeys_and_preserves_full_payload(route):
    raw = _housing()
    raw['h9q4'] = pd.Categorical(raw['h9q4'])
    out = route(raw, _cover(('old-b', 'cover-only', 'old-a'),
                            ('new-b', 'new-only', 'new-a')))
    assert list(out.index) == [('2015-16', 'new-a'), ('2015-16', 'new-b')]
    expected = raw[['h9q4', 'h9q6']].rename(columns={'h9q4': 'Roof', 'h9q6': 'Floor'})
    pd.testing.assert_frame_equal(out.reset_index(drop=True), expected)
    assert route.calls == ['gsec9.dta', 'gsec1.dta', 'AGSEC1.dta']
    assert list(out.index.names) == ['t', 'i']
    assert 'v' not in out.columns


def test_agreeing_pairs_collapse_after_formatting(route):
    out = route(_housing(('001', '2.0')),
                _cover(('001', '001', 2.0, '2.0'), (10.0, '10', '20.0', 20)))
    assert list(out.index.get_level_values('i')) == ['10', '20']
    assert len(out) == 2


def test_missing_target_does_not_override_recorded_target(route):
    out = route(_housing(), _cover(('old-a', 'old-a', 'old-b'),
                                   (None, 'new-a', 'new-b')))
    assert list(out.index.get_level_values('i')) == ['new-a', 'new-b']


def test_identity_bridge_preserves_existing_keys_and_payloads(route):
    """Healthy control: already-equal keys remain equal on the former route."""
    raw = _housing()
    out = route(raw, _cover(targets=('old-a', 'old-b')))
    assert list(out.index.get_level_values('i')) == ['old-a', 'old-b']
    pd.testing.assert_frame_equal(out.reset_index(drop=True),
                                 raw[['h9q4', 'h9q6']].set_axis(['Roof', 'Floor'], axis=1))


def test_unmatched_and_null_source_keys_keep_payloads_without_null_match(route):
    raw = _housing(('unmatched', None))
    raw.loc[1, ['h9q4', 'h9q6']] = None  # even an entirely blank payload survives
    with pytest.warns(UserWarning, match=r'2 of 2 housing rows \(1 missing source IDs\)'):
        out = route(raw, _cover((None, 'cover-only'), ('do-not-borrow', 'new-only')))
    ids = out.index.get_level_values('i')
    assert ids[0] == 'unmatched' and pd.isna(ids[1])
    assert len(out) == 2 and out.iloc[1].isna().all()
    pd.testing.assert_frame_equal(out.reset_index(drop=True),
                                 raw[['h9q4', 'h9q6']].set_axis(['Roof', 'Floor'], axis=1))


@pytest.mark.parametrize('targets', [('new-a', 'different'), (10.0, '11.0')])
def test_conflicting_bridge_targets_raise_before_join(route, targets):
    with pytest.raises(ValueError, match='ambiguous hh/HHID bridge'):
        route(_housing(), _cover(('old-a', 'old-a'), targets))


@pytest.mark.parametrize('cover', [
    FileNotFoundError('synthetic missing cover'),
    pd.DataFrame({'hh': ['old-a']}),
    _cover((None,), ('new-a',)),
    _cover(('old-a',), (None,)),
    _cover((), ()),
])
def test_unavailable_or_invalid_bridge_cannot_serve_old_namespace(route, cover):
    with pytest.raises(ValueError, match='bridge (unavailable|pairs)'):
        route(_housing(), cover)


@pytest.mark.parametrize('housing,cover', [
    (_housing(), _cover(targets=('same', 'same'))),
    (_housing(('old-a', 'new-a')), _cover(('old-a',), ('new-a',))),
    (_housing(('old-a', 'old-a')), _cover(('old-a',), ('new-a',))),
])
def test_rekey_collision_is_fatal_including_unmatched_namespace_collision(route, housing, cover):
    with pytest.raises(ValueError, match='duplicate household keys'):
        route(housing, cover)


@pytest.fixture
def actual_carrier(monkeypatch):
    """Actual Country/getter/finalizer/Feature; invented reader/sample leaves.

    The Feature constructor seam returns the SAME real Country with an explicit
    in-memory ID-map fixture. No asserted method is replaced. In particular the
    sample leaf already has the served `(i,t)` + v contract, not raw sample data.
    """
    import lsms_library as ll
    import lsms_library.country as C

    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')
    c = C.Country('Uganda')
    assert type(c) is C.Country
    c._panel_ids_attempted = True
    # A second walk would erroneously reach terminal-a. The real walk must
    # apply only native-a -> served-a on this explicitly raw wave.
    c._updated_ids_cache = {'2015-16': {'native-a': 'served-a',
                                      'served-a': 'terminal-a'},
                            '2013-14': {'older-native': 'older-served'}}
    invented_housing = _housing()
    invented_housing['h9q4'] = ['Grass', 'Iron Sheets']
    state = SimpleNamespace(housing=invented_housing, cover=_cover(
        targets=('native-a', 'native-b')),
                            agriculture=_cover(('ag-only',), ('native-ag',)),
                            reads=[], walks=[], samples=[])

    def reader(path, **kwargs):
        p = Path(path)
        if '2015-16' in p.parts and p.name in {'gsec9.dta', 'gsec1.dta', 'AGSEC1.dta'}:
            state.reads.append(('2015-16', p.name))
            return {'gsec9.dta': state.housing, 'gsec1.dta': state.cover,
                    'AGSEC1.dta': state.agriculture}[p.name].copy(deep=True)
        if '2013-14' in p.parts and p.name == 'GSEC9_1.dta':
            state.reads.append(('2013-14', p.name))
            return pd.DataFrame({'HHID': ['older-native'], 'h9q4': ['Thatch'],
                                 'h9q6': ['Sand']})
        raise AssertionError(f'undeclared synthetic source request: {path}')

    def sample():
        state.samples.append('served sample leaf')
        return pd.DataFrame({
            'i': ['served-a', 'native-b', 'unmatched', 'do-not-borrow', 'native-ag', 'older-served'],
            't': ['2015-16'] * 5 + ['2013-14'],
            'v': ['cluster-a', 'cluster-b', 'cluster-u', 'forbidden-cluster', 'cluster-ag', 'cluster-old'],
        }).set_index(['i', 't'])

    real_walk = C.id_walk

    def walk(frame, *args, **kwargs):
        state.walks.append((frame.index.to_frame(index=False).copy(), dict(kwargs)))
        return real_walk(frame, *args, **kwargs)

    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(c, 'sample', sample)
    monkeypatch.setattr(C, 'id_walk', walk)

    def factory(name, **kwargs):
        assert name == 'Uganda' and not kwargs.get('trust_cache')
        return c

    monkeypatch.setattr(ll, 'Country', factory)

    def build(kind='country', waves=('2015-16',)):
        if kind == 'country':
            return c.housing(waves=list(waves))
        return ll.Feature('housing')(['Uganda'], waves=list(waves), currency=None)

    state.build = build
    state.country = c
    yield state
    C._GRAIN_LEDGER.pop(('Uganda', 'housing'), None)
    C._BUILD_DROP_LEDGER.pop(('Uganda', 'housing'), None)


@pytest.mark.parametrize('kind', ['country', 'feature'])
def test_actual_carrier_rekeys_walks_once_and_preserves_payload(actual_carrier, kind):
    s = actual_carrier
    s.housing['h9q4'] = pd.Categorical(s.housing['h9q4'])
    out = s.build(kind)
    flat = out.reset_index()
    assert flat.i.tolist() == ['served-a', 'native-b']
    assert flat.v.tolist() == ['cluster-a', 'cluster-b']
    assert flat.t.tolist() == ['2015-16'] * 2
    assert flat.Roof.tolist() == ['Grass', 'Iron Sheets']
    assert flat.Floor.iloc[0] == 'Mud' and pd.isna(flat.Floor.iloc[1])
    assert list(out.columns) == ['Roof', 'Floor']
    assert len(s.walks) == 1 and s.walks[0][1] == {'state': 'raw'}
    assert s.walks[0][0].i.tolist() == ['native-a', 'native-b']
    assert s.samples == ['served sample leaf']
    assert s.reads == [('2015-16', 'gsec9.dta'), ('2015-16', 'gsec1.dta'),
                       ('2015-16', 'AGSEC1.dta')]
    assert ('country' in out.index.names) == (kind == 'feature')
    if kind == 'country':
        assert out.attrs['id_converted'] is True
    else:
        assert flat.country.tolist() == ['Uganda', 'Uganda']


@pytest.mark.parametrize('kind', ['country', 'feature'])
def test_actual_carrier_unresolved_recorded_identity_and_null_nonmatch(actual_carrier, kind):
    s = actual_carrier
    s.housing = _housing(('unmatched', None))
    s.housing['h9q4'] = ['Grass', 'Iron Sheets']
    s.cover = _cover((None, 'cover-only'), ('do-not-borrow', 'new-only'))
    with pytest.warns(UserWarning, match='2 of 2 housing rows'):
        out = s.build(kind)
    flat = out.reset_index()
    assert len(flat) == 2
    assert flat.i.iloc[0] == 'unmatched' and pd.isna(flat.i.iloc[1])
    assert flat.v.iloc[0] == 'cluster-u' and pd.isna(flat.v.iloc[1])
    assert flat.Roof.tolist() == ['Grass', 'Iron Sheets']
    assert flat.Floor.iloc[0] == 'Mud' and pd.isna(flat.Floor.iloc[1])
    assert len(s.walks) == 1
    assert 'do-not-borrow' not in flat.i.dropna().tolist()


@pytest.mark.parametrize('kind', ['country', 'feature'])
@pytest.mark.parametrize('failure', ['ambiguous', 'collision'])
def test_actual_carrier_refuses_ambiguous_bridge_and_rekey_collision(actual_carrier, kind, failure):
    s = actual_carrier
    if failure == 'ambiguous':
        s.cover = _cover(('old-a', 'old-a'), ('native-a', 'different'))
        message = 'ambiguous hh/HHID bridge'
    else:
        s.cover = _cover(targets=('same', 'same'))
        message = 'duplicate household keys'
    if kind == 'country':
        with pytest.raises(ValueError, match=message):
            s.build(kind)
    else:
        with pytest.warns(UserWarning, match=message):
            out = s.build(kind)
        assert out.empty  # real Feature failure policy: loud per-country degrade
    assert not s.walks and not s.samples


@pytest.mark.parametrize('kind', ['country', 'feature'])
@pytest.mark.parametrize('waves', [('2013-14',), ('2015-16',), ('2013-14', '2015-16')])
def test_actual_carrier_requested_waves_select_readers_before_assembly(actual_carrier, kind, waves):
    s = actual_carrier
    out = s.build(kind, waves)
    assert set(out.index.get_level_values('t')) == set(waves)
    assert {wave for wave, _ in s.reads} == set(waves)
    assert len(s.walks) == len(waves)
    assert len(out) == (3 if len(waves) == 2 else (1 if waves[0] == '2013-14' else 2))
    ids = set(out.index.get_level_values('i'))
    assert 'terminal-a' not in ids
    if '2013-14' in waves:
        assert 'older-served' in ids


@pytest.mark.parametrize('kind', ['country', 'feature'])
def test_actual_carrier_identity_bridge_is_a_healthy_original_control(actual_carrier, kind):
    s = actual_carrier
    s.housing['hhid'] = ['native-a', 'native-b']
    s.cover = _cover(('native-a', 'native-b'), ('native-a', 'native-b'))
    out = s.build(kind).reset_index()
    assert out.i.tolist() == ['served-a', 'native-b']
    assert out.v.tolist() == ['cluster-a', 'cluster-b']
    assert out.Roof.tolist() == ['Grass', 'Iron Sheets']
    assert out.Floor.iloc[0] == 'Mud' and pd.isna(out.Floor.iloc[1])
    assert len(s.walks) == 1


def test_recorded_ag_pair_fills_only_missing_primary_and_keeps_order(route):
    raw = _housing(('ag-old', 'old-a'))
    raw['h9q4'] = pd.Categorical(raw['h9q4'])
    out = route(raw, _cover(('old-a',), ('new-a',)),
                _cover(('unused-ag', 'ag-old', 'old-a'),
                       ('native-unused', 'native-ag', 'new-a')))
    assert out.index.get_level_values('i').tolist() == ['native-ag', 'new-a']
    pd.testing.assert_frame_equal(out.reset_index(drop=True),
                                 raw[['h9q4', 'h9q6']].set_axis(['Roof', 'Floor'], axis=1))
    assert len(out) == len(raw) and list(out.columns) == ['Roof', 'Floor']


def test_recorded_ag_pair_does_not_invent_missing_or_unsupported_keys(route):
    raw = _housing(('unsupported', None))
    with pytest.warns(UserWarning, match=r'2 of 2 housing rows \(1 missing source IDs\)'):
        out = route(raw, _cover(('cover-only',), ('native-hh',)),
                    _cover((None, 'ag-only'), ('not-for-null', 'native-ag')))
    assert out.index.get_level_values('i')[0] == 'unsupported'
    assert pd.isna(out.index.get_level_values('i')[1])
    pd.testing.assert_frame_equal(out.reset_index(drop=True),
                                 raw[['h9q4', 'h9q6']].set_axis(['Roof', 'Floor'], axis=1))


def test_recorded_ag_pairs_collapse_only_agreeing_formatted_repeats(route):
    out = route(_housing(('ag-old',)), _cover(('cover-only',), ('native-hh',)),
                _cover(('ag-old', 'ag-old'), (10.0, '10')))
    assert out.index.get_level_values('i').tolist() == ['10']


def test_recorded_ag_ambiguity_refuses_before_join(route):
    with pytest.raises(ValueError, match='ambiguous hh/HHID bridge'):
        route(_housing(('ag-old',)), _cover(('cover-only',), ('native-hh',)),
              _cover(('ag-old', 'ag-old'), ('native-ag', 'different')))


def test_recorded_cross_cover_identity_disagreement_refuses(route):
    with pytest.raises(ValueError, match='conflicting household/agriculture'):
        route(_housing(), _cover(), _cover(('old-a',), ('different',)))


@pytest.mark.parametrize('agriculture', [
    FileNotFoundError('synthetic missing agriculture identity'),
    pd.DataFrame({'hh': ['ag-old']}),
    _cover((None,), ('native-ag',)),
])
def test_recorded_ag_projection_unavailable_refuses(route, agriculture):
    with pytest.raises(ValueError, match='bridge (unavailable|pairs)'):
        route(_housing(), _cover(), agriculture)


@pytest.mark.parametrize('housing,cover,agriculture', [
    (_housing(('old-a', 'ag-old')), _cover(('old-a',), ('same',)),
     _cover(('ag-old',), ('same',))),
    (_housing(('ag-old', 'native-ag')), _cover(('cover-only',), ('native-hh',)),
     _cover(('ag-old',), ('native-ag',))),
])
def test_recorded_ag_target_collision_refuses(route, housing, cover, agriculture):
    with pytest.raises(ValueError, match='duplicate household keys'):
        route(housing, cover, agriculture)


@pytest.mark.parametrize('kind', ['country', 'feature'])
def test_recorded_ag_identity_uses_existing_sample_and_single_walk(actual_carrier, kind):
    s = actual_carrier
    s.housing = _housing(('old-a', 'ag-old'))
    s.housing['h9q4'] = ['Grass', 'Iron Sheets']
    s.agriculture = _cover(('ag-old',), ('native-ag',))
    flat = s.build(kind).reset_index()
    assert flat.i.tolist() == ['served-a', 'native-ag']
    assert flat.v.tolist() == ['cluster-a', 'cluster-ag']
    assert flat.Roof.tolist() == ['Grass', 'Iron Sheets']
    assert flat.Floor.iloc[0] == 'Mud' and pd.isna(flat.Floor.iloc[1])
    assert len(s.walks) == 1 and s.walks[0][1] == {'state': 'raw'}
    assert s.walks[0][0].i.tolist() == ['native-a', 'native-ag']
    assert s.samples == ['served sample leaf']
