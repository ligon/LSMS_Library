"""Invalid wave selections cannot mutate caches before they are rejected.

Real Malawi registry, cache gates and assembly with controlled native frames;
no microdata. Held-source API/frame checks are recorded separately.
"""
import hashlib
import os
import stat

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library.local_tools import read_parquet_cache_waves, to_parquet
from lsms_library.paths import data_root


def _physical_tree(root):
    """Bind pathsets, bytes and physical state, including empty directories."""
    rows = {}
    for path in sorted(root.rglob('*')):
        info = path.lstat()
        row = (stat.S_IMODE(info.st_mode), info.st_size, info.st_mtime_ns,
               info.st_nlink, info.st_dev, info.st_ino)
        if path.is_file():
            row += (hashlib.sha256(path.read_bytes()).hexdigest(),)
        elif path.is_symlink():
            row += (os.readlink(path),)
        rows[str(path.relative_to(root))] = row
    return rows


@pytest.fixture
def native_food(tmp_path, monkeypatch):
    root = tmp_path / 'data'
    monkeypatch.setenv('LSMS_DATA_DIR', str(root))
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    data_root.cache_clear()
    c = C.Country('Malawi', preload_panel_ids=False)
    c._panel_ids_attempted = True
    c._updated_ids_cache = {}
    state = {'quantity': 2.5, 'built': []}

    def grab(wave, request, *, _with_origin=False):
        assert request == 'food_acquired'
        state['built'].append(wave.year)
        frame = pd.DataFrame({
            't': [wave.year], 'i': ['house'], 'j': ['Rice'],
            'u': ['kg'], 's': ['purchased'],
            'Quantity': [state['quantity']], 'Expenditure': [10.0],
            'Quantity_kg': [state['quantity']],
        }).set_index(['t', 'i', 'j', 'u', 's'])
        # Native scripts write a hashless wave parquet before returning.
        path = root / 'Malawi' / wave.year / '_' / 'food_acquired.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        to_parquet(frame, path, absolute_path=True)
        return (frame, 'parquet') if _with_origin else frame

    monkeypatch.setattr(C.Wave, 'grab_data', grab)
    # The v-join is separately covered. Retain real finalization/id_walk but
    # prevent a second table from asking for microdata in this controlled test.
    monkeypatch.setattr(C.Country, '_join_v_from_sample', lambda self, df, *a, **k: df)
    # A synthetic unused L1 blob proves that invalid input cannot touch L1.
    blob = root / 'dvc-cache' / 'aa' / 'unused-synthetic-blob'
    blob.parent.mkdir(parents=True)
    blob.write_bytes(b'synthetic L1 control; never read as survey data')
    try:
        yield c, root, state
    finally:
        data_root.cache_clear()


_ROUTES = [
    ('internal', {}), ('food_acquired', {}),
    ('food_expenditures', {}), ('food_prices', {}), ('food_quantities', {}),
    ('food_prices', {'units': 'unitvalue'}),
    ('food_expenditures', {'basis': 'total', 'valuation': 'median_price'}),
    ('food_acquired', {'_backend': 'make'}),
]


@pytest.mark.parametrize('route,kwargs', _ROUTES)
@pytest.mark.parametrize('mixed', [False, True])
def test_invalid_request_preserves_complete_cache_tree(native_food, monkeypatch,
                                                       route, kwargs, mixed):
    c, root, state = native_food
    c.food_acquired()  # ordinary writer establishes hash and all-wave coverage
    assert len(list((root / 'Malawi').glob('*/_/food_acquired.parquet'))) == 5
    bad = 'invalid-wave'
    with pytest.raises(KeyError) as reference:
        c[bad]
    request = [c.waves[2], bad] if mixed else [bad]
    kwargs = dict(kwargs)
    if kwargs.pop('_backend', None):
        monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')

    def valuation_prework(waves):
        (root / 'unexpected-valuation-prework').write_text('invalid request')
        return None

    monkeypatch.setattr(c, '_valuation_geo', valuation_prework)
    getitem = C.Country.__getitem__
    looked_up = []

    def lookup(self, year):
        looked_up.append(year)
        return getitem(self, year)

    monkeypatch.setattr(C.Country, '__getitem__', lookup)
    # A bad Make request must be rejected before any external builder; the
    # f8 country builder is independently broken and needs no raw-data replay.
    def no_builder(*args, **kwargs):
        raise AssertionError('invalid selection reached an external builder')

    monkeypatch.setattr(C.subprocess, 'run', no_builder)
    before = _physical_tree(root)
    with pytest.raises(KeyError) as caught:
        if route == 'internal':
            c._aggregate_wave_data(request, 'food_acquired')
        else:
            getattr(c, route)(waves=request, **kwargs)
    assert caught.value.args == reference.value.args
    assert _physical_tree(root) == before
    assert looked_up == [bad], 'validation constructed a valid Wave'


@pytest.mark.parametrize('selection', ['all', 'subset', 'empty', 'reverse', 'duplicates', 'tuple', 'iterator'])
def test_valid_warm_selections_keep_values_and_physical_state(native_food, selection):
    c, root, state = native_food
    full = c.food_acquired()
    waves = c.waves
    choices = {'all': None, 'subset': [waves[2]], 'empty': [],
               'reverse': list(reversed(waves)), 'duplicates': [waves[2], waves[2]],
               'tuple': (waves[2],), 'iterator': iter([waves[2]])}
    requested = choices[selection]
    before = _physical_tree(root)
    result = c.food_acquired(waves=requested)
    if selection in {'all', 'reverse'}:
        expected = full
    elif selection == 'empty':
        expected = full.iloc[:0]
    else:
        expected = full[full.index.get_level_values('t') == waves[2]]
    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    assert _physical_tree(root) == before
    assert state['built'] == waves


@pytest.mark.parametrize('country_cache', ['absent', 'stale'])
def test_valid_rebuild_still_evicts_hashless_waves(native_food, monkeypatch, country_cache):
    c, root, state = native_food
    c.food_acquired()
    state['built'].clear()
    state['quantity'] = 7.0
    country_path = root / 'Malawi/var/food_acquired.parquet'
    if country_cache == 'absent':
        country_path.unlink()  # synthetic fixture; exercise ordinary missing descent
    else:
        compute = C.Country._table_cache_hash
        monkeypatch.setattr(C.Country, '_table_cache_hash',
                            lambda self, table, waves: compute(self, table, waves) + '-changed')
    result = c.food_acquired()
    assert state['built'] == c.waves
    assert (result['Quantity'] == 7.0).all()
    assert (result['Quantity_kg'] == 7.0).all()


def test_valid_subset_coverage_grows_then_serves_without_writes(native_food):
    c, root, state = native_food
    a, b = c.waves[2], c.waves[-1]
    c.food_acquired(waves=[a])
    country_path = root / 'Malawi/var/food_acquired.parquet'
    assert read_parquet_cache_waves(country_path) == [a]
    second = c.food_acquired(waves=[b])
    assert set(read_parquet_cache_waves(country_path)) == {a, b}
    assert set(second.index.get_level_values('t')) == {b}
    before = _physical_tree(root)
    calls = list(state['built'])
    first = c.food_acquired(waves=[a])
    assert set(first.index.get_level_values('t')) == {a}
    assert state['built'] == calls and _physical_tree(root) == before


def test_existing_argument_errors_precede_wave_validation(native_food):
    c, root, state = native_food
    with pytest.raises(ValueError, match='basis='):
        c.food_expenditures(waves=['invalid-wave'], basis='invalid-basis')
    assert state['built'] == []


def test_string_is_not_reinterpreted_as_a_single_wave(native_food):
    c, root, state = native_food
    # Preserve the existing iterable-of-characters behavior; no new scalar API.
    with pytest.raises(KeyError) as reference:
        c['2']
    with pytest.raises(KeyError) as caught:
        c.food_acquired(waves='2013-14')
    assert caught.value.args == reference.value.args
