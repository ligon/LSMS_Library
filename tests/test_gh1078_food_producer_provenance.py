"""Actual producer routes, not cache-path observations, choose food handling."""
from __future__ import annotations

import inspect
from pathlib import Path

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import _build_registry as R
from lsms_library.paths import countries_root, data_root


COUNTRY = 'Fakeland'
WAVE = '2001'
TABLE = 'food_acquired'


@pytest.fixture
def producer(tmp_path, monkeypatch):
    """Private config and real CSV/script/parquet operations, no microdata."""
    croot = tmp_path / 'countries'
    droot = tmp_path / 'data'
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(croot))
    monkeypatch.setenv('LSMS_DATA_DIR', str(droot))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    country_dir = croot / COUNTRY / '_'
    wave_dir = croot / COUNTRY / WAVE / '_'
    country_dir.mkdir(parents=True)
    wave_dir.mkdir(parents=True)
    (country_dir / 'data_scheme.yml').write_text(
        f'Country: {COUNTRY}\nWaves: ["{WAVE}"]\nData Scheme:\n'
        f'  {TABLE}:\n    index: (t, i, j, u, s)\n'
        '    Quantity: float\n    Expenditure: float\n    Price: float\n')
    makefile = country_dir / 'Makefile'
    makefile.write_text(
        '../%/_/food_acquired.parquet: ../%/_/food_acquired.py\n'
        '\tcd $(@D) && python food_acquired.py\n')
    source = tmp_path / 'observations.csv'  # outside the private config tree
    source.write_text(
        'id,item,unit,source,quantity,expenditure,produced,price,extra\n'
        'h1,A,Kg,purchased,2,10,4,5,keep-in-memory\n'
        'h1,A,Kg,purchased,bad,3,bad,9,discard-by-legacy\n'
        'h2,B,Kg,produced,,7,,11,missing-quantity\n'
        'h3,C,Kg,inkind,4,,1,13,missing-expenditure\n')

    class Fixture:
        external = droot / COUNTRY / WAVE / '_' / 'food_acquired.parquet'
        ran = wave_dir / 'RAN'

        def wave(self):
            return C.Country(COUNTRY, preload_panel_ids=False)[WAVE]

        def yaml(self, empty=False):
            if empty:
                source.write_text(source.read_text().splitlines()[0] + '\n')
            (wave_dir / 'data_info.yml').write_text(
                f'{TABLE}:\n  file: {source}\n'
                '  idxvars: {i: id, j: item, u: unit, s: source}\n'
                '  myvars:\n    Quantity: quantity\n'
                '    Expenditure: expenditure\n    Produced: produced\n'
                '    Price: price\n    Extra: extra\n')
            return self.wave()

        def script(self, empty=False, output=True):
            # Same established fixture pattern as GH #803: the child does not
            # import ll under a fake config tree without a DVC repository.
            text = (
                'import os\nfrom pathlib import Path\nimport pandas as pd\n'
                'df = pd.DataFrame({"t": ["2001", "2001"], '
                '"i": ["h1", "h2"], "j": ["A", "B"], '
                '"u": ["Kg", "Kg"], "s": ["purchased", "produced"], '
                '"Quantity": [float("nan"), 2.0], '
                '"Expenditure": [3.0, float("nan")], '
                '"Price": [4.0, 5.0]}).set_index(["t","i","j","u","s"])\n'
                'df.attrs["survey_note"] = "preserve this metadata"\n'
            )
            if empty:
                text += 'df = df.iloc[:0]\n'
            if output:
                text += (
                    'out = Path(os.environ["LSMS_DATA_DIR"]) / '
                    '"Fakeland/2001/_/food_acquired.parquet"\n'
                    'out.parent.mkdir(parents=True, exist_ok=True)\n'
                    'df.to_parquet(out)\n')
            text += 'Path(__file__).with_name("RAN").write_text("1")\n'
            (wave_dir / 'food_acquired.py').write_text(text)
            return self.wave()

        def without_makefile(self):
            makefile.unlink()

    try:
        yield Fixture()
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def assert_same(actual, expected):
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.attrs == expected.attrs
    assert not any('origin' in str(key) for key in actual.attrs)


def test_default_public_interface_and_yaml_cache_origins(producer):
    wave = producer.yaml()
    built, origin = wave.grab_data(TABLE, _with_origin=True)
    assert origin == 'materialized' and producer.external.exists()
    stored = pd.read_parquet(producer.external)
    assert_same(built, stored)
    cached, origin = producer.wave().grab_data(TABLE, _with_origin=True)
    assert origin == 'parquet'
    assert_same(cached, stored)
    default = producer.wave().grab_data(TABLE)
    assert isinstance(default, pd.DataFrame)
    assert_same(default, stored)
    parameter = inspect.signature(C.Wave.grab_data).parameters['_with_origin']
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is False


@pytest.mark.parametrize('empty', [False, True])
def test_script_post_make_and_existing_parquet_are_real_producers(producer, empty):
    wave = producer.script(empty=empty)
    result, origin = wave.grab_data(TABLE, _with_origin=True)
    assert producer.ran.exists() and producer.external.exists()
    assert origin == 'parquet' and result.empty == empty
    assert_same(result, pd.read_parquet(producer.external))
    again, origin = producer.wave().grab_data(TABLE, _with_origin=True)
    assert origin == 'parquet'
    assert_same(again, result)
    assert_same(producer.wave().food_acquired(), result)


@pytest.mark.parametrize('route', ['script', 'yaml'])
def test_actual_producer_survives_real_post_load_unlink(producer, monkeypatch, route):
    wave = getattr(producer, route)()
    real_grab = wave.grab_data
    observed = {}

    def grab_then_unlink(request, *, _with_origin=False):
        frame, origin = real_grab(request, _with_origin=True)
        assert producer.external.exists()
        observed.update(frame=frame.copy(deep=True), origin=origin)
        producer.external.unlink()  # real operation; no forged exists result
        return (frame, origin) if _with_origin else frame

    monkeypatch.setattr(wave, 'grab_data', grab_then_unlink)
    actual = wave.food_acquired()
    assert not producer.external.exists()
    assert observed['origin'] == ('parquet' if route == 'script' else 'materialized')
    assert_same(actual, observed['frame'])
    if route == 'script':
        assert actual['Quantity'].isna().sum() == 1
        assert actual['Expenditure'].isna().sum() == 1


@pytest.mark.parametrize('reason', ['disabled', 'write_failure'])
@pytest.mark.parametrize('old_file', [False, True])
def test_real_memory_route_keeps_legitimate_legacy_reducer(producer, monkeypatch, reason, old_file):
    wave = producer.yaml()
    if old_file:
        producer.external.parent.mkdir(parents=True)
        C.to_parquet(pd.DataFrame({'unrelated': [999]}), producer.external,
                     absolute_path=True, cache_hash='old-unrelated-stale')
    if reason == 'disabled':
        # Synthetic route contract only; real-source validation never bypasses.
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
    else:
        def unwritable(*args, **kwargs):
            raise OSError('controlled private write failure')
        monkeypatch.setattr(C, 'to_parquet', unwritable)

    def read():
        if reason == 'write_failure':
            with pytest.warns(UserWarning, match='L2-wave cache write failed'):
                return wave.grab_data(TABLE, _with_origin=True)
        return wave.grab_data(TABLE, _with_origin=True)

    raw, origin = read()
    assert origin == 'memory' and len(raw) == 4
    if reason == 'write_failure':
        with pytest.warns(UserWarning, match='L2-wave cache write failed'):
            actual = wave.food_acquired()
    else:
        actual = wave.food_acquired()
    assert actual.index.names == ['t', 'i', 'j', 'u', 's']
    assert list(actual.columns) == ['Expenditure', 'Quantity', 'Produced', 'Price']
    assert len(actual) == 3 and actual.index.is_unique
    assert actual['Expenditure'].tolist() == [13.0, 7.0, 0.0]
    assert actual['Quantity'].tolist() == [2.0, 0.0, 4.0]
    assert actual['Produced'].tolist() == [4.0, 0.0, 1.0]
    assert actual['Price'].tolist() == [raw['Price'].iloc[n] for n in (0, 2, 3)]
    assert producer.external.exists() == old_file
    assert not any('origin' in str(key) for key in actual.attrs)


def test_empty_yaml_is_memory_and_food_returns_empty(producer):
    wave = producer.yaml(empty=True)
    frame, origin = wave.grab_data(TABLE, _with_origin=True)
    assert frame.empty and origin == 'memory' and not producer.external.exists()
    assert_same(wave.food_acquired(), frame)


@pytest.mark.parametrize('route', ['undeclared', 'no_makefile', 'no_output'])
def test_missing_routes_return_adapted_empty_without_attrs(producer, route):
    wave = producer.script(output=False)
    if route == 'undeclared':
        with pytest.warns(UserWarning, match='Data scheme does not contain'):
            frame, origin = wave.grab_data('unlisted', _with_origin=True)
    elif route == 'no_makefile':
        producer.without_makefile()
        with pytest.warns(UserWarning, match='Makefile not found'):
            frame, origin = wave.grab_data(TABLE, _with_origin=True)
    else:
        frame, origin = wave.grab_data(TABLE, _with_origin=True)
        assert producer.ran.exists()
    assert frame.empty and origin == 'missing' and frame.attrs == {}


def test_food_producer_is_versioned_for_its_actual_table():
    name = 'lsms_library.country.Wave.food_acquired'
    fn, tables = R._BUILD_TRANSFORMS[name]
    assert fn is C.Wave.food_acquired and tables == ('food_acquired',)
    for table in ('food_acquired', 'household_roster'):
        seen = set()
        for _, (entry, scope) in sorted(R._BUILD_TRANSFORMS.items()):
            if not scope or table in scope:
                R._closure_parts(entry, seen)
        assert (name in seen) == (table == 'food_acquired')
