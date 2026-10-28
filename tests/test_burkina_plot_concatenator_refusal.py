"""GH #1287: exercise the real country script with synthetic reader leaves.

No Country/Feature builds, provider reads or real parquet operations. Dummy
output markers and copied script declarations are private tmp_path fixtures.
"""
from __future__ import annotations

from pathlib import Path
import runpy

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library import Country, local_tools, paths


SOURCE = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Burkina_Faso'
SCRIPT = SOURCE / '_' / 'plot_features.py'
WAVES = ['2018-19', '2021-22']


def _frame(wave):
    # Retain distinct native EHCVM formats and nullable parcel coordinates.
    ids = ['10126', '3027'] if wave == '2018-19' else ['2126', 'CS_586001']
    df = pd.DataFrame({
        't': [wave, wave], 'i': ids, 'plot_id': ['1_1', '2_1'],
        'Area': pd.array([1.25, pd.NA], dtype='Float64'),
        'Tenure': pd.array(['Owned', pd.NA], dtype='string'),
        'Latitude': pd.array([pd.NA, pd.NA], dtype='Float64'),
    }).set_index(['t', 'i', 'plot_id'])
    df.attrs = {'id_converted': False, 'synthetic': {'source': 'native'}}
    return df


@pytest.fixture
def run_concat(tmp_path, monkeypatch):
    country = tmp_path / 'countries' / 'Burkina_Faso'
    build = country / '_'
    build.mkdir(parents=True)
    (build / 'data_scheme.yml').write_text(
        "Country: Burkina_Faso\nWaves: ['2014', '2018-19', '2021-22']\n"
        'Data Scheme:\n  plot_features:\n    materialize: make\n')
    target = build / 'plot_features.py'
    target.write_text(SCRIPT.read_text())
    for wave in ['2014', *WAVES]:
        (country / wave / '_').mkdir(parents=True)
        if wave in WAVES:
            (country / wave / '_' / 'plot_features.py').write_text('# declaration only\n')
    root = tmp_path / 'data'
    frames = {wave: _frame(wave) for wave in WAVES}
    reads, writes = [], []

    def read(fn, **kwargs):
        path = Path(fn)
        # Baseline relative paths and candidate absolute paths share one stub.
        wave = path.parts[-3]
        reads.append((wave, path))
        if wave not in present:
            raise FileNotFoundError(f'injected absent wave {wave}')
        if wave in failures:
            raise failures[wave]
        return frames[wave].copy(deep=True)

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, *a, **k: writes.append(frame.copy(deep=True)))
    monkeypatch.setattr(paths, 'data_root',
                        lambda country=None: root / country if country else root)
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(country.parent))
    paths.countries_root.cache_clear()
    monkeypatch.chdir(build)
    present, failures = set(WAVES), {}

    def run(*, missing=None, error=None, undeclare=None, outside_cwd=False,
            duplicate=False):
        nonlocal present, failures
        present = set(WAVES) - ({missing} if missing else set())
        failures = {WAVES[-1]: error} if error else {}
        for wave in present:
            d = root / 'Burkina_Faso' / wave / '_'
            d.mkdir(parents=True, exist_ok=True)
            (d / 'plot_features.parquet').touch()  # marker only, never decoded
        if undeclare:
            (country / undeclare / '_' / 'plot_features.py').unlink()
        if duplicate:
            frames[WAVES[-1]] = pd.concat([frames[WAVES[-1]], frames[WAVES[-1]]])
        if outside_cwd:
            monkeypatch.chdir(tmp_path)
        runpy.run_path(str(target), run_name='__burkina_concat_test__')
        return writes

    run.reads, run.writes, run.root, run.frames = reads, writes, root, frames
    yield run
    paths.countries_root.cache_clear()


def test_current_wave_metadata_declares_exactly_the_ehcvm_waves():
    country = Country('Burkina_Faso')
    assert [w for w in country.waves if 'plot_features' in country[w].data_scheme] == WAVES
    assert not (SOURCE / '2014/_/plot_features.py').exists()


@pytest.mark.parametrize('missing', WAVES)
def test_missing_declared_output_refuses_before_country_write(run_concat, missing):
    with pytest.raises(FileNotFoundError, match=f'wave {missing} declares plot_features'):
        run_concat(missing=missing)
    assert run_concat.writes == []


@pytest.mark.parametrize('error', [ValueError('injected corrupt parquet'),
                                  PermissionError('injected unreadable parquet')])
def test_reader_errors_propagate_without_partial_write(run_concat, error):
    with pytest.raises(type(error), match=str(error)) as caught:
        run_concat(error=error)
    assert caught.value is error
    assert run_concat.writes == []


@pytest.mark.parametrize('outside_cwd', [False, True])
def test_healthy_concat_preserves_typed_native_frame(run_concat, outside_cwd):
    written = run_concat(outside_cwd=outside_cwd)
    assert len(written) == 1
    expected = pd.concat([run_concat.frames[w] for w in WAVES])
    assert_frame_equal(written[0], expected, check_exact=True)
    assert written[0].attrs == expected.attrs
    assert [w for w, _ in run_concat.reads] == WAVES  # undeclared 2014 unread


def test_reads_configured_data_root(run_concat):
    run_concat()
    assert all(p == run_concat.root / 'Burkina_Faso' / w / '_' / 'plot_features.parquet'
               for w, p in run_concat.reads)


@pytest.mark.parametrize('undeclare', WAVES)
def test_retired_wave_is_skipped_before_reader(run_concat, undeclare):
    # Root's explicit GH1287 correction replaces the earlier CONFIGERROR
    # expectation. Original test bytes/results remain in private historical
    # evidence; an absent script AND absent YAML means genuinely unwired.
    run_concat(undeclare=undeclare)
    retained = [w for w in WAVES if w != undeclare]
    assert [w for w, _ in run_concat.reads] == retained
    assert_frame_equal(run_concat.writes[0], run_concat.frames[retained[0]], check_exact=True)


def test_duplicate_index_still_refuses(run_concat):
    with pytest.raises(AssertionError, match='Non-unique'):
        run_concat(duplicate=True)
    assert run_concat.writes == []
