"""GH1204: real Uganda routing respects native crop wave coverage.

The actual registry and Makefile select the builder. Synthetic native writes
exercise the shipped Wave read/audit path without requiring microdata.
Held-source raw/public/Feature and fresh-process checks are separate evidence.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import local_tools as lt
from lsms_library.paths import data_root


needs_make = pytest.mark.skipif(shutil.which('make') is None,
                                reason='GNU make not on PATH')
SUPPORTED = '2018-19'
BARREN = '2005-06'
NATIVE_INDEX = ['t', 'i', 'plot_id', 'j', 'u', 'condition', 'season',
                'production_obs']


@needs_make
def test_uganda_crop_has_no_country_builder():
    assert C.Country('Uganda', preload_panel_ids=False)._has_country_builder(
        'crop_production') is False


@needs_make
def test_direct_make_crop_target_builds_native_waves_only(tmp_path):
    c = C.Country('Uganda', preload_panel_ids=False)
    result = subprocess.run(
        ['make', '-n', f'LSMS_DATA_ROOT={tmp_path / "data"}', 'crop_production'],
        cwd=c.file_path / '_', capture_output=True, text=True, check=True)
    lines = [line for line in result.stdout.splitlines()
             if 'python ./crop_production.py' in line]
    assert len(lines) == 7
    assert all(f'../{w}/_' in result.stdout for w in c.waves if w != BARREN)
    assert f'../{BARREN}/_' not in result.stdout
    assert 'var/crop_production.parquet' not in result.stdout
    assert not (tmp_path / 'data').exists()


@pytest.fixture
def native_crop(tmp_path, monkeypatch):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    data_root.cache_clear()
    C._GRAIN_LEDGER.clear()
    built = []

    def write_native(cwd, relative, *, force=False):
        wave = Path(relative).parts[0]
        assert wave != BARREN
        built.append(wave)
        frame = pd.DataFrame({
            't': [wave] * 3, 'i': ['raw-house'] * 3,
            'plot_id': ['plot'] * 3, 'j': ['Maize'] * 3, 'u': ['Kg'] * 3,
            'condition': ['fresh'] * 3, 'season': ['A'] * 3,
            'production_obs': ['slot:1', 'slot:2', 'slot:3'],
            'Quantity': [2.5, 2.5, 0.0], 'KgFactor': [1.0, 1.0, 1.0],
            'Quantity_sold': [1.0, 1.0, 0.0], 'Value_sold': [9.0, 9.0, 0.0],
            'harvest_month': pd.array([6, 6, None], dtype='Int64'),
            'intercropped': pd.array([False, False, None], dtype='boolean'),
            'Unit_sold': ['Kg', 'Kg', None],
        }).set_index(NATIVE_INDEX)
        frame.attrs['transport_marker'] = {'flag': True, 'integer': 1}
        # Synthetic audit payload tests transport through the real wave read.
        # It makes no claim about Uganda's current native source observations.
        report = {'site': 'reduce_to_agreed', 'kind': 'cells_blanked',
                  'country': 'Uganda', 'table': 'crop_production', 'wave': wave,
                  'levels': NATIVE_INDEX, 'rows': 3, 'groups': 3,
                  'destroyed': 0, 'nan_key_rows': 0, 'cells_blanked': 1,
                  'conflicting_groups': 1, 'columns': {'Unit_sold': 1},
                  'examples': ['synthetic GH1204 transport fixture']}
        target = data_root('Uganda') / wave / '_' / 'crop_production.parquet'
        target.parent.mkdir(parents=True, exist_ok=True)
        lt.to_parquet(frame, target, absolute_path=True, grain_audit=[report])

    monkeypatch.setattr(C, '_build_wave_script_target', write_native)
    monkeypatch.setattr(lt, '_warm_dvc_cache_for_feature', lambda *a, **k: None)
    # Preserve real metadata/finalization, without building a second feature.
    monkeypatch.setattr(C.Country, '_join_v_from_sample', lambda self, df: df)
    real_run = C.subprocess.run

    def probe_only(command, *args, **kwargs):
        if isinstance(command, list) and command and command[0] == 'make' and '-n' in command:
            return real_run(command, *args, **kwargs)
        raise AssertionError('selected the retired country crop builder')

    monkeypatch.setattr(C.subprocess, 'run', probe_only)

    def country():
        c = C.Country('Uganda', preload_panel_ids=False)
        c._panel_ids_attempted = True
        c._updated_ids_cache = {w: {'raw-house': 'canonical-house'} for w in c.waves}
        return c

    yield country, built, data_root('Uganda') / 'var' / 'crop_production.parquet'
    C._GRAIN_LEDGER.clear()
    data_root.cache_clear()


def _waves(frame):
    return set(frame.index.get_level_values('t'))


@needs_make
@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_supported_cold_subset_keeps_native_observations_and_audit(
        native_crop, monkeypatch, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    country, built, var = native_crop
    out = country().crop_production(waves=[SUPPORTED], currency=None)
    assert built == [SUPPORTED]
    assert len(out) == 3 and out.index.is_unique
    assert _waves(out) == {SUPPORTED}
    assert out.index.get_level_values('i').tolist() == ['canonical-house'] * 3
    assert out.index.get_level_values('production_obs').tolist() == ['slot:1', 'slot:2', 'slot:3']
    assert out.Quantity.tolist() == [2.5, 2.5, 0.0]
    assert out.KgFactor.tolist() == [1.0, 1.0, 1.0]
    assert out.harvest_month.iloc[:2].tolist() == [6, 6]
    assert out.intercropped.iloc[:2].tolist() == [False, False]
    assert pd.isna(out.harvest_month.iloc[2]) and pd.isna(out.intercropped.iloc[2])
    assert out.attrs['id_converted'] is True
    assert out.attrs['transport_marker'] == {'flag': True, 'integer': 1}
    assert [r['wave'] for r in C.grain_reports('Uganda', 'crop_production')] == [SUPPORTED]
    if backend == 'dvc':
        raw = pd.read_parquet(var)
        assert _waves(raw) == {SUPPORTED} and len(raw) == 3
        assert raw.attrs['id_converted'] is True
        assert lt.read_parquet_cache_waves(var) == [SUPPORTED]
        assert lt.read_parquet_grain_audit(var)[0]['wave'] == SUPPORTED
    else:
        assert not var.exists()


@needs_make
@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_barren_cold_subset_never_populates_crop_cache(native_crop, monkeypatch, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    country, built, var = native_crop
    out = country().crop_production(waves=[BARREN], currency=None)
    assert out.empty and not built and not var.exists()
    assert 'production_obs' in out.index.names and 'Quantity' in out.columns


@needs_make
@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_alternating_requests_keep_raw_coverage_true(native_crop, monkeypatch, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    country, built, var = native_crop
    country().crop_production(waves=[SUPPORTED], currency=None)
    before = var.read_bytes() if var.exists() else None
    empty = country().crop_production(waves=[BARREN], currency=None)
    assert empty.empty
    if before is not None:
        assert var.read_bytes() == before, 'barren request replaced other-wave raw cache'
    full = country().crop_production(currency=None)
    assert len(full) == 21 and _waves(full) == set(country().waves) - {BARREN}
    before = var.read_bytes() if var.exists() else None
    subset = country().crop_production(waves=[SUPPORTED], currency=None)
    assert len(subset) == 3 and _waves(subset) == {SUPPORTED}
    assert country().crop_production(waves=[BARREN], currency=None).empty
    if backend == 'dvc':
        assert var.read_bytes() == before
        assert len(pd.read_parquet(var)) == 21
        assert lt.read_parquet_cache_waves(var) == country().waves
    else:
        assert not var.exists()
