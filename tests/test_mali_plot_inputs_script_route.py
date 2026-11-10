"""Own-wave null declarations must reach the existing script route (GH1494).

Actual config/loader/Wave dispatch and source-sidecar metadata are used. Frames,
producer outputs and reader leaves are invented; no actual source or API build.
"""
from copy import deepcopy
from pathlib import Path
import subprocess

import pandas as pd
import pytest

from lsms_library import country as C
from lsms_library.paths import countries_root, data_root
from tests.test_gh1196_parquet_index_policy import _exact


WAVES = ('2018-19', '2021-22')
TABLE = 'plot_inputs'


class _CountryMetadata:
    name = 'Mali'
    waves = WAVES

    @property
    def file_path(self):
        return countries_root() / self.name

    def __getitem__(self, year):
        return C.Wave(year, year, self)


def _frame(year):
    ids = ['12003', '12004']
    if year == '2021-22':
        ids = ['EHCVM21_' + value for value in ids]
    index = pd.MultiIndex.from_arrays([
        pd.array([year, year], dtype='string'),
        pd.array(ids, dtype='string'),
        pd.array(['Farm-level'] * 2, dtype='string'),
        pd.array(['Urea', 'DAP et autres engrais inorganiques'], dtype='string'),
        pd.array(['(not crop-specific)'] * 2, dtype='string'),
    ], names=['t', 'i', 'plot_id', 'input', 'crop'])
    frame = pd.DataFrame({
        'Quantity': pd.array([1.25, None], dtype='Float64'),
        'Purchased': pd.array([False, None], dtype='boolean'),
        'u': pd.array(['Kg', 'Unknown'], dtype='string'),
    }, index=index)
    frame.attrs = {'id_converted': False, 'invented': {'typed': (1, True, None)}}
    return frame


@pytest.mark.parametrize('year', WAVES)
def test_script_classification_and_own_source_sidecar_are_retained(year, monkeypatch):
    country = _CountryMetadata()
    wave = country[year]
    assert wave.resources[TABLE] is None
    assert TABLE in wave.data_scheme
    inputs = C.Country._make_wave_inputs(country, TABLE)
    assert inputs[Path('Mali') / year / '_' / 'plot_inputs.parquet'] == ([year], False)

    # The normal fingerprint path must now inspect the wrapper's own literal
    # source reference, not treat a truthy marker as an empty YAML source block.
    observed = []
    original = C.source_fingerprint

    def tracked(path):
        observed.append(Path(path).resolve())
        return original(path)

    monkeypatch.setattr(C, 'source_fingerprint', tracked)
    digest = wave._input_hash(TABLE)
    expected = wave.file_path / 'Data' / ('s16b_me_mli' + year[:4] + '.dta')
    assert observed == [expected.resolve()]
    assert expected.with_suffix('.dta.dvc').is_file()
    assert isinstance(digest, str) and len(digest) == 64


@pytest.mark.parametrize('year', WAVES)
@pytest.mark.parametrize('mode', ['warm-script', 'cold-script', 'recipe-failure'])
def test_actual_wave_uses_script_route_and_preserves_boundaries(year, mode, monkeypatch, tmp_path):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    wave = _CountryMetadata()[year]
    frame = _frame(year)
    before = deepcopy(frame)
    output = data_root('Mali') / year / '_' / 'plot_inputs.parquet'
    reads, builds, stamps = [], [], []
    error = subprocess.CalledProcessError(17, ['invented-script'])

    def forbidden(*args, **kwargs):
        raise AssertionError('script declaration entered YAML extraction/cache')

    monkeypatch.setattr(C.Wave, 'column_mapping', forbidden)
    monkeypatch.setattr(C.Wave, '_read_l2_wave_cache', forbidden)
    monkeypatch.setattr(C, 'df_data_grabber', forbidden)

    def selected(self, request, candidates):
        assert self is wave and request == TABLE and candidates == [output]
        if mode == 'warm-script':
            return output, deepcopy(frame), False
        return None, None, False

    monkeypatch.setattr(C.Wave, '_fresh_script_parquet', selected)

    def producer(cwd, target, *, force):
        builds.append((cwd, target, force))
        assert cwd == wave.file_path.parent / '_'
        assert target == Path(year) / '_' / 'plot_inputs.parquet'
        assert force is False and not output.exists()
        if mode == 'recipe-failure':
            raise error
        output.parent.mkdir(parents=True)
        output.write_bytes(b'invented producer-reader boundary')

    def reader(stream):
        reads.append(stream.name)
        assert Path(stream.name) == output and not stream.closed
        assert stream.read() == b'invented producer-reader boundary'
        return deepcopy(frame), None

    def stamp(path, country, request, wave_name):
        stamps.append((path, country, request, wave_name))

    monkeypatch.setattr(C, '_build_wave_script_target', producer)
    monkeypatch.setattr(C, '_read_parquet_with_index_policy', reader)
    monkeypatch.setattr(C, '_replay_wave_stamp', stamp)
    if mode == 'warm-script':
        output.parent.mkdir(parents=True)
        output.write_bytes(b'invented warm script selection')
    if mode == 'recipe-failure':
        with pytest.raises(subprocess.CalledProcessError) as caught:
            wave.grab_data(TABLE)
        assert caught.value is error
        assert reads == stamps == [] and not output.exists()
    else:
        got, origin = wave.grab_data(TABLE, _with_origin=True)
        assert origin == 'parquet'
        _exact(got, before)
        assert stamps == [(output, 'Mali', TABLE, year)]
        assert reads == ([] if mode == 'warm-script' else [str(output)])
    assert builds == ([] if mode == 'warm-script' else [(
        wave.file_path.parent / '_', Path(year) / '_' / 'plot_inputs.parquet', False)])
    _exact(frame, before)


@pytest.mark.parametrize('year', WAVES)
def test_malformed_truthy_yaml_is_not_silently_rerouted(year, monkeypatch):
    wave = _CountryMetadata()[year]
    monkeypatch.setattr(C.Wave, 'resources', property(
        lambda self: {TABLE: {'file': 'invented-invalid.dta'}}))
    monkeypatch.setattr(C.Wave, 'formatting_functions', property(lambda self: {}))
    monkeypatch.setattr(C.Wave, '_read_l2_wave_cache', lambda *args: None)
    reached = []

    def forbidden(*args, **kwargs):
        reached.append('unexpected source/script')
        raise AssertionError('malformed extraction should fail before IO')

    monkeypatch.setattr(C, '_build_wave_script_target', forbidden)
    monkeypatch.setattr(C, 'df_data_grabber', forbidden)
    with pytest.raises(AttributeError, match='items'):
        wave.grab_data(TABLE)
    assert reached == []
