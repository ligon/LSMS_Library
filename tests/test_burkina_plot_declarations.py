"""GH1287 root-authorized declaration controls; invented IO, real metadata.

Ledger burkina-owner-source-review-20261028 §7. No getters/builds or payload
files: Country/Wave read only private metadata; output markers stay empty.
"""
from pathlib import Path
import os
import runpy
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library import Country, local_tools, paths


SCRIPT = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Burkina_Faso/_/plot_features.py'
EHCVM = ['2018-19', '2021-22']


def _frame(wave):
    ids = ['10126', '3027'] if wave == EHCVM[0] else ['2126', 'CS_586001']
    frame = pd.DataFrame({
        't': [wave] * 2, 'i': ids, 'plot_id': ['2_1', '1_1'],
        'Area': pd.array([0., pd.NA], dtype='Float64'),
        'Tenure': pd.Categorical(['Owned', None], categories=['Rented', 'Owned']),
        'Latitude': pd.array([pd.NA, 11.5], dtype='Float64'),
        'Irrigated': pd.array([False, pd.NA], dtype='boolean'),
    }).set_index(['t', 'i', 'plot_id'])
    frame.attrs = {'id_converted': False, 'synthetic': {'wave': wave}}
    return frame


@pytest.fixture
def metadata_compiler(tmp_path, monkeypatch):
    config = tmp_path / 'countries'
    country_dir = config / 'Burkina_Faso'
    build = country_dir / '_'
    build.mkdir(parents=True)
    source = Path(os.environ.get('BURKINA_CONCAT_SOURCE', str(SCRIPT)))
    target = build / 'plot_features.py'
    target.write_text(source.read_text())
    data = tmp_path / 'invented-output-markers'
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
    monkeypatch.setenv('LSMS_DATA_DIR', str(data))
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()
    reads, writes = [], []
    frames = {w: _frame(w) for w in EHCVM}
    folders, failures = {}, {}

    def read(path):
        path = Path(path)
        reads.append(path)
        logical = next(w for w, folder in folders.items() if folder == path.parts[-3])
        if logical in failures:
            raise failures[logical]
        return frames[logical].copy(deep=True)

    def write(frame, path):
        assert path == '../var/plot_features.parquet'
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.setattr(local_tools, 'to_parquet', write)

    def prepare(*, retired=(), yaml_only=(), mapped=False, empty=(),
                missing=(), errors=None, extra=False, equal_attrs=False):
        registered = ['2014', *EHCVM, *(['2024-25'] if extra else [])]
        for w in registered:
            folders[w] = f'folder-{w}' if mapped else w
        (build / 'data_scheme.yml').write_text(
            'Country: Burkina_Faso\nWaves: ' + repr(registered) + '\n'
            'wave_folder_map: ' + repr(folders) + '\n'
            'Data Scheme:\n  plot_features:\n    materialize: make\n')
        if extra:
            frames['2024-25'] = _frame('2024-25')
        for w in registered:
            directory = country_dir / folders[w] / '_'
            directory.mkdir(parents=True)
            if w == '2014' or w in retired:
                (directory / 'data_info.yml').write_text('Country: Burkina_Faso\n')
            elif w in yaml_only:
                (directory / 'data_info.yml').write_text(
                    'Country: Burkina_Faso\nplot_features:\n'
                    '  file: NEVER_READ.dta\n  idxvars: {i: hhid}\n')
            else:
                (directory / 'plot_features.py').write_text('# declaration only\n')
            marker = data / 'Burkina_Faso' / folders[w] / '_' / 'plot_features.parquet'
            marker.parent.mkdir(parents=True, exist_ok=True)
            if w not in missing:
                # Also place a stale marker for retired/2014 waves. It is
                # deliberately not sufficient to make the table declared.
                marker.touch()
        for w in empty:
            frames[w] = frames[w].iloc[:0].copy()
        if equal_attrs:
            for frame in frames.values():
                frame.attrs = {'id_converted': False, 'synthetic': {'same': True}}
        failures.update(errors or {})
        country = Country('Burkina_Faso')
        declared = [w for w in country.waves if 'plot_features' in country[w].data_scheme]
        assert declared == [w for w in registered if w != '2014' and w not in retired]
        # Run from outside the config tree, preserving explicit data-root IO.
        monkeypatch.chdir(tmp_path)
        return declared

    def run():
        return runpy.run_path(str(target), run_name='__burkina_metadata_test__')

    yield SimpleNamespace(prepare=prepare, run=run, frames=frames, reads=reads,
                          writes=writes, data=data, folders=folders)
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()


def _assert_exact_output(case, declared):
    assert case.reads == [case.data / 'Burkina_Faso' / case.folders[w] / '_' /
                          'plot_features.parquet' for w in declared]
    assert len(case.writes) == 1
    expected = pd.concat([case.frames[w] for w in declared])
    pd.testing.assert_frame_equal(case.writes[0], expected, check_exact=True)
    assert case.writes[0].attrs == expected.attrs


@pytest.mark.parametrize('retired', EHCVM)
def test_each_retired_wave_skips_stale_output_before_reader(metadata_compiler, retired):
    declared = metadata_compiler.prepare(retired=[retired])
    metadata_compiler.run()
    _assert_exact_output(metadata_compiler, declared)


def test_all_retired_has_no_reader_or_writer(metadata_compiler):
    metadata_compiler.prepare(retired=EHCVM)
    with pytest.raises(AssertionError, match='no wave-level parquets found'):
        metadata_compiler.run()
    assert metadata_compiler.reads == [] and metadata_compiler.writes == []


@pytest.mark.parametrize('yaml_wave', EHCVM)
def test_yaml_only_table_remains_declared(metadata_compiler, yaml_wave):
    declared = metadata_compiler.prepare(yaml_only=[yaml_wave])
    metadata_compiler.run()
    _assert_exact_output(metadata_compiler, declared)


@pytest.mark.parametrize('yaml_only', [False, True])
@pytest.mark.parametrize('failed', EHCVM)
def test_declared_native_or_yaml_reader_error_stays_loud(metadata_compiler, yaml_only, failed):
    error = PermissionError('invented declared reader refusal')
    declared = metadata_compiler.prepare(yaml_only=[failed] if yaml_only else [],
                                        errors={failed: error})
    with pytest.raises(PermissionError) as caught:
        metadata_compiler.run()
    assert caught.value is error
    assert metadata_compiler.writes == []
    assert len(metadata_compiler.reads) == declared.index(failed) + 1


@pytest.mark.parametrize('yaml_only', [False, True])
def test_declared_missing_output_refuses_before_reader(metadata_compiler, yaml_only):
    metadata_compiler.prepare(yaml_only=[EHCVM[0]] if yaml_only else [],
                              missing=[EHCVM[0]])
    with pytest.raises(FileNotFoundError, match='wave 2018-19 declares plot_features'):
        metadata_compiler.run()
    assert metadata_compiler.reads == [] and metadata_compiler.writes == []


@pytest.mark.parametrize('empty', [(), (EHCVM[0],), tuple(EHCVM)])
@pytest.mark.parametrize('equal_attrs', [False, True])
def test_changed_metadata_path_preserves_healthy_and_declared_empty_frames(
        metadata_compiler, empty, equal_attrs):
    declared = metadata_compiler.prepare(empty=empty, equal_attrs=equal_attrs)
    metadata_compiler.run()
    _assert_exact_output(metadata_compiler, declared)


def test_declared_folder_map_controls_output_path(metadata_compiler):
    declared = metadata_compiler.prepare(mapped=True, yaml_only=[EHCVM[1]])
    metadata_compiler.run()
    _assert_exact_output(metadata_compiler, declared)


def test_new_registered_table_is_not_hidden_by_old_wave_list(metadata_compiler):
    declared = metadata_compiler.prepare(extra=True)
    metadata_compiler.run()
    _assert_exact_output(metadata_compiler, declared)


def test_native_compiler_output_receives_one_real_framework_id_walk(metadata_compiler):
    """Invented chain witnesses exercise actual id_walk after native concat.

    These strings are fabricated input identities, not a new population or
    production crosswalk. The compiler must never pre-apply the panel map.
    """
    declared = metadata_compiler.prepare(equal_attrs=True)
    metadata_compiler.run()
    _assert_exact_output(metadata_compiler, declared)
    raw = metadata_compiler.writes[0]
    assert raw.attrs['id_converted'] is False
    before = raw.copy(deep=True)
    # Both keys are present: each household moves once, with no closure.
    updated = {EHCVM[1]: {'2126': 'CS_586001', 'CS_586001': 'canonical'}}
    wanted = raw.rename(index=updated[EHCVM[1]], level='i')
    wanted.attrs = {**raw.attrs, 'id_converted': True}
    walked = local_tools.id_walk(raw.copy(deep=True), updated, state='raw')
    pd.testing.assert_frame_equal(walked, wanted, check_exact=True)
    assert walked.attrs == wanted.attrs
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    assert raw.attrs == before.attrs
    # A walked-only witness makes the real second call a no-op even with
    # attrs lost; no fake ID-walk function or finalizer is substituted.
    without_attrs = walked.copy(deep=True)
    without_attrs.attrs = {}
    second = local_tools.id_walk(without_attrs, updated)
    pd.testing.assert_frame_equal(second, walked, check_exact=True)
    assert second.attrs == {'id_converted': True}
