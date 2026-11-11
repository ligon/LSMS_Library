"""Own PH cards/routing and actual script→writer→country compiler, invented data.

No native/parser/table API is called. Script getter leaves return invented
frames; the real sanctioned writer and existing country concatenator are used.
"""
import ast
from copy import deepcopy
import hashlib
from pathlib import Path
import runpy
import sys

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.countries.Nigeria._ import nigeria
from lsms_library.countries.Nigeria._ import nigeria_ph_plot_features as ph
from lsms_library import local_tools
from lsms_library.paths import data_root
from tests.test_gh1196_parquet_index_policy import _exact

ROOT = Path(__file__).resolve().parents[1]
NIGERIA = ROOT / 'lsms_library/countries/Nigeria'
FOLDERS = ('2010-11', '2012-13', '2015-16')
MD5 = ('79dd6900d209b11330d952d5588d4ed1',
       '4eafe18762b5c2c6ef7b64c307388974',
       'a010e45cc651a983d420553e951f870d')
TIMES = ('2011Q1', '2013Q1', '2016Q1')
OLD_PP_SHA = ('1d0795acfa0bbd4341c410e96d5af4c7c68bdc194d9764481befc3aeb62889c3',
              '157f5759465a232fe79faa5da96ad3fc5e29ae2425c2ba8f587e3ca8c6e0f0ab',
              '4998beaf33e5de759395ed608ff37bdb7530e4d1d12230101fdc7767451c8b94')


@pytest.fixture(autouse=True)
def private_cache_per_case(tmp_path, monkeypatch):
    private = tmp_path / 'data'
    private.mkdir()
    monkeypatch.setenv('LSMS_DATA_DIR', str(private))
    data_root.cache_clear()
    local_tools._resolve_data_path.cache_clear()
    yield
    data_root.cache_clear()
    local_tools._resolve_data_path.cache_clear()


def invented(folder):
    spec = ph.ph_plot_source(folder)
    raw = pd.DataFrame({c: [np.nan] * 7 for c in spec['raw_columns']})
    raw.hhid = pd.Series([101, 101, 102, 102, 103, 103, 104], dtype='int32')
    raw.plotid = pd.Series([1, 2, 1, 2, 1, 2, 1], dtype='int8')
    raw[spec['estimate']] = [30., 2., 3., 4., 5., 6., np.nan]
    raw[spec['unit']] = [7., 5., 6., 8., 99., 7., np.nan]
    raw[spec['gps']] = [10000., np.nan, np.nan, np.nan, 0., 20000000., np.nan]
    raw[spec['acquire']] = [1., 2., 3., 4., 5., 99., np.nan]
    raw.sa1q3 = [1., 2., 2., np.nan, 9., 1., 2.]
    raw.sa1q4 = [np.nan, 1., 2., 9., np.nan, 2., 1.]
    if spec['other_unit']:
        raw[spec['other_unit']] = pd.array(['', 'nan', 'None', 'local unit', '', None, ''], dtype='string')
    raw.index = pd.Index([40, 10, 90, 30, 60, 20, 80], name='native_row')
    raw.attrs = {'invented': {'source': folder, 'uninterpreted': True}}
    return raw


@pytest.mark.parametrize('folder,t,md5', zip(FOLDERS, TIMES, MD5))
def test_exact_own_source_and_card_binding(folder, t, md5):
    spec = ph.ph_plot_source(folder)
    assert spec['t'] == t and spec['md5'] == md5
    entry = yaml.safe_load((NIGERIA / (spec['path'] + '.dvc')).read_text(encoding='utf-8'))['outs'][0]
    assert entry['md5'] == md5 and entry['size'] == spec['bytes']
    assert set(spec['acquire_labels']) == {1, 2, 3, 4, 5}
    assert spec['acquire_labels'][5].casefold() == 'family/inheritance' if folder != FOLDERS[2] else spec['acquire_labels'][5] == '5. FAMILY/INHERITANCE'
    assert set(spec['unit_labels']) == set(range(1, 9 if folder == FOLDERS[0] else 8))
    if folder == FOLDERS[0]:
        assert spec['unit_labels'][8] == 'other specify'
    spec['unit_labels'][8] = 'poisoned'
    assert ph.ph_plot_source(folder)['unit_labels'].get(8) != 'poisoned'


@pytest.mark.parametrize('folder', FOLDERS)
def test_positive_admission_of_actual_committed_sidecar(folder):
    spec = ph.ph_plot_source(folder)
    ph.admit_ph_plot_source(folder, NIGERIA / spec['path'])


@pytest.mark.parametrize('mutation', ['md5', 'size', 'path', 'malformed', 'wrong-source'])
def test_changed_version_or_wrong_path_refuses_before_getter(tmp_path, monkeypatch, mutation):
    folder = FOLDERS[0]
    spec = ph.ph_plot_source(folder)
    root = tmp_path / 'countries'
    target = root / 'Nigeria' / spec['path']
    target.parent.mkdir(parents=True)
    entry = {'md5': spec['md5'], 'size': spec['bytes'], 'path': target.name}
    if mutation in entry:
        entry[mutation] = 1 if mutation == 'size' else 'unreviewed'
    declaration = 'outs: []\n' if mutation == 'malformed' else yaml.safe_dump({'outs': [entry]})
    Path(str(target) + '.dvc').write_text(declaration, encoding='utf-8')
    monkeypatch.setattr(ph, 'countries_root', lambda: root)
    with pytest.raises(RuntimeError, match='source|declaration'):
        ph.admit_ph_plot_source(folder, target.with_name('other.dta') if mutation == 'wrong-source' else target)
    assert not target.exists()


@pytest.mark.parametrize('folder,t', zip(FOLDERS, TIMES))
def test_all_native_keys_order_routes_and_unknowns_survive(folder, t):
    raw = invented(folder)
    before = deepcopy(raw)
    got = ph.ph_plot_features_for_wave(folder, raw)
    _exact(raw, before)
    assert got.index.tolist() == [(t, str(h), str(p)) for h, p in zip(raw.hhid, raw.plotid)]
    assert len(got) == 7 and got.index.is_unique
    spec = ph.ph_plot_source(folder)
    for col in spec['raw_columns']:
        values = got[f'NativePH_{spec["role"]}_{col}']
        expected = raw[col].tolist()
        for actual, original in zip(values, expected):
            assert pd.isna(actual) if pd.isna(original) else actual == original
    assert got[f'NativePH_{spec["role"]}_sa1q3'].iloc[4] == 9
    assert got[f'NativePH_{spec["role"]}_sa1q4'].iloc[2] == 2
    assert got[f'NativePH_{spec["role"]}_hhid'].iloc[-1] == 104
    assert got.SoilType.isna().all() and got.Irrigated.isna().all()
    assert got.PlotSlope.isna().all() and got.TenureSystem.isna().all()


@pytest.mark.parametrize('folder', FOLDERS)
def test_free_private_use_is_not_communal_and_family_is_not_narrowed(folder):
    got = ph.ph_plot_features_for_wave(folder, invented(folder))
    spec = ph.ph_plot_source(folder)
    assert got.PHReportedAcquisition.iloc[3] == spec['acquire_labels'][4]
    assert got.PHReportedAcquisition.iloc[4] == spec['acquire_labels'][5]
    assert got.Tenure.iloc[:2].tolist() == ['owned', 'rented_in']
    # This literal source answer is also compatible with free use of a
    # privately owned plot: it cannot establish communal ownership.
    assert 'free' in got.PHReportedAcquisition.iloc[2].casefold()
    assert got[f'NativePH_{spec["role"]}_sa1q12'].iloc[2] == 3
    assert pd.isna(got.Tenure.iloc[2])
    assert got.Tenure.iloc[2:].isna().all()
    assert pd.isna(got.PHReportedAcquisition.iloc[5])
    assert got[f'NativePH_{spec["role"]}_sa1q12'].iloc[5] == 99


@pytest.mark.parametrize('folder', FOLDERS)
def test_only_existing_matching_metric_policy_and_own_other_card(folder):
    got = ph.ph_plot_features_for_wave(folder, invented(folder))
    assert got.Area.iloc[0] == 1.0  # GPS wins over the estimate.
    assert got.Area.iloc[1] == 2 * nigeria.HECTARES_PER_ACRE
    assert got.Area.iloc[2] == 3.0
    assert pd.isna(got.Area.iloc[3])  # No factor for native unit8.
    assert got.Area.iloc[4] == 0.0  # Retain reported zero; no cleaning.
    assert pd.isna(got.Area.iloc[5])  # Existing >1000ha policy, raw retained.
    assert pd.isna(got.Area.iloc[6])
    if folder == FOLDERS[0]:
        assert got.AreaUnit.iloc[3] == got.PHReportedAreaUnit.iloc[3] == 'other specify'
        assert got.NativePH_W1_sa1q9c.iloc[3] == 'local unit'
        assert got.NativePH_W1_sa1q9c.iloc[1] == 'nan'
    else:
        assert pd.isna(got.AreaUnit.iloc[3])
        assert pd.isna(got.PHReportedAreaUnit.iloc[3])


@pytest.mark.parametrize('folder', FOLDERS)
@pytest.mark.parametrize('failure', ['missing', 'fractional', 'bool', 'duplicate', 'normalized-collision', 'missing-column'])
def test_invalid_keys_never_firstkeep_or_truncate(folder, failure):
    raw = invented(folder)
    if failure == 'missing-column':
        raw = raw.drop(columns=[ph.ph_plot_source(folder)['gps']])
    elif failure == 'duplicate':
        raw = pd.concat([raw, raw.iloc[:1]])
    elif failure == 'normalized-collision':
        raw = raw.iloc[:2].copy()
        raw.hhid = pd.array(['101', '101.0'], dtype='string')
        raw.plotid = 1
    else:
        raw.hhid = raw.hhid.astype('object')
        raw.loc[40, 'hhid'] = {'missing': None, 'fractional': .5, 'bool': True}[failure]
    before = deepcopy(raw)
    with pytest.raises((ValueError, KeyError), match='key|operand'):
        ph.ph_plot_features_for_wave(folder, raw)
    _exact(raw, before)


def _run_wave(folder, monkeypatch):
    script = NIGERIA / folder / '_/plot_features.py'
    raw = invented(folder)
    tree = ast.parse(script.read_text(encoding='utf-8'))
    colmap = next(n.value for n in tree.body if isinstance(n, ast.Assign)
                  and any(isinstance(t, ast.Name) and t.id == 'colmap' for t in n.targets))
    columns = {kw.arg: ast.literal_eval(kw.value) for kw in colmap.keywords}
    pp = pd.DataFrame({c: [1., 1.] for c in columns.values()})
    pp[columns['hhid']] = [501, 502]
    pp[columns['plot_id']] = [1, 1]
    pp[columns['area_gps']] = [10000., 20000.]
    pp[columns['area_unit']] = 5
    reads, writes = [], []
    original_parquet_leaf = pd.DataFrame.to_parquet

    def reader(filename, **kwargs):
        assert kwargs == {'convert_categoricals': False}
        reads.append(filename)
        return raw.copy(deep=True) if 'secta1_harvest' in filename else pp.copy(deep=True)

    def parquet_leaf(frame, filename, **kwargs):
        # Observe only after the actual script→to_parquet stack has resolved
        # its target; wrapping to_parquet itself changes that source identity.
        assert Path(filename).resolve().is_relative_to(data_root().resolve())
        writes.append(frame.copy(deep=True))
        return original_parquet_leaf(frame, filename, **kwargs)

    with monkeypatch.context() as patch:
        patch.setitem(sys.modules, 'nigeria', nigeria)
        patch.setitem(sys.modules, 'nigeria_ph_plot_features', ph)
        patch.setattr(local_tools, 'get_dataframe', reader)
        patch.setattr(pd.DataFrame, 'to_parquet', parquet_leaf)
        patch.chdir(script.parent)
        # Production executes each wave script in a fresh interpreter.
        local_tools._resolve_data_path.cache_clear()
        runpy.run_path(str(script))
    assert sum('secta1_harvest' in name for name in reads) == 1
    assert len(reads) == 4 and len(writes) == 1
    saved = data_root() / 'Nigeria' / folder / '_/plot_features.parquet'
    assert saved.is_file() and not (script.parent / 'plot_features.parquet').exists()
    got = pd.read_parquet(saved)
    assert len(got) == 9 and got.index.is_unique
    pp_t = nigeria.PP_QUARTER[folder]
    assert got.loc[pp_t].index.tolist() == [('501', '1'), ('502', '1')]
    assert got.loc[pp_t, 'Area'].tolist() == [1., 2.]
    spec = ph.ph_plot_source(folder)
    assert got.loc[spec['t']].index.tolist() == [(str(h), str(p)) for h, p in zip(raw.hhid, raw.plotid)]
    assert got.loc[spec['t'], f'NativePH_{spec["role"]}_sa1q12'].iloc[4] == 5
    assert got.loc[pp_t, f'NativePH_{spec["role"]}_sa1q12'].isna().all()
    return got


@pytest.mark.parametrize('folder', FOLDERS)
def test_actual_wave_script_writes_both_rounds_with_invented_getter_leaves(folder, monkeypatch):
    _run_wave(folder, monkeypatch)


@pytest.mark.parametrize('folder', FOLDERS)
def test_existing_empty_pp_refusal_precedes_any_ph_getter(folder, monkeypatch):
    script = NIGERIA / folder / '_/plot_features.py'
    reads = []

    def reader(filename, **kwargs):
        reads.append(filename)
        assert 'secta1_harvest' not in filename
        return pd.DataFrame()

    monkeypatch.setitem(sys.modules, 'nigeria', nigeria)
    monkeypatch.setitem(sys.modules, 'nigeria_ph_plot_features', ph)
    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(nigeria, 'plot_features_for_wave', lambda *args, **kwargs:
        pd.DataFrame(index=pd.MultiIndex.from_arrays([[], [], []], names=['t', 'i', 'plot_id'])))
    monkeypatch.chdir(script.parent)
    with pytest.raises(AssertionError, match='produced no rows'):
        runpy.run_path(str(script))
    assert len(reads) == 3
    assert not (data_root() / 'Nigeria' / folder / '_/plot_features.parquet').exists()


@pytest.mark.parametrize('folder', FOLDERS)
def test_ph_getter_failure_never_writes_a_pp_only_partial(folder, monkeypatch):
    script = NIGERIA / folder / '_/plot_features.py'
    pp = pd.DataFrame({'Area': [1.]}, index=pd.MultiIndex.from_tuples(
        [(nigeria.PP_QUARTER[folder], '501', '1')], names=['t', 'i', 'plot_id']))
    error = OSError('invented PH source failed')
    reads, writes = [], []

    def reader(filename, **kwargs):
        reads.append(filename)
        if 'secta1_harvest' in filename:
            raise error
        return pd.DataFrame()

    monkeypatch.setitem(sys.modules, 'nigeria', nigeria)
    monkeypatch.setitem(sys.modules, 'nigeria_ph_plot_features', ph)
    monkeypatch.setattr(nigeria, 'plot_features_for_wave', lambda *a, **kw: pp.copy())
    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', lambda *a, **kw: writes.append(a))
    monkeypatch.chdir(script.parent)
    with pytest.raises(OSError) as caught:
        runpy.run_path(str(script))
    assert caught.value is error and len(reads) == 4 and writes == []


@pytest.mark.parametrize('folder', FOLDERS)
def test_empty_ph_keeps_empty_identity_and_does_not_invent_rows(folder):
    raw = invented(folder).iloc[:0].copy()
    before = deepcopy(raw)
    got = ph.ph_plot_features_for_wave(folder, raw)
    assert got.empty and got.index.names == ['t', 'i', 'plot_id']
    assert got.index.is_unique
    assert got.Area.dtype == pd.Float64Dtype()
    _exact(raw, before)


def test_existing_country_compiler_reads_all_folders_once_and_keeps_new_ph(monkeypatch):
    frames = {folder: _run_wave(folder, monkeypatch) for folder in FOLDERS}
    for folder in ('2018-19', '2023-24'):
        frames[folder] = pd.DataFrame({'Area': pd.array([3.], dtype='Float64')},
            index=pd.MultiIndex.from_tuples([(nigeria.PP_QUARTER[folder], '801', '1')], names=['t', 'i', 'plot_id']))
    expected = pd.concat(list(frames.values()))
    reads = []
    original_writer = local_tools.to_parquet

    def reader(filename, **kwargs):
        assert kwargs == {}
        folder = Path(filename).parts[1]
        reads.append(folder)
        return frames[folder].copy(deep=True)

    monkeypatch.setitem(sys.modules, 'nigeria', nigeria)
    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', original_writer)
    monkeypatch.chdir(NIGERIA / '_')
    runpy.run_path(str(NIGERIA / '_/plot_features.py'))
    assert reads == list(nigeria.Waves)
    got = pd.read_parquet(data_root() / 'Nigeria/var/plot_features.parquet')
    oracle = data_root() / 'expected-country.parquet'
    original_writer(expected, oracle, absolute_path=True)
    _exact(got, pd.read_parquet(oracle))
    assert set(TIMES).issubset(got.index.get_level_values('t'))
    assert not {'2019Q1', '2024Q1'}.intersection(got.index.get_level_values('t'))


def test_original_pp_executable_source_inverse_and_literal_ph_dependency_closure():
    for folder, sha in zip(FOLDERS, OLD_PP_SHA):
        script = NIGERIA / folder / '_/plot_features.py'
        text = script.read_text(encoding='utf-8')
        start = text.index('\n# GH587: own PH A1 records;')
        end = text.index("\nto_parquet(df, 'plot_features.parquet')", start)
        original = text[:start] + text[end:]
        assert hashlib.sha256(original.encode()).hexdigest() == sha
        refs = local_tools.scan_script_data_refs(script)
        expected = (NIGERIA / ph.ph_plot_source(folder)['path']).resolve()
        assert expected in {(script.parent / p).resolve() for p in refs}
        entry = yaml.safe_load(Path(str(expected) + '.dvc').read_text(encoding='utf-8'))['outs'][0]
        assert entry['md5'] == ph.ph_plot_source(folder)['md5']
