"""Actual-Wave controls for the currently selected PH attainment series.

Ledger 1292-nigeria-ph-education §§2-5. Only source-reader leaves supply
invented frames; Country/Wave discovery, YAML resources, extractor, format_id,
education mappings and visit filtering remain real. No Country aggregation.
"""
from copy import deepcopy
from pathlib import Path
import os
import shutil
import sys

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
COUNTRY_SOURCE = ROOT / 'lsms_library/countries/Nigeria'
TABLE = 'individual_education'
VALUE = 'Educational Attainment'
BINDINGS = (
    ('2010-11', '2010Q3', '2011Q1',
     'Post Harvest Wave 1/Household/sect2a_harvestw1.dta', 's2aq9'),
    ('2012-13', '2012Q3', '2013Q1',
     'Post Harvest Wave 2/Household/sect2a_harvestw2.dta', 's2aq9'),
    ('2015-16', '2015Q3', '2016Q1', 'sect2_harvestw3.dta', 's2aq9'),
    ('2018-19', '2018Q3', '2019Q1', 'sect2_harvestw4.dta', 's2aq9'),
    ('2023-24', '2023Q3', '2024Q1',
     'Post Harvest Wave 5/Household/sect2_harvestw5.dta', 's2q9'),
)
POPULATIONS = ('singleton', 'mixed', 'all_missing', 'nullable_string',
               'categorical', 'native_duplicates')


def baseline_yaml(candidate, source, ph):
    """Restore only the independent public predecessor's file/t spelling."""
    override = f"    file:\n        - {source}:\n            t: '{ph}'\n"
    assert candidate.count(override) == 1
    text = candidate.replace(override, f'    file: {source}\n', 1)
    start = text.index(TABLE + ':\n')
    tail = text[start:]
    following = [line for line in tail.splitlines()[1:]
                 if line and not line.startswith((' ', '#'))]
    stop = text.index('\n' + following[0], start) if following else len(text)
    block = text[start:stop]
    assert block.count('        t: hhid\n') == 1
    return text[:start] + block.replace('        t: hhid\n', '', 1) + text[stop:]


def _snapshot(root):
    return {p.relative_to(root).as_posix(): p.read_bytes()
            for p in root.rglob('*') if p.is_file()}


@pytest.fixture
def metadata(tmp_path, monkeypatch):
    """Exact public metadata copies, except baseline restores the five dates."""
    from lsms_library import paths

    try:
        with monkeypatch.context() as scoped:
            # Real Country.waves imports the copied executable metadata.
            # Keep the complete byte snapshot invariant under ordinary CI,
            # rather than exempting generated files from that invariant.
            scoped.setattr(sys, 'dont_write_bytecode', True)
            roots = {}
            for arm in ('baseline', 'candidate'):
                root = tmp_path / arm / 'countries'
                roots[arm] = root
                for name in ('nigeria.py', 'data_scheme.yml', 'categorical_mapping.org'):
                    target = root / 'Nigeria/_' / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(COUNTRY_SOURCE / '_' / name, target)
                for folder, pp, ph, source, column in BINDINGS:
                    original = COUNTRY_SOURCE / folder / '_/data_info.yml'
                    target = root / 'Nigeria' / folder / '_/data_info.yml'
                    target.parent.mkdir(parents=True, exist_ok=True)
                    content = original.read_text(encoding='utf-8')
                    if arm == 'baseline':
                        content = baseline_yaml(content, source, ph)
                    target.write_text(content, encoding='utf-8')
                    mapping = original.parent / 'mapping.py'
                    if mapping.is_file():
                        shutil.copyfile(mapping, target.parent / mapping.name)
            data = tmp_path / 'data'
            data.mkdir()
            scoped.setenv('LSMS_NO_CACHE', '1')
            scoped.setenv('LSMS_DATA_DIR', str(data))
            scoped.setenv('LSMS_SKIP_AUTH', '1')
            paths.data_root.cache_clear()
            before = {arm: _snapshot(root) for arm, root in roots.items()}

            def country(arm):
                from lsms_library import Country

                scoped.setenv('LSMS_COUNTRIES_ROOT', str(roots[arm]))
                paths.countries_root.cache_clear()
                return Country('Nigeria', preload_panel_ids=False)

            try:
                yield country, roots
            finally:
                assert {arm: _snapshot(root) for arm, root in roots.items()} == before
                assert not [p for p in data.rglob('*') if p.is_file()]
    finally:
        # The child context restores env/bytecode before clearing resolution
        # caches. Invariant assertions and setup/case failures cannot skip this.
        paths.countries_root.cache_clear()
        paths.data_root.cache_clear()


@pytest.mark.parametrize('binding', BINDINGS, ids=[r[0] for r in BINDINGS])
def test_selected_question_mapping_and_actual_quarter_registry(metadata, binding):
    country, roots = metadata
    folder, pp, ph, source, column = binding
    expected_question = {VALUE: [column, {'mappings': [
        'harmonize_education', 'Original Label', 'Preferred Label']}]}
    specs = {}
    # Country.file_path follows the current override dynamically; read each
    # arm's real resources while that arm is selected, rather than retaining
    # an object across a later override and accidentally reading candidate twice.
    for arm in ('baseline', 'candidate'):
        obj = country(arm)
        assert pp in obj.waves and ph in obj.waves
        assert obj.wave_folder_map[pp] == obj.wave_folder_map[ph] == folder
        assert obj[pp].wave_folder == obj[ph].wave_folder == folder
        assert TABLE in obj[pp].data_scheme and TABLE in obj[ph].data_scheme
        specs[arm] = obj[ph].resources[TABLE]
    old_spec, new_spec = specs['baseline'], specs['candidate']
    assert old_spec['file'] == source
    assert new_spec['file'] == [{source: {'t': ph}}]
    assert old_spec['idxvars'] == {'i': 'hhid', 'pid': 'indiv'}
    assert new_spec['idxvars'] == {'t': 'hhid', 'i': 'hhid', 'pid': 'indiv'}
    assert old_spec['myvars'] == new_spec['myvars'] == expected_question
    restored = deepcopy(new_spec)
    restored['file'] = source
    restored['idxvars'].pop('t')
    assert restored == old_spec
    assert (roots['baseline'] / 'Nigeria/_/nigeria.py').read_bytes() == (
        roots['candidate'] / 'Nigeria/_/nigeria.py').read_bytes()


def source_frame(column, population):
    """Nonmonotone/native repeated keys, literal zero, None and nullable types."""
    values = {
        'singleton': ['0. NONE'],
        'mixed': ['16. P6', None, 0, '0. NONE', pd.NA],
        'all_missing': [None, np.nan, pd.NA],
        'nullable_string': pd.array(['P6', pd.NA, 'NONE'], dtype='string'),
        'categorical': pd.Categorical(['0. NONE', None, '16. P6'],
                                     categories=['16. P6', '0. NONE', 'P1']),
        'native_duplicates': ['P6', '0. NONE', 'P6', pd.NA],
    }[population]
    n = len(values)
    hh = [303., 101., 202., 303., 0.][:n]
    person = [3., 1., 2., 3., 1.][:n]
    raw = pd.DataFrame({'hhid': hh, 'indiv': person, column: values})
    # W5's legacy respondent question is deliberately a different payload.
    if column != 's2q3':
        raw['s2q3'] = pd.array([7] * n, dtype='Int64')
    raw['ignored_native_column'] = pd.array(range(n), dtype='Int64')
    raw.index = pd.Index([90, 11, 70, 4, 22][:n], name='native_source_row')
    raw.attrs = {'source': 'invented-no-file', 'population': population,
                 'nested': {'keep': ['order', 'missing', 0]}}
    return raw


@pytest.mark.parametrize('binding', BINDINGS, ids=[r[0] for r in BINDINGS])
@pytest.mark.parametrize('population', POPULATIONS)
def test_actual_wave_keeps_ph_payload_and_refuses_false_pp_date(
        metadata, monkeypatch, binding, population):
    from lsms_library import local_tools as L

    country, roots = metadata
    folder, pp, ph, source, column = binding
    operand = source_frame(column, population)
    protected = operand.copy(deep=True)
    attrs = deepcopy(operand.attrs)
    active = {'arm': None}
    reads = []

    def reader(filename, *, convert_categoricals=True, encoding=None,
               categories_only=False):
        assert convert_categoricals is True and encoding is None
        assert categories_only is False
        path = Path(os.path.abspath(filename))
        wanted = roots[active['arm']] / 'Nigeria' / folder / 'Data' / source
        assert path == wanted
        reads.append((active['arm'], path))
        return operand.copy(deep=True)

    # Reuse actual df_data_grabber; only its sanctioned source-reader leaf moves.
    monkeypatch.setattr(L, 'get_dataframe', reader)
    outputs = {}
    for arm in ('baseline', 'candidate'):
        active['arm'] = arm
        obj = country(arm)
        outputs[arm] = {quarter: obj[quarter].individual_education()
                        for quarter in (ph, pp)}
    old, new = outputs['baseline'], outputs['candidate']
    assert len(reads) == 4
    for frame in (old[ph], old[pp], new[ph], new[pp]):
        assert isinstance(frame, pd.DataFrame)
        assert list(frame.index.names) == ['t', 'i', 'pid']
        assert list(frame.columns) == [VALUE]
    for frame in (old[ph], old[pp], new[ph]):
        assert len(frame) == len(operand)
        assert frame.index.get_level_values('i').tolist() == [
            L.format_id(v) for v in operand['hhid']]
        assert frame.index.get_level_values('pid').tolist() == [
            L.format_id(v) for v in operand['indiv']]
    pd.testing.assert_frame_equal(old[ph], new[ph], check_exact=True)
    assert old[ph].attrs == new[ph].attrs
    assert old[ph].isna().equals(new[ph].isna())
    retimed = old[pp].reset_index()
    retimed['t'] = ph
    retimed = retimed.set_index(['t', 'i', 'pid'])
    pd.testing.assert_frame_equal(retimed, old[ph], check_exact=True)
    pd.testing.assert_frame_equal(new[pp], new[ph].iloc[:0], check_exact=True)
    assert new[pp].attrs == new[ph].attrs
    assert set(new[ph].index.get_level_values('t')) == {ph}
    assert set(old[pp].index.get_level_values('t')) == {pp}
    if population == 'all_missing':
        assert new[ph][VALUE].isna().all()
    if population == 'singleton':
        assert new[ph][VALUE].tolist() == ['No education']
    if population == 'mixed':
        assert new[ph][VALUE].iloc[2] == 0
        assert new[ph][VALUE].iloc[1:2].isna().all()
    if population == 'native_duplicates':
        assert not new[ph].index.is_unique
    pd.testing.assert_frame_equal(operand, protected, check_exact=True)
    assert operand.attrs == attrs == protected.attrs


@pytest.mark.parametrize('prior_bytecode', [False, True])
def test_metadata_refusal_restores_process_paths(tmp_path, monkeypatch, prior_bytecode):
    """A real private metadata mutation must fail without poisoning later tests."""
    from lsms_library import paths

    monkeypatch.setattr(sys, 'dont_write_bytecode', prior_bytecode)
    keys = ('LSMS_COUNTRIES_ROOT', 'LSMS_DATA_DIR', 'LSMS_SKIP_AUTH', 'LSMS_NO_CACHE')
    environment = {key: os.environ.get(key) for key in keys}
    paths.countries_root.cache_clear()
    paths.data_root.cache_clear()
    original_country_root = paths.countries_root()
    original_data_root = paths.data_root()
    transaction = metadata.__wrapped__(tmp_path, monkeypatch)
    try:
        country, roots = next(transaction)
        country('candidate')
        assert paths.countries_root() == roots['candidate']
        assert paths.data_root() == tmp_path / 'data'
        assert sys.dont_write_bytecode is True
        # Deliberately violate the full source snapshot after the fixture has
        # yielded. No ignored filename or weaker mutation rule is introduced.
        target = roots['candidate'] / 'Nigeria/_/data_scheme.yml'
        target.write_bytes(target.read_bytes() + b'\n# deliberate invented mutation\n')
        with pytest.raises(AssertionError):
            next(transaction)
        assert {key: os.environ.get(key) for key in keys} == environment
        assert sys.dont_write_bytecode is prior_bytecode
        assert paths.countries_root.cache_info().currsize == 0
        assert paths.data_root.cache_info().currsize == 0
        assert paths.countries_root() == original_country_root
        assert paths.data_root() == original_data_root
    finally:
        transaction.close()
        paths.countries_root.cache_clear()
        paths.data_root.cache_clear()
