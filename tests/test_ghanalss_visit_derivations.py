"""Integration of index-valued GhanaLSS visit derivations and their raw inputs."""
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import local_tools as tools
from lsms_library.derivations import attach, records_for, resolve_callable
from lsms_library.feature import _load_global_columns


MODULE = 'lsms_library.countries.GhanaLSS._.visit_dates'
CASES = [('1998-99', 'glss4', 7), ('2005-06', 'glss5', 11)]


def _source_frames(wave):
    """One fully identified household, one ambiguous, one wholly undated."""
    if wave == '1998-99':
        return {
            'sec0a.dta': pd.DataFrame({'clust': [4001] * 3, 'nh': [1, 2, 3],
                'dd': [1, 2, 31], 'mm': [10, 10, 2], 'yy': [98] * 3}),
            'sec0c.dta': pd.DataFrame({'clust': [4001] * 3, 'nh': [1] * 3,
                'visitcd': [1, 2, 3], 's0vday': [1, 6, 11],
                's0vmonth': [10] * 3, 's0vyear': [98] * 3}),
            'sec0b.dta': pd.DataFrame({'clust': [4001] * 8,
                'nh': [1] * 7 + [2], 'visitcd': [1, 2, 2, 4, 5, 6, 6, 1],
                's0dy_sum': [6, 11, 16, 21, 26, 31, 31, 12],
                's0mt_sum': [10] * 8, 's0yr_sum': [98] * 8}),
        }
    return {
        'sec0.dta': pd.DataFrame({'hhid': [500101, 500102, 500103],
            'ddate': [1, 2, 31], 'mdate': [10, 10, 2], 'ydate': [2005] * 3}),
        'secb0.dta': pd.DataFrame({'hhid': [500101], 'day': [1],
            'month': [10], 'year': [2005]}),
        'secb1.dta': pd.DataFrame({'hhid': [500101] * 10 + [500102],
            'visitno': list(range(2, 12)) + [2],
            'intday': list(range(3, 22, 2)) + [12],
            'intmon': [10] * 11, 'intyr': [2005] * 11}),
        'secb2.dta': pd.DataFrame({'hhid': [500101] * 10,
            'visitcd': list(range(1, 11)), 'vday': list(range(3, 22, 2)),
            'vmon': [10] * 10, 'vyr': [2005] * 10}),
    }


@pytest.fixture
def module():
    builder = resolve_callable(f'{MODULE}:build_visit_dates')
    return sys.modules[builder.__module__]


@pytest.fixture
def source_reader(monkeypatch, module):
    def install(wave):
        frames = _source_frames(wave)

        def read(fn, **kwargs):
            assert kwargs.get('convert_categoricals') is False
            return frames[Path(fn).name.lower()].copy()

        monkeypatch.setattr(module, 'get_dataframe', read)
        return frames
    return install


@pytest.mark.parametrize('wave,name,n', CASES)
def test_index_target_registry_and_raw_inputs_join_after_household_rekey(
        monkeypatch, module, source_reader, wave, name, n):
    originals = source_reader(wave)
    key = f'GhanaLSS::interview_date::anchored-chronology-{name}'
    record = records_for('GhanaLSS', 'interview_date')[key]
    assert record.columns == ['visit']
    assert record.waves == [wave]
    assert 'modelling-choice' in record.bases
    assert record.function == f'{MODULE}:derive_{name}_visit_assignments'
    assert callable(resolve_callable(record.function))
    assert callable(resolve_callable(record.inputs))
    decl = _load_global_columns()['interview_date']['Derivation']
    assert decl['type'] == 'str' and decl['optional'] is True

    old = '400101' if name == 'glss4' else '500101'
    mapping = {wave: {old: 'harmonized-household'}}
    monkeypatch.setattr(type(ll.Country('GhanaLSS')), 'updated_ids', property(lambda self: mapping))
    country = ll.Country('GhanaLSS')
    raw = country.derivation_inputs(key, wave=wave)
    served = tools.id_walk(module.build_visit_dates(wave), mapping)
    attach(served, 'GhanaLSS', 'interview_date')
    assert raw.index.names == ['t', 'i', 'source', 'source_record']
    assert raw.index.is_unique
    assert len(raw) == sum(len(df) for df in originals.values())
    flat = raw.reset_index()
    # Original source fields and record identities survive input annotation.
    for source, group in flat.groupby('source'):
        original = originals[Path(source).name.lower()]
        for _, row in group.iterrows():
            expected = original.iloc[int(row.source_record) - 1]
            for field in original.columns:
                assert (pd.isna(row[field]) and pd.isna(expected[field])) or row[field] == expected[field]

    inferred = served[served.Derivation.notna()].reset_index()
    assert set(inferred.Derivation) == {key}
    assert len(inferred) == (n - 3 if name == 'glss4' else n - 1)
    linked = inferred.merge(flat, left_on=['t', 'i', 'visit'],
                            right_on=['t', 'i', 'assigned_visit'], how='left')
    assert linked.source.notna().all()
    assert linked.Int_t.eq(linked.parsed_date).all()
    assert set(linked.i) == {'harmonized-household'}
    # An entirely invalid household and an ambiguous date remain discoverable.
    assert 'invalid_or_missing_date' in set(flat.assignment_reason)
    assert 'ambiguous_visit' in set(flat.assignment_reason)
    assert flat.loc[flat.assignment_reason == 'ambiguous_visit', 'assigned_visit'].isna().all()
    assert served.attrs['derivations']['GhanaLSS'][key]['columns'] == ['visit']
    assert served.attrs['derivations']['GhanaLSS'][key]['rows'] == len(inferred)


@pytest.mark.parametrize('wave,name,n', CASES)
def test_provenance_survives_parquet_and_mixed_country_feature(
        monkeypatch, tmp_path, module, source_reader, wave, name, n):
    source_reader(wave)
    served = module.build_visit_dates(wave)
    # Cluster lookup is independent of the derivation; provide its known key.
    served = served.assign(v='cluster').set_index('v', append=True)
    served = served.reorder_levels(['t', 'v', 'i', 'visit'])
    attach(served, 'GhanaLSS', 'interview_date')
    path = tmp_path / 'dates.parquet'
    tools.to_parquet(served, str(path), absolute_path=True)
    restored = tools.get_dataframe(str(path))
    # Parquet restores a fully populated nullable integer index as int64.
    # Both satisfy the visit contract; compare every value after one explicit
    # integer representation normalization, without relaxing other dtypes.
    assert pd.api.types.is_integer_dtype(restored.index.get_level_values('visit').dtype)
    pd.testing.assert_frame_equal(
        restored.reset_index().astype({'visit': 'Int64'}),
        served.reset_index().astype({'visit': 'Int64'}))
    assert restored.attrs['derivations'] == served.attrs['derivations']

    class Country:
        def __init__(self, name, **kwargs):
            self.name = name

        def interview_date(self, **kwargs):
            if self.name == 'GhanaLSS':
                return restored.copy()
            index = pd.MultiIndex.from_tuples([('2007-08', 'other', '1', 1)],
                                              names=['t', 'v', 'i', 'visit'])
            return pd.DataFrame({'Int_t': [pd.Timestamp('2007-07-10')]}, index=index)

    monkeypatch.setattr(ll, 'Country', Country)
    feature = ll.Feature('interview_date')
    object.__setattr__(feature, '_countries', ['GhanaLSS', 'Timor-Leste'])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        combined = feature(['GhanaLSS', 'Timor-Leste'])
    assert combined.index.names == ['country', 't', 'v', 'i', 'visit']
    assert combined.index.is_unique
    assert len(combined) == len(served) + 1
    assert combined.xs('Timor-Leste', level='country').Derivation.isna().all()
    assert combined.Derivation.notna().sum() == served.Derivation.notna().sum()
    assert combined.attrs['derivations']['GhanaLSS'] == served.attrs['derivations']['GhanaLSS']


@pytest.mark.parametrize('wave,name,n', CASES)
def test_unresolved_inputs_are_retrievable_when_no_date_row_survives(
        monkeypatch, module, source_reader, wave, name, n):
    source_reader(wave)
    raw = module._load_raw(wave)
    missing = '400103' if name == 'glss4' else '500103'
    raw = raw[raw.index.get_level_values('i') == missing]
    monkeypatch.setattr(module, '_load_raw', lambda wave: raw.copy())
    monkeypatch.setattr(type(ll.Country('GhanaLSS')), 'updated_ids', property(lambda self: None))
    country = ll.Country('GhanaLSS')
    key = f'GhanaLSS::interview_date::anchored-chronology-{name}'
    assert key in country.derivations('interview_date')
    served = module.build_visit_dates(wave)
    assert served.empty
    inputs = country.derivation_inputs(key, wave=wave)
    assert len(inputs) == len(raw) > 0
    assert inputs.assigned_visit.isna().all()
    assert set(inputs.assignment_reason) == {'invalid_or_missing_date'}
    attach(served, 'GhanaLSS', 'interview_date')
    assert served.attrs['derivations']['GhanaLSS'][key]['rows'] == 0


@pytest.mark.parametrize('wave,name,n', CASES)
def test_wave_hook_keeps_inferred_rows_and_their_provenance(
        monkeypatch, tmp_path, module, source_reader, wave, name, n):
    from lsms_library.paths import data_root
    frames = source_reader(wave)
    original = tools.get_dataframe

    def read(fn, *args, **kwargs):
        if Path(fn).name.lower() in frames:
            return frames[Path(fn).name.lower()].copy()
        if Path(fn).suffix.lower() in ('.dta', '.dat'):
            raise AssertionError(f'Unexpected source read: {fn}')
        return original(fn, *args, **kwargs)

    monkeypatch.setattr(tools, 'get_dataframe', read)
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    data_root.cache_clear()
    try:
        result = ll.Country('GhanaLSS')[wave].interview_date()
        expected = module.build_visit_dates(wave)
        pd.testing.assert_frame_equal(result, expected)
        assert result.Derivation.notna().sum() == (n - 3 if name == 'glss4' else n - 1)
        assert result.index.is_unique
    finally:
        data_root.cache_clear()


@pytest.mark.parametrize('wave,source', [
    ('1998-99', 'SEC0A.DTA'), ('1998-99', 'SEC0B.DTA'),
    ('1998-99', 'SEC0C.DTA'), ('2005-06', 'parta/sec0.dta'),
    ('2005-06', 'partb/secb0.dta'), ('2005-06', 'partb/secb1.dta'),
    ('2005-06', 'partb/secb2.dta'),
])
def test_each_raw_source_fingerprint_invalidates_wave(monkeypatch, wave, source):
    import importlib
    country_module = importlib.import_module('lsms_library.country')
    survey = ll.Country('GhanaLSS')[wave]
    target = survey.file_path / 'Data' / source
    changed = False

    def fingerprint(path):
        return str(path) + (':changed' if changed and Path(path) == target else ':original')

    monkeypatch.setattr(country_module, 'source_fingerprint', fingerprint)
    before = survey._input_hash('interview_date')
    changed = True
    assert survey._input_hash('interview_date') != before


@pytest.mark.parametrize('conflicting', [False, True])
def test_actual_wave_duplicate_glss4_anchor_reaches_reconciler(
        monkeypatch, tmp_path, module, source_reader, conflicting):
    from lsms_library.paths import data_root
    frames = source_reader('1998-99')
    duplicate = frames['sec0c.dta'].iloc[[1]].copy()
    if conflicting:
        duplicate['s0vday'] = 7
    frames['sec0c.dta'] = pd.concat([frames['sec0c.dta'], duplicate], ignore_index=True)
    original = tools.get_dataframe

    def read(fn, *args, **kwargs):
        key = Path(fn).name.lower()
        return frames[key].copy() if key in frames else original(fn, *args, **kwargs)

    monkeypatch.setattr(tools, 'get_dataframe', read)
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    data_root.cache_clear()
    try:
        expected = module.build_visit_dates('1998-99')
        actual = ll.Country('GhanaLSS')['1998-99'].interview_date()
        pd.testing.assert_frame_equal(actual, expected)
        assert actual.index.is_unique
        assert actual.xs(1, level='visit').loc[('1998-99', '400101'), 'Int_t'] == pd.Timestamp('1998-10-01')
    finally:
        data_root.cache_clear()
