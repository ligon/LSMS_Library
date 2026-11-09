"""Invented Stata-tag transport: normal decode plus one scoped native scan."""
from copy import deepcopy
import io
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from pandas.io.stata import StataMissingValue
import pytest
import yaml

import lsms_library.country as cm
import lsms_library.local_tools as lt
from lsms_library.country import Wave, _coerce_to_boolean

ROOT = Path(__file__).resolve().parents[1]
CIV = ROOT / 'lsms_library/countries/CotedIvoire'
BLOCK = yaml.safe_load((CIV / '2018-19/_/data_info.yml').read_text())['shocks']
EFFECTS = dict(zip(['AffectedIncome', 'AffectedAssets', 'AffectedProduction',
                    'AffectedFoodStock', 'AffectedConsumption'],
                   ['s14q04' + x for x in 'abcef']))
OPTION = {'answer': {'.d': 'Non-concerné'}}
LABELS = {1: 'Diminué', 2: 'Augmenté', 3: 'Inchangé', 4: 'Non-concerné'}


def synthetic(path, storage='int32', full=False):
    """Same finite-writer/verified payload patch as closed diagnosis6cb48dcb.

    Fixture constants are only its authored int32/int8 representation; the
    implementation recognizes pandas typed tag strings instead.
    """
    base = [1, 2, 3, 4, 0, -91, -92, -93]
    values = {'answer': base}
    if full:
        values = {'grappe': list(range(11, 19)), 'menage': [2]*8,
                  's14q01': list(range(21, 29))}
        values.update({col: base[n:] + base[:n] for n, col in enumerate(EFFECTS.values())})
        values['s14q04d'] = [-92]*8
        values.update({f's14q05__{n}': [1, 0]*4 if n == 1 else [0]*8 for n in range(1, 27)})
    frame = pd.DataFrame({k: np.array(v, dtype=storage) for k, v in values.items()})
    columns = list(EFFECTS.values()) + ['s14q04d'] if full else ['answer']
    codes = [2147483621, 2147483625, 2147483622] if storage == 'int32' else [101, 105, 102]
    labels = {col: {**LABELS, codes[1]: 'Non-concerné', codes[2]: 'Unrelated'} for col in columns}
    frame.to_stata(path, version=118, byteorder='<', write_index=False, value_labels=labels)
    data = path.read_bytes(); start, end = data.index(b'<data>')+6, data.index(b'</data>')
    dtype = '<i4' if storage == 'int32' else 'i1'
    assert data[start:end] == frame.to_numpy().astype(dtype).tobytes()
    changed = frame.to_numpy().copy()
    for col in columns:
        position = frame.columns.get_loc(col)
        for marker, code in zip([-91, -92, -93], codes):
            changed[changed[:, position] == marker, position] = code
    path.write_bytes(data[:start] + changed.astype(dtype).tobytes() + data[end:])
    return path


@pytest.mark.parametrize('storage', ['int32', 'int8'])
@pytest.mark.parametrize('categoricals', [True, False])
def test_real_reader_maps_only_typed_requested_tag(tmp_path, storage, categoricals):
    path = synthetic(tmp_path/'invented.dta', storage)
    ordinary = lt.get_dataframe(path, convert_categoricals=categoricals)
    before = path.read_bytes()
    out = lt.get_dataframe(path, convert_categoricals=categoricals, stata_missing_map=OPTION)
    expected = ordinary.copy(); expected['answer'] = expected.answer.astype(object)
    expected.loc[6, 'answer'] = 'Non-concerné'
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    assert out.answer.iloc[6] == 'Non-concerné'
    assert out.answer.iloc[[5, 7]].isna().all()
    assert not any(isinstance(x, StataMissingValue) for x in out.answer)
    assert path.read_bytes() == before


@pytest.mark.parametrize('option', [None, {}])
def test_unopted_route_does_not_scan_or_change_decode(tmp_path, monkeypatch, option):
    path = synthetic(tmp_path/'ordinary.dta')
    expected = lt.get_dataframe(path)
    monkeypatch.setattr(lt, '_overlay_stata_missing_map', lambda *a: pytest.fail('extra scan'))
    pd.testing.assert_frame_equal(lt.get_dataframe(path, stata_missing_map=option), expected)


@pytest.mark.parametrize('option', [[], 'all', {'answer': {}}, {'': {'.d': 'x'}},
                                    {'answer': {'.': 'x'}}, {'answer': {'d': 'x'}},
                                    {'answer': {'.D': 'x'}}, {'answer': {'.d': False}}])
def test_invalid_option_fails_before_source_io(option, monkeypatch):
    monkeypatch.setattr(lt, '_resolve_data_path', lambda *a: pytest.fail('source lookup'))
    with pytest.raises(ValueError, match='stata_missing_map'):
        lt.get_dataframe('unopened.dta', stata_missing_map=option)


def test_metadata_only_and_nonstata_refuse(tmp_path):
    with pytest.raises(ValueError, match='data read'):
        lt.get_dataframe('unopened.dta', categories_only=True, stata_missing_map=OPTION)
    path = tmp_path/'plain.csv'; path.write_text('answer\n1\n')
    with pytest.raises(ValueError, match='only supported for Stata'):
        lt.get_dataframe(path, stata_missing_map=OPTION)


def test_opted_decoder_failure_refuses_lossy_fallback_default_retains_it(tmp_path, monkeypatch):
    path = synthetic(tmp_path/'fallback.dta')
    cause = ValueError('normal decoder cannot handle invented source')
    def fail(*a, **k): raise cause
    monkeypatch.setattr(lt, 'from_dta', fail)
    calls = []
    fallback = pd.DataFrame({'ordinary': [7]})
    monkeypatch.setattr(lt, '_is_polluted_workspace_copy', lambda *a: False)
    def legacy(*a, **k): calls.append(1); return fallback
    monkeypatch.setattr(lt.pyreadstat, 'read_dta', lambda *a, **k: (legacy(), None))
    monkeypatch.setattr(lt, 'check_read', lambda frame, *a: frame)
    assert lt.get_dataframe(path) is fallback
    assert len(calls) == 1
    with pytest.raises(ValueError, match='tag-losing') as exc:
        lt.get_dataframe(path, stata_missing_map=OPTION)
    assert exc.value.__cause__ is cause and len(calls) == 1


@pytest.mark.parametrize('fault', ['length', 'index', 'column'])
def test_scan_alignment_refuses(tmp_path, monkeypatch, fault):
    path = synthetic(tmp_path/'alignment.dta')
    ordinary = lt.get_dataframe(path)
    if fault == 'length': ordinary = ordinary.iloc[:-1]
    elif fault == 'index': ordinary.index = pd.Index(range(8), name='changed')
    else: ordinary = ordinary.rename(columns={'answer': 'other'})
    monkeypatch.setattr(lt, 'from_dta', lambda *a, **k: ordinary)
    with pytest.raises(ValueError, match='does not match'):
        lt.get_dataframe(path, stata_missing_map=OPTION)


def test_old_format_extended_distinction_refuses():
    with pytest.raises(ValueError, match='format supporting'):
        lt._overlay_stata_missing_map(pd.DataFrame(), io.BytesIO(), OPTION, 111)


def test_unselected_payload_attrs_and_empty_requested_tag_exact(tmp_path, monkeypatch):
    path = synthetic(tmp_path/'unchanged.dta', full=True)
    ordinary = lt.get_dataframe(path); ordinary.attrs = {'invented': {'nested': [1, None]}}
    before = deepcopy(ordinary)
    monkeypatch.setattr(lt, 'from_dta', lambda *a, **k: ordinary)
    out = lt.get_dataframe(path, stata_missing_map={'s14q04a': {'.z': 'Not seen'}})
    pd.testing.assert_frame_equal(out, before, check_exact=True)
    assert out.attrs == before.attrs
    mapped = lt.get_dataframe(path, stata_missing_map={'s14q04a': {'.d': 'Non-concerné'}})
    pd.testing.assert_frame_equal(mapped.drop(columns='s14q04a'), before.drop(columns='s14q04a'))
    pd.testing.assert_frame_equal(ordinary, before)
    assert ordinary.attrs == mapped.attrs == before.attrs


def test_dvc_filelike_route_uses_same_scoped_scan(tmp_path, monkeypatch):
    path = synthetic(tmp_path/'stream.dta'); data = path.read_bytes()
    virtual = tmp_path/'virtual.dta'; Path(str(virtual)+'.dvc').write_text('outs: []\n')
    monkeypatch.setattr(lt, '_ensure_dvc_pulled', lambda *a: None)
    monkeypatch.setattr(lt, '_dvc_cache_path', lambda *a: None)
    monkeypatch.setattr(lt, 'DVCFS', SimpleNamespace(open=lambda *a, **k: io.BytesIO(data)))
    out = lt.get_dataframe(virtual, stata_missing_map=OPTION)
    assert out.answer.iloc[6] == 'Non-concerné' and out.answer.iloc[[5, 7]].isna().all()


class InventedWave(Wave):
    def __init__(self, root, block):
        super().__init__('invented', 'invented', SimpleNamespace(name='Invented1561', resources={'Data Scheme': {'shocks': {}}}))
        self.root = root; self.block = block
    @property
    def file_path(self): return self.root
    @property
    def resources(self): return {'shocks': deepcopy(self.block)}
    @property
    def data_scheme(self): return ['shocks']
    @property
    def formatting_functions(self):
        return {'i': lt.get_formatting_functions(CIV/'_/cotedivoire.py', 'invented1561_i')['i']}
    def _read_l2_wave_cache(self, request, path): return None
    def _input_hash(self, request): return 'invented1561hash'


def test_file_record_option_never_enters_myvars_or_other_files(tmp_path):
    block = {'file': [{'first.dta': {'stata_missing_map': OPTION}}, 'second.dta'],
             'idxvars': {'i': 'id'}, 'myvars': {'x': 'answer'}}
    mapping = InventedWave(tmp_path, block).column_mapping('shocks', block)
    assert set(mapping) == {'df_edit', 'first.dta', 'second.dta'}
    assert mapping['first.dta']['stata_missing_map'] == OPTION
    assert 'stata_missing_map' not in mapping['first.dta']['myvars']
    assert 'stata_missing_map' not in mapping['second.dta']
    assert mapping['first.dta']['myvars'] == mapping['second.dta']['myvars'] == {'x': 'answer'}


def test_two_source_scope_before_mapping(tmp_path):
    path = synthetic(tmp_path/'two-scope.dta')
    one = lt.df_data_grabber(path, {'i': 'answer'}, result=('answer', {'Non-concerné': False}), stata_missing_map=OPTION)
    two = lt.df_data_grabber(path, {'i': 'answer'}, result=('answer', {'Non-concerné': False}))
    assert one.result.iloc[6] is False or one.result.iloc[6] == False
    assert pd.isna(two.result.iloc[6])
    assert one.result.iloc[[5, 7]].isna().all()


def test_own_yaml_real_wave_mapping_and_private_parquet(tmp_path, monkeypatch):
    data = tmp_path/'Data/Menage'; data.mkdir(parents=True)
    path = synthetic(data/'s14_me_CIV2018.dta', full=True)
    original = lt.get_dataframe(path)
    wave = InventedWave(tmp_path, deepcopy(BLOCK))
    monkeypatch.setattr(cm, 'data_root', lambda *a: tmp_path/'private-data')
    # Use normal Wave extraction, existing map dictionaries and actual writer.
    out = wave.grab_data('shocks')
    assert len(out) == 8 and out.index.names == ['t', 'i', 'Shock']
    for n, (target, col) in enumerate(EFFECTS.items()):
        expected = original[col].map(lambda value: {'Diminué': True, 'Augmenté': True,
                                                   'Inchangé': False, 'Non-concerné': False}.get(value, value))
        special_position = (6-n) % 8
        expected.iloc[special_position] = False
        pd.testing.assert_series_equal(_coerce_to_boolean(out[target]).reset_index(drop=True),
                                       _coerce_to_boolean(expected).reset_index(drop=True), check_names=False)
    assert 's14q04d' not in BLOCK['stata_missing_map']
    pd.testing.assert_series_equal(out.Cope1.reset_index(drop=True), original.s14q05__1, check_names=False)
    cache = tmp_path/'private-data/invented/_/shocks.parquet'
    assert cache.exists()
    restored = lt.get_dataframe(cache)
    pd.testing.assert_frame_equal(restored, out)


def test_opt_in_adds_exactly_one_selected_native_scan(tmp_path, monkeypatch):
    path = synthetic(tmp_path/'scan-cost.dta')
    calls = []
    original = pd.io.stata.StataReader.read
    def observed(self, *args, **kwargs):
        calls.append(dict(kwargs))
        return original(self, *args, **kwargs)
    monkeypatch.setattr(pd.io.stata.StataReader, 'read', observed)
    lt.get_dataframe(path, stata_missing_map=OPTION)
    assert len(calls) == 2
    assert calls[0] == {'convert_dates': True, 'convert_categoricals': False}
    assert calls[1] == {'columns': ['answer'], 'convert_missing': True, 'convert_categoricals': False}
