"""Field-directed string index policy, without changing writer/data semantics."""
import io
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pandas.api.extensions import ExtensionDtype

from lsms_library import local_tools as lt


def _typed(a, b):
    assert type(a) is type(b)
    if a is None or a is pd.NA:
        assert a is b
    elif isinstance(a, dict):
        assert list(a) == list(b)
        for key in a:
            _typed(key, next(k for k in b if k == key))
            _typed(a[key], b[key])
    elif isinstance(a, (list, tuple)):
        assert len(a) == len(b)
        for x, y in zip(a, b):
            _typed(x, y)
    elif isinstance(a, float) and math.isnan(a):
        assert math.isnan(b)
    else:
        assert a == b


def _axis(a, b):
    assert type(a) is type(b)
    pd.testing.assert_index_equal(a, b, exact=True, check_exact=True, rtol=0, atol=0)
    _typed(list(a.names), list(b.names))
    assert type(a.dtype) is type(b.dtype) and a.dtype == b.dtype
    if isinstance(a.dtype, pd.StringDtype):
        _typed(a.dtype.storage, b.dtype.storage)
        _typed(a.dtype.na_value, b.dtype.na_value)
    if isinstance(a, pd.MultiIndex):
        _typed(a.sortorder, b.sortorder)
        for x, y, c, d in zip(a.levels, b.levels, a.codes, b.codes):
            _axis(x, y)
            np.testing.assert_array_equal(c, d, strict=True)


def _exact(a, b):
    pd.testing.assert_frame_equal(a, b, check_exact=True, rtol=0, atol=0)
    _axis(a.index, b.index)
    _axis(a.columns, b.columns)
    np.testing.assert_array_equal(a.isna().to_numpy(), b.isna().to_numpy(), strict=True)
    # pandas frame equality can accept tiny Float64 extension differences even
    # with check_exact/zero tolerances. Check column operands independently.
    for position in range(a.shape[1]):
        x, y = a.iloc[:, position], b.iloc[:, position]
        if isinstance(x.dtype, ExtensionDtype):
            pd.testing.assert_extension_array_equal(
                x.array, y.array, check_exact=True, rtol=0, atol=0)
        present = ~x.isna().to_numpy()
        np.testing.assert_array_equal(
            x.array[present].to_numpy(), y.array[present].to_numpy(), strict=True)
    _typed(a.attrs, b.attrs)


def _frame(shape='multi', missing=True, policy=pd.NA, special=None):
    values = ['None', 'nan', '<NA>', 'repeat', 'repeat', pd.NA if missing else 'last']
    if special == 'empty':
        values = []
    elif special == 'allnull':
        values = [pd.NA] * 3
    dtype = pd.StringDtype(storage='pyarrow', na_value=policy)
    idx = pd.Index(pd.array(values, dtype=dtype), name='u')
    if shape == 'multi':
        idx = pd.MultiIndex.from_arrays(
            [pd.array(['w'] * len(values), dtype=dtype), idx], names=['t', 'u'])
    result = pd.DataFrame({
        'value': np.arange(len(idx), dtype=float),
        'mirror': pd.array(values, dtype='string[pyarrow]'),
    }, index=idx)
    result.attrs = {'nested': {'typed': [1, 1.0, True], 'missing': None, 'label': 'None'}}
    return result


@pytest.mark.parametrize('branch', ['plain', 'metadata'])
@pytest.mark.parametrize('shape', ['plain', 'multi'])
@pytest.mark.parametrize('missing', [False, True])
def test_recorded_policy_roundtrips(tmp_path, branch, shape, missing):
    original = _frame(shape, missing)
    target = tmp_path / 'roundtrip.parquet'
    prepared = lt.to_parquet(original, target, absolute_path=True,
                             **({'cache_hash': 'synthetic'} if branch == 'metadata' else {}))
    _exact(prepared, lt.get_dataframe(target))


@pytest.mark.parametrize('branch', ['plain', 'metadata'])
@pytest.mark.parametrize('shape', ['plain', 'multi'])
@pytest.mark.parametrize('special', ['empty', 'allnull'])
def test_empty_and_allnull_string_indexes(tmp_path, branch, shape, special):
    target = tmp_path / 'empty-null.parquet'
    prepared = lt.to_parquet(_frame(shape, special=special), target, absolute_path=True,
                             **({'cache_hash': 'synthetic'} if branch == 'metadata' else {}))
    # A non-StringDtype reconstruction is a distinct bug, not a reason to cast it.
    raw = pd.read_parquet(target)
    levels = raw.index.levels if isinstance(raw.index, pd.MultiIndex) else [raw.index]
    assert all(isinstance(level.dtype, pd.StringDtype) for level in levels)
    _exact(prepared, lt.get_dataframe(target))


@pytest.mark.parametrize('names', [[None, None], ['same', 'same'], ['value', 'u']])
@pytest.mark.parametrize('reverse', [False, True])
def test_physical_fields_with_mixed_policy_and_logical_names(tmp_path, names, reverse):
    policies = [pd.NA, np.nan][::(-1 if reverse else 1)]
    arrays = [pd.array(['b', pd.NA, 'a', 'b'],
                       dtype=pd.StringDtype(storage='pyarrow', na_value=policy))
              for policy in policies]
    original = pd.DataFrame({'value': [np.nextafter(1., 2.), 2., 3., 4.]},
                            index=pd.MultiIndex.from_arrays(arrays, names=names))
    # The legacy sanctioned writer can reject duplicate/colliding names at
    # reset_index. Exercise this reader contract with actual Arrow metadata.
    table = pa.Table.from_pandas(original, preserve_index=True)
    metadata = table.schema.pandas_metadata
    fields = metadata['index_columns']
    assert len(set(fields)) == 2
    recorded = {c['field_name']: c['numpy_type'] for c in metadata['columns']}
    assert [recorded[f] for f in fields] == ['str' if p is np.nan else 'string' for p in policies]
    target = tmp_path / 'physical.parquet'
    pq.write_table(table, target)
    _exact(original, lt.get_dataframe(target))


@pytest.mark.parametrize('policy', [pd.NA, np.nan])
def test_plain_unnamed_io(tmp_path, policy):
    original = _frame('plain', policy=policy)
    original.index.name = None
    target = tmp_path / 'unnamed.parquet'
    prepared = lt.to_parquet(original, target, absolute_path=True)
    _exact(prepared, lt.get_dataframe(target))


@pytest.mark.parametrize('storage', ['python', 'pyarrow'])
@pytest.mark.parametrize('policy', [pd.NA, np.nan])
def test_helper_preserves_unused_levels_codes_sortorder_and_storage(storage, policy):
    old_policy = np.nan if policy is pd.NA else pd.NA
    old = pd.Index(['z', 'unused', 'a'], dtype=pd.StringDtype(storage=storage, na_value=old_policy))
    integers = pd.Index([8, 4, 2], dtype='int64')
    index = pd.MultiIndex(levels=[old, integers], codes=[[2, -1, 0, 2], [1, 0, 2, 1]],
                          names=['same', 'same'], sortorder=0)
    original = pd.DataFrame({'mirror': pd.array(['x', None, 'nan', '<NA>'], dtype='string[pyarrow]')}, index=index)
    original.attrs = {'typed': [1, 1.0, True, None]}
    expected = original.copy()
    expected.index = pd.MultiIndex(
        levels=[pd.Index(['z', 'unused', 'a'], dtype=pd.StringDtype(storage=storage, na_value=policy)), integers],
        codes=[[2, -1, 0, 2], [1, 0, 2, 1]], names=['same', 'same'], sortorder=0)
    metadata = {'index_columns': ['physical0', 'physical1'], 'columns': [
        {'field_name': 'physical1', 'numpy_type': 'int64'},
        {'field_name': 'physical0', 'numpy_type': 'string' if policy is pd.NA else 'str'}]}
    _exact(expected, lt._restore_parquet_index_string_policy(original, metadata))


def _metadata():
    return {'index_columns': ['a', 'b'], 'columns': [
        {'field_name': 'a', 'numpy_type': 'str'}, {'field_name': 'b', 'numpy_type': 'str'}]}


@pytest.mark.parametrize('case', [
    'none', 'not-dict', 'missing-fields', 'fields-not-list', 'empty-fields',
    'range', 'mixed-descriptors', 'count', 'duplicate-descriptor',
    'missing-columns', 'columns-not-list', 'bad-column', 'missing-mapping',
    'duplicate-mapping', 'missing-field-name', 'unknown-policy', 'missing-policy',
])
def test_helper_metadata_noops(case):
    metadata = _metadata()
    if case == 'none': metadata = None
    elif case == 'not-dict': metadata = []
    elif case == 'missing-fields': del metadata['index_columns']
    elif case == 'fields-not-list': metadata['index_columns'] = 'a'
    elif case == 'empty-fields': metadata['index_columns'] = []
    elif case == 'range': metadata['index_columns'] = [{'kind': 'range'}]
    elif case == 'mixed-descriptors': metadata['index_columns'][1] = {'kind': 'range'}
    elif case == 'count': metadata['index_columns'] = ['a']
    elif case == 'duplicate-descriptor': metadata['index_columns'] = ['a', 'a']
    elif case == 'missing-columns': del metadata['columns']
    elif case == 'columns-not-list': metadata['columns'] = {}
    elif case == 'bad-column': metadata['columns'].append(None)
    elif case == 'missing-mapping': metadata['columns'].pop()
    elif case == 'duplicate-mapping': metadata['columns'].append(metadata['columns'][0].copy())
    elif case == 'missing-field-name': del metadata['columns'][0]['field_name']
    elif case == 'unknown-policy': metadata['columns'][0]['numpy_type'] = 'unknown'
    elif case == 'missing-policy': del metadata['columns'][0]['numpy_type']
    original = _frame(policy=np.nan)
    expected = original.copy(deep=True)
    result = lt._restore_parquet_index_string_policy(original, metadata)
    assert result is original
    _exact(expected, result)


@pytest.mark.parametrize('case', ['absent', 'unknown', 'empty-descriptors'])
def test_actual_metadata_noops(tmp_path, case):
    original = _frame('plain', policy=np.nan)
    table = pa.Table.from_pandas(original, preserve_index=True)
    metadata = dict(table.schema.metadata)
    if case == 'absent':
        del metadata[b'pandas']
    else:
        pandas_metadata = json.loads(metadata[b'pandas'])
        if case == 'unknown':
            for column in pandas_metadata['columns']:
                if column['field_name'] in pandas_metadata['index_columns']:
                    column['numpy_type'] = 'unknown'
        else:
            pandas_metadata['index_columns'] = []
        metadata[b'pandas'] = json.dumps(pandas_metadata).encode()
    target = tmp_path / 'noop.parquet'
    pq.write_table(table.replace_schema_metadata(metadata), target)
    if case == 'unknown':
        # Arrow itself rejects this unknown dtype before a successful read.
        with pytest.raises(TypeError) as expected: pd.read_parquet(target)
        with pytest.raises(TypeError) as observed: lt.get_dataframe(target)
        assert observed.value.args == expected.value.args
    else:
        _exact(pd.read_parquet(target), lt.get_dataframe(target))


@pytest.mark.parametrize('kind', ['range', 'integer', 'datetime', 'categorical'])
def test_nonstring_and_range_unchanged(tmp_path, kind):
    indexes = {
        'range': pd.RangeIndex(7, 19, 3, name='r'),
        'integer': pd.Index([9, 2, 2, 1], name='n'),
        'datetime': pd.date_range('2020-01-01', periods=4, name='d'),
        'categorical': pd.CategoricalIndex(['b', 'a', 'b', None], categories=['c', 'b', 'a'], ordered=True, name='c'),
    }
    original = pd.DataFrame({'value': [1., 2., 3., 4.]}, index=indexes[kind])
    target = tmp_path / 'nonstring.parquet'
    pq.write_table(pa.Table.from_pandas(original, preserve_index=None), target)
    _exact(pd.read_parquet(target), lt.get_dataframe(target))


@pytest.mark.parametrize('route', ['local', 'dvc-stream'])
def test_footer_uses_same_open_input_and_stream_stays_open(tmp_path, monkeypatch, route):
    target = tmp_path / 'stream.parquet'
    prepared = lt.to_parquet(_frame(), target, absolute_path=True)
    if route == 'dvc-stream':
        raw = target.read_bytes()
        target = tmp_path / 'not-local.parquet'
        monkeypatch.setattr(lt.DVCFS, 'open', lambda *a, **kw: io.BytesIO(raw))
        monkeypatch.setattr(lt, '_ensure_dvc_pulled', lambda *a: None)
        monkeypatch.setattr(lt, '_dvc_cache_path', lambda *a: None)
    original_read, original_schema = pd.read_parquet, pq.read_schema
    seen = []
    def reader(stream, *args, **kwargs):
        assert not isinstance(stream, (str, bytes)) and not stream.closed
        seen.append(stream)
        return original_read(stream, *args, **kwargs)
    def schema(stream, *args, **kwargs):
        assert stream is seen[-1] and not stream.closed
        seen.append(stream)
        return original_schema(stream, *args, **kwargs)
    monkeypatch.setattr(pd, 'read_parquet', reader)
    monkeypatch.setattr(pq, 'read_schema', schema)
    result = lt.get_dataframe(target)
    assert len(seen) == 2 and seen[0] is seen[1] and seen[0].closed
    _exact(prepared, result)


@pytest.mark.parametrize('error_type', [OSError, pa.ArrowInvalid])
def test_new_footer_failure_is_loud(tmp_path, monkeypatch, error_type):
    target = tmp_path / 'footer.parquet'
    lt.to_parquet(_frame(), target, absolute_path=True)
    error = error_type('controlled footer failure')
    def fail(*args, **kwargs): raise error
    monkeypatch.setattr(pq, 'read_schema', fail)
    with pytest.raises(error_type) as result:
        lt.get_dataframe(target)
    assert result.value is error


@pytest.mark.parametrize('bad', ['json', 'index-kind'])
def test_original_metadata_parser_failure_is_unchanged(tmp_path, bad):
    table = pa.Table.from_pandas(_frame(), preserve_index=True)
    metadata = dict(table.schema.metadata)
    if bad == 'json': metadata[b'pandas'] = b'{bad-json'
    else:
        value = json.loads(metadata[b'pandas'])
        value['index_columns'] = [{'kind': 'not-a-range'}]
        metadata[b'pandas'] = json.dumps(value).encode()
    target = tmp_path / 'malformed.parquet'
    pq.write_table(table.replace_schema_metadata(metadata), target)
    with pytest.raises(Exception) as expected: pd.read_parquet(target)
    with pytest.raises(type(expected.value)) as observed: lt.get_dataframe(target)
    assert observed.value.args == expected.value.args
    assert type(observed.value.__cause__) is type(expected.value.__cause__)


@pytest.mark.parametrize('branch', ['plain', 'metadata'])
def test_atomic_partial_failure_keeps_target(tmp_path, monkeypatch, branch):
    target = tmp_path / 'atomic.parquet'
    lt.to_parquet(_frame(), target, absolute_path=True)
    before = target.read_bytes()
    def fail(*args, **kwargs):
        path = Path(args[1])
        path.write_bytes(b'controlled partial write')
        raise RuntimeError('controlled write failure')
    if branch == 'plain': monkeypatch.setattr(pd.DataFrame, 'to_parquet', fail)
    else: monkeypatch.setattr(pq, 'write_table', fail)
    with pytest.raises(RuntimeError, match='controlled write failure'):
        lt.to_parquet(_frame(), target, absolute_path=True,
                      **({'cache_hash': 'synthetic'} if branch == 'metadata' else {}))
    assert target.read_bytes() == before
    assert not list(tmp_path.glob('*.tmp.*'))


@pytest.mark.parametrize('delta', ['tiny', 'nextafter'])
def test_exact_comparator_rejects_nullable_float_difference(delta):
    reference = pd.DataFrame({'value': pd.array([1., 2.], dtype='Float64')},
                             index=pd.MultiIndex.from_arrays([['a', 'b'], [1, 2]]))
    mutant = reference.copy(deep=True)
    value = 1. + 1e-12 if delta == 'tiny' else np.nextafter(1., 2.)
    mutant['value'] = pd.array([value, 2.], dtype='Float64')
    with pytest.raises(AssertionError):
        _exact(reference, mutant)
