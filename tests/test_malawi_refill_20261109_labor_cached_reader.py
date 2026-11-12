"""Inert-byte controls for the 2016 Malawi labor restricted-reader route.

Source-only dispatch: authored for ROOT's later execution, not run by its
worker. Extract only the actual helper AST; never import the executable wave
script, the library, pandas or a native decoder. These controls establish byte
routing and the existing two-stage column selection, not survey/API parity.
"""
import ast
import io
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = (Path(__file__).resolve().parents[1]
          / 'lsms_library/countries/Malawi/2016-17/_/plot_labor.py')
SOURCE = 'Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta'


def _isolated_reader(namespace):
    tree = ast.parse(SCRIPT.read_text())
    node = next(n for n in tree.body
                if isinstance(n, ast.FunctionDef) and n.name == '_read_usecols')
    module = ast.Module(body=[node], type_ignores=[])
    exec(compile(module, str(SCRIPT), 'exec'), namespace)
    return namespace['_read_usecols']


@pytest.mark.parametrize('cached', [True, False])
@pytest.mark.parametrize('failure_stage', [None, 'metadata', 'columns'])
def test_labor_cached_or_fallback_bytes_two_decodes_and_cleanup(
        tmp_path, cached, failure_stage):
    payload = b'invented non-Stata bytes\x00\xff'
    blob = tmp_path / 'inert-blob'
    blob.write_bytes(payload)
    requested = ['ag_d42c1', 'absent', 'case_id', 'ag_d42b1']
    available = ['case_id', 'ag_d42b1', 'ag_d42c1']
    expected_columns = ['ag_d42c1', 'case_id', 'ag_d42b1']
    calls = []
    decode_calls = []
    temporary_paths = []
    expected = object()
    failure = ValueError('invented ' + str(failure_stage) + ' failure')

    def ensure(path):
        assert path == SOURCE
        calls.append('ensure')

    def resolve(path):
        assert path == SOURCE
        calls.append('resolve')
        return blob if cached else None

    @contextmanager
    def dvc_directory(path):
        assert not cached, 'held blob must bypass DVC directory/index setup'
        assert path == tmp_path
        calls.append('dvc_enter')
        try:
            yield
        finally:
            calls.append('dvc_exit')

    def dvc_open(path):
        assert not cached, 'held blob must bypass DVCFS.open'
        assert path == SOURCE
        calls.append('dvc_open')
        return io.BytesIO(payload)

    def decode(path, **kwargs):
        p = Path(path)
        temporary_paths.append(p)
        assert p != blob and p.suffix == '.dta'
        assert p.read_bytes() == payload
        decode_calls.append(kwargs)
        if len(decode_calls) == 1:
            assert kwargs == {'metadataonly': True}
            if failure_stage == 'metadata':
                raise failure
            return object(), SimpleNamespace(column_names=available)
        assert len(decode_calls) == 2
        assert kwargs == {'usecols': expected_columns, 'apply_value_formats': False}
        if failure_stage == 'columns':
            raise failure
        return expected, object()

    read = _isolated_reader(dict(
        os=os, tempfile=tempfile, _ensure_dvc_pulled=ensure,
        _dvc_cache_path=resolve, _COUNTRIES_DIR=tmp_path,
        _dvc_working_directory=dvc_directory,
        DVCFS=SimpleNamespace(open=dvc_open),
        pyreadstat=SimpleNamespace(read_dta=decode)))
    if failure_stage is not None:
        with pytest.raises(ValueError) as caught:
            read(SOURCE, requested)
        assert caught.value is failure
    else:
        assert read(SOURCE, requested) is expected

    assert calls == ['ensure', 'resolve'] + (
        [] if cached else ['dvc_enter', 'dvc_open', 'dvc_exit'])
    assert len(decode_calls) == (1 if failure_stage == 'metadata' else 2)
    assert len(set(temporary_paths)) == 1
    assert all(not p.exists() for p in temporary_paths)
    assert blob.read_bytes() == payload
    assert requested == ['ag_d42c1', 'absent', 'case_id', 'ag_d42b1']
