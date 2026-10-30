"""Restricted 2016 reader uses held bytes without constructing the DVC index."""
import ast
import io
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Malawi/2016-17/_/plot_inputs.py'


def reader(namespace):
    # Isolate the real helper from this executable script's top-level raw builds.
    tree = ast.parse(SCRIPT.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_read_usecols')
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SCRIPT), 'exec'), namespace)
    return namespace['_read_usecols']


@pytest.mark.parametrize('cached', [True, False])
@pytest.mark.parametrize('decoder_failure', [False, True])
def test_restricted_reader_bytes_options_and_cleanup(tmp_path, cached, decoder_failure):
    import os
    import tempfile
    payload = b'invented source bytes, including \xff'
    blob = tmp_path / 'blob'
    blob.write_bytes(payload)
    calls = []
    temporary = []
    columns = ['case_id', 'ag_d39d']
    expected = object()
    failure = ValueError('invented decoder failure')

    def ensure(path):
        calls.append(('ensure', path))

    def resolve(path):
        calls.append(('resolve', path))
        return blob if cached else None

    def dvc_open(path):
        assert not cached, 'cached source must not construct the DVC index'
        calls.append(('dvc', path))
        return io.BytesIO(payload)

    def decode(path, **kwargs):
        temporary.append(Path(path))
        assert Path(path).read_bytes() == payload
        assert kwargs == {'usecols': columns, 'apply_value_formats': False}
        if decoder_failure:
            raise failure
        return expected, object()

    fn = reader(dict(os=os, tempfile=tempfile, _ensure_dvc_pulled=ensure,
                     _dvc_cache_path=resolve, _COUNTRIES_DIR=tmp_path,
                     _dvc_working_directory=lambda _: nullcontext(),
                     DVCFS=SimpleNamespace(open=dvc_open),
                     pyreadstat=SimpleNamespace(read_dta=decode)))
    if decoder_failure:
        with pytest.raises(ValueError) as exc:
            fn('Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta', columns)
        assert exc.value is failure
    else:
        assert fn('Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta', columns) is expected
    assert [c[0] for c in calls] == ['ensure', 'resolve'] + ([] if cached else ['dvc'])
    assert len(temporary) == 1 and not temporary[0].exists()
    assert blob.read_bytes() == payload
