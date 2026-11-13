"""Authoritative imported paths remain discoverable; no raw/clinical IO."""
import ast
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library import local_tools

ROOT = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Uganda'
# Read the actual literal metadata dictionary without executing its module.
_MAP_AST = ast.parse((ROOT / '_/uganda.py').read_text())
SOURCES = ast.literal_eval(next(node.value for node in _MAP_AST.body
    if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
    and target.id == 'ANTHRO_FILES' for target in node.targets)))
WAVES = tuple(SOURCES)


@pytest.mark.parametrize('wave', WAVES)
def test_real_scanner_discovers_the_authoritative_committed_source(wave):
    script = ROOT / wave / '_/anthropometry.py'
    refs = {ref for ref in local_tools.scan_script_data_refs(script)
            if ref.lower().endswith('.dta')}
    assert refs == {SOURCES[wave]}
    source = (script.parent / SOURCES[wave]).resolve()
    assert source.with_name(source.name + '.dvc').is_file()
    assert local_tools.source_fingerprint(source).startswith(f'dvc:{source.name}:')


@pytest.mark.parametrize('wave', WAVES)
def test_private_preparation_collects_the_authoritative_source(wave):
    class MetadataOnly:
        name = 'Uganda'
        file_path = ROOT
        waves = [wave]
        resources = {'Data Scheme': {'anthropometry': {}}}

        def __getitem__(self, year):
            assert year == wave
            return SimpleNamespace(file_path=ROOT / wave, resources={})

        def _make_other_references(self, method):
            assert method == 'anthropometry'
            return {}

        def _make_wave_inputs(self, method):
            assert method == 'anthropometry'
            return {Path('Uganda') / wave / '_/anthropometry.parquet': ([wave], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), 'anthropometry', _with_sources=True)
    expected = (ROOT / wave / '_' / SOURCES[wave]).resolve()
    # Preserve the unchanged country compiler f-string suffix metadata.
    fragment = Path('/_/anthropometry.parquet')
    assert sources == {expected, fragment}
    assert record == {'tables': {}, 'sources': {
        str(fragment): 'missing:anthropometry.parquet'}}


def _run_actual_script(monkeypatch, wave, *, drift=False, optimize=0):
    script = ROOT / wave / '_/anthropometry.py'
    native = pd.DataFrame({'raw': pd.Series([1, pd.NA], dtype='Int64')})
    original = native.copy(deep=True)
    native.attrs['witness'] = ('native', wave)
    original.attrs = native.attrs.copy()
    result = pd.DataFrame({'Weight': pd.Series([12.5], dtype='Float64')},
                          index=pd.Index([0], name='i'))
    result.attrs['witness'] = ('result', wave)
    calls = []
    colmap = object()
    helper = ModuleType('uganda')
    helper.ANTHRO_FILES = dict(SOURCES)
    if drift:
        helper.ANTHRO_FILES[wave] = '../Data/changed-authoritative-source.dta'
    helper.ANTHRO_COLMAPS = {wave: colmap}

    def reader(path, **kwargs):
        calls.append(('read', path, kwargs))
        return native

    def transform(t, frame, mapping):
        assert t == wave and frame is native and mapping is colmap
        calls.append(('transform', t))
        return result

    def writer(frame, path):
        assert frame is result and path == 'anthropometry.parquet'
        calls.append(('write', path))

    helper.anthropometry_for_wave = transform
    with monkeypatch.context() as fixture:
        fixture.setitem(sys.modules, 'uganda', helper)
        fixture.setattr(local_tools, 'get_dataframe', reader)
        fixture.setattr(local_tools, 'to_parquet', writer)
        # Match existing real-script/IO-leaf tests; restore appended sys.path.
        fixture.setattr(sys, 'path', sys.path.copy())
        code = compile(script.read_bytes(), str(script), 'exec', optimize=optimize)
        if drift:
            with pytest.raises(RuntimeError, match='source declaration differs from ANTHRO_FILES'):
                exec(code, {'__file__': str(script), '__name__': '__main__'})
            assert calls == []  # Before getter, transform AND writer.
        else:
            exec(code, {'__file__': str(script), '__name__': '__main__'})
            assert calls == [('read', SOURCES[wave], {}), ('transform', wave),
                             ('write', 'anthropometry.parquet')]
    pd.testing.assert_frame_equal(native, original)
    assert native.attrs == original.attrs
    assert result.attrs == {'witness': ('result', wave)}


@pytest.mark.parametrize('wave', WAVES)
def test_current_authoritative_getter_and_processing_are_unchanged(monkeypatch, wave):
    _run_actual_script(monkeypatch, wave)


@pytest.mark.parametrize('optimize', [0, 2])
def test_authoritative_map_drift_refuses_before_any_io_even_optimized(monkeypatch, optimize):
    _run_actual_script(monkeypatch, '2011-12', drift=True, optimize=optimize)
