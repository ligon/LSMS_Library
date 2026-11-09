"""GH381: real private Make routes distinguish target absence from failure.

Reuse GH1590's finite source-free survey and GH1039's Fakeland. No actual
country data/provider: all frames and subprocess scripts are invented.
"""
from pathlib import Path
import shlex
import subprocess
import sys
import warnings

import pytest

from lsms_library import country as C
from lsms_library.paths import data_root
from tests.test_gh1590_wave_failure_manifest import survey, BAD, GOOD, NAME
from tests.test_gh1039_make_backend_no_country_rule import fakeland, _PREAMBLE, _COUNTRY_SCRIPT

REAL_RUN = subprocess.run


def _legacy(survey, monkeypatch, *, script=True):
    survey.script()
    survey.fault = 'missing'  # Existing primary-getter IO leaf, not a builder.
    monkeypatch.setattr(C.subprocess, 'run', REAL_RUN)
    path = data_root() / NAME / BAD / '_/facts.parquet'
    native = survey.country_path / BAD / '_/facts.py'
    if script:
        native.write_text(_writer(path))
    return path, native, survey.country_path / BAD / '_/Makefile'


def _writer(path, *, messages=False):
    return ("from pathlib import Path\nimport pandas as pd\n"
            + ("import sys\nprint('healthy-stdout')\nsys.stderr.buffer.write(b'healthy-native-\\xff\\r\\n')\n" if messages else '')
            + f"p=Path({str(path)!r});p.parent.mkdir(parents=True,exist_ok=True)\n"
            + f"pd.DataFrame({{'t':[{BAD!r}],'i':['synthetic'],'x':[7.0]}}).set_index(['t','i']).to_parquet(p)\n")


def test_exact_absence_then_healthy_python_is_quiet(survey, monkeypatch, capfd):
    path, script, makefile = _legacy(survey, monkeypatch)
    makefile.write_text('all:\n\t@true\n')  # Every requested parquet target absent.
    calls = []
    def tracked(argv, *a, **k):
        calls.append(list(map(str, argv)))
        return REAL_RUN(argv, *a, **k)
    monkeypatch.setattr(C.subprocess, 'run', tracked)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        out = survey.load([BAD])
    assert out['x'].tolist() == [7.0] and path.exists()
    assert calls[-1] == [sys.executable, str(script)]
    assert len([x for x in calls if x[0] == 'make']) == 5
    assert not any('Makefile execution failed' in str(w.message) for w in seen)
    assert 'No rule to make target' not in capfd.readouterr().err
    assert not list(survey.country_path.rglob('*.parquet'))


def test_all_absent_retains_named_final_diagnostic(survey, monkeypatch, caplog, capfd):
    path, script, makefile = _legacy(survey, monkeypatch)
    script.write_text('# healthy no-output compatibility case\n')
    makefile.write_text('all:\n\t@true\n')
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        with pytest.raises(RuntimeError, match='Could not materialize Coverage1590/facts') as caught:
            survey.load([BAD])
    assert str(survey.cache) in str(caught.value)
    assert not path.exists() and not survey.cache.exists()
    assert any('Data file ' + str(path) + ' still missing after running fallbacks.' in x.message
               for x in caplog.records)
    assert not any('Makefile execution failed' in str(w.message) for w in seen)
    assert 'No rule to make target' not in capfd.readouterr().err


@pytest.mark.parametrize('kind', ['recipe', 'prerequisite', 'nested', 'extra-unknown'])
def test_legacy_real_failure_remains_loud_before_healthy_python(
        survey, monkeypatch, tmp_path, capfdbinary, kind):
    path, script, makefile = _legacy(survey, monkeypatch)
    if kind == 'prerequisite':
        marker = b'needed by'
        body = f'{path}: {tmp_path}/missing-prerequisite\n\t@true\n'
    elif kind == 'nested':
        marker = b'nested-missing'
        nested = tmp_path/'nested.mk';nested.write_text('all:\n\t@true\n')
        body = f'{path}: FORCE\n\t$(MAKE) -sf {nested} nested-missing\n.PHONY: FORCE\n'
    else:
        fail = tmp_path/'failure.py'
        payload = b'legacy-native-\xff\r\n'
        if kind == 'extra-unknown':
            payload = (f"make: *** No rule to make target '{path}'. Stop.\nunknown-extra\n".encode())
        marker = b'legacy-native-\xff\r\n' if kind == 'recipe' else b'unknown-extra'
        fail.write_text('import sys\nsys.stderr.buffer.write('+repr(payload)+')\nraise SystemExit(7)\n')
        body = f'{path}: FORCE\n\t{shlex.quote(sys.executable)} {shlex.quote(str(fail))}\n.PHONY: FORCE\n'
    makefile.write_text(body)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        out = survey.load([BAD])
    assert out['x'].tolist() == [7.0] and path.exists()
    assert any('Makefile execution failed' in str(w.message) for w in seen)
    assert capfdbinary.readouterr().err.count(marker) == 1
    assert not list(survey.country_path.rglob('*.parquet'))


def test_legacy_success_reemits_native_stderr_and_streams_stdout(
        survey, monkeypatch, capfdbinary):
    path, script, makefile = _legacy(survey, monkeypatch)
    script.write_text(_writer(path, messages=True))
    makefile.write_text(f'{path}: FORCE\n\t{shlex.quote(sys.executable)} {script.name}\n.PHONY: FORCE\n')
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        out = survey.load([BAD])
    assert out['x'].tolist() == [7.0]
    captured = capfdbinary.readouterr()
    assert captured.out.count(b'healthy-stdout') == 1
    assert captured.err.count(b'healthy-native-\xff\r\n') == 1
    assert not any('Makefile execution failed' in str(w.message) for w in seen)


def test_actual_nested_failure_is_terminal_on_strict_route(fakeland, tmp_path, capfd):
    nested=tmp_path/'nested.mk';nested.write_text('all:\n\t@true\n')
    makefile = _PREAMBLE + f'$(VAR_DIR)/build_me.parquet: FORCE\n\t$(MAKE) -sf {nested} nested-missing\n.PHONY: FORCE\n'
    country = fakeland.build(makefile=makefile, country_script=_COUNTRY_SCRIPT)
    with pytest.raises(RuntimeError, match='Make build failed for Fakeland/build_me') as caught:
        country.build_me()
    cause = caught.value.__cause__
    assert isinstance(cause, subprocess.CalledProcessError) and cause.returncode == 2
    assert b'nested-missing' in cause.stderr and b'Error 2' in cause.stderr
    assert 'nested-missing' in capfd.readouterr().err
    assert not fakeland.var.exists()


@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_healthy_yaml_never_invokes_make(survey, monkeypatch, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    def forbidden(*a, **k):
        raise AssertionError('healthy YAML invoked a subprocess')
    monkeypatch.setattr(C.subprocess, 'run', forbidden)
    out = survey.load([GOOD])
    assert out['x'].tolist() == [float(GOOD)]
    assert survey.reads == [GOOD]
