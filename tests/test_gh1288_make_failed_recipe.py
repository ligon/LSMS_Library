"""GH1288: a real failed country Make recipe cannot recover stale outputs.

Private synthetic frames only. Reuse Niger's real recipe/module/concatenator
fixture and GH1039's fake-country fixture; no provider build is involved.
The strict policy is only the direct non-DVC materialize:make country route.
See .coder/ledger/1288-make-failed-recipe.md §2–§4.
"""
from __future__ import annotations

import errno
import hashlib
import io
import os
from pathlib import Path
import subprocess
import warnings

import pandas as pd
import pytest

from lsms_library import country as C
from lsms_library.paths import data_root
from tests.test_niger_concatenator_refusal import niger_tree, WAVES, _frame
from tests.test_gh1039_make_backend_no_country_rule import (
    fakeland, TABLE, _COUNTRY_SCRIPT, _MAKEFILE_WAVE_ONLY, _PREAMBLE,
    _MAKEFILE_UNMAKEABLE_PREREQ,
)

NIGER_TABLE = 'crop_production'
OLD = {w: [float(100*(j+1)+1), float(100*(j+1)+2)] for j,w in enumerate(WAVES)}


def _niger_paths():
    return {w: data_root()/'Niger'/w/'_'/f'{NIGER_TABLE}.parquet' for w in WAVES} | {
        'country': data_root()/'Niger'/'var'/f'{NIGER_TABLE}.parquet'}


def _plant_old_waves():
    for w,path in _niger_paths().items():
        if w == 'country':
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        frame = _frame(NIGER_TABLE, w, n=2)
        frame['Quantity'] = OLD[w]
        frame.to_parquet(path)


def _snapshot(paths):
    return {key: (path.stat().st_ino, path.stat().st_mtime_ns,
                  path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest())
            for key,path in paths.items() if path.exists()}


def _assert_niger_fresh(frame):
    # Reorder the expected oracle only, per the shipped finalizer's preferred
    # i,t,v,m order. Never normalize the actual result to observed values.
    expected = pd.concat([_frame(NIGER_TABLE, w, n=2) for w in WAVES])
    expected = expected.reorder_levels(['i', 't', 'plot_id', 'crop', 'u'])
    pd.testing.assert_frame_equal(frame, expected, check_exact=True)
    assert frame.attrs['country'] == 'Niger'
    assert frame.attrs['id_converted'] is True
    assert frame.index.is_unique


@pytest.mark.parametrize('failing', [WAVES[0], WAVES[2], WAVES[-1]])
@pytest.mark.parametrize('planted', [False, True], ids=['no-old-outputs', 'all-old-outputs'])
def test_failed_niger_recipe_never_serves_old_or_partial_payload(
        niger_tree, monkeypatch, capfd, failing, planted):
    monkeypatch.setenv('LSMS_MAKE_JOBS', '1')
    country = niger_tree(NIGER_TABLE, failing=failing)
    if planted:
        _plant_old_waves()
    paths = _niger_paths()
    before = _snapshot(paths)
    assert not paths['country'].exists()
    calls = []
    real_run = subprocess.run

    def tracked(argv, *args, **kwargs):
        calls.append(list(map(str, argv)))
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(C.subprocess, 'run', tracked)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        with pytest.raises(RuntimeError, match='Make build failed for Niger/crop_production') as caught:
            country.crop_production()
    cause = caught.value.__cause__
    assert isinstance(cause, subprocess.CalledProcessError)
    assert cause.returncode == 2
    assert str(paths['country']) in str(caught.value)
    assert b'injected wave failure' in cause.stderr
    assert 'injected wave failure' in capfd.readouterr().err
    assert not any('Python fallback' in str(w.message) for w in seen)
    table_calls = [a for a in calls if any('crop_production' in p for p in a)]
    assert table_calls == [['make', '-s', str(paths['country'])]], table_calls
    assert not paths['country'].exists(), 'must not write/serve a partial country'
    failed_at = WAVES.index(failing)
    for j,w in enumerate(WAVES):
        if j < failed_at:
            assert pd.read_parquet(paths[w])['Quantity'].tolist() == [1.0, 2.0]
        elif planted:
            assert _snapshot({w: paths[w]})[w] == before[w], 'failed/unreached old output changed'
            assert pd.read_parquet(paths[w])['Quantity'].tolist() == OLD[w]
        else:
            assert not paths[w].exists(), 'failed/unreached wave unexpectedly built'


def test_healthy_niger_recipe_replaces_all_old_waves(niger_tree):
    country = niger_tree(NIGER_TABLE)
    _plant_old_waves()
    before = _snapshot(_niger_paths())
    frame = country.crop_production()
    _assert_niger_fresh(frame)
    for w in WAVES:
        assert _snapshot(_niger_paths())[w] != before[w]
        assert pd.read_parquet(_niger_paths()[w])['Quantity'].tolist() == [1.0, 2.0]
    assert len(pd.read_parquet(_niger_paths()['country'])) == 8


def test_healthy_niger_warm_make_keeps_outputs_and_exact_result(niger_tree):
    country = niger_tree(NIGER_TABLE)
    first = country.crop_production()
    _assert_niger_fresh(first)
    before = _snapshot(_niger_paths())
    second = country.crop_production()
    _assert_niger_fresh(second)
    pd.testing.assert_frame_equal(first, second, check_exact=True)
    assert first.attrs == second.attrs
    assert _snapshot(_niger_paths()) == before


@pytest.mark.parametrize('target,diagnostics,expected', [
    ('/tmp/a.parquet', "make: *** No rule to make target '/tmp/a.parquet'.  Stop.\n", True),
    ('/tmp/a.parquet', "make: *** No rule to make target `/tmp/a.parquet'.  Stop.\n", True),
    (r'C:\private\a.parquet', "make.exe: *** No rule to make target 'C:\\private\\a.parquet'.  Stop.\r\n", True),
    ('C:/private/a.parquet', "make.exe: *** No rule to make target 'C:/private/a.parquet'. Stop.\r\n", True),
    ('/tmp/a.parquet', "make: *** No rule to make target '/tmp/a.parquet.bak'.  Stop.\n", False),
    ('/tmp/a.parquet', "make: *** No rule to make target '/tmp/missing', needed by '/tmp/a.parquet'. Stop.\n", False),
    ('/tmp/a.parquet', "make: *** No rule to make target '/tmp/a.parquet', needed by '/tmp/other'. Stop.\n", False),
    ('/tmp/a.parquet', "make[1]: *** No rule to make target '/tmp/a.parquet'. Stop.\nmake: *** [Makefile:3: /tmp/a.parquet] Error 2\n", False),
    ('/tmp/a.parquet', "make: *** No rule to make target '/tmp/a.parquet'. Stop.\nunknown failure\n", False),
    ('/tmp/a.parquet', "make: *** No rule to make target '/tmp/a.parquet'. Stop.\nmake: *** No rule to make target '/tmp/other'. Stop.\n", False),
    ('/tmp/a.parquet', "make: *** Aucune règle pour fabriquer la cible '/tmp/a.parquet'. Arrêt.\n", False),
    ('/tmp/a.parquet', 'make: *** [Makefile:1: /tmp/a.parquet] Error 7\n', False),
    ('/tmp/a.parquet', '', False),
])
def test_shared_probe_runner_missing_target_classifier(target, diagnostics, expected):
    assert C._make_target_is_missing(diagnostics, target) is expected
    assert C._make_target_is_missing(diagnostics.encode('utf-8'), target) is expected


def test_missing_requested_target_keeps_real_intree_alternate(fakeland, capfd):
    country = fakeland.build(makefile=_MAKEFILE_WAVE_ONLY, country_script=_COUNTRY_SCRIPT)
    with pytest.warns(UserWarning, match='Makefile execution failed') as seen:
        frame = country.build_me()
    assert len(seen) == 2, 'data-root and in-tree-var rule absent; _/ alternate builds'
    assert frame['x'].tolist() == [7.0]
    assert fakeland.var.exists()
    assert 'No rule to make target' in capfd.readouterr().err
    assert not list((fakeland.country_dir/'_').glob('*.parquet'))


def test_script_only_country_still_builds_without_makefile(fakeland):
    country = fakeland.build(makefile=None, country_script=_COUNTRY_SCRIPT)
    assert country.build_me()['x'].tolist() == [7.0]


def test_missing_make_executable_keeps_country_python_fallback(fakeland, monkeypatch):
    country = fakeland.build(country_script=_COUNTRY_SCRIPT)
    real_run = subprocess.run
    missing = []

    def no_make(argv, *args, **kwargs):
        if argv[0] == 'make':
            missing.append(argv)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), 'make')
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(C.subprocess, 'run', no_make)
    with pytest.warns(UserWarning, match='Makefile execution failed'):
        frame = country.build_me()
    assert len(missing) == 3
    assert frame['x'].tolist() == [7.0]


def test_broken_prerequisite_is_terminal_even_with_viable_python(fakeland, capfd):
    country = fakeland.build(makefile=_MAKEFILE_UNMAKEABLE_PREREQ, country_script=_COUNTRY_SCRIPT)
    with pytest.raises(RuntimeError, match='Make build failed for Fakeland/build_me') as caught:
        country.build_me()
    assert isinstance(caught.value.__cause__, subprocess.CalledProcessError)
    assert b'needed by' in caught.value.__cause__.stderr
    assert 'needed by' in capfd.readouterr().err
    assert not fakeland.var.exists()


@pytest.mark.parametrize('recovery', ['python', 'alternate', 'old-country-var'])
def test_real_primary_recipe_failure_prevents_even_viable_recovery(fakeland, capfd, recovery):
    makefile = _PREAMBLE + '''\
.PHONY: force
$(VAR_DIR)/build_me.parquet: force
\t@echo primary-stdout
\t@echo primary-stderr >&2
\t@exit 7
'''
    if recovery == 'alternate':
        makefile += f'''\
{fakeland.country_dir}/var/build_me.parquet:
\tpython build_me.py
'''
    country = fakeland.build(makefile=makefile, country_script=_COUNTRY_SCRIPT)
    if recovery == 'old-country-var':
        fakeland.plant_var()
    before = _snapshot({'country': fakeland.var})
    with pytest.raises(RuntimeError, match='Make build failed for Fakeland/build_me') as caught:
        country.build_me()
    assert caught.value.__cause__.returncode == 2
    assert str(fakeland.var) in str(caught.value)
    output = capfd.readouterr()
    assert output.out.count('primary-stdout') == 1
    assert output.err.count('primary-stderr') == 1
    assert _snapshot({'country': fakeland.var}) == before


@pytest.mark.parametrize('diagnostics', [b'', b'unknown failure\n', b'Aucune regle pour fabriquer la cible\n'])
def test_unknown_recipe_diagnostics_fail_closed_and_preserve_cause(fakeland, monkeypatch, capfd, diagnostics):
    country = fakeland.build(country_script=_COUNTRY_SCRIPT)
    calls = []
    error = None

    def failed(argv, *args, **kwargs):
        nonlocal error
        calls.append(list(argv))
        assert argv[0] == 'make'
        assert kwargs['env']['LC_ALL'] == kwargs['env']['LANGUAGE'] == 'C'
        assert kwargs['stderr'] == subprocess.PIPE
        assert 'stdout' not in kwargs and 'text' not in kwargs
        error = subprocess.CalledProcessError(2, argv, stderr=diagnostics)
        raise error

    monkeypatch.setattr(C.subprocess, 'run', failed)
    with pytest.raises(RuntimeError, match='Make build failed for Fakeland/build_me') as caught:
        country.build_me()
    assert caught.value.__cause__ is error
    assert len(calls) == 1
    assert diagnostics.decode() in capfd.readouterr().err
    assert not fakeland.var.exists()


def test_non_utf8_recipe_stderr_retains_native_bytes_and_original_cause(fakeland, capfdbinary):
    makefile = _PREAMBLE + '''\
$(VAR_DIR)/build_me.parquet:
\tpython failure.py
'''
    country = fakeland.build(makefile=makefile, country_script=_COUNTRY_SCRIPT)
    (fakeland.country_dir/'_'/'failure.py').write_text(
        "import sys\nsys.stderr.buffer.write(b'native-diagnostic-\\xff\\r\\n')\nraise SystemExit(9)\n")
    with pytest.raises(RuntimeError, match='Make build failed for Fakeland/build_me') as caught:
        country.build_me()
    cause = caught.value.__cause__
    assert isinstance(cause, subprocess.CalledProcessError)
    assert b'native-diagnostic-\xff\r\n' in cause.stderr
    assert capfdbinary.readouterr().err.count(b'native-diagnostic-\xff\r\n') == 1
    assert not fakeland.var.exists()


def test_non_utf8_stderr_text_only_redirect_is_visible(monkeypatch):
    stream = io.StringIO()
    monkeypatch.setattr(C.sys, 'stderr', stream)
    C._reemit_make_stderr(b'bad-byte-\xff\r\n')
    assert stream.getvalue() == 'bad-byte-\\xff\r\n'


def test_successful_recipe_reemits_stderr_and_streams_stdout(fakeland, capfd):
    makefile = _PREAMBLE + '''\
$(VAR_DIR)/build_me.parquet:
\t@echo healthy-stdout
\t@echo healthy-stderr >&2
\tpython build_me.py
'''
    country = fakeland.build(makefile=makefile, country_script=_COUNTRY_SCRIPT)
    assert country.build_me()['x'].tolist() == [7.0]
    output = capfd.readouterr()
    assert output.out.count('healthy-stdout') == 1
    assert output.err.count('healthy-stderr') == 1
