"""GH #1123, extended: Niger's country concatenators refuse a missing wave.

PR #1241 found that ``LSMS_BUILD_BACKEND=make`` could serve Niger's
``plot_inputs`` with a whole wave missing and no Python warning: the country
recipe ran the four wave scripts as one shell line joined by ``;`` (only the
last status reached make), and the country concatenator -- which is also the
framework's Python fallback after a failed ``make`` -- skipped any wave whose
parquet was absent.  #1241 fixed ``plot_inputs`` both ways (``&&``, and a
refusal).  The maintainer accepted the refusal (2026-10-23) and extended the
same pair to Niger's other four country concatenations, tested here:
``crop_production``, ``livestock``, ``plot_labor`` and ``people_last7days``.
``&&`` alone is not enough: make then fails, the framework runs the
concatenator as its fallback, and a skipping concatenator still serves the
other waves.

Per table, following #1241 (``tests/test_ehcvm_plot_inputs_identity.py``):
the real recipe with a failing ``python`` shim, and the real concatenator run
on synthetic wave parquets (refusal, positive control, and the assert that
every listed wave has a script).  Beyond #1241, the whole route -- real
Makefile, real concatenator, real ``niger.py``, synthetic wave scripts --
runs through ``Country(...)`` under the make backend, in the layout of
``tests/test_gh1091_niger_community_builder.py`` (about 100 s; ``-k 'not
make_backend'`` is the few-second subset).  No microdata is read; every
frame here is synthetic.

The runpy tests read wave parquets under ``data_root()``, where the branch
looks; on the pre-fix tree the concatenators read the in-tree relative path,
so there they fail on "no wave-level parquets found" rather than on the
defect.  The end-to-end refusal tests are the ones whose pre-fix failure IS
the defect ("DID NOT RAISE": the other waves were served).
"""
from __future__ import annotations

import os
import runpy
import shutil
import subprocess
import sys

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root, data_root


SOURCE = countries_root() / 'Niger'
WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']
# Each table's declared index (Niger/_/data_scheme.yml) and one declared column.
TABLES = {
    'crop_production': (['t', 'i', 'plot_id', 'crop', 'u'], 'Quantity'),
    'livestock': (['t', 'i', 'animal'], 'HeadCount'),
    'plot_labor': (['t', 'i', 'plot_id', 'source'], 'PersonDays'),
    'people_last7days': (['t', 'i', 'pid'], 'farm_hrs'),
}
NAMES = list(TABLES)


def _frame(table, wave, n=1):
    index, column = TABLES[table]
    data = {lvl: [f'{lvl}{k}' for k in range(n)] for lvl in index}
    data['t'] = [wave] * n
    data[column] = [float(k + 1) for k in range(n)]
    return pd.DataFrame(data).set_index(index)


# ---------------------------------------------------------------------------
# 1. The recipe: a failed wave script must fail make, and nothing may run after
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('failing', ['2011-12', '2018-19'])
@pytest.mark.parametrize('table', NAMES)
def test_make_recipe_stops_when_any_wave_script_fails(tmp_path, table, failing):
    """Run the real recipe with a `python` shim that fails in one wave."""
    if shutil.which('make') is None:                          # pragma: no cover
        pytest.skip('make not available')
    shim = tmp_path / 'bin' / 'python'
    shim.parent.mkdir()
    log = tmp_path / 'ran.log'
    shim.write_text('#!/bin/sh\n'
                    f'echo "$PWD" >> {log}\n'
                    f'case "$PWD" in */{failing}/_) exit 1;; esac\n'
                    'exit 0\n')
    shim.chmod(0o755)
    env = dict(os.environ, PATH=f'{shim.parent}:{os.environ["PATH"]}')
    target = tmp_path / 'var' / f'{table}.parquet'
    proc = subprocess.run(['make', '-s', str(target), f'VAR_DIR={target.parent}'],
                          cwd=SOURCE / '_', env=env, capture_output=True, text=True)
    rc = proc.returncode
    assert rc != 0, f'{table}: a failed {failing} wave script must fail the recipe'
    ran = log.read_text().split()
    assert ran[-1].endswith(f'/{failing}/_'), f'{table}: something ran after the failed wave'
    n_concat = sum(r.endswith('/Niger/_') for r in ran)
    assert n_concat == 0, f'{table}: the concatenator ran {n_concat} time(s)'


# ---------------------------------------------------------------------------
# 2. The concatenator: refuse a missing wave parquet (it is also the fallback)
# ---------------------------------------------------------------------------

def _run_concatenator(monkeypatch, tmp_path, table, waves_present):
    root = tmp_path / 'data'
    for t in waves_present:
        d = root / 'Niger' / t / '_'
        d.mkdir(parents=True)
        _frame(table, t).to_parquet(d / f'{table}.parquet')
    written = []
    monkeypatch.setattr(local_tools, 'data_root',
                        lambda country=None: root / country if country else root)
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda fn, **k: pd.read_parquet(fn))
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, *a, **k: written.append(frame.copy()))
    # The concatenators import the shared refusal from the country module;
    # runpy.run_path does not put the script's own directory on sys.path.
    monkeypatch.syspath_prepend(str(SOURCE / '_'))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)
    monkeypatch.chdir(SOURCE / '_')
    runpy.run_path(str(SOURCE / '_' / f'{table}.py'), run_name=f'__concat_{table}__')
    return written


@pytest.mark.parametrize('table', NAMES)
def test_concatenator_refuses_a_missing_wave(monkeypatch, tmp_path, table):
    present = [t for t in WAVES if t != '2018-19']
    with pytest.raises(FileNotFoundError, match=f'wave 2018-19 declares {table}'):
        _run_concatenator(monkeypatch, tmp_path, table, present)


@pytest.mark.parametrize('table', NAMES)
def test_concatenator_concatenates_every_wave(monkeypatch, tmp_path, table):
    written = _run_concatenator(monkeypatch, tmp_path, table, WAVES)
    n = len(written)
    assert n == 1, f'{n} writes'
    got = sorted(written[0].index.get_level_values('t'))
    assert got == WAVES, f'{table}: waves {got}'


@pytest.mark.parametrize('table', NAMES)
def test_concatenator_refuses_a_listed_wave_with_no_script(monkeypatch, tmp_path, table):
    """The refusal is only correct because every listed wave HAS a wave
    script: a missing parquet then means a failed build, never "not wired".
    The concatenator asserts that premise (here, in a copy of the tree with
    one wave script removed)."""
    tree = tmp_path / 'Niger'
    shutil.copytree(SOURCE / '_', tree / '_', ignore=shutil.ignore_patterns('__pycache__'))
    for t in WAVES:
        (tree / t / '_').mkdir(parents=True)
        if t != '2014-15':
            (tree / t / '_' / f'{table}.py').write_text('')
    monkeypatch.syspath_prepend(str(tree / '_'))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)
    monkeypatch.chdir(tree / '_')
    with pytest.raises(AssertionError, match=r"WAVES lists \['2014-15'\] with no wave script"):
        runpy.run_path(str(tree / '_' / f'{table}.py'), run_name=f'__unwired_{table}__')


# ---------------------------------------------------------------------------
# 3. The whole route through Country(...) under LSMS_BUILD_BACKEND=make
# ---------------------------------------------------------------------------

SCHEME = """Country: Niger
Waves: ['2011-12', '2014-15', '2018-19', '2021-22']
Data Scheme:
  {table}:
    index: ({index})
    {column}: float
    materialize: make
  panel_ids: !make
  updated_ids: !make
"""

SCRIPT = """from pathlib import Path
import pandas as pd
from lsms_library.local_tools import to_parquet
w = Path.cwd().parent.name
{fail}
df = pd.DataFrame({{lvl: [lvl + '0', lvl + '1'] for lvl in {index!r}}})
df['t'] = w
df[{column!r}] = [1.0, 2.0]
to_parquet(df.set_index({index!r}), '{table}.parquet')
"""


@pytest.fixture
def niger_tree(tmp_path, monkeypatch):
    """A countries root holding the REAL Niger Makefile, country module,
    categorical table and concatenator, with synthetic wave scripts."""
    if shutil.which('make') is None:                          # pragma: no cover
        pytest.skip('GNU make required')
    root = tmp_path / 'countries'
    build = root / 'Niger' / '_'
    build.mkdir(parents=True)
    for name in ['Makefile', 'niger.py', 'categorical_mapping.org',
                 'panel_ids.json', 'updated_ids.json']:
        shutil.copy2(SOURCE / '_' / name, build / name)
    for w in WAVES:
        (root / 'Niger' / w / '_').mkdir(parents=True)
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(root))
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('VAR_DIR', raising=False)
    monkeypatch.delenv('LSMS_DATA_ROOT', raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()

    def install(table, failing=None):
        index, column = TABLES[table]
        (build / 'data_scheme.yml').write_text(
            SCHEME.format(table=table, index=', '.join(index), column=column))
        shutil.copy2(SOURCE / '_' / f'{table}.py', build / f'{table}.py')
        for w in WAVES:
            fail = "raise SystemExit('injected wave failure')" if w == failing else ''
            (root / 'Niger' / w / '_' / f'{table}.py').write_text(
                SCRIPT.format(fail=fail, index=index, column=column, table=table))
        return Country('Niger')

    try:
        yield install
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


@pytest.mark.parametrize('table', NAMES)
def test_make_backend_refuses_instead_of_serving_the_other_waves(niger_tree, capfd, table):
    """Before: the three good waves came back, 2018-19 silently absent."""
    c = niger_tree(table, failing='2018-19')
    with pytest.warns(UserWarning, match='Python fallback failed'):
        with pytest.raises(RuntimeError, match=f'Could not materialize Niger/{table}'):
            getattr(c, table)()
    # The refusal names the wave (the concatenator's stderr, via make's fallback).
    err = capfd.readouterr().err
    assert f'wave 2018-19 declares {table}' in err, f'{table}: refusal not seen'
    # Not vacuous: the waves before the failed one did build.
    built = sorted(p.parent.parent.name for p in data_root().glob(f'Niger/*/_/{table}.parquet'))
    assert built == ['2011-12', '2014-15'], f'{table}: wave parquets {built}'
    assert not (data_root() / 'Niger' / 'var' / f'{table}.parquet').exists()


def test_make_backend_serves_every_wave_when_every_wave_builds(niger_tree):
    """Positive control for the harness (one table; the per-table runpy
    controls above cover each concatenator)."""
    c = niger_tree('crop_production')
    df = c.crop_production()
    n = len(df)
    assert n == 2 * len(WAVES), f'{n} rows'
    got = sorted(set(df.index.get_level_values('t')))
    assert got == WAVES, f'waves {got}'
