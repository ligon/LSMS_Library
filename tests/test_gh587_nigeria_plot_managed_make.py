"""Actual Nigeria Make graph, invented marker producers; no LSMS/data IO."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = ROOT / 'lsms_library/countries/Nigeria/_/Makefile'
FOLDERS = ('2010-11', '2012-13', '2015-16', '2018-19', '2023-24')

# Same capability convention as the existing native Make regressions.
pytestmark = pytest.mark.skipif(shutil.which('make') is None,
                                reason='GNU make not on PATH')


@pytest.fixture
def graph(tmp_path):
    country = tmp_path / 'config/Nigeria'
    cwd = country / '_'
    cwd.mkdir(parents=True)
    shutil.copyfile(MAKEFILE, cwd / 'Makefile')
    cache = tmp_path / 'cache'
    events = tmp_path / 'events.jsonl'
    for folder in FOLDERS:
        wave = country / folder / '_'
        wave.mkdir(parents=True)
        (wave / 'plot_features.py').write_text(
            'from pathlib import Path\nimport os,json\n'
            'folder=Path(__file__).resolve().parents[1].name\n'
            'assert Path.cwd()==Path(__file__).resolve().parent\n'
            'out=Path(os.environ["LSMS_DATA_ROOT"])/"Nigeria"/folder/"_"/"plot_features.parquet"\n'
            'out.parent.mkdir(parents=True,exist_ok=True)\n'
            'with open(os.environ["INVENTED_EVENTS"],"a") as stream:stream.write(json.dumps({"wave":folder})+"\\n")\n'
            'out.write_text(json.dumps({"folder":folder,"origin":"managed-producer"}))\n',
            encoding='utf-8')
    (cwd / 'plot_features.py').write_text(
        'from pathlib import Path\nimport os,json\n'
        f'folders={FOLDERS!r}\n'
        'root=Path(os.environ["LSMS_DATA_ROOT"])/"Nigeria"\n'
        'parts=[json.loads((root/f/"_"/"plot_features.parquet").read_text()) for f in folders]\n'
        'assert [p["folder"] for p in parts]==list(folders)\n'
        'out=root/"var/plot_features.parquet"\n'
        'out.parent.mkdir(parents=True,exist_ok=True)\n'
        'out.write_text(json.dumps(parts))\n'
        'with open(os.environ["INVENTED_EVENTS"],"a") as stream:stream.write(json.dumps({"country":True})+"\\n")\n',
        encoding='utf-8')
    return {'country': country, 'cwd': cwd, 'cache': cache, 'events': events}


def output(g, folder):
    return g['cache'] / 'Nigeria' / folder / '_/plot_features.parquet'


def prepared(g):
    for folder in FOLDERS:
        p = output(g, folder)
        p.parent.mkdir(parents=True)
        p.write_text(json.dumps({'folder': folder, 'origin': 'already-prepared'}), encoding='utf-8')


def run(g, target=None):
    target = target or str(g['cache'] / 'Nigeria/var/plot_features.parquet')
    env = dict(os.environ, LSMS_DATA_ROOT=str(g['cache']), INVENTED_EVENTS=str(g['events']),
               PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'],
               MAKEFLAGS='-j1')
    return subprocess.run(['make', '-s', '-j1', f'LSMS_DATA_ROOT={g["cache"]}', target],
                          cwd=g['cwd'], env=env, text=True, capture_output=True, timeout=30)


def events(g):
    return [json.loads(line) for line in g['events'].read_text().splitlines()] if g['events'].exists() else []


def test_country_target_reuses_all_prepared_managed_inputs(graph):
    prepared(graph)
    before = {f: (output(graph, f).read_bytes(), output(graph, f).stat().st_mtime_ns) for f in FOLDERS}
    result = run(graph)
    assert result.returncode == 0, result.stderr
    assert events(graph) == [{'country': True}]
    result_frame = json.loads((graph['cache'] / 'Nigeria/var/plot_features.parquet').read_text())
    assert result_frame == [{'folder': f, 'origin': 'already-prepared'} for f in FOLDERS]
    assert before == {f: (output(graph, f).read_bytes(), output(graph, f).stat().st_mtime_ns) for f in FOLDERS}
    assert not list(graph['country'].glob('*/_/plot_features.parquet'))


def test_cold_country_target_builds_each_physical_wave_once(graph):
    result = run(graph)
    assert result.returncode == 0, result.stderr
    assert sorted(e['wave'] for e in events(graph) if 'wave' in e) == sorted(FOLDERS)
    assert events(graph)[-1] == {'country': True}
    assert len(events(graph)) == len(FOLDERS) + 1
    assert all(output(graph, f).is_file() for f in FOLDERS)
    assert not list(graph['country'].glob('*/_/plot_features.parquet'))


@pytest.mark.parametrize('already_prepared', [False, True])
def test_legacy_alias_uses_one_managed_producer_and_reuses_it(graph, already_prepared):
    if already_prepared:
        prepared(graph)
    target = '../2010-11/_/plot_features.parquet'
    for _ in range(2):
        result = run(graph, target)
        assert result.returncode == 0, result.stderr
    assert events(graph) == ([] if already_prepared else [{'wave': '2010-11'}])
    assert output(graph, '2010-11').is_file()
    assert not (graph['country'] / '2010-11/_/plot_features.parquet').exists()


@pytest.mark.parametrize('reason', ['missing', 'source-newer'])
def test_country_regenerates_only_the_needed_physical_native(graph, reason):
    prepared(graph)
    if reason == 'missing':
        output(graph, '2012-13').unlink()
    else:
        source = graph['country'] / '2012-13/_/plot_features.py'
        older = source.stat().st_mtime - 10
        os.utime(output(graph, '2012-13'), (older, older))
    result = run(graph)
    assert result.returncode == 0, result.stderr
    assert events(graph) == [{'wave': '2012-13'}, {'country': True}]
    parts = json.loads((graph['cache'] / 'Nigeria/var/plot_features.parquet').read_text())
    assert [p['folder'] for p in parts] == list(FOLDERS)
    assert [p['origin'] for p in parts] == ['managed-producer' if f == '2012-13' else 'already-prepared' for f in FOLDERS]


def test_failed_native_prerequisite_is_terminal_before_country_assembly(graph):
    prepared(graph)
    output(graph, '2015-16').unlink()
    (graph['country'] / '2015-16/_/plot_features.py').write_text('raise SystemExit(7)\n', encoding='utf-8')
    result = run(graph)
    assert result.returncode != 0
    assert 'Error 7' in result.stderr
    assert not (graph['cache'] / 'Nigeria/var/plot_features.parquet').exists()
    assert events(graph) == []
