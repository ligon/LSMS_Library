"""Stdlib private Make-command fixture; no project imports or survey IO.

The exact production Makefile and make_path function are read as ordinary source.
An invented CLI discovers inert metadata at execution time. This exercises GNU
Make expansion/recipes, not the actual Country/Wave producer implementation.
"""
from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys


TABLES = ('crop_production', 'plot_inputs', 'livestock', 'plot_labor', 'people_last7days')
WAVES = ('2011-12', '2014-15', '2018-19', '2021-22')
PROJECT = Path(__file__).resolve().parents[1]

# This is an invented stdlib command. Never exec/import the argv source script.
DISPATCH = r'''
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(os.environ['NIGER_TEST_ROOT'])
build, data = root / 'Niger/_', root / 'data'
log, ran = root / 'events.jsonl', root / 'ran.log'

def event(stage, **fields):
    with log.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps({'stage': stage, **fields}, sort_keys=True) + '\n')

def observe_cwd():
    with ran.open('a', encoding='utf-8') as stream:
        stream.write(str(Path.cwd()) + '\n')

def marker(folder, table):
    path = data / 'Niger' / folder / '_' / (table + '.parquet')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'')

def discover(table):
    registry = build / 'data_scheme.yml'
    scheme = json.loads(registry.read_text(encoding='utf-8'))
    dependencies, members = [registry], []
    for year in sorted(scheme['Waves']):
        folder = scheme.get('wave_folder_map', {}).get(year, year)
        directory = root / 'Niger' / folder / '_'
        source = directory / (table + '.py')
        info = directory / 'data_info.yml'
        for name in (info, directory / 'mapping.py', directory / (folder + '.py')):
            if name.is_file():
                dependencies.append(name)
        metadata = json.loads(info.read_text(encoding='utf-8')) if info.exists() else {}
        if source.is_file():
            dependencies.append(source)
            members.append((folder, 'native'))
        elif table in metadata:
            if not isinstance(metadata[table], dict) or not metadata[table]:
                raise ValueError('invented declaration lacks executable producer')
            members.append((folder, 'yaml'))
    return list(dict.fromkeys(map(str, dependencies))), list(dict.fromkeys(members))

args = sys.argv[1:]
try:
    if args[0] == '--native':
        table, folder = args[1:]
        event('native', table=table, folder=folder)
        observe_cwd()
        if folder == os.environ.get('NIGER_TEST_FAIL'):
            print('injected wave failure', file=sys.stderr)
            raise SystemExit(7)
        marker(folder, table)
    elif args[0] == 'wave_producers.py':
        dependencies_only = args[1] == '--dependencies'
        table = args[2] if dependencies_only else args[1]
        dependencies, members = discover(table)
        if dependencies_only:
            names = [table + '.py', 'niger.py', 'wave_producers.py', 'data_scheme.yml']
            if table in ('crop_production', 'plot_inputs', 'livestock'):
                names.append('categorical_mapping.org')
            static = [build / name for name in names]
            for path in static:
                if not path.is_file():
                    raise FileNotFoundError('required Make input missing: ' + str(path))
            dependencies = list(dict.fromkeys(map(str, static + list(map(Path, dependencies)))))
            # Complete representation before reporting a successful discovery.
            rendered = ' '.join(make_path(name) for name in dependencies)
            event('dependencies', table=table, members=members)
            print(rendered)
        else:
            event('produce', table=table)
            for folder, mechanism in members:
                if mechanism == 'native':
                    subprocess.run([sys.executable, '-I', '-B', '-S', __file__, '--native', table, folder],
                                   cwd=root / 'Niger' / folder / '_', check=True, timeout=5)
                else:
                    event('yaml', table=table, folder=folder)
                    marker(folder, table)
    elif args[0].endswith('.py') and Path(args[0]).stem in (
            'crop_production', 'plot_inputs', 'livestock', 'plot_labor', 'people_last7days'):
        event('compiler', table=Path(args[0]).stem)
        observe_cwd()
        path = Path(os.environ['NIGER_TEST_TARGET'])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'')
    else:
        raise ValueError('unapproved invented command')
except Exception as error:
    event('failure', kind=type(error).__name__)
    print('injected discovery/producer failure: ' + str(error), file=sys.stderr)
    raise SystemExit(9)
'''


class NigerMake:
    """A fresh owned invented tree; real Make, fixed inert producer/driver CLI."""
    def __init__(self, tmp_path, table, makefile, *, waves=WAVES):
        self.root, self.table = tmp_path, table
        self.build = tmp_path / 'Niger/_'
        self.build.mkdir(parents=True)
        self.data = tmp_path / 'data'
        self.var = self.data / 'Niger/var'
        self.target = self.var / (table + '.parquet')
        self.make = shutil.which('make')
        if self.make is None:
            import pytest
            pytest.skip('GNU make required for synthetic Make integration')
        shutil.copyfile(makefile, self.build / 'Makefile')
        self.makefile_bytes = (self.build / 'Makefile').read_bytes()
        for name in ('niger.py', 'categorical_mapping.org', 'wave_producers.py') + tuple(t + '.py' for t in TABLES):
            (self.build / name).write_text('# inert private metadata; never executed\n')
        producer_source = (PROJECT / 'lsms_library/countries/Niger/_/wave_producers.py').read_text()
        node = next(n for n in ast.parse(producer_source).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'make_path')
        make_path_source = '\n'.join(producer_source.splitlines()[node.lineno - 1:node.end_lineno])
        self.dispatch = tmp_path / 'invented_command.py'
        self.dispatch.write_text(make_path_source + '\n' + DISPATCH, encoding='utf-8')
        self.fail = ''
        self.install(waves=waves)
        for path in tmp_path.rglob('*'):
            if path.is_file():
                os.utime(path, (1000, 1000))

    def install(self, *, waves=WAVES, mechanisms=None, folders=None):
        mechanisms, folders = mechanisms or {}, folders or {}
        registry = {'Country': 'Niger', 'Waves': list(waves), 'wave_folder_map': folders,
                    'Data Scheme': {self.table: {'index': '(t, i)'}}}
        (self.build / 'data_scheme.yml').write_text(json.dumps(registry))
        for year in waves:
            folder = folders.get(year, year)
            directory = self.root / 'Niger' / folder / '_'
            directory.mkdir(parents=True, exist_ok=True)
            kind = mechanisms.get(year, 'native')
            script = directory / (self.table + '.py')
            info = directory / 'data_info.yml'
            if script.exists():
                script.unlink()
            if info.exists():
                info.unlink()
            if kind in ('native', 'both'):
                script.write_text('raise AssertionError("inert metadata must never execute")\n')
            if kind in ('yaml', 'both', 'null'):
                info.write_text(json.dumps({self.table: None if kind == 'null' else {'file': 'invented-never-read.dta'}}))

    def events(self):
        path = self.root / 'events.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def run(self, *, goal=None, extra=(), relative=False):
        env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'PYTHONDONTWRITEBYTECODE': '1',
               'NIGER_TEST_ROOT': str(self.root), 'NIGER_TEST_TARGET': str(self.target),
               'NIGER_TEST_FAIL': self.fail}
        for key, name in [('HOME', 'home'), ('TMPDIR', 'tmp'), ('XDG_CONFIG_HOME', 'config'), ('XDG_CACHE_HOME', 'cache')]:
            path = self.root / 'private' / name
            path.mkdir(parents=True, exist_ok=True)
            env[key] = str(path)
        variable = os.path.relpath(self.var, self.build) if relative else str(self.var)
        target = os.path.relpath(self.target, self.build) if relative else str(self.target)
        python = ' '.join(map(shlex.quote, [sys.executable, '-I', '-B', '-S', str(self.dispatch)]))
        command = [self.make, '-s', '-f', 'Makefile', 'VAR_DIR=' + variable,
                   'LSMS_DATA_ROOT=' + str(self.data), 'PYTHON=' + python, *map(str, extra), str(goal or target)]
        result = subprocess.run(command, cwd=self.build, env=env, capture_output=True,
                                text=True, timeout=20)
        assert (self.build / 'Makefile').read_bytes() == self.makefile_bytes
        return result
