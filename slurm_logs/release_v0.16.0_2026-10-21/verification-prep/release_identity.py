"""Explicit checkout/environment identity, including every xdist worker."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys


def assert_identity():
    repo = Path(os.environ['RELEASE_REPO']).resolve()
    out = Path(os.environ['RELEASE_OUT']).resolve()
    data = Path('/local/job40255479/release-v016-seed-data')
    expected = os.environ['RELEASE_EXPECTED_SHA']
    assert len(expected) == 40
    assert Path.cwd().resolve() == repo
    assert Path(sys.executable).resolve() == Path('/local/job40255479/venv_img/bin/python').resolve()
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == expected
    assert not subprocess.check_output(['git', 'diff', 'HEAD', '--name-only'], text=True).strip()
    assert Path(os.environ['LSMS_EXPECTED_ROOT']).resolve() == repo
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == repo / 'lsms_library/countries'
    assert Path(os.environ['LSMS_DATA_DIR']).resolve() == data
    allowed = {repo, Path(__file__).resolve().parent}
    assert {Path(p).resolve() for p in os.environ['PYTHONPATH'].split(os.pathsep)} == allowed
    for key in ['OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS',
                'LSMS_BUILD_WORKERS', 'LSMS_MAKE_JOBS']:
        assert os.environ.get(key) == '1', (key, os.environ.get(key))
    makeflags = shlex.split(os.environ.get('MAKEFLAGS', ''))
    assert '-j1' in makeflags
    assert not any('jobserver' in flag for flag in makeflags)
    for key in ['LSMS_NO_CACHE', 'LSMS_SKIP_AUTH', 'PYTEST_ADDOPTS', 'PYTEST_PLUGINS',
                'LSMS_ALLOW_INTREE_SOURCES']:
        assert not os.environ.get(key), key
    assert os.environ.get('PYTHONDONTWRITEBYTECODE') == '1'
    assert set(os.sched_getaffinity(0)) == set(range(8)) | set(range(20, 28))
    import lsms_library as ll
    from lsms_library.paths import countries_root, data_root
    assert Path(ll.__file__).resolve() == repo / 'lsms_library/__init__.py'
    assert countries_root().resolve() == repo / 'lsms_library/countries'
    assert data_root().resolve() == data
    from threadpoolctl import threadpool_info
    pools = threadpool_info()
    assert all(pool['num_threads'] == 1 for pool in pools), pools
    return {'sha': expected, 'cwd': str(repo), 'package': str(Path(ll.__file__).resolve()),
            'countries_root': str(countries_root()), 'data_root': str(data_root()),
            'python': sys.executable, 'affinity': sorted(os.sched_getaffinity(0)),
            'threadpools': pools}


def pytest_configure(config):
    record = assert_identity()
    assert Path(config.rootpath).resolve() == Path(os.environ['RELEASE_REPO']).resolve()
    worker = getattr(config, 'workerinput', {}).get('workerid', 'controller')
    if worker == 'controller':
        assert config.option.numprocesses == 16
        assert config.option.dist == 'loadfile'
    else:
        # xdist.remote.setup_config deliberately resets these in each worker.
        assert config.option.numprocesses is None and config.option.dist == 'no'
        assert os.environ['PYTEST_XDIST_WORKER_COUNT'] == '16'
    assert config.getoption('timeout') == 600
    assert config.getoption('timeout_method') == 'thread'
    out = Path(os.environ['RELEASE_OUT'])
    (out / f'identity-{worker}.json').write_text(json.dumps(record, indent=2) + '\n')
