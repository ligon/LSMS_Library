"""Shared guards for the served-corrections-final2 scripts.  Private roots only."""
import importlib.util
import os
import sys

FORBIDDEN = ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
             os.path.expanduser('~/.local/share/lsms_library'))
RESULTS = os.environ['RESULTS']
assert RESULTS.endswith('/served-corrections-final2/results'), RESULTS


def guard():
    """Assert the import identity and a private data root; return (clone, data_root)."""
    import lsms_library
    from lsms_library.paths import countries_root, data_root
    clone = os.path.realpath(os.environ['LSMS_CLONE'])
    got = os.path.realpath(lsms_library.__file__)
    assert got.startswith(clone + os.sep), (got, clone)
    want = os.path.realpath(os.environ['LSMS_DATA_DIR'])
    dr = os.path.realpath(str(data_root()))
    assert dr == want or dr.startswith(want + os.sep), (dr, want)
    assert dr.startswith('/local/job'), dr
    for bad in FORBIDDEN:
        assert not dr.startswith(os.path.realpath(bad)), dr
    cr = os.path.realpath(str(countries_root()))
    assert cr.startswith(clone + os.sep), (cr, clone)
    assert 'LSMS_SKIP_AUTH' not in os.environ
    print(f'GUARD OK lsms_library={got} data_root={dr} countries_root={cr}', flush=True)
    return clone, dr


def load_wave_module(country, wave, name='mapping.py'):
    """A wave's module, loaded by path as the framework loads it."""
    from lsms_library.paths import countries_root
    path = countries_root() / country / wave / '_' / name
    spec = importlib.util.spec_from_file_location(f'_s2_{country}_{wave}_{path.stem}'.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(countries_root() / country / '_'))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module
