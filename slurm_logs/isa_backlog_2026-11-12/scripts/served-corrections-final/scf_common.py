"""Shared guards for the served-corrections-final scripts.  Private roots only."""
import importlib.util
import os
import sys

FORBIDDEN = ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
             os.path.expanduser('~/.local/share/lsms_library'))
RESULTS = '/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections-final/results'


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
    for bad in FORBIDDEN:
        assert not dr.startswith(os.path.realpath(bad)), dr
    cr = os.path.realpath(str(countries_root()))
    assert cr.startswith(clone + os.sep), (cr, clone)
    print(f'GUARD OK lsms_library={got} data_root={dr} countries_root={cr}', flush=True)
    return clone, dr


def load_wave_module(country, wave, name='mapping.py'):
    """A wave's module, loaded by path as the framework loads it."""
    from lsms_library.paths import countries_root
    path = countries_root() / country / wave / '_' / name
    spec = importlib.util.spec_from_file_location(f'_scf_{country}_{wave}_{path.stem}'.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(countries_root() / country / '_'))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def num(s):
    import numpy as np
    import pandas as pd
    return pd.to_numeric(pd.Series(np.ravel(s)), errors='coerce').to_numpy(dtype=float)
