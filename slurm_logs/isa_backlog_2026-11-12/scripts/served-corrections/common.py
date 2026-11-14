"""Shared guards for the served-corrections scripts (private roots only)."""
import os
import subprocess

FORBIDDEN = ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
             os.path.expanduser('~/.local/share/lsms_library'))


def assert_env(clone):
    import lsms_library
    import lsms_library.paths as paths
    top = os.path.realpath(clone)
    got = os.path.realpath(lsms_library.__file__)
    assert got.startswith(top + os.sep), (got, top)
    priv = os.path.realpath(os.environ['PRIVATE_ROOT'])
    dr = os.path.realpath(str(paths.data_root()))
    assert dr.startswith(priv + os.sep) or dr == priv, (dr, priv)
    for bad in FORBIDDEN:
        assert not dr.startswith(os.path.realpath(bad)), dr
    cr = os.path.realpath(str(paths.countries_root()))
    assert cr.startswith(top + os.sep), (cr, top)
    head = subprocess.check_output(['git', '-C', clone, 'rev-parse', 'HEAD'], text=True).strip()
    print(f'ENV OK: lsms_library={got} data_root={dr} countries_root={cr} HEAD={head}', flush=True)
    return head
