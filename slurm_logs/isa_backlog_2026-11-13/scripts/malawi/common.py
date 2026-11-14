"""Shared guards for the malawi stream scripts (private roots only)."""
import os
import subprocess


def assert_env(clone):
    import lsms_library
    import lsms_library.paths as paths
    top = os.path.realpath(clone)
    got = os.path.realpath(lsms_library.__file__)
    assert got.startswith(top + os.sep), (got, top)
    dr = os.path.realpath(str(paths.data_root()))
    assert dr.startswith('/local/job') or '/isa-sm-20261113/private/' in dr, dr
    for bad in ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
                os.path.expanduser('~/.local/share/lsms_library')):
        assert not dr.startswith(os.path.realpath(bad)), dr
    cr = os.path.realpath(str(paths.countries_root()))
    assert cr.startswith(top + os.sep), (cr, top)
    head = subprocess.check_output(['git', '-C', clone, 'rev-parse', 'HEAD'], text=True).strip()
    print(f'ENV OK: lsms_library={got} data_root={dr} countries_root={cr} HEAD={head}', flush=True)
    return head
