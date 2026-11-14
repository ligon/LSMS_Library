"""Shared guards for the sold-kg (GH #1560/#1571) red-team scripts.

Read-only.  Private roots only.  Every script calls assert_env() first.
"""
import json
import os
import subprocess
import sys
import warnings

SHARED_BAD = ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
              os.path.expanduser('~/.local/share/lsms_library'))


def assert_env(clone):
    import lsms_library
    import lsms_library.paths as paths
    top = os.path.realpath(clone)
    got = os.path.realpath(lsms_library.__file__)
    if not got.startswith(top + os.sep):
        sys.exit(f'IMPORT IDENTITY FAIL: {got} not under {top}')
    priv = os.path.realpath(os.environ['PRIVATE_ROOT'])
    dr = os.path.realpath(str(paths.data_root()))
    if not (dr == priv or dr.startswith(priv + os.sep)):
        sys.exit(f'DATA ROOT NOT PRIVATE: {dr} vs {priv}')
    for bad in SHARED_BAD:
        if dr.startswith(os.path.realpath(bad)):
            sys.exit(f'DATA ROOT IS SHARED: {dr}')
    cr = os.path.realpath(str(paths.countries_root()))
    if not cr.startswith(top + os.sep):
        sys.exit(f'COUNTRIES ROOT NOT CLONE: {cr}')
    for v in ('LSMS_SKIP_AUTH', 'LSMS_NO_CACHE', 'LSMS_COUNTRIES_ROOT'):
        if os.environ.get(v):
            sys.exit(f'{v} is set')
    head = subprocess.check_output(['git', '-C', clone, 'rev-parse', 'HEAD'], text=True).strip()
    print(f'ENV OK: lsms_library={got} data_root={dr} countries_root={cr} HEAD={head} '
          f'backend={os.environ.get("LSMS_BUILD_BACKEND")}', flush=True)
    return head


def warn_summary(caught):
    """Category -> count; no message text (messages can name households)."""
    out = {}
    for w in caught:
        k = w.category.__name__
        out[k] = out.get(k, 0) + 1
    return out


def dump(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=1, default=str, sort_keys=True)
