"""Shared guards for the GH #587 red-team scripts (read-only, private roots)."""
import json
import os
import subprocess
import traceback
import warnings
from pathlib import Path

BAD_ROOTS = ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
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
    for bad in BAD_ROOTS:
        assert not dr.startswith(os.path.realpath(bad)), dr
    cr = os.path.realpath(str(paths.countries_root()))
    assert cr.startswith(top + os.sep), (cr, top)
    for k in ('LSMS_SKIP_AUTH', 'LSMS_NO_CACHE', 'LSMS_COUNTRIES_ROOT', 'LSMS_BUILD_BACKEND'):
        assert k not in os.environ, k
    head = subprocess.check_output(['git', '-C', clone, 'rev-parse', 'HEAD'], text=True).strip()
    print(f'ENV OK: lsms_library={got} data_root={dr} countries_root={cr} HEAD={head}', flush=True)
    return head


class Recorder:
    """Write a JSON record incrementally; every step wrapped in try/except."""

    def __init__(self, path):
        self.path = Path(path)
        self.data = {}

    def step(self, name, fn, store=True):
        rec = {}
        res = None
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            try:
                res = fn()
                rec['result'] = res if store else f'<{type(res).__name__} not stored>'
            except Exception as e:  # noqa: BLE001
                rec['error'] = f'{type(e).__name__}: {e}'[:2000]
                rec['traceback'] = traceback.format_exc()[-4000:]
            cats = {}
            for x in w:
                cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
            rec['warnings'] = cats
            rec['warning_msgs'] = sorted({f'{x.category.__name__}: {str(x.message)[:400]}' for x in w})[:60]
        self.data[name] = rec
        self.flush()
        print(name, 'ERROR' if 'error' in rec else 'ok', rec.get('error', '')[:300], rec['warnings'], flush=True)
        return res

    def flush(self):
        self.path.write_text(json.dumps(self.data, indent=1, default=str))
