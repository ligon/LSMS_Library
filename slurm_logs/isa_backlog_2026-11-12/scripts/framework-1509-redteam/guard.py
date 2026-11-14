"""Import-identity and private-root guard.  Import FIRST in every script."""
import os
import subprocess
import sys

import lsms_library
from lsms_library import paths as _P

_top = os.environ['CHECKOUT']
_real = os.path.realpath(lsms_library.__file__)
if not _real.startswith(os.path.realpath(_top) + os.sep):
    sys.exit(f'IDENTITY FAIL: {_real} not under {_top}')
_head = subprocess.check_output(['git', '-C', _top, 'rev-parse', 'HEAD'],
                                text=True).strip()
if _head != os.environ['WANT_SHA']:
    sys.exit(f'SHA FAIL: {_head} != {os.environ["WANT_SHA"]}')
_priv = os.path.realpath(os.environ['PRIVATE_ROOT'])
_dr = os.path.realpath(str(_P.data_root()))
if not (_dr == _priv or _dr.startswith(_priv + os.sep)):
    sys.exit(f'ROOT FAIL: data_root {_dr} not under {_priv}')
for _bad in ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
             os.path.expanduser('~/.local/share/lsms_library')):
    if _dr.startswith(os.path.realpath(_bad)):
        sys.exit(f'ROOT FAIL: forbidden root {_dr}')
print(f'[guard] lsms_library={_real} head={_head} data_root={_dr}', flush=True)
