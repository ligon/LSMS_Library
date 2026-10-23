"""Run hash_census.py with pandas' ``mode.string_storage`` set BEFORE the
library is imported (``country._SCHEME_DTYPE_MAP`` snapshots
``pd.StringDtype()`` at import).

Usage: python census_string_storage.py <storage> <checkout> <out.json>
"""
import os
import runpy
import sys

import pandas as pd

storage, checkout, out = sys.argv[1:4]
pd.set_option("mode.string_storage", storage)
assert pd.StringDtype().storage == storage
sys.argv = ["hash_census.py", checkout, out]
runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "hash_census.py"),
               run_name="__main__")
from lsms_library.country import _SCHEME_DTYPE_MAP  # noqa: E402
assert _SCHEME_DTYPE_MAP["str"].storage == storage, _SCHEME_DTYPE_MAP["str"]
print("string_storage =", storage)
