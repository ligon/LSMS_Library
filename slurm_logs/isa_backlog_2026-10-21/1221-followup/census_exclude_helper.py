"""Decomposition control: run hash_census.py on a checkout with
``country._empty_declared_frame`` added to ``_EXCLUDED_CALLABLES`` at runtime.

On pre-#1221 60aa81e9e this isolates the one closure part the branch drops
(the helper's own source text) from every other difference.

Usage: python census_exclude_helper.py <checkout> <out.json>
"""
import os
import runpy
import sys

from lsms_library import _build_registry as R

R._EXCLUDED_CALLABLES = R._EXCLUDED_CALLABLES | {"lsms_library.country._empty_declared_frame"}
R.clear_caches()
checkout, out = sys.argv[1:3]
sys.argv = ["hash_census.py", checkout, out]
runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "hash_census.py"),
               run_name="__main__")
