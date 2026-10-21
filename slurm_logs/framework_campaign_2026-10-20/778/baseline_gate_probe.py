"""Run current race tests with ONLY the old gate bound from frozen852.

No source file is changed. This is a reproducible negative control, not a
candidate test run: selected race regressions must fail on the old function.
"""
import ast
import os
from pathlib import Path
import subprocess
import textwrap

import lsms_library as ll
import lsms_library.country as C
import pytest

top = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == top / 'lsms_library/countries'
source = subprocess.check_output(['git', 'show', '852ac2bfc:lsms_library/country.py'], text=True)
node = next(node for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.FunctionDef) and node.name == '_read_assumed_fresh_cache')
namespace = {}
exec(compile(textwrap.dedent(ast.get_source_segment(source, node)), '<frozen852 gate>', 'exec'), C.__dict__, namespace)
original = C.Country._read_assumed_fresh_cache
C.Country._read_assumed_fresh_cache = namespace['_read_assumed_fresh_cache']
try:
    code = pytest.main(['tests/test_gh1040_wave_read_gates.py', '-q', '-k',
                        'site3_disappear or site3_other_data or site3_missing_file_error'])
finally:
    C.Country._read_assumed_fresh_cache = original
raise SystemExit(int(code))
