"""Locale behavior of the package-owned categorical level graph."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX C text locale required')


def _run(tmp_path, statements, *, ascii_mode=True, alternate=None):
    env = dict(PATH=os.defpath, LC_ALL='C', PYTHONCOERCECLOCALE='0',
               PYTHONUTF8='0' if ascii_mode else '1', LSMS_SKIP_AUTH='1',
               PYTHONPATH=str(REPO), PYTHONDONTWRITEBYTECODE='1',
               AWS_EC2_METADATA_DISABLED='true',
               OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
    for name, key in (
        ('home', 'HOME'), ('config', 'XDG_CONFIG_HOME'), ('data', 'LSMS_DATA_DIR'),
        ('xdg-data', 'XDG_DATA_HOME'), ('cache', 'XDG_CACHE_HOME'),
        ('site', 'DVC_SITE_CACHE_DIR'), ('tmp', 'TMPDIR')):
        folder = tmp_path / name
        folder.mkdir(parents=True, exist_ok=True)
        env[key] = str(folder)
    if alternate is not None:
        env['LSMS_COUNTRIES_ROOT'] = str(alternate)
    script = """
import codecs, json, locale, pathlib, sys
assert codecs.lookup(locale.getpreferredencoding(False)).name == sys.argv[2]
import lsms_library as ll
assert pathlib.Path(ll.__file__).resolve() == pathlib.Path(sys.argv[1]) / 'lsms_library/__init__.py'
""" + statements
    result = subprocess.run(
        [sys.executable, '-Bs', '-c', script, str(REPO),
         'ascii' if ascii_mode else 'utf-8'],
        env=env, cwd=tmp_path, capture_output=True)
    assert result.returncode == 0, result.stderr.decode('utf-8', errors='replace')
    return json.loads(result.stdout)


def test_stock_country_and_physical_wave_views_have_same_keys_in_ascii(tmp_path):
    statements = """
names = ('Burkina_Faso', 'Ethiopia', 'Malawi', 'Mali', 'Niger',
         'Nigeria', 'Tanzania', 'Uganda')
out = {}
for name in names:
    c = ll.Country(name, preload_panel_ids=False)
    assert len(c.categorical_mapping['ehcvm_units']) == 175
    out[name] = [[k, len(v)] for k, v in c.categorical_mapping.items()]
    assert c.waves, name
    out[name + '-waves'] = {
        w: [c[w].wave_folder, [[k, len(v)] for k, v in c[w].categorical_mapping.items()]]
        for w in c.waves}
print(json.dumps(out))
"""
    expected = _run(tmp_path, statements, ascii_mode=False)
    assert _run(tmp_path, statements) == expected


def test_matching_external_copy_but_not_modified_file_uses_package_codec(tmp_path):
    src = REPO / 'lsms_library/countries/Mali/_'
    alt = tmp_path / 'alternate'
    local = alt / 'Mali/_'
    local.mkdir(parents=True)
    for f in ('data_scheme.yml', 'categorical_mapping.org'):
        (local / f).write_bytes((src / f).read_bytes())
    assert _run(tmp_path, """
print(json.dumps('harmonize_food' in ll.Country('Mali').categorical_mapping))
""", alternate=alt)
    (local / 'categorical_mapping.org').write_bytes(
        b'#+name: custom\n| Code | Preferred Label |\n| 1 | Caf\xe9 |\n')
    assert _run(tmp_path, """
try:
    ll.Country('Mali').categorical_mapping
except UnicodeDecodeError as exc:
    print(json.dumps(exc.encoding == 'ascii'))
else:
    raise AssertionError('custom level inherited the package codec')
""", alternate=alt)
