"""The opt-in named resolver does not change custom-rung defaults."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(os.name != 'posix', reason='POSIX C text locale required')


def _child(tmp_path, script, *, utf8=False, alt=None):
    env = dict(PATH=os.defpath, LC_ALL='C', PYTHONCOERCECLOCALE='0',
               PYTHONUTF8='1' if utf8 else '0', LSMS_SKIP_AUTH='1',
               PYTHONPATH=str(REPO), PYTHONDONTWRITEBYTECODE='1',
               AWS_EC2_METADATA_DISABLED='true', OPENBLAS_NUM_THREADS='1',
               OMP_NUM_THREADS='1')
    for name, var in [('home', 'HOME'), ('config', 'XDG_CONFIG_HOME'),
                      ('cache', 'XDG_CACHE_HOME'), ('data', 'LSMS_DATA_DIR'),
                      ('xdg', 'XDG_DATA_HOME'), ('tmp', 'TMPDIR'),
                      ('dvc', 'DVC_SITE_CACHE_DIR')]:
        path = tmp_path / name
        path.mkdir(exist_ok=True)
        env[var] = str(path)
    if alt is not None:
        env['LSMS_COUNTRIES_ROOT'] = str(alt)
    prelude = """
import codecs, json, locale, sys
from pathlib import Path
assert codecs.lookup(locale.getpreferredencoding(False)).name == sys.argv[2]
import lsms_library as ll
from lsms_library import local_tools as tools
assert ll.SKIP_AUTH and Path(ll.__file__).resolve() == Path(sys.argv[1]) / 'lsms_library/__init__.py'
legacy = tools.get_categorical_mapping
opt = getattr(tools, '_get_packaged_categorical_mapping', legacy)
def run(resolve, name, dirs, codec=None):
    return resolve(tablename=name, dirs=list(map(str, dirs)), idxvars='Original Label',
                   **({} if codec is None else {'encoding': codec}),
                   **{'Preferred Label': 'Preferred Label'})
"""
    p = subprocess.run([sys.executable, '-Bs', '-c', prelude + script,
                        str(REPO), 'utf-8' if utf8 else 'ascii'],
                       env=env, cwd=tmp_path, capture_output=True)
    assert p.returncode == 0, p.stderr.decode('utf-8', errors='replace')
    return json.loads(p.stdout)


def test_named_opt_in_restores_shipped_original_label_maps(tmp_path):
    script = """
rung = Path(sys.argv[1]) / 'lsms_library/countries/Niger/_'
out = [list(run(opt, n, [rung]).items()) for n in ('harmonize_food', 'u')]
if sys.argv[2] == 'ascii':
    try:
        run(legacy, 'u', [rung])
    except UnicodeDecodeError as e:
        assert e.encoding == 'ascii'
    else:
        raise AssertionError('legacy named call unexpectedly changed codec')
print(json.dumps(out))
"""
    assert _child(tmp_path, script) == _child(tmp_path, script, utf8=True)
    org = REPO / 'lsms_library/countries/Niger/_/categorical_mapping.org'
    alt = tmp_path / 'alternate'
    rung = alt / 'Niger/_'
    rung.mkdir(parents=True)
    (rung / org.name).write_bytes(org.read_bytes())
    assert _child(tmp_path, """
print(json.dumps(len(run(opt, 'u', [Path('alternate/Niger/_')])) > 0))
""", alt=alt)


def test_named_opt_in_preserves_custom_and_explicit_codec(tmp_path):
    rung = tmp_path / 'alternate/Niger/_'
    rung.mkdir(parents=True)
    (rung / 'categorical_mapping.org').write_text(
        '#+name: u\n| Original Label | Preferred Label |\n| custom | Modifié |\n',
        encoding='utf-8')
    result = _child(tmp_path, """
rung = Path('alternate/Niger/_')
try:
    run(opt, 'u', [rung])
except UnicodeDecodeError as e:
    assert e.encoding == 'ascii'
else:
    raise AssertionError('custom override was silently read as shipped UTF-8')
print(json.dumps(run(opt, 'u', [rung], codec='iso-8859-1')))
""", alt=tmp_path / 'alternate')
    assert result == {'custom': 'ModifiÃ©'}
