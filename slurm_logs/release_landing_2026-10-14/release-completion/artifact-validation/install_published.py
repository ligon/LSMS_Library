"""Install the actual pinned public PyPI release into the prepared clean venv."""
from pathlib import Path
import json
import os
import subprocess

out = Path(__file__).resolve().parent
python = '/local/job39919092/release-work/published-install-env/bin/python'
env = {k: v for k, v in os.environ.items()
       if not k.startswith(('AWS_', 'LSMS_', 'MICRODATA_'))
       and k not in {'PYTHONPATH', 'PYTHONSAFEPATH'}}
probe = subprocess.run([python, '-c', 'import importlib.util; assert importlib.util.find_spec("lsms_library") is None'], cwd=out, env=env, check=True)
command = [python, '-m', 'pip', '--isolated', 'install', '--index-url', 'https://pypi.org/simple',
           '--no-cache-dir', '--report', str(out/'pip-install-report.json'), 'LSMS_Library==0.15.0']
with (out/'pip-install.log').open('x') as log:
    result = subprocess.run(command, cwd=out, env=env, stdout=log, stderr=subprocess.STDOUT)
assert result.returncode == 0, 'Inspect private pip log'
report = json.loads((out/'pip-install-report.json').read_text())
records = [r for r in report['install'] if r['metadata']['name'].lower().replace('-', '_') == 'lsms_library']
assert len(records) == 1
r = records[0]
assert r['metadata']['version'] == '0.15.0'
a = json.loads((out/'published-artifacts.json').read_text())
w = next(x for x in a['artifacts'] if x['packagetype'] == 'bdist_wheel')
assert r['download_info']['url'] == w['url']
assert r['download_info']['archive_info']['hashes']['sha256'] == w['sha256']
(out/'installation.json').write_text(json.dumps({'version':'0.15.0', 'wheel_url':w['url'],
 'wheel_sha256':w['sha256'], 'interpreter':python, 'installed_from_public_pypi':True,
 'fresh_environment_without_prior_lsms':True, 'dependencies_installed':len(report['install'])-1}, indent=2)+'\n')
print('Installed the exact verified PyPI wheel and its dependencies into the clean environment.')
