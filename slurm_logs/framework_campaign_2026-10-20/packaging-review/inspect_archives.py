"""Verify packaged runtime payload without importing or building survey data."""
from pathlib import Path
import configparser
import email
import hashlib
import json
import subprocess
import sys
import tarfile
import tomllib
import zipfile

repo=Path('/local/job40255479/worktrees/packaging-4cab5f0')
assert Path.cwd().resolve()==repo
source_sha='4cab5f0db7d375ad543e652f61f43bd28fa94ecd'
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==source_sha
out=Path('/local/job40255479/control/framework-campaign/packaging-review')
wheel=next((out/'dist').glob('*.whl'));sdist=next((out/'dist').glob('*.tar.gz'))
tracked=set(subprocess.check_output(['git','ls-files','lsms_library'],text=True).splitlines())
with zipfile.ZipFile(wheel) as z,tarfile.open(sdist) as t:
 wheel_files=set(z.namelist())
 top=t.getmembers()[0].name.split('/')[0]
 sdist_files={m.name[len(top)+1:] for m in t.getmembers() if m.isfile() or m.issym()}
 wheel_meta=email.message_from_bytes(z.read(next(p for p in wheel_files if p.endswith('.dist-info/METADATA'))))
 sdist_meta=email.message_from_bytes(t.extractfile(top+'/PKG-INFO').read())
 version=wheel_meta['Version'];assert version==sdist_meta['Version']!='0.0.0'
 assert '4cab5f0db' in version
 missing_wheel=sorted(tracked-wheel_files);missing_sdist=sorted(tracked-sdist_files)
 assert not missing_wheel and not missing_sdist
 wheel_diff=[];sdist_diff=[]
 for name in sorted(tracked):
  file=repo/name
  if file.is_symlink():continue
  source=file.read_bytes()
  if z.read(name)!=source:wheel_diff.append(name)
  if t.extractfile(top+'/'+name).read()!=source:sdist_diff.append(name)
 assert not wheel_diff and not sdist_diff
 entrypoints=configparser.ConfigParser()
 entrypoints.read_string(z.read(next(p for p in wheel_files if p.endswith('.dist-info/entry_points.txt'))).decode())
 assert entrypoints['console_scripts']['lsms-library']=='lsms_library.cli:main'
project=tomllib.loads((repo/'pyproject.toml').read_text())
placeholder=(project['project']['dynamic']==['version'] and 'version' not in project['project'] and project['tool']['poetry']['version']=='0.0.0' and project['tool']['poetry-dynamic-versioning']['enable'] is True)
assert placeholder
assert not subprocess.check_output(['git','status','--porcelain'],text=True).strip()
binary_suffixes={'.dta','.sav','.parquet','.pkl'}
raw_wheel=sorted(n for n in wheel_files if Path(n).suffix.lower() in binary_suffixes)
raw_sdist=sorted(n for n in sdist_files if Path(n).suffix.lower() in binary_suffixes)
assert not raw_wheel and not raw_sdist
result={'source_sha':source_sha,'version':version,'nonzero_dynamic_version':True,
 'wheel':{'filename':wheel.name,'bytes':wheel.stat().st_size,'sha256':hashlib.sha256(wheel.read_bytes()).hexdigest(),'files':len(wheel_files)},
 'sdist':{'filename':sdist.name,'bytes':sdist.stat().st_size,'sha256':hashlib.sha256(sdist.read_bytes()).hexdigest(),'files':len(sdist_files),'wheel_built_from_this_sdist':True},
 'tracked_package_files':len(tracked),'missing_package_files_in_wheel':missing_wheel,'missing_package_files_in_sdist':missing_sdist,
 'wheel_package_byte_differences':wheel_diff,'sdist_package_byte_differences':sdist_diff,
 'CLI_entrypoint_present':True,'dynamic_version_placeholder_restored':placeholder,'private_worktree_clean':True,
 'test_fixture_in_wheel':'tests/fixtures/uganda_baseline.json' in wheel_files,
 'test_fixture_in_sdist':'tests/fixtures/uganda_baseline.json' in sdist_files,
 'raw_binary_data_in_wheel':raw_wheel,'raw_binary_data_in_sdist':raw_sdist,
 'scope':'All tracked lsms_library runtime/config/script/provenance/sidecar files. Checkout-only tests/fixtures are not distributed by current packaging policy.'}
(out/'archive-inspection.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps(result,sort_keys=True))
