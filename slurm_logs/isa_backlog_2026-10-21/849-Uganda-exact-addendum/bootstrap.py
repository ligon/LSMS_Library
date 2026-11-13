# Initial assertions precede pandas/record imports; resolved package/config
# assertions finish before the artifact verifier can unpickle anything.
from pathlib import Path
import os,sys,subprocess,socket,importlib.util,runpy,sitecustomize,json
P=Path('/local/job40255479/control/isa-backlog-20261021/Uganda/849-exact-addendum');ROOT=Path('/local/job40255479/worktrees/isa-Uganda-849')
assert Path(sys.executable).resolve()==Path('/local/job40255479/venv_img/bin/python').resolve()
assert Path.cwd().resolve()==ROOT and Path(os.environ['LSMS_EXPECTED_ROOT']).resolve()==ROOT
assert os.environ['PYTHONPATH'].split(os.pathsep)==[str(ROOT),str(P)]
assert Path(importlib.util.find_spec('lsms_library').origin).resolve()==ROOT/'lsms_library/__init__.py'
for env,target in {'LSMS_COUNTRIES_ROOT':ROOT/'lsms_library/countries','LSMS_DATA_DIR':P/'data','LSMS_CONFIG_DIR':P/'config','TMPDIR':P/'tmp','DVC_SITE_CACHE_DIR':P/'dvc-site','MPLCONFIGDIR':P/'matplotlib','XDG_CACHE_HOME':P/'xdg-cache','XDG_CONFIG_HOME':P/'xdg-config'}.items():assert Path(os.environ[env]).resolve()==target
assert Path(sitecustomize.__file__).resolve()==P/'sitecustomize.py'
assert socket.socket.connect.__module__==socket.socket.connect_ex.__module__=='sitecustomize'
assert {x.split(':')[0].strip()for x in Path('/proc/net/dev').read_text().splitlines()if ':'in x}=={'lo'}
for name in ['OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS','VECLIB_MAXIMUM_THREADS','LSMS_BUILD_WORKERS','LSMS_MAKE_JOBS']:assert os.environ[name]=='1'
assert os.environ['MAKEFLAGS']=='-j1'and os.environ['PYTHONDONTWRITEBYTECODE']=='1'
assert not sitecustomize.allowed(ROOT/'lsms_library/countries/.dvc/tmp/btime')
assert not sitecustomize.allowed(P.parent/'849/anything')
assert not sitecustomize.allowed('/local/job40255479/control/isa-backlog-20261021/Mali/1091-fix/anything')
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()=='f8d236694c26fe9e709c5548f02c70f74ea72c33'
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],text=True)
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library.config import _config_dir
assert Path(ll.__file__).resolve()==ROOT/'lsms_library/__init__.py'
assert countries_root().resolve()==ROOT/'lsms_library/countries' and data_root().resolve()==P/'data' and _config_dir().resolve()==P/'config'
assert {str(q.relative_to(P/'data'))for q in (P/'data').rglob('*')}=={'dvc-cache'}
assert not any(q.is_file()for q in (P/'data').rglob('*'))
# Fail loudly if this artifact-only task accidentally invokes an API or source
# reader. No old source/helper/measurement script is executed.
def denied(*a,**kw):raise AssertionError('Artifact-only addendum forbids API/source/cache operations')
ll.Country.__init__=denied;ll.Feature.__init__=denied
from lsms_library import local_tools
for name in ['get_dataframe','get_data_file','to_parquet']:
 if hasattr(local_tools,name):setattr(local_tools,name,denied)
import pandas as pd
pd.read_stata=denied
sys.argv=sys.argv[1:]
script=Path(sys.argv[0]).resolve();assert script.parent==P
runpy.run_path(str(script),run_name='__main__')
