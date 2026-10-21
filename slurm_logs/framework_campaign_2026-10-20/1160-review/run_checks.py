"""Same-process frozen-source/import/config/cache assertion before pytest."""
import argparse,hashlib,json,os,subprocess,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--sha',required=True);p.add_argument('--source-sha',required=True);p.add_argument('--identity',type=Path,required=True);a,args=p.parse_known_args()
root=a.root.resolve();data=Path(os.environ['LSMS_DATA_DIR']).resolve()
assert Path.cwd().resolve()==root
assert os.environ['PYTHONPATH']==str(root)
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==a.sha
assert not subprocess.check_output(['git','diff','HEAD','--name-only'],text=True).strip()
assert hashlib.sha256((root/'lsms_library/transformations.py').read_bytes()).hexdigest()==a.source_sha
for name in ['OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','LSMS_BUILD_WORKERS','LSMS_MAKE_JOBS']:assert os.environ[name]=='1',name
assert data.is_relative_to(Path('/local/job40255479/control/framework-campaign/review-1160'))
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
assert Path(ll.__file__).resolve().parent.parent==root
assert countries_root().resolve()==root/'lsms_library/countries'
assert data_root().resolve()==data
identity={'head':a.sha,'root':str(root),'package':ll.__file__,'countries_root':str(countries_root()),'data_root':str(data_root()),'source_sha256':a.source_sha,'affinity':sorted(os.sched_getaffinity(0))}
a.identity.write_text(json.dumps(identity,indent=2)+'\n')
import pytest
raise SystemExit(pytest.main(args))
