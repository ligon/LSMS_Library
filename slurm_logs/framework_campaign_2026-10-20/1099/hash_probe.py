import json,os,subprocess,sys
from pathlib import Path
import lsms_library
from lsms_library import Country
from lsms_library._build_registry import build_transforms_fingerprint
root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip())
assert Path(lsms_library.__file__).resolve().is_relative_to(root.resolve())
out={'build_transforms':{},'country':{},'wave':{}}
for t in ('sample','household_roster','livestock','shocks'):
 out['build_transforms'][t]=build_transforms_fingerprint(t)
 for name in ('Albania','Senegal','Uganda'):
  c=Country(name)
  if t not in c.data_scheme:continue
  out['country'][name+'/'+t]=c._table_cache_hash(t,c.waves)
  for w in c.waves:
   out['wave'][name+'/'+str(w)+'/'+t]=c[w]._input_hash(t)
Path(sys.argv[1]).write_text(json.dumps(out,indent=2)+'\n')
print({k:len(v) for k,v in out.items()})
