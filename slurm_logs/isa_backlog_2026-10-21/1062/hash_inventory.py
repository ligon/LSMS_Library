import json,pathlib,subprocess,os
import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library._build_registry import build_transforms_fingerprint
p=pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert pathlib.Path(ll.__file__).resolve().is_relative_to(p)
assert countries_root().resolve()==p/'lsms_library/countries'
out={};tables=set()
for path in sorted(countries_root().glob('*/_/data_scheme.yml')):
 c=ll.Country(path.parent.parent.name,preload_panel_ids=False)
 for table in c.data_scheme:
  tables.add(table);out[f'{c.name}/{table}']=c._table_cache_hash(table,c.waves)
summary={'country':out,'build':{t:build_transforms_fingerprint(t) for t in sorted(tables)}}
pathlib.Path(os.environ['HASH_OUT']).write_text(json.dumps(summary,indent=2)+'\n');print(len(out),len(tables))
