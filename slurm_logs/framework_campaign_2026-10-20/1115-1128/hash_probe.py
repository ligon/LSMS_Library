"""Reuse complete #1093 country/wave hash census, with no table builds."""
import argparse
import json
from pathlib import Path
import subprocess
import warnings
import lsms_library as ll
from lsms_library.paths import countries_root, data_root
from lsms_library._build_registry import build_transforms_fingerprint

p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args()
root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert Path.cwd().resolve()==root
assert Path(ll.__file__).resolve()==root/'lsms_library/__init__.py'
assert countries_root().resolve()==root/'lsms_library/countries'
assert '1115-1128-hash-' in str(data_root())
assert not list(data_root().rglob('*.parquet'))
report={'source_root':str(root),'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'country_hashes':{},'wave_hashes':{},'build_fingerprints':{}}
tables=set()
with warnings.catch_warnings():
 warnings.simplefilter('ignore')
 for name in ll.countries():
  country=ll.Country(name,preload_panel_ids=False)
  for table in country.data_scheme:
   tables.add(table)
   report['country_hashes'][f'{name}/{table}']=country._table_cache_hash(table,country.waves)
   for wave in country.waves:
    report['wave_hashes'][f'{name}/{wave}/{table}']=country[wave]._input_hash(table)
 for table in sorted(tables):
  report['build_fingerprints'][table]=build_transforms_fingerprint(table)
 report['build_fingerprints']['<all>']=build_transforms_fingerprint()
assert not list(data_root().rglob('*.parquet'))
a.out.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:len(v) for k,v in report.items() if isinstance(v,dict)},sort_keys=True))
