"""Read-only complete shipped SOURCE parse/render census, occurrence-aware."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library import provenance as pv

p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args()
repo=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert Path.cwd().resolve()==repo
assert Path(ll.__file__).resolve()==repo/'lsms_library/__init__.py'
root=countries_root().resolve();assert root==repo/'lsms_library/countries'
files=sorted(root.glob('*/*/Documentation/SOURCE.org'))
report={'source_root':str(repo),'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'files':len(files),'source_counts':{},'lost_information_lines':{},'unstable':[],
        'parsed_record_drift':[],'canonical_render_different':[],'inputs_unchanged':True,
        'recorded_held_release_missing':[],'input_sha256':{}}
info_free={getattr(pv,'URL_PLACEHOLDER','(source URL not recorded)'),pv.NOTES_HEADING}
counts=Counter()
for path in files:
 stat=path.stat();blob=path.read_bytes();text=blob.decode('utf-8')
 country,wave=path.parts[-4:-2];name=str(path.relative_to(root))
 report['input_sha256'][name]=hashlib.sha256(blob).hexdigest()
 rec=pv.parse_source_org(text,country,wave);counts[rec.source]+=1
 rendered=pv.render_source_org(rec);back=pv.parse_source_org(rendered,country,wave)
 again=pv.render_source_org(back)
 def lines(value):
  return Counter(line.rstrip() for line in value.splitlines() if line.strip() and line.strip() not in info_free)
 missing=lines(text)-lines(rendered)
 if missing:report['lost_information_lines'][name]={'line_occurrences':sum(missing.values())}
 if rendered!=again:report['unstable'].append(name)
 if rec!=back:report['parsed_record_drift'].append(name)
 if text!=rendered:report['canonical_render_different'].append(name)
 if rec.is_worldbank and not rec.idno:report['recorded_held_release_missing'].append(name)
 assert blob==path.read_bytes() and (stat.st_size,stat.st_mtime_ns)==(path.stat().st_size,path.stat().st_mtime_ns)
report['source_counts']=dict(counts)
a.out.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ('input_sha256','canonical_render_different')},sort_keys=True))
