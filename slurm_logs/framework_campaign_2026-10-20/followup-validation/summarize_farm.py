"""Compare scoped farm audit families without exporting raw warning messages."""
import argparse
from collections import Counter
import hashlib,json
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--baseline',type=Path,required=True)
p.add_argument('--candidate',type=Path,required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args();features={'crop_production','plot_labor','plot_inputs'}
def read(path):return [r for l in path.read_text().splitlines() if (r:=json.loads(l)).get('feature') in features]
old,new=read(a.baseline),read(a.candidate)
def pair(r):return (r.get('feature'),r.get('country'),r.get('check'),r.get('status'),json.dumps(r.get('kwargs'),sort_keys=True))
def summary(rows):
 return {'records':len(rows),'status_counts':dict(Counter(r.get('status') for r in rows)),'severity_counts':dict(Counter(str(r.get('severity')) for r in rows)),'families':dict(Counter('|'.join(str(r.get(k)) for k in ['feature','check','severity','status']) for r in rows)),'error_fail_pairs':[pair(r) for r in rows if r.get('status') in {'error','fail'}],'assembly_findings':[pair(r) for r in rows if r.get('severity')=='B' and r.get('check')!='runtime_warning']}
b,c=summary(old),summary(new)
blocking=[r for r in c['error_fail_pairs'] if r not in b['error_fail_pairs']]+[r for r in c['assembly_findings'] if r not in b['assembly_findings']]
report={'baseline_file_sha256':hashlib.sha256(a.baseline.read_bytes()).hexdigest(),'candidate_file_sha256':hashlib.sha256(a.candidate.read_bytes()).hexdigest(),'baseline':b,'candidate':c,'new_structural_or_error_fail_pairs':blocking,'verdict':'requires investigation' if blocking else 'no new structural/error/fail pairs; retained warnings require classification','raw_messages_exported':False}
a.out.write_text(json.dumps(report,indent=2)+'\n')
raise SystemExit(bool(blocking))
