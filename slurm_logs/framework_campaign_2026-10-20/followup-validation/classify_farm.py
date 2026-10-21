"""Review exact farm-warning deltas privately; publish only family counts."""
from collections import Counter,defaultdict
import json
from pathlib import Path
import re
root=Path('/local/job40255479/control/framework-campaign/followup-validation')
files={'baseline':Path('/local/job40255479/canonical-audit-compare/combined-raw.jsonl'),'candidate':root/'farm-final-4cab5f0d/farm-results-private.jsonl'}
features={'crop_production','plot_labor','plot_inputs'}
roots=['/local/job40255479/canonical-audit-compare/data-combined','/local/job40255479/followup-farm-data','/local/job40255479/worktrees/framework-integration','/local/job40255479/worktrees/framework-followup']
def norm(value):
 for s in roots:value=value.replace(s,'<ROOT>')
 return value

def key(r):return (r['phase'],r['feature'],r.get('country'),r['check'],r.get('severity'),json.dumps(r.get('kwargs',{}),sort_keys=True),norm(r['detail']))
rows={name:[r for l in p.read_text().splitlines() if (r:=json.loads(l)).get('feature') in features] for name,p in files.items()}
counts={name:Counter(key(r) for r in records) for name,records in rows.items()}
def family(message):
 for needle,label in [('Cache miss for ','cache_rebuild_notice'),('could not be decoded using utf-8','stata_utf8_fallback'),('Makefile not found in ','makefile_fallback'),('Makefile execution failed for Nigeria/_ ','baseline_only_nigeria_make_candidate_failure'),('Issue with categorical mapping:','categorical_mapping'),('grain conflict on ','source_grain_conflict'),('reduce_to_agreed(on_conflict=','cached_grain_report_replay')]:
  if needle in message:return label
 return 'unclassified'
summary={};private_unknown=[]
for label,delta in [('added',counts['candidate']-counts['baseline']),('removed',counts['baseline']-counts['candidate'])]:
 groups=defaultdict(lambda:{'count':0,'features':Counter(),'checks':Counter(),'severities':Counter(),'countries':set()})
 for k,n in delta.items():
  phase,feature,country,check,severity,kwargs,message=k;kind=family(message);g=groups[kind]
  g['count']+=n;g['features'][feature]+=n;g['checks'][check]+=n;g['severities'][severity]+=n
  if country:g['countries'].add(country)
  if kind=='cache_rebuild_notice':g['countries'].add(re.match(r'Cache miss for (.*?)/',message).group(1))
  if kind=='unclassified':private_unknown.append({'direction':label,'count':n,'feature':feature,'country':country,'check':check,'message':message})
 summary[label]={k:{**v,'countries':sorted(v['countries'])} for k,v in groups.items()}
report={'baseline_records':len(rows['baseline']),'candidate_records':len(rows['candidate']),'net_records':len(rows['candidate'])-len(rows['baseline']),'delta_families':summary,'unclassified_records':sum(r['count'] for r in private_unknown),'raw_messages_exported':False}
(root/'farm-final-4cab5f0d/warning-classification.json').write_text(json.dumps(report,indent=2)+'\n')
(root/'farm-final-4cab5f0d/unclassified-private.json').write_text(json.dumps(private_unknown,indent=2)+'\n')
print(json.dumps(report,indent=2))
