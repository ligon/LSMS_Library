# Read-only proposal generator. It does not call GitHub or alter repository config.
from pathlib import Path
import copy,json
r=Path(__file__).parent
before=json.loads((r/'development-ruleset.json').read_text())
payload={k:copy.deepcopy(before[k]) for k in ('name','target','enforcement','bypass_actors','conditions','rules')}
rule=next(x for x in payload['rules'] if x['type']=='required_status_checks')
rule['parameters']['required_status_checks'].append({'context':'data-tests'})
(r/'proposed-development-ruleset.json').write_text(json.dumps(payload,indent=2)+'\n')
(r/'ruleset-change.json').write_text(json.dumps([{'op':'add','path':'/rules/2/parameters/required_status_checks/-','value':{'context':'data-tests'}}],indent=2)+'\n')
