"""Aggregate actual terminal marker streams; no survey log text is emitted."""
from pathlib import Path
import collections,hashlib,json,re,subprocess,datetime
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
P=Path(__file__).resolve().parent
TREE=Path('/local/job39919092/release-work/framework/uganda-assets')
assert Path(ll.__file__).resolve()==TREE/'lsms_library/__init__.py'
assert countries_root().resolve()==TREE/'lsms_library/countries'
assert data_root().resolve()==P/'empty-data'
assert not subprocess.check_output(['git','diff','3914d8804','HEAD','--','lsms_library','tests'],cwd=TREE,text=True)
assert not subprocess.check_output(['git','diff','9d5bf200f','3914d8804','--','tests/test_table_structure.py','tests/test_uganda_invariance.py'],cwd=TREE,text=True)
ANSI=re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
def extract(path):
 lines=[re.sub(r'^\d{4}-\d\d-\d\dT\S+\s*','',ANSI.sub('',x))for x in path.read_text().splitlines()]
 streams={};active=None;failed=[]
 for line in lines:
  if line.startswith('=') and ('warnings summary'in line or'FAILURES'in line):active=None
  m=re.fullmatch(r'(tests/\S+\.py) ([.sxXFEP]+)\s*(?:\[.*\])?',line)
  if m:active=m[1];streams.setdefault(active,'');streams[active]+=m[2];continue
  m=re.fullmatch(r'([.sxXFEP]+)\s*(?:\[.*\])?',line)
  if active and m:streams[active]+=m[1]
  m=re.match(r'FAILED (tests/\S+)',line)
  if m:failed.append(m[1])
 return lines,streams,failed
oldpath=P.parent/'credentialed-ci/38697245468-data-tests.log';newpath=P/'38746123119-data-tests.log'
oldlines,old,failures=extract(oldpath);newlines,new,currentfailures=extract(newpath)
ids=collections.defaultdict(list)
for line in(P/'affected-collection.log').read_text().splitlines():
 if line.startswith('tests/')and'::'in line:ids[line.split('::',1)[0]].append(line)
resolved=[]
for node in failures:
 mod=node.split('::',1)[0]
 assert len(ids[mod])==len(old[mod])==len(new[mod]),(mod,len(ids[mod]),len(old[mod]),len(new[mod]))
 index=ids[mod].index(node)
 assert old[mod][index]=='F'and new[mod][index]=='.',(node,index,old[mod][index],new[mod][index])
 resolved.append({'node_id':node,'zero_based_module_position':index,'old_outcome':'failed','final_outcome':'passed','module_items':len(ids[mod])})
assert len(resolved)==3 and not currentfailures
oldcounts=collections.Counter(''.join(old.values()))
assert dict(oldcounts)=={'.':4919,'s':2768,'x':4,'X':1,'F':3},oldcounts
counts={m:dict(collections.Counter(s))for m,s in new.items()}
allcounts=collections.Counter(''.join(new.values()))
assert sum(allcounts.values())==7700,allcounts
assert dict(allcounts)=={'.':4927,'s':2768,'x':4,'X':1},allcounts
record={'reviewed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_sha':'3914d8804d7b2936d75041cea98bbc9ddb6d4c9f','run_id':38746123119,'previous_run_id':38697245468,'source_identity_and_affected_test_source_unchanged':True,'collection_only':True,'collection_log_sha256':hashlib.sha256((P/'affected-collection.log').read_bytes()).hexdigest(),'resolved_previous_failures':resolved,'all_module_marker_counts':counts,'old_marker_totals':dict(oldcounts),'marker_totals':dict(allcounts),'skip_reason_limit':'Terminal output has module outcome markers and total skips, but no per-node skipped reason ledger. No reason counts are inferred.','old_skip_total':sum(s.count('s')for s in old.values()),'new_skip_total':allcounts['s'],'gha_regression_seconds':55.74,'semantic_review_complete':True}
(P/'semantic-review.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps({'resolved_previous_failures':resolved,'old_marker_totals':dict(oldcounts),'marker_totals':dict(allcounts),'old_skip_total':record['old_skip_total'],'new_skip_total':record['new_skip_total']},indent=2))
