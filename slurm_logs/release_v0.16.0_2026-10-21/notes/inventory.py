"""Map every supplied post-v0.15 PR in the release ancestry to the notes.

Only git-object reads and JSON/Markdown reads are used. No library imports.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

parser=argparse.ArgumentParser()
parser.add_argument('--metadata',type=Path,required=True)
parser.add_argument('--notes-dir',type=Path,required=True)
args=parser.parse_args()
out=args.notes_dir.resolve()
assert out.is_relative_to(Path('/local'))
base='cb44e3522a2426ba03431ca9a218cc63fe03f6c9'
head='acbd8eb34710e85d3bf66eb04f13dd0d971d1f3c'
sections={
'feature':'Feature indexes, pooled quantities and sampling metadata',
'country':'Country data and unit corrections',
'mapping':'Mapping, errors and metadata contracts',
'cache':'Readers, dates and cache behavior',
'provenance':'Provenance and credentials',
'validation':'Validation, tooling and upgrade notes',
'limits':'Remaining data and scientific limits'}
placement={
1017:['validation'],1018:['mapping'],1019:['cache'],1020:['country'],
1021:['cache'],1022:['provenance'],1023:['country'],1024:['mapping'],
1025:['cache'],1026:['cache'],1027:['cache'],1028:['mapping'],1029:['mapping'],
1030:['validation'],1046:['country'],1049:['validation','limits'],1052:['cache'],
1053:['country'],1054:['country'],1065:['provenance'],1067:['country','limits'],
1076:['country','limits'],1086:['feature'],1092:['feature'],1094:['country'],
1097:['validation'],1098:['mapping'],1104:['country'],
1105:['mapping','cache'],1112:['feature','limits'],1117:['provenance'],
1126:['feature'],1132:['validation'],1137:['feature'],1147:['validation'],
1151:['country'],1152:['country'],1153:['cache'],1154:['feature'],1155:['cache'],
1156:['country'],1157:['validation'],1161:['validation'],1162:['cache','validation'],
1163:['provenance','limits'],1164:['feature'],1165:['feature','limits']}
omitted={
1016:'Post-v0.15 release-completion documentation; no new0.16 runtime feature. Older publication claims are not repeated as0.16 publication validation.',
1158:'Campaign evidence/handoff only; cited in validation rather than presented as another behavior change.',
1166:'Final landing/CI evidence and handoff only; cited in validation, with no runtime/config/test/build change.'}
notes=(out/'RELEASE_NOTES.md').read_text()
data=json.loads(args.metadata.read_text())
def ancestor(sha,dest):
    code=subprocess.run(['git','merge-base','--is-ancestor',sha,dest],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode
    assert code in[0,1],(sha,dest,code)
    return code==0
rows=[];excluded=[]
for pr in data:
    number=pr['number'];sha=pr['mergeCommit']['oid'];inh=ancestor(sha,head);intag=ancestor(sha,base)
    if not inh or intag:
        excluded.append({'pr':number,'merge_sha':sha,'in_development_ancestry':inh,'in_v015_ancestry':intag,'reason':'already in v0.15.0'if intag else'not in release development ancestry'})
        continue
    assert number in placement or number in omitted,number
    if number in placement:assert f'https://github.com/ligon/LSMS_Library/pull/{number}'in notes,number
    rows.append({'pr':number,'url':pr['url'],'title':pr['title'],'merge_sha':sha,'merged_at':pr['mergedAt'],'base_ref':pr['baseRefName'],'verified_ancestor_of_release_development':inh,'verified_absent_from_v015':not intag,'sections':[sections[k]for k in placement.get(number,[])],'included_as_behavior_or_validation':number in placement,'omitted_doc_only_reason':omitted.get(number),'body_sha256':hashlib.sha256(pr['body'].encode()).hexdigest()})
assert len(rows)==50 and len(rows)==len(placement)+len(omitted)
assert len({r['pr']for r in rows})==len(rows)
# Bind all exact evidence links to files actually present at the source frontier.
for urlpath in re.findall(r'https://github.com/ligon/LSMS_Library/blob/'+head+r'/([^\s)]+)',notes):
    subprocess.run(['git','cat-file','-e',head+':'+urlpath],check=True)
report={'release':'0.16.0','v015_tag':'v0.15.0','v015_tag_commit':base,'v015_tagged_utc':'2026-10-15T15:37:21Z','reviewed_development':head,'validated_implementation':'532d58700fa46e52a0b390198604eca1dc4de00e','metadata_entries':len(data),'metadata_sha256':hashlib.sha256(args.metadata.read_bytes()).hexdigest(),'actual_posttag_pr_count':len(rows),'included_pr_count':len(placement),'omitted_doc_only_count':len(omitted),'records':sorted(rows,key=lambda r:r['pr']),'excluded_entries':excluded,'scope':'All supplied200merged PR entries checked against both commit ancestries; all50actual posttag merges mapped. This is a change inventory, not automatic issue-closure authority.','notes_target':'docs/releases/v0.16.0.md','migration_target':'docs/migration/v0.16.0.md','publication_claims':'No tag/PyPI/upload success is asserted before the release owner verifies it.','runtime_or_data_execution':False,'github_mutations':False}
(out/'PR_INVENTORY.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:report[k]for k in['metadata_entries','actual_posttag_pr_count','included_pr_count','omitted_doc_only_count']}))
