"""Root stdlib-only authentication of the frozen GH1215 author evidence. Sue."""
from pathlib import Path
import ast
import collections
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import xml.etree.ElementTree as ET

assert sys.flags.no_site and sys.dont_write_bytecode
C = Path('/local/job40255479/control/isa-backlog-20261021')
P = C / 'uganda1215-order-fix'
PIN = {
 'READY.json':'f9fe07454b338b1793463587d46edb77b2304e066f775b40eb030b96520eee3f',
 'MANIFEST.private.json':'45c6e8e2f528fa282c59636ca74261d21318520c77a7b7e3ae40c45b5d24efcf',
 'EXPORT-ALLOWLIST.json':'a57d05d6b8b45b0fb7dd9ad9b67949029d7dc7856b7a8790c071ec03de00495a',
 'safe-proof.json':'02677e69069394bbad1696859555cb1bcdf18478677a455e4eefdfebe6d5faa2',
 'postexit-result-private.json':'6f48a3d9ca9ce8570829d12d4cf4ca86b94f6864f3395c0d8bdbf15a3c3ef9a9',
 'seal-tool-record-private.json':'88ea9f53ff28c35fe79a5dcb82d6ce37e177dee447b04f5550052b22e3e6fca7',
 'postexit-tool-record-private.json':'a23c44854d2dae2f89428eec15ee68fde61cfd2cac445e9b90c82fe43f95ce4b',
}
def read(n):
    return json.loads((P/n).read_text())
def physical(q, hashed=True):
    q=Path(q);s=q.lstat()
    d=dict(bytes=s.st_size,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,
           inode=s.st_ino,device=s.st_dev,nlink=s.st_nlink,uid=s.st_uid,gid=s.st_gid,
           symlink=stat.S_ISLNK(s.st_mode))
    if d['symlink']:
        d['link_target']=os.readlink(q)
    elif hashed:
        assert stat.S_ISREG(s.st_mode), 'non-regular endpoint'
        h=hashlib.sha256()
        with q.open('rb') as f:
            for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
        d['sha256']=h.hexdigest()
    after=q.lstat()
    protected=('st_mode','st_size','st_mtime_ns','st_ctime_ns','st_ino','st_dev','st_nlink','st_uid','st_gid')
    assert all(getattr(after,k)==getattr(s,k) for k in protected), 'protected endpoint changed during inspection'
    return d
def check(q,pin):
    got=physical(q,hashed='sha256' in pin)
    for k,v in pin.items():
        if k=='opaque_tracked_data_not_opened':continue
        if k in ('md5','genuine_stata_binary_magic'):continue
        assert got[k]==v, ('receipt mismatch',str(q),k)
    return got
for n,sha in PIN.items():assert physical(P/n)['sha256']==sha,n
m=read('MANIFEST.private.json');r=read('READY.json');allow=read('EXPORT-ALLOWLIST.json')
assert (len(m['files']),len(m['links']),len(m['directories']))==(1498,30,1228)
assert sum(v['bytes'] for v in m['files'].values())==508088006
for n,v in m['files'].items():
    assert not Path(n).is_absolute() and '..' not in Path(n).parts
    assert v['nlink']==1 and not v['symlink']
    check(P/n,v)
for n,v in m['links'].items():
    check(P/n,v)
    assert (P/n).resolve().is_relative_to(P)
for n,v in m['directories'].items():
    q=P/n;s=q.lstat();assert stat.S_ISDIR(s.st_mode) and not q.is_symlink()
    d=dict(mode=stat.S_IMODE(s.st_mode),device=s.st_dev,inode=s.st_ino,nlink=s.st_nlink,uid=s.st_uid,gid=s.st_gid)
    if n!='.':d.update(bytes=s.st_size,mtime_ns=s.st_mtime_ns)
    assert d==v,('directory receipt',n)
expected_late={'READY.json','MANIFEST.private.json','seal-final-private.log','seal-final-private.stderr',
 'seal-tool-record-private.json','postexit-final-private.log','postexit-final-private.stderr',
 'postexit-result-private.json','postexit-tool-record-private.json'}
assert set(m['planned_exclusions'])==expected_late
assert m['root_directory_volatile_fields']==['bytes','mtime_ns']
all_files=set();all_dirs={'.'}
for root,dirs,files in os.walk(P,followlinks=False):
    for n in list(dirs):
        q=Path(root)/n;rel=str(q.relative_to(P))
        if q.is_symlink():all_files.add(rel);dirs.remove(n)
        else:all_dirs.add(rel)
    all_files.update(str((Path(root)/n).relative_to(P)) for n in files)
assert all_files==set(m['files'])|set(m['links'])|expected_late
assert all_dirs==set(m['directories'])

before=read('source-pins/source-before-private.json')['source_epochs']
after=read('actors/final-protection-v1/source-after-private.json')
assert after['differences']==[] and after['source_epochs']==before
source_counts={};unopened={}
for side,group in before.items():
    root=Path(group['root']);head=group['head']
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==head
    assert subprocess.check_output(['git','status','--porcelain','--untracked-files=normal'],cwd=root,text=True)==''
    names=subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')[:-1]
    assert set(names)==set(group['tracked_files'])
    unopened[side]=0
    for n,pin in group['tracked_files'].items():
        check(root/n,pin)
        unopened[side]+=bool(pin.get('opaque_tracked_data_not_opened'))
    source_counts[side]=len(names)
assert source_counts=={'candidate':14914,'base':14913} and set(unopened.values())=={3}
source_head='f7c33dd64d4b2d6e9a62838c33cc30d07863d706'
parent='a417b69d71c460ab33f6a494b4df5d67cd792de5'
assert before['candidate']['head']==source_head and before['base']['head']==parent
candidate=Path(before['candidate']['root'])
diff=subprocess.check_output(['git','diff','--no-ext-diff','--binary',parent,source_head,'--','.'],cwd=candidate)
assert diff==(P/'source.diff').read_bytes() and hashlib.sha256(diff).hexdigest()=='1b69f1947dd284e45b6f3a0f6e0b312a56c97827f635698a05c4a0988b953db2'
paths=subprocess.check_output(['git','diff','--name-only',parent,source_head],cwd=candidate,text=True).splitlines()
assert len(paths)==5 and [p for p in paths if p.startswith('lsms_library/') and p.endswith('.py')]==['lsms_library/country.py']
trees=[]
for side in ['candidate','base']:
    tree=ast.parse((Path(before[side]['root'])/'lsms_library/country.py').read_text())
    removed=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name=='_empty_declared_frame'];assert len(removed)==1
    tree.body.remove(removed[0]);trees.append(ast.dump(tree,include_attributes=False))
assert trees[0]==trees[1]

plan=read('genuine-plan-v3-private.json');raw=read('actors/final-protection-v1/providers-and-copies-after-private.json')
assert len(plan['providers'])==37 and len(plan['roots'])==4
def binary(q,pin,md5):
    got=check(q,pin);h=hashlib.md5();first=b''
    with Path(q).open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):
            if not first:first=b[:64]
            h.update(b)
    assert h.hexdigest()==md5
    assert first and (first.startswith(b'<stata_dta>') or first[0] in (104,105,108,110,111,113,114,115,116,117))
    assert not got['symlink'] and got['nlink']==1
    return got
provider_by_md5={};physical_ids=set()
for row in plan['providers']:
    q=Path(row['provider']);pin=raw['providers'][row['logical']]
    got=binary(q,pin,row['declared_md5']);provider_by_md5[row['declared_md5']]=pin
    key=(got['device'],got['inode']);assert key not in physical_ids;physical_ids.add(key)
    for side,pair in row['current_source_text_bindings'].items():
        for name in ['sidecar','authority']:check(pair[name],pair['receipts'][name])
        text=Path(pair['sidecar']).read_text()
        assert re.search(r'\bmd5:\s*'+re.escape(row['declared_md5'])+r'\b',text)
        size=re.search(r'\bsize:\s*(\d+)',text)
        if size:assert int(size.group(1))==got['bytes']
for group in plan['source_declaration_bindings'].values():
    for n,pin in group['text_receipts'].items():check(n,pin)
assert len(raw['copies'])==148 and sum(v['bytes'] for v in raw['copies'].values())==177769212
for n,pin in raw['copies'].items():
    got=binary(n,pin,pin['md5']);assert got['mode']==0o600
    assert got['sha256']==provider_by_md5[pin['md5']]['sha256']
    key=(got['device'],got['inode']);assert key not in physical_ids;physical_ids.add(key)
assert len(physical_ids)==185

f1=read('actors/fingerprints-candidate-v1/fingerprints-private.json');f0=read('actors/fingerprints-base-v1/fingerprints-private.json')
for kind,total in [('country',549),('framework',39),('declared_wave',1362)]:
    assert len(f1[kind])==len(f0[kind])==total and f1[kind].keys()==f0[kind].keys()
    assert all(f1[kind][k]!=f0[kind][k] for k in f1[kind])
assert f1['ordered_public_names']==f0['ordered_public_names']
tests=read('synthetic-results-gate-private.json')['results']
for label,total,passed,failed,exitcode in [('new-controls-candidate-v2',13,13,0,0),('new-controls-base-v2',13,7,6,1),('supporting-candidate-v2',88,88,0,0)]:
    x=tests[label];assert (x['total'],x['passed'],len(x['failed']),x['terminal']['exit_code'])==(total,passed,failed,exitcode)
    assert x['errors']==x['skipped']==[] and x['terminal']['source_pins_after_exact']
    matches=[n for n,v in m['files'].items() if v['sha256']==x['junit_receipt']['sha256']]
    assert len(matches)==1
    cases=ET.parse(P/matches[0]).getroot().findall('.//testcase')
    assert len(cases)==total and sum(c.find('failure') is not None for c in cases)==failed
    assert not any(c.find('error') is not None or c.find('skipped') is not None for c in cases)
for side in ['base','candidate']:
    labels=['grid-'+side+'-'+b+'-'+mode+'-v1' for b in ['dvc','make'] for mode in ['sequence','warm']]+['grid-'+side+'-dvc-scanner-v1']
    for label in labels:
        t=read('launch-records/'+label+'-terminal-private.json');assert t['exit_code']==0 and t['source_pins_after_exact']
comparison=read('actors/saved-comparisons-v1/comparison-private.json')
assert len(comparison['cases'])==44
assert collections.Counter(v['strict'] for v in comparison['cases'])=={'PASS':38,'FAIL':6}
assert comparison['approved_order_change_qualified']==6 and len(comparison['order_diagnostics'])==6
assert comparison['ordered_scanner_records_exact'] is True and comparison['no_API_or_raw_decode'] is True
scanners=[]
for side in ['base','candidate']:
    b=(P/('actors/grid-'+side+'-dvc-scanner-v1/scanner-private.jsonl')).read_bytes()
    rows=[json.loads(l) for l in b.splitlines()]
    assert len(rows)==22 and all(v['status']=='warn' for v in rows)
    scanners.append(b)
assert scanners[0]==scanners[1]
cal=read('actors/comparator-calibration-v1/comparator-calibration-private.json')
assert cal['passed']==cal['total']==30 and len(cal['checks'])==30
guard=read('actors/final-protection-v2/guard-final-private.json')
assert guard['total_rows']==172 and guard['original_calibration_seed_rows']==38 and guard['no_unmatched_rows'] is True
assert len((P/'guard-private.jsonl').read_bytes().splitlines())==172
check(P/'guard-private.jsonl',guard['guard_log_receipt'])

assert set(allow['files'])=={'author-report-safe.md','safe-proof.json','source.diff','privacy-scan-safe.json'}
tokens=set(read('all-warning-example-identifiers-private.json')['tokens'])
tokens.update(read('historical-source-example-output-tokens-v3-private.json')['tokens']);assert len(tokens)==54
for n,pin in allow['files'].items():
    check(P/n,pin);b=(P/n).read_bytes()
    assert not any(t.encode() in b for t in tokens), 'private identifier in export'
for n,sid,chunk in [('seal-tool-record-private.json',15242,'80d51d'),('postexit-tool-record-private.json',65267,'89b801')]:
    x=read(n);assert x['session_id']==sid and x['original_terminal_chunk']==chunk
    for row in x['literal_original_records']:
        for kind in ['call','return']:
            b=row['literal_original_'+kind+'_jsonl_utf8'].encode()
            assert hashlib.sha256(b).hexdigest()==row['original_'+kind+'_line_sha256']
    terminal=json.loads(x['literal_original_records'][-1]['literal_original_return_jsonl_utf8'])['payload']['output']
    terminal=json.dumps(terminal)
    assert chunk in terminal and ('Process exited with code 0' in terminal or '\\"exit_code\\":0' in terminal or '\\"exit_code\\": 0' in terminal)
post=read('postexit-result-private.json');assert post['status']=='SEPARATE_FULL_POSTEXIT_PASS'
assert (post['files'],post['links'],post['directories'],post['full_bytes'])==(1498,30,1228,508088006)
for name,key in [('READY.json','ready_receipt'),('MANIFEST.private.json','manifest_receipt'),('EXPORT-ALLOWLIST.json','allowlist_receipt')]:check(P/name,post[key])
report={
 'status':'ROOT_AUTHENTICATED_AUTHOR_PACKET_PENDING_INDEPENDENT_GENUINE_REVIEW',
 'issue':1215,'source_head':source_head,'actual_parent':parent,'author_pins':PIN,
 'full_frozen_files':1498,'frozen_links':30,'frozen_directories':1228,'frozen_file_bytes':508088006,
 'source_paths':source_counts,'historical_tracked_binary_sources_unopened_metadata_only_each':3,
 'providers_fresh_MD5_SHA_magic_physical':37,'distinct_private_L1_copies':148,'L1_copy_bytes':177769212,
 'candidate_tests':{'new':13,'supporting':88},'actual_parent_new':{'pass':7,'fail':6},
 'predata_fingerprint_moves':{'country':549,'framework':39,'declared_wave':1362},
 'full_strict':{'pass':38,'fail':6,'approved_empty_column_order_deltas':6},
 'scanners_each':{'WARN':22,'ordered_full_JSONL_bytes_exact':True},
 'guard_events':{'total':172,'seed':38,'denied_IPv6_import_binds':90,'existing_directory_mkdir_refusals':44},
 'author_comparator':{'total':30,'positives':3,'faults':27},
 'safe_exports':4,'privacy_union_literals':54,'export_identifier_matches':0,
 'sealer':{'session_id':15242,'final_chunk':'80d51d','exit_code':0},
 'postexit':{'session_id':65267,'final_chunk':'89b801','exit_code':0},
 'root_inspection_output_exception':'Private root-1215-output-scope-268055 retains exact original truncated tool call/return; no output replay or inferred unreturned tail.',
 'limits':['Independent genuine-data review still required','No full-corpus rebuild or full-suite verdict','Six approved cross-head empty-order comparisons remain FAIL','All-wave raw country cache operands retained opaque; raw-partial operands not recovered','Three historical tracked binary files per head remain unopened metadata-only','Original filesystem method/target stacks were not recorded','All original stops and exposure qualifications retained'],
 'signed':'Sue'}
out=C/'1215-AUTHOR-ROOT-AUTHENTICATION.json'
fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2);f.write('\n')
print(json.dumps({'status':report['status'],'files':1498,'sources':source_counts,'providers':37,'copies':148,'safe_exports':4,'full_strict_pass':38,'full_strict_fail':6,'receipt_sha256':physical(out)['sha256'],'identifiers_printed':0,'signed':'Sue'}))
