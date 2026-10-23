"""Root authentication of frozen independent BF1188 proof, stdlib only. Sue."""
from pathlib import Path
import collections
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
assert sys.flags.no_site and sys.dont_write_bytecode
C=Path('/local/job40255479/control/isa-backlog-20261021')
P=C/'review-Burkina_Faso-1188'
PINS={
 'READY.json':'92cdabce0c32d5a8d140c99f7190a1e8437d67eb7928ffb61de0b92342d2a615',
 'final-private-manifest.json':'dc36563c3d321b3e6397e2d27f523d029db911ab89203e3b603b65e67b0deff0',
 'safe-exports-v2/ALLOWLIST.json':'624e1a8705208e58f2fb5fdc250d559e64a010e850e5de1684cc319d8cecd9d3',
 'FINAL-PROTECTION.json':'770b8adac0ecdf1b1a135f86ed54a9b84d4d7fa488d3dd457453cd854b200c5a',
 'ROOT-DATA-READ-RELEASE.json':'757ffa868fa6827d3a6f9f784cecf83af9b79bd041eb39046a35f51eeb6348b9',
 'LIFECYCLE-POLICY.json':'edb56e95d782dfaa661dd8bbe669bfac6f417546409f046bc6a77b49c346993e',
}
def read(n):return json.loads((P/n).read_text())
def receipt(q):
    q=Path(q);s=q.lstat();link=stat.S_ISLNK(s.st_mode)
    assert link or stat.S_ISREG(s.st_mode)
    if link:b=os.fsencode(os.readlink(q));h=hashlib.sha256(b).hexdigest();size=len(b)
    else:
        hobj=hashlib.sha256()
        with q.open('rb') as f:
            for b in iter(lambda:f.read(8*1024*1024),b''):hobj.update(b)
        h=hobj.hexdigest();size=s.st_size
    after=q.lstat()
    assert all(getattr(s,k)==getattr(after,k) for k in ['st_mode','st_size','st_mtime_ns','st_ctime_ns','st_ino','st_dev','st_nlink','st_uid','st_gid'])
    return dict(sha256=h,bytes=size,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,nlink=s.st_nlink,device=s.st_dev,inode=s.st_ino,uid=s.st_uid,gid=s.st_gid,symlink=link,link=os.readlink(q) if link else None,hash_semantics='link-text' if link else 'file-bytes')
for n,pin in PINS.items():assert receipt(P/n)['sha256']==pin,n
m=read('final-private-manifest.json');r=read('READY.json')
assert len(m['entries'])==r['manifest_entries']==1045
assert sum(v['bytes'] for v in m['entries'])==r['manifest_payload_bytes']==162253383
expected={}
for row in m['entries']:
    name=row['path'];assert name not in expected and '..' not in Path(name).parts and not Path(name).is_absolute()
    pin={k:v for k,v in row.items() if k!='path'}
    assert receipt(P/name)==pin,('payload',name)
    assert row['nlink']==1 and row['uid']==43292 and row['mode']==(0o777 if row['symlink'] else 0o600)
    expected[name]=pin
exclusions={'final-private-manifest.json','READY.json','actors/seal-v2/stdout.txt','actors/seal-v2/stderr.txt','actors/seal-v2/terminal.json','actors/postexit-v2/launch-before.json','actors/postexit-v2/original-Popen.json','actors/postexit-v2/stdout.txt','actors/postexit-v2/stderr.txt','actors/postexit-v2/terminal.json'}
assert set(m['exclusions'])==exclusions
actual=set();directories={}
exception={v['path']:v for v in m['exact_directory_mode_qualification']['exceptions']};assert len(exception)==3
for root,dirs,files in os.walk(P,followlinks=False):
    for n in list(dirs):
        q=Path(root)/n;rel=str(q.relative_to(P))
        if q.is_symlink():actual.add(rel);dirs.remove(n)
        else:
            s=q.lstat();now=dict(path=rel,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,nlink=s.st_nlink,inode=s.st_ino,device=s.st_dev,uid=s.st_uid,gid=s.st_gid)
            assert now['uid']==43292 and now['mode']==(exception[rel]['mode'] if rel in exception else 0o700)
            if rel in exception:assert now==exception[rel]
            directories[rel]=now
    actual.update(str((Path(root)/n).relative_to(P)) for n in files)
assert actual==set(expected)|exclusions
assert stat.S_IMODE(P.stat().st_mode)==0o700

src=read('full-source-before-runtime.json');source_counts={}
for role,row in src['roots'].items():
    root=Path(row['root']);assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==row['head']
    assert subprocess.check_output(['git','status','--porcelain','--untracked-files=normal'],cwd=root,text=True)==''
    assert set(subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0')[:-1])==set(row['files'])
    for name,pin in row['files'].items():assert receipt(root/name)==pin,('source',role,name)
    source_counts[role]=len(row['files'])
assert source_counts=={'candidate':14895,'base':14893}
assert src['roots']['candidate']['head']=='3d39096d55bc2b2a8a4e387e9d939aabe8951ecd'
assert src['roots']['base']['head']=='3dc9d28510dd783c648413987011832669f8bcf7'
authorroot=Path('/local/job40255479/worktrees/isa-Burkina_Faso-1188')
final='657fed11a63354886772d0f978cec8adb2ad647e'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=authorroot,text=True).strip()==final
paths=subprocess.check_output(['git','diff','--name-only',src['roots']['candidate']['head'],final],cwd=authorroot,text=True).splitlines()
assert len(paths)==2 and all(n.startswith('.coder/ledger/') or n=='lsms_library/countries/Burkina_Faso/_/CONTENTS.org' for n in paths)

prep=read('SOURCE-PREPARATION.json');assert len(prep['originals'])==6 and len(prep['copies'])==24 and prep['copied_bytes']==25343300 and prep['sidecar_copies']==0
seen=set();raw_by_path={}
def binary(q,md5,sha,physical):
    q=Path(q);s=q.lstat();assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and not q.is_symlink()
    got=dict(type='file',bytes=s.st_size,mode=stat.S_IMODE(s.st_mode),mtime_ns=s.st_mtime_ns,nlink=s.st_nlink,inode=s.st_ino,device=s.st_dev,uid=s.st_uid,gid=s.st_gid)
    assert got==physical,('raw physical',str(q))
    h=hashlib.sha256();d=hashlib.md5();first=b''
    with q.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):
            if not first:first=b[:64]
            h.update(b);d.update(b)
    assert h.hexdigest()==sha and d.hexdigest()==md5
    assert first and (first.startswith(b'<stata_dta>') or first[0] in (104,105,108,110,111,113,114,115,116,117))
    now=q.lstat();assert all(getattr(now,k)==getattr(s,k) for k in ['st_mode','st_size','st_mtime_ns','st_ino','st_dev','st_nlink','st_uid','st_gid'])
    key=(s.st_dev,s.st_ino);assert key not in seen;seen.add(key)
    return got
for row in prep['originals']:
    binary(row['path'],row['md5'],row['sha256'],row['full_physical_after']);raw_by_path[row['path']]=row
for row in prep['copies']:
    binary(row['path'],row['md5'],row['sha256'],row['physical'])
    assert row['physical']['mode']==0o600
    old=raw_by_path[row['provider_path']];assert row['sha256']==old['sha256'] and row['md5']==old['md5']
assert len(seen)==30
protection=read('FINAL-DATA-PROTECTION.json');assert protection['all_six_original_and24_private_copy_full_SHA_MD5_physical_equal'] is True and len(protection['checked'])==30
assert len((P/'guard-private.jsonl').read_bytes().splitlines())==483
assert receipt(P/'guard-private.jsonl')['sha256']==read('FINAL-PROTECTION.json')['guard_log']['sha256']

for label,exitcode,count in [('tests-candidate-v2',0,14),('tests-base-v2',1,7),('tests-supporting',0,397)]:
    term=read('actors/'+label+'/terminal.json');assert term['actual_exit_code']==exitcode
    assert read('actors/'+label+'/pytest-return.json')['pytest_return_code']==exitcode
    text=(P/('actors/'+label+'/stdout.txt')).read_text()
    assert re.search(r'\b'+str(count)+r' passed\b',text)
    if label=='tests-base-v2':assert re.search(r'\b7 failed\b',text)
for role in ['candidate','base']:
    for backend in ['default','make']:
        for state in ['cold','warm']:
            label='grid-'+role+'-'+backend+'-'+state+'-v1'
            assert read('actors/'+label+'/terminal.json')['actual_exit_code']==0
for label in ['own-source-oracle-v1','saved-operands-proof-v1','new-empty-reference-v1','scanner-candidate-v1','scanner-base-v1']:
    assert read('actors/'+label+'/terminal.json')['actual_exit_code']==0
proof=read('actors/saved-operands-proof-v1/strict-results-private.json')
assert len(proof['results'])==151 and collections.Counter(v['status'] for v in proof['results'])=={'PASS':111,'FAIL':40}
assert collections.Counter(v['group'] for v in proof['results'] if v['status']=='FAIL')=={'native_original_pre_writer':40}
assert sum(v['group']=='native_exact_writer_readback' and v['status']=='PASS' for v in proof['results'])==40
assert proof['strict_fault_calibration']==12 and len(proof['source_lineage_named_payload'])==28
empty=read('actors/new-empty-reference-v1/four-empty-results-private.json');assert len(empty['rows'])==4
assert empty['original151matrix_unchanged']=={'total':151,'PASS':111,'FAIL':40,'PASS_includes40_writer_stage_not_additional40_outside151':True}
for row in empty['rows']:
    v=row['full_strict'];status=v.get('status') if isinstance(v,dict) else v
    assert status==('PASS' if row['backend']=='default' else 'FAIL')
sc=[]
for side in ['candidate','base']:
    b=(P/('actors/scanner-'+side+'-v1/findings-private.jsonl')).read_bytes();rows=[json.loads(v) for v in b.splitlines()]
    assert len(rows)==4 and all(v['status']=='warn' for v in rows);sc.append(b)
assert sc[0]==sc[1] and hashlib.sha256(sc[0]).hexdigest()=='3e395b68d67db3da11b295c81cecba7a5aa61024d83c91236dab74ac1f087f29'
assert expected['actors/scanner-candidate-v1/findings-private.jsonl']['sha256']==expected['actors/scanner-base-v1/findings-private.jsonl']['sha256']=='3e395b68d67db3da11b295c81cecba7a5aa61024d83c91236dab74ac1f087f29'

allow=read('safe-exports-v2/ALLOWLIST.json');assert len(allow['entries'])==8
for row in allow['entries']:
    got=receipt(P/'safe-exports-v2'/row['path']);assert all(got[k]==row[k] for k in ['sha256','bytes','mode'])
sealer=read('actors/seal-v2/terminal.json');post=read('actors/postexit-v2/terminal.json')
assert sealer['actual_exit_code']==post['actual_exit_code']==0 and sealer['original_Popen_pid']==210629 and post['original_Popen_pid']==211180
posttext=(P/'actors/postexit-v2/stdout.txt').read_text();postout=json.loads(posttext)
assert postout['status']=='PASS_SEPARATE_READ_ONLY_POSTEXIT_ALL_PAYLOAD_AND_EXTERNAL_HASHES'
assert postout['external']['tracked_sources']==29788 and postout['external']['provider_bytes_hashed'] is True and postout['external']['source_bytes_hashed'] is True
assert postout['manifest_entries']==1045 and postout['zero_live_package_data_consumers'] is True

report={'status':'ROOT_AUTHENTICATED_INDEPENDENT_BF1188_PACKET_PENDING_EXACT_SDK_TOOL_CHAIN',
 'issue':1188,'final_author_source':final,'tested_candidate':src['roots']['candidate']['head'],'actual_parent':src['roots']['base']['head'],
 'pins':PINS,'manifest_entries':1045,'payload_bytes':162253383,'source_counts':source_counts,
 'providers':6,'copies':24,'copy_bytes':25343300,'distinct_original_and_copy_inodes':30,
 'targeted_candidate_PASS':14,'actual_parent_controls':{'FAIL':7,'PASS':7},'supporting_PASS':397,
 'original_strict_matrix':{'total':151,'PASS':111,'FAIL':40,'writer_readback_PASS_within111':40,'other_PASS_within111':71},
 'four_NEW_same_parent_empty_references':{'Default_PASS':2,'Make_FAIL':2,'residual_issue':1218},
 'scanners_each_WARN':4,'scanners_ordered_full_JSONL_bytes_exact':True,'safe_exports':8,
 'sealer_original_Popen_pid':210629,'sealer_exit_code':0,'postexit_original_Popen_pid':211180,'postexit_exit_code':0,
 'directory_scope':'Three exact non0700 inner-directory receipts are preserved beneath0700 ancestors. Other directories had original mode/owner checks, not complete historical inode/mtime receipts. Root records a NEW full-directory physical snapshot only; no historical continuity is inferred.',
 'new_root_directory_snapshot':directories,
 'limits':['Exact SDK terminal tool captures still required before final acceptance','40 prewriter comparisons remain FAIL; writer-stage passes are distinct groups already inside151','Two Make optional-empty dtype failures remain1218','52 phantom read source-chain attribution is inference; historical individual stacks/absence receipts unavailable','156 earlier missing raw-read denials and16 existing-mkdir noops plus repeated16-probe capability correction remain recorded','Full-suite/corpus/science verdict not claimed'],
 'signed':'Sue'}
q=C/'1188-INDEPENDENT-ROOT-PACKET-AUTHENTICATION.json';fd=os.open(q,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2);f.write('\n')
print(json.dumps({'status':report['status'],'files':1045,'sources':29788,'providers':6,'copies':24,'safe_exports':8,'strict_PASS':111,'strict_FAIL':40,'receipt_sha256':receipt(q)['sha256'],'identifiers_printed':0,'signed':'Sue'}))
