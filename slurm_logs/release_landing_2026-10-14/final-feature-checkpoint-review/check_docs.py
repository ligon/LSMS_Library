"""Read-only exact frozen diff and archive consistency check."""
import csv
import hashlib
import io
import json
from pathlib import Path
import subprocess

HERE=Path(__file__).resolve().parent
DOCS=Path('/local/job39919092/release-work/release-docs')
CONTROL=Path('/local/job39919092/release-work/control')
ARCHIVE=DOCS/'slurm_logs/release_landing_2026-10-14'
BASE='ae890a48f16f9cdb1b7b857af9d89eadf463265e'
DIFF='b43f338f32420244e02dc349f7fe6efae3d94781a924f561fb14702a42f390f5'

def git(*args):
    return subprocess.check_output(['git','-C',str(DOCS),*args])

def digest(p):
    b=p.read_bytes()
    return {'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}

assert git('rev-parse','HEAD').decode().strip()==BASE
diff=git('diff','--no-ext-diff','--no-color')
assert hashlib.sha256(diff).hexdigest()==DIFF
assert diff==(CONTROL/'feature-docs-validation/frozen-reviewed.diff').read_bytes()
assert not git('diff','--cached','--name-only').strip()
assert not git('ls-files','--others','--exclude-standard').strip()
paths=git('diff','--name-only').decode().splitlines()
allowed={'.sucoder/handoff.org','docs/releases/v0.15.0.md','lsms_library/countries/Panama/_/CONTENTS.org'}
assert len(paths)==52
assert all(p in allowed or p.startswith('slurm_logs/release_landing_2026-10-14/') for p in paths)
assert not git('diff','--check').strip()

core_source=CONTROL/'runtime/feature-audit/publishable'
core_dest=ARCHIVE/'feature-audit'
source_manifest=json.loads((core_source/'MANIFEST.json').read_text())
dest_manifest=json.loads((core_dest/'MANIFEST.json').read_text())
assert dest_manifest['source_manifest_sha256']==digest(core_source/'MANIFEST.json')['sha256']
assert set(source_manifest['files'])==set(dest_manifest['files'])
transforms=[]
for name, record in dest_manifest['files'].items():
    before=core_source/name;after=core_dest/name
    assert digest(after)=={k:record[k] for k in ('bytes','sha256')}
    assert digest(before)==source_manifest['files'][name]
    if name.endswith('.tsv'):
        assert after.read_bytes()==before.read_bytes().replace(b'\r\n',b'\n')
        a=list(csv.reader(io.StringIO(before.read_text()),delimiter='\t'))
        b=list(csv.reader(io.StringIO(after.read_text()),delimiter='\t'))
        assert a==b and len(a)==record['rows_including_header']
        assert digest(before)=={'bytes':record['source_bytes'],'sha256':record['source_sha256']}
        transforms.append({'file':name,'rows':len(a),'source':digest(before),'archived':digest(after)})
    else:assert before.read_bytes()==after.read_bytes()
assert len(transforms)==5
assert set(p.name for p in core_dest.iterdir())==set(dest_manifest['files'])|{'MANIFEST.json'}

for source,dest in [(CONTROL/'runtime/feature-audit/prior-comparison',ARCHIVE/'feature-prior-comparison'),
                    (CONTROL/'countries/feature-audit-review',ARCHIVE/'feature-audit-review'),
                    (CONTROL/'framework/1010-publishable',ARCHIVE/'test-isolation-1010'),
                    (CONTROL/'countries/1010-review',ARCHIVE/'test-isolation-1010-review')]:
    for item in dest.iterdir():
        if item.name=='archive-manifest.json':
            records=json.loads(item.read_text())['files']
            for name,record in records.items():assert digest(dest/name)==record
        else:assert item.read_bytes()==(source/item.name).read_bytes(), item

build=json.loads((ARCHIVE/'feature-docs-build.json').read_text())
assert build['exit_code']==0 and build['source_identity_asserted'] and build['empty_config_and_data_roots'] and build['AWS_environment_scrubbed']
assert digest(CONTROL/'feature-docs-validation/docs.log')['sha256']==build['log_sha256']
assert 'Documentation built in' in (CONTROL/'feature-docs-validation/docs.log').read_text()
candidate=json.loads((ARCHIVE/'validation-candidate-1010.json').read_text())
assert git('diff','--name-only',candidate['previous_integration'],candidate['corrected_candidate']).decode().splitlines()==candidate['changed_paths']
for name in candidate['changed_paths']:
    assert git('show',candidate['corrected_candidate']+':'+name)==git('show',candidate['implementation_tested']+':'+name)
assert git('rev-parse','bf917d438^{tree}')==git('rev-parse',candidate['corrected_candidate']+'^{tree}')
out={'verdict':'APPROVE exact frozen documentation/evidence checkpoint; pending release/scientific/CI/shared-cache gates retained',
     'base':BASE,'diff_sha256':DIFF,'diff_bytes':len(diff),'changed_paths':paths,
     'all_changes_documentation_or_archival_evidence':True,'archive_transforms':transforms,
     'archive_sources_byte_identical_except_five_TSV_line_endings':True,
     'source_core_manifest':digest(core_source/'MANIFEST.json'),'archived_core_manifest':digest(core_dest/'MANIFEST.json'),
     'strict_docs_build_record_verified':build,'candidate_source_equivalence_verified':True,
     'reviewed_files':{p:digest(DOCS/p) for p in paths}}
(HERE/'docs-validation.json').write_text(json.dumps(out,indent=2)+'\n')
(HERE/'reviewed.diff').write_bytes(diff)
print(json.dumps({k:v for k,v in out.items() if k in ('verdict','base','diff_sha256','diff_bytes','source_core_manifest','archived_core_manifest')},indent=2))
