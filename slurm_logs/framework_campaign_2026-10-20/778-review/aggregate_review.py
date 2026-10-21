"""Produce aggregate independent-review evidence, never survey rows/messages."""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import xml.etree.ElementTree as ET

import lsms_library as ll
from lsms_library.paths import countries_root, data_root

source=Path('/local/job40255479/worktrees/778-cache-race').resolve()
review=Path('/local/job40255479/control/framework-campaign/review-778')
expected='3c97f20db8ca30c18f80a6aed681c76fc0a82aa3'
assert Path.cwd().resolve()==source
assert Path(ll.__file__).resolve().is_relative_to(source),ll.__file__
assert countries_root().resolve()==source/'lsms_library/countries'
assert data_root().resolve()==Path(os.environ['LSMS_DATA_DIR']).resolve()
assert data_root().resolve().is_relative_to(review)
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==expected
assert not subprocess.check_output(['git','diff','--name-only','HEAD'],text=True)
isolation={}
for name in ['warm','cold']:
    result=json.loads((review/f'private-{name}-isolation.json').read_text())
    unlinks=sum(bool(r.get('outer_root')) for r in result['events'] if 'name' in r)
    assert result['pytest_exit_code']==0
    assert not result['outer_byte_changes'] and not result['outer_file_replacements'] and not unlinks
    assert result['source_bytes_and_stats_unchanged'] and result['root_restored']
    assert result['copy_sha_matches'] and result['copy_distinct_inodes']
    isolation[name]={
        'passed':3 if name=='warm' else 2,'seed_files':result['copied_files'],
        'outer_byte_changes':0,'outer_file_replacements':0,'outer_parquet_unlinks':0,
        'source_bytes_inodes_mtimes_unchanged':True,'outer_data_root_restored':True,
        'physical_seed_copy_checked':name=='warm',
    }
assert isolation['warm']['seed_files']==67 and isolation['cold']['seed_files']==0
producer=source/'slurm_logs/framework_campaign_2026-10-20/778'
before=json.loads((producer/'hashes-before.json').read_text())
after=json.loads((producer/'hashes-after.json').read_text())
independent=json.loads((review/'independent-hashes.json').read_text())
hashes={}
for tier in ['country','wave','build_transforms']:
    assert before[tier]==after[tier]==independent[tier],tier
    hashes[tier]={'entries':len(independent[tier]),'changed':0,
                  'existing_none_entries':sum(v is None for v in independent[tier].values())}
suites=list(ET.parse(review/'hermetic.xml').iter('testsuite'))
assert sum(int(s.get('tests','0')) for s in suites)==62
assert not sum(int(s.get('failures','0'))+int(s.get('errors','0'))+int(s.get('skipped','0')) for s in suites)
negative=(review/'negative-control.log').read_text().strip().splitlines()[-1]
assert negative.startswith('6 failed, 11 passed, 31 deselected')
baseline=json.loads((producer/'isolation-before.json').read_text())
assert len(baseline['outer_file_replacements'])==18
assert json.loads((producer/'verification.json').read_text())['isolation']['baseline']['outer_file_replacements']==18
assert '19 file replacements' in (producer/'REPORT.org').read_text()
report={
    'reviewer':'Sue (independent of implementation)',
    'target_head':expected,'runtime_commit':'2aef1c2ac','base':'852ac2bfcd5f798533f56e7efa1f77db80c0138f',
    'verdict':'PASS for production/tests; nonblocking aggregate-report count correction',
    'production_or_test_defects_found':[],
    'findings':[{'priority':'P3','kind':'evidence_consistency',
        'locations':['slurm_logs/framework_campaign_2026-10-20/778/REPORT.org:33','.coder/ledger/778-cache-race.md:72'],
        'reported_baseline_file_replacements':19,'recorded_baseline_file_replacements':18,
        'supporting_evidence':'verification.json:44 and private isolation-before.json agree on18',
        'requested_correction':'Use18 or explicitly identify a separate19-replacement run; no runtime change needed.'}],
    'independent_tests':{'hermetic_total':62,'existing_gate_controls':48,'additional_controls':14,
        'additional_families':['real sanctioned-reader disappearance full/subset/legacy',
            'restriction/date-replay/finalization missing dependencies propagate',
            'three fixtures physically dereference L2 and restore after normal/error exit',
            'cold Guyana/India fixture setup has no warm prerequisite'],
        'candidate_passes_including_live':67,'candidate_failures':0,'candidate_skips':0,
        'old_gate_negative_control':negative},
    'live_isolation':isolation,'cache_hashes':hashes,
    'exception_contract':'Only FileNotFoundError from the two assumed-fresh get_dataframe calls recovers; mapping, restriction, replay, finalization and ordinary-path errors stay outside those catches.',
    'country_guidance':{
        'Guyana':'Household(ED,SN,HH), cluster(ED,SN); ED repeats across segments.',
        'India':'Employment(t,i,pid,act); per-day wage is not additive; Rural is workplace.',
        'Togo':'One vague per grappe; plot_features is field/parcel grain from Data1/s16a.',
        'Albania':'1996EWS is not a declared wave;2003/2004 are narrower panel subsamples.'},
    'existing_documentation_contradiction':'Guyana CONTENTS:42-44 says a warm collapse warning never fires twice; current stamped grain reports can replay. Cold category-based oracle was preserved, not waived.',
    'unchanged_limits':['#1071 corrupt full/subset assumed-fresh policy','assumed-fresh unstamped script output serve','no partial-aggregate theory established or revived'],
    'identity':{'cwd':str(source),'package':ll.__file__,'countries_root':str(countries_root()),'data_root':str(data_root())},
    'source_country_py_sha256':hashlib.sha256((source/'lsms_library/country.py').read_bytes()).hexdigest(),
    'additional_controls_sha256':hashlib.sha256((review/'test_additional_controls.py').read_bytes()).hexdigest(),
    'all_review_readers_stopped':True,'tracked_source_unchanged':True,
    'raw_logs_and_messages_private':True,
}
(review/'review.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({'verdict':report['verdict'],'passed':67,'findings':report['findings'],'cache_hashes':hashes},indent=2))
