"""Repopulate and verify exactly eight previously skipped invariance entries.

Run from frozen integration checkout with PYTHONPATH/countries/data env pinned.
Uses a new physical Uganda copy and preserves copied wave files in a backup.
No fixtures, source, shared L2, or existing corpus caches are changed.
"""
from pathlib import Path
import hashlib, importlib.util, json, os, shutil, subprocess, sys, time
import xml.etree.ElementTree as ET

ROOT=Path('/local/job40255479/worktrees/framework-integration')
SOURCE=Path('/local/job40255479/corpus-baseline-data')
DATA=Path('/local/job40255479/uganda-invariance-review-data')
BACKUP=Path('/local/job40255479/uganda-invariance-review-copied-waves')
CONTROL=Path('/local/job40255479/control/framework-campaign')
REPORT=Path('/local/job40255479/worktrees/framework-reliability/slurm_logs/framework_campaign_2026-10-20/reliability/uganda-wave-supplement.json')
WAVES=['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']
EXPECTED_SHA='852ac2bfcd5f798533f56e7efa1f77db80c0138f'
assert Path.cwd().resolve()==ROOT
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==EXPECTED_SHA
assert Path(os.environ['LSMS_DATA_DIR'])==DATA
assert Path(os.environ['LSMS_COUNTRIES_ROOT'])==ROOT/'lsms_library/countries'
assert not DATA.exists() and not BACKUP.exists(), 'requires new private roots'
assert (Path('/local/job40255479/corpus-baseline-out')/'completion.json').exists()
source_files=sorted(p for p in (SOURCE/'Uganda').rglob('*') if p.is_file())
source_hashes={str(p.relative_to(SOURCE)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
DATA.mkdir()
shutil.copytree(SOURCE/'Uganda',DATA/'Uganda',copy_function=shutil.copy2,symlinks=True)
for rel,h in source_hashes.items():
    copied=DATA/rel
    assert hashlib.sha256(copied.read_bytes()).hexdigest()==h,rel
    assert (copied.stat().st_dev,copied.stat().st_ino)!=((SOURCE/rel).stat().st_dev,(SOURCE/rel).stat().st_ino),rel
    assert copied.stat().st_ino!=(SOURCE/rel).stat().st_ino,rel
l1=(SOURCE/'dvc-cache').resolve()
assert l1.is_dir()
(DATA/'dvc-cache').symlink_to(l1,target_is_directory=True)
assert not any(p.is_symlink() for p in (DATA/'Uganda').rglob('*'))
BACKUP.mkdir()
for w in WAVES:
    f=DATA/'Uganda'/w/'_'/'food_acquired.parquet'
    assert f.exists(),f
    f.rename(BACKUP/(w+'.parquet'))
assert not any((DATA/'Uganda'/w/'_'/'food_acquired.parquet').exists() for w in WAVES)
import lsms_library as ll
from lsms_library.paths import data_root,countries_root
assert Path(ll.__file__).resolve().is_relative_to(ROOT),ll.__file__
assert Path(data_root()).resolve()==DATA
assert Path(countries_root()).resolve()==ROOT/'lsms_library/countries'
spec=importlib.util.spec_from_file_location('supplement_gate',ROOT/'tests/conftest.py')
gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
assert gate.aws_creds_available(), 'requires_s3 gate must be open, no credential skips'
uga=ll.Country('Uganda',preload_panel_ids=False)
assert list(uga.waves)==WAVES
started=time.monotonic();built=[]
for w in WAVES:
    tick=time.monotonic()
    frame=uga[w].food_acquired()
    f=DATA/'Uganda'/w/'_'/'food_acquired.parquet'
    assert f.exists(),f
    assert len(frame)>0,w
    built.append({'wave':w,'rows':len(frame),'columns':len(frame.columns),'seconds':round(time.monotonic()-tick,3),'parquet_recreated':True})
print('POPULATION',json.dumps({'source_files':len(source_files),'copy_sha256_equal':True,'distinct_inodes':True,'builds':built}),flush=True)
ids=[f'tests/test_uganda_invariance.py::test_parquet_matches_baseline[{w}/_/food_acquired.parquet]' for w in WAVES]
xml=CONTROL/'uganda-wave-supplement.xml'
with (CONTROL/'uganda-wave-supplement-pytest.log').open('w') as log:
    run=subprocess.run([sys.executable,'-m','pytest','-q','-ra','--disable-warnings',f'--junitxml={xml}',*ids],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
r=ET.parse(xml).getroot();cases=list(r.iter('testcase'))
summary={'source':EXPECTED_SHA,'data_root':str(DATA),'source_copy_root':str(SOURCE),'shared_only':'L1 dvc-cache symlink','copied_files':len(source_files),'copy_bytes_sha256_equal':True,'copy_distinct_inodes':True,'wave_copies_preserved_in_private_backup':True,'population':built,'pytest_exit_code':run.returncode,'tests':len(cases),'failed':sum(c.find('failure') is not None for c in cases),'errors':sum(c.find('error') is not None for c in cases),'skipped':sum(c.find('skipped') is not None for c in cases),'seconds':round(time.monotonic()-started,3),'case_ids':[c.get('classname')+'::'+c.get('name') for c in cases],'fixture_or_baseline_edits':False}
summary['source_copy_unchanged']=all(hashlib.sha256((SOURCE/rel).read_bytes()).hexdigest()==h for rel,h in source_hashes.items())
REPORT.write_text(json.dumps(summary,indent=2)+'\n')
print('SUPPLEMENT',json.dumps(summary),flush=True)
assert run.returncode==0 and len(cases)==8 and summary['failed']==summary['errors']==summary['skipped']==0,summary
assert summary['source_copy_unchanged']
