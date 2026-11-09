"""PRIVATE GH1066 proposal: stored audit replay, pending maintainer choice.

Fresh interpreters, tiny invented parquets, actual normal/assumed read gates.
No normal Country constructor, country metadata, source/provider or build.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Existing fresh-process testing pattern from test_gh1045_parse_datetime.
_CHILD = r'''
import json,os,sys,warnings,hashlib
from pathlib import Path
from types import SimpleNamespace,MethodType
import pandas as pd
import lsms_library
import lsms_library.country as C
from lsms_library import local_tools as L
from lsms_library import datetime_parse as DP
from lsms_library.paths import data_root,countries_root
cfg=json.loads(sys.argv[1]);root=Path(cfg['checkout']).resolve()
assert Path.cwd().resolve()==root
assert Path(lsms_library.__file__).resolve().is_relative_to(root)
assert countries_root().resolve()==root/'lsms_library/countries'
assert data_root().resolve()==Path(os.environ['LSMS_DATA_DIR']).resolve()
route=cfg['route'];case=cfg['case'];branch=cfg['branch'];name='SyntheticReplay';table='updated_ids' if case=='json' else 'housing';key=(name,table)
frame=pd.DataFrame({'Walls':['A','B','C','D','E']},index=pd.MultiIndex.from_arrays([pd.array(['w1','w2',None,'outside','w1'],dtype='string'),pd.array(['h1','h2','h3','h4',None],dtype='string')],names=['t','i']))
report=lambda wave:{'levels':['t','i','detail'],'rows':3,'dropped':1,'destroyed':1,'conflicting_groups':1,'nan_key_rows':0,**({'wave':wave} if wave is not None else {})}
grain=[report(w) for w in ['w1','w2','folder1',None]]
dates=[{'wave':w,'column':'Int_t','n':1,'kind':'components','calendar':'gregorian','sentinel':1,'parsed':0} for w in ['w1','w2',None]]
if case in ['no_audits','strict_date']:grain=None
if case=='no_audits':dates=None
if case=='unrelated_strict':grain=[report('w2')]
request=['w1'] if branch=='covered' else ['w1','w2']
if case=='legacy_subset':request=['w1']
if case=='uncovered':request=['not-covered']
path=data_root(name)/'var'/(table+'.parquet');path.parent.mkdir(parents=True,exist_ok=True)
meta={'cache_hash':'expected' if route=='normal' else 'deliberately-not-checked','grain_audit':grain,'datetime_audit':dates}
if branch=='covered':meta['cache_waves']=['w1','w2']
if route=='assumed':meta['build_drops']=[{'wave':'w1','file':'SyntheticReplay/w1/Data/never-created.csv','kind':'unavailable','sub':'missing','missing_required':['Extra']}]
if case!='absent':L.to_parquet(frame,path,absolute_path=True,**meta)
before=None if not path.exists() else (hashlib.sha256(path.read_bytes()).hexdigest(),path.stat().st_mtime_ns,path.stat().st_size)
calls=[]
def forbidden(*a,**k):raise AssertionError('unexpected build/hash/probe/write')
def expected(*a,**k):calls.append('expected-hash');return 'expected'
c=SimpleNamespace(name=name,waves=['w1','w2'],wave_folder_map={'w1':'folder1','w2':'folder2'},assume_cache_fresh=True)
for method in ['_restrict_to_waves','_grain_reports_for_waves','_cache_covers_waves']:setattr(c,method,MethodType(getattr(C.Country,method),c))
c._table_cache_hash=expected if route=='normal' else forbidden
C._probe_source=forbidden;C._probe_source_fast=forbidden;C.Wave.grab_data=forbidden
L._ensure_dvc_pulled=forbidden;L.to_parquet=forbidden;C.to_parquet=forbidden;L.stamp_parquet_hash=forbidden;C.stamp_parquet_hash=forbidden
if route=='assumed':C.cache_freshness=forbidden;C._degraded_cache_verdict=forbidden
# Like a previous call in the same process: the successful hit must describe
# THIS read only and preserve another table's ledger. No stale evidence leak.
C._GRAIN_LEDGER[key]=[{'stale':True}];C._DATETIME_LEDGER[key]=[{'stale':True}]
C._GRAIN_LEDGER[(name,'unrelated')]=[{'unrelated':True}]
if case=='race':
 def gone(*a,**k):raise FileNotFoundError('invented disappearance after footer')
 C.get_dataframe=gone
if case in ['strict_grain','unrelated_strict']:os.environ['LSMS_GRAIN_STRICT']='1'
if case in ['strict_grain','strict_date']:os.environ['LSMS_DATE_STRICT']='1'
error=None;out=None
with warnings.catch_warnings(record=True) as caught:
 warnings.simplefilter('always')
 try:
  if route=='normal':
   # Exact existing normal caller initialization before its actual read gate.
   C._GRAIN_LEDGER.pop(key,None);C._DATETIME_LEDGER.pop(key,None)
   out,covered,reason=C.Country._read_l2_country_cache(c,table,path,request)
   assert reason is None
  else:out=C.Country._read_assumed_fresh_cache(c,table,request)
 except (C.GrainCollapseError,DP.DatetimeParseError) as e:error=type(e).__name__
after=None if not path.exists() else (hashlib.sha256(path.read_bytes()).hexdigest(),path.stat().st_mtime_ns,path.stat().st_size)
assert after==before
assert C._GRAIN_LEDGER[(name,'unrelated')]==[{'unrelated':True}]
clean=lambda x:None if pd.isna(x) else x
print(json.dumps({'route':route,'case':case,'branch':branch,'package':lsms_library.__file__,'error':error,'hit':out is not None,'tuples':None if out is None else [[clean(v) for v in row] for row in out.index.tolist()],'values':None if out is None else out['Walls'].tolist(),'dtypes':None if out is None else list(map(str,out.dtypes)),'grain_warnings':[str(w.message) for w in caught if issubclass(w.category,C.GrainCollapseWarning)],'grain_reports':C.grain_reports(name,table),'date_warnings':[str(w.message) for w in caught if issubclass(w.category,DP.DatetimeParseWarning)],'date_reports':DP.datetime_reports(name,table),'hash_calls':calls,'cache_unchanged':True,'unrelated_ledger_preserved':True}))
'''


def probe(tmp_path, *, route='assumed', branch='covered', case='audits', checkout=ROOT):
    private = tmp_path / (route + '-' + branch + '-' + case)
    for sub in ('data', 'config', 'cache', 'tmp'):
        (private / sub).mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(('AWS_', 'LSMS_')) or key in ('DIAGNOSIS_OBSERVATIONS',):
            env.pop(key, None)
    env.update(PYTHONPATH=str(checkout), PYTHONDONTWRITEBYTECODE='1',
               LSMS_COUNTRIES_ROOT=str(checkout / 'lsms_library/countries'),
               LSMS_DATA_DIR=str(private / 'data'), LSMS_CONFIG_DIR=str(private / 'config'),
               LSMS_S3_CREDS=str(private / 'absent-reader'), LSMS_S3_WRITE_CREDS=str(private / 'absent-writer'),
               LSMS_SKIP_AUTH='1', AWS_EC2_METADATA_DISABLED='true',
               AWS_SHARED_CREDENTIALS_FILE=str(private / 'absent-aws'), AWS_CONFIG_FILE=str(private / 'absent-aws-config'),
               DVC_SITE_CACHE_DIR=str(private / 'cache/dvc-site'),
               XDG_CONFIG_HOME=str(private / 'config'), XDG_CACHE_HOME=str(private / 'cache'),
               MPL_IGNORE_SYSTEM_FONTS='1', MPLCONFIGDIR=str(private / 'cache/mpl'),
               OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', LSMS_BUILD_WORKERS='1',
               TMPDIR=str(private / 'tmp'))
    args = dict(route=route, branch=branch, case=case, checkout=str(checkout))
    result = subprocess.run([sys.executable, '-c', _CHILD, json.dumps(args)],
                            cwd=checkout, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize('branch', ['covered', 'legacy'])
def test_stored_warning_and_ledgers_match_normal_read(tmp_path, branch):
    normal = probe(tmp_path, route='normal', branch=branch)
    assumed = probe(tmp_path, branch=branch)
    for key in ('error', 'hit', 'tuples', 'values', 'dtypes', 'grain_warnings',
                'grain_reports', 'date_warnings', 'date_reports'):
        assert assumed[key] == normal[key], key
    assert len(assumed['grain_warnings']) == (3 if branch == 'covered' else 4)
    assert len(assumed['date_warnings']) == (2 if branch == 'covered' else 3)
    assert all(r['from_cache'] for r in assumed['grain_reports'])
    assert assumed['hash_calls'] == [] and normal['hash_calls'] == ['expected-hash']
    assert assumed['cache_unchanged'] and assumed['unrelated_ledger_preserved']
    if branch == 'covered':
        assert assumed['tuples'] == [['w1', 'h1'], ['w1', None]]
    else:
        assert len(assumed['tuples']) == 5
        assert [None, 'h3'] in assumed['tuples'] and ['outside', 'h4'] in assumed['tuples']


@pytest.mark.parametrize('branch', ['covered', 'legacy'])
def test_missing_audit_keys_clear_old_table_evidence_without_inventing_loss(tmp_path, branch):
    out = probe(tmp_path, branch=branch, case='no_audits')
    assert out['hit'] and out['grain_warnings'] == out['grain_reports'] == []
    assert out['date_warnings'] == out['date_reports'] == []
    assert out['hash_calls'] == [] and out['unrelated_ledger_preserved']


@pytest.mark.parametrize('branch', ['covered', 'legacy'])
def test_existing_strict_grain_semantics_precede_date_replay(tmp_path, branch):
    out = probe(tmp_path, branch=branch, case='strict_grain')
    assert out['error'] == 'GrainCollapseError'
    assert len(out['grain_reports']) == 1 and out['grain_reports'][0]['from_cache']
    assert out['date_reports'] == []


@pytest.mark.parametrize('branch', ['covered', 'legacy'])
def test_existing_strict_date_semantics_remain(tmp_path, branch):
    out = probe(tmp_path, branch=branch, case='strict_date')
    assert out['error'] == 'DatetimeParseError' and out['grain_reports'] == []
    assert len(out['date_reports']) == 1 and out['date_reports'][0]['from_cache']


def test_other_wave_loss_does_not_fail_requested_wave_in_strict_mode(tmp_path):
    out = probe(tmp_path, case='unrelated_strict')
    assert out['hit'] and out['error'] is None
    assert out['grain_warnings'] == out['grain_reports'] == []
    assert len(out['date_warnings']) == 2


@pytest.mark.parametrize('case,branch', [('uncovered', 'covered'), ('legacy_subset', 'legacy'), ('absent', 'covered')])
def test_existing_cache_misses_do_not_replay_or_clear_ledgers(tmp_path, case, branch):
    out = probe(tmp_path, case=case, branch=branch)
    assert not out['hit'] and out['error'] is None
    assert out['grain_warnings'] == out['date_warnings'] == []
    assert out['grain_reports'] == out['date_reports'] == [{'stale': True}]


@pytest.mark.parametrize('branch', ['covered', 'legacy'])
def test_existing_disappeared_cache_race_stays_a_miss(tmp_path, branch):
    out = probe(tmp_path, branch=branch, case='race')
    assert not out['hit'] and out['error'] is None
    assert out['grain_warnings'] == out['date_warnings'] == []
    assert out['grain_reports'] == out['date_reports'] == [{'stale': True}]


def test_json_methods_keep_their_existing_exclusion(tmp_path):
    out = probe(tmp_path, case='json')
    assert not out['hit'] and out['grain_warnings'] == out['date_warnings'] == []
    assert out['grain_reports'] == out['date_reports'] == [{'stale': True}]
