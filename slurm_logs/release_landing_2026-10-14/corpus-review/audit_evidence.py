"""Read saved corpus artifacts and Git objects; never open/build a survey cache."""
import ast, collections, hashlib, json, subprocess
from pathlib import Path
from xml.etree import ElementTree as ET
P = Path('/local/job39919092/release-work/control/runtime/corpus')
OUT = Path(__file__).resolve().parent
REPO = Path('/local/job39919092/mirrors/LSMS_Library')
SOURCE = '7fa05179c8519f44ea329ba6387f5d6d6441a8ea'
TEST_SOURCE = '4f1f044bb9b3079e7f427898f77627ad1503ec5e'
def git(*args):
    return subprocess.check_output(['git', '-C', str(REPO), *args])
def get(path): return json.loads(path.read_text())
inv=get(P/'inventory.json')
expected={(c['country'], t) for c in inv['countries'] for t in c['tables']}
rows=[json.loads(line) for f in P.glob('*/results.jsonl') for line in f.read_text().splitlines()]
actual=[(r['country'],r['table']) for r in rows]
assert len(actual)==len(set(actual))==len(expected)==549 and set(actual)==expected
assert all(r['source_sha']==SOURCE for r in rows)
assert not any('measurement_error' in r or 'cache_measurement_error' in r for r in rows)
counts=collections.Counter(r['status'] for r in rows)
assert counts=={'success':530,'error':19}
errors=collections.Counter(r['country'] for r in rows if r['status']=='error')
assert errors=={'Armenia':2,'Nepal':11,'Peru':6}
completion=get(P/'completion.json')
assert not completion['inventory_errors']
assert len(completion['processes'])==38
assert {r['country'] for r in completion['processes'] if r['exit_code']}==set(errors)
assert all(r['exit_code']==(1 if r['country'] in errors else 0) for r in completion['processes'])
final_country=[]
for c in inv['countries']:
    country=c['country']; ident=get(P/country/'identity.json'); status=get(P/country/'status.json')
    assert ident['source_sha']==SOURCE
    content=git('show',f'{SOURCE}:lsms_library/countries/{country}/_/CONTENTS.org')
    assert hashlib.sha256(content).hexdigest()==ident['contents_sha256']
    assert status['stage']=='complete' and status['git_status']=='' and status['in_tree_parquets']==[]
    for path, rec in get(P/country/'private-final-cache.json')['files'].items():
        assert rec['stable_during_read'] and 'error' not in rec
        if path.startswith('var/') and path.endswith('.parquet'):
            final_country.append({'country':country,'path':path,**rec})
nonfresh=[r for r in final_country if r['freshness']!='fresh']
assert {(r['country'],r['path']) for r in nonfresh}=={('Uganda','var/fct.parquet'),('Uganda','var/earnings.parquet')}
baseline=json.loads(git('show',f'{SOURCE}:tests/fixtures/uganda_baseline.json'))
fields=['shape','columns','index_names','dtypes','content_hash']
for r in nonfresh:
    assert all(r['frame'][k]==baseline[r['path']][k] for k in fields)
    assert r['stored_hash'] is None and r['footer_waves'] is None and r['country_read_policy']=='stale'
old=get(P/'Tajikistan/shared-baseline.json')['files']; new=get(P/'Tajikistan/private-final-cache.json')['files']
assert old['2007/_/plot_features.parquet']['frame']==new['2007/_/plot_features.parquet']['frame']==new['var/plot_features.parquet']['frame']
assert old['var/plot_features.parquet']['frame']['numeric_sums']['Area']/100==new['var/plot_features.parquet']['frame']['numeric_sums']['Area']
assert git('diff','v0.14.0',SOURCE,'--','lsms_library/countries/Tajikistan/2007/_/plot_features.py')==b''
subprocess.run(['git','-C',str(REPO),'merge-base','--is-ancestor','dedb1c7a7','v0.14.0'],check=True)
weights={}
for c in ['China','Kazakhstan','Pakistan','Peru']:
    rec=get(P/c/'private-final-cache.json')['files']['var/sample.parquet']
    assert rec['freshness']=='fresh' and 'weight' not in rec['frame']['columns']
    weights[c]={k:rec['frame'][k] for k in ['shape','columns','content_hash']}
xmls={};skipnodes=[]
for group in ['ordinary','structural','uganda_baseline']:
    f=Path('/local/job39919092/release-work/logs/cache-coverage')/(group+'.xml')
    cases=list(ET.parse(f).getroot().iter('testcase')); outcomes=collections.Counter()
    for case in cases:
        skip=case.find('skipped');fail=case.find('failure');err=case.find('error')
        kind='failure' if fail is not None else 'error' if err is not None else 'xfail' if skip is not None and skip.get('type')=='pytest.xfail' else 'skipped' if skip is not None else 'passed'
        outcomes[kind]+=1
        if group=='ordinary' and skip is not None:
            skipnodes.append({'node':case.get('classname')+'::'+case.get('name'),'reason':skip.get('message')})
    xmls[group]={'counts':dict(outcomes),'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}
assert xmls['ordinary']['counts']=={'passed':15,'skipped':18}
assert xmls['structural']['counts']=={'passed':2352,'skipped':150,'xfail':5}
assert xmls['uganda_baseline']['counts']=={'passed':30,'failure':1}
source=ast.parse(git('show',f'{TEST_SOURCE}:tests/test_food_prices_dtype.py').decode())
column_lists={n.targets[0].id:ast.literal_eval(n.value) for n in source.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ['_PRICE_COLS','_QUANTITY_COLS']}
served={r['table']:r for r in rows if r['country']=='Uganda' and r['table'] in ['food_expenditures','food_prices','food_quantities','household_characteristics']}
for t in ['food_expenditures','food_prices','food_quantities','household_characteristics']:
    assert served[t]['status']=='success' and served[t]['country_cache'] is None
assert not set(column_lists['_PRICE_COLS']) & set(served['food_prices']['served']['columns'])
assert not set(column_lists['_QUANTITY_COLS']) & set(served['food_quantities']['served']['columns'])
progress=get(P/'progress.json')
assert not progress['harness_issues'] and not progress['worktree_issues']
assert not progress['final_country_manifest_gaps']
summary={'corpus_source':SOURCE,'gap_test_source':TEST_SOURCE,'countries':38,'pairs':549,'outcomes':dict(counts),'error_countries':dict(errors),'success_return_types':dict(collections.Counter(r['return_type'] for r in rows if r['status']=='success')),'all_country_contents_hashes_match':True,'all_worker_status_clean_complete':True,'all_final_snapshot_reads_stable':True,'final_country_cache_grades':dict(collections.Counter(r['freshness'] for r in final_country)),'hashless_country_caches_matching_baseline':[{k:r[k] for k in ['country','path','stored_hash','footer_waves','country_read_policy','frame']} for r in nonfresh],'tajikistan':{'old_area_sum':88238.0,'new_area_sum':882.38,'exact_old_new_wave_and_new_country_fingerprint':True,'script_unchanged_since_v014':True,'runtime_exact_transformation_record':get(P/'tajikistan-area-comparison.json'),'independent_frame_replay':False},'sample_weight_absence':weights,'xml_results':xmls,'ordinary_skip_nodes':skipnodes,'derived_outputs':{k:{'shape':v['served']['shape'],'columns':v['served']['columns'],'index_names':v['served']['index_names'],'dtypes':v['served']['dtypes'],'country_cache':v['country_cache']} for k,v in served.items()},'dtype_test_expected_columns':column_lists,'dtype_test_column_intersections_empty':True,'limits':['No original corpus root access or survey API calls','Snapshot evidence review, not independent raw-source scientific audit or all changed-frame replays','Feature audit and mutating checks owned by other reviewers','Uganda nutrition baseline scientific hold retained']}
(OUT/'validation.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:summary[k] for k in ['countries','pairs','outcomes','error_countries','final_country_cache_grades','xml_results']},indent=2))
