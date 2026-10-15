"""Read-only, aggregate-only historical scan comparison; no imports of lsms_library."""
from pathlib import Path
import ast
import argparse
import collections
import hashlib
import json
import runpy
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-root',type=Path,default=Path('/local/job39919092/release-work/control/runtime/feature-audit'),
                    help='Private Feature audit evidence directory; independent of this archived script location.')
ROOT=parser.parse_args().evidence_root.resolve()
TREE=Path('/local/job39919092/release-work/runtime/corpus')
OLD=Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/bench/feature_audit/results')
OUT=ROOT/'prior-comparison'
OUT.mkdir(exist_ok=True)
paths=[OLD/'2026-06-18/results.jsonl', OLD/'2026-06-19/scan_run1.jsonl',
       OLD/'2026-06-19/scan_perfeature.jsonl',
       TREE/'slurm_logs/2026-06-20_food_acquired_analysis/results.jsonl']
def digest(p):return {'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
def read(p):return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
current=read(ROOT/'results.jsonl')
prior=[read(p) for p in paths]
normalize=runpy.run_path(str(TREE/'bench/feature_audit/cluster.py'))['_normalize']
countries={r['country'] for rows in [current,*prior] for r in rows if r.get('country')}
def key(r):return (r.get('check'),r.get('severity'),r.get('status'),normalize(r.get('detail',''),countries))
old_keys={key(r) for rows in prior for r in rows if r.get('status') in {'warn','fail','error'}}
new_keys=collections.Counter(key(r) for r in current if r.get('status')=='warn')
snapshots=[]
for path,rows in zip(paths,prior):
    snapshots.append({'private_path':str(path),**digest(path),'features':sorted({r['feature'] for r in rows if r.get('feature')}),
        'status_counts':dict(collections.Counter(r.get('status') for r in rows)),
        'phase_counts':dict(collections.Counter(r.get('phase') for r in rows)),
        'explicit_source_sha_and_cold_cache_manifest':'not present in these scan records'})
patterns=[('required_null_content','required declared column'),('null_index_keys','NaN key(s)'),
 ('cluster_grain','projected from HOUSEHOLD grain'),('grain_collapse','is NOT UNIQUE'),
 ('grain_reducer_replay','reduce_to_agreed(on_conflict='),('quantity_screen','percentile of comparable rows'),
 ('population','population universe'),('population','population statement'),
 ('labels_pass_through','UNRENAMED'),('labels_unavailable','-- they curate no such label'),
 ('index_shape_exclusion','divergent index shape'),('caught_country_exception','Failed to load '),
 ('source_fallback','Makefile execution failed'),('source_fallback','No Makefile or script found'),
 ('null_reader_content','read of '),('all_nan_column_drop','dropping all-NaN column'),
 ('canonical_index_alignment','canonical index alignment'),('canonical_index_alignment','dropping extra index level'),
 ('duplicate_collapse','duplicate canonical index'),('residence_missing','no usable residence duration'),
 ('join_missing_cluster','NaN v after _join_v_from_sample'),('price_loss','food_prices(units='),
 ('unit_mapping','dropping '),('unit_mapping','unit label'),('kinship','kinship'),
 ('duplicate_collapse','collaps'),('undeclared_wave_dependency','Data scheme does not contain'),
 ('empty_price_mode','No data for food_prices'),('mover_sentinel','by sentinel')]
categories=collections.Counter()
for r in current:
    if r.get('status')!='warn':continue
    category=r['check'] if r['check']!='runtime_warning' else next((k for k,s in patterns if s in r.get('detail','')),'unclassified_runtime')
    categories[category]+=1
assert not categories['unclassified_runtime'], categories
refs=['1528fa937bcf87f146b3af9ebfdbe18b8f912750','452302adcfdbf3af538ef06375df1ac6c5aaa8a0','7fa05179c8519f44ea329ba6387f5d6d6441a8ea']
def git(*args):return subprocess.check_output(['git',*args],cwd=TREE,text=True)
source={ref:{} for ref in refs}
for ref in refs:
    for file in ['lsms_library/feature.py','lsms_library/quantity_audit.py','lsms_library/null_read_audit.py','lsms_library/population.py']:
        source[ref][file]=git('rev-parse',f'{ref}:{file}').strip()
    for file,names in {'lsms_library/country.py':['_join_v_from_sample','_finalize_result','_enforce_canonical_spellings','_normalize_dataframe_index','_audit_index_collapse'],
                       'lsms_library/transformations.py':['roster_to_characteristics','food_prices_from_acquired','food_quantities_from_acquired']}.items():
        s=git('show',f'{ref}:{file}');tree=ast.parse(s)
        for name in names:
            node=next(n for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==name)
            source[ref][file+'::'+name]=hashlib.sha256(ast.get_source_segment(s,node).encode()).hexdigest()
unchanged_train=[k for k in source[refs[-1]] if source[refs[1]][k]==source[refs[-1]][k]]
assert len(unchanged_train)==len(source[refs[-1]])
corpus=ROOT.parent/'corpus'
shape_features={'assets','community_prices','crop_production','livestock','people_last7days','plot_inputs','plot_labor'}
equal=[];missing=[];changed=[]
for country in json.loads((corpus/'inventory.json').read_text())['countries']:
    c=country['country'];a=json.loads((corpus/c/'shared-baseline.json').read_text())['files'];b=json.loads((corpus/c/'private-final-cache.json').read_text())['files']
    for f in sorted(shape_features.intersection(country['tables'])):
        k='var/'+f+'.parquet'
        if k not in a or k not in b:
            missing.append({'country':c,'feature':f,'shared_present':k in a,'private_present':k in b});continue
        if a[k]['frame']['index_names']==b[k]['frame']['index_names']:equal.append([c,f])
        else:changed.append([c,f])
assert not changed, changed
result={'comparison_kind':'partial historical pattern corroboration; NOT a matched before/after regression baseline',
 'scope_limit':'Prior records cover13,8,13,1features, skip known no-source countries, predate later diagnostics/country additions, and lack this run\'s frozen source/private cold-cache provenance. June20 requested4features but completed only food_acquired; its HANDOFF explicitly says the scan was stopped. No comparable all39features/all549pairs baseline found in searched workspaces.',
 'prior_snapshots':snapshots,'current_raw':digest(ROOT/'results.jsonl'),
 'normalization':'Frozen cluster.py _normalize, with the same union of recorded country names for old and new; match key is(check,severity,status,normalized_detail). Patterns/examples are not published.',
 'current_warning_patterns':len(new_keys),'current_patterns_seen_in_prior_union':sum(k in old_keys for k in new_keys),
 'current_patterns_unmatched_in_prior_union':sum(k not in old_keys for k in new_keys),
 'current_warning_records_in_matched_patterns':sum(v for k,v in new_keys.items() if k in old_keys),
 'warning_category_record_counts':dict(categories),'emitter_source_hashes':source,
 'all_listed_emitters_unchanged_pretrain_to_frozen':True,
 'shape_exclusion_inputs':{'equal_cached_index_name_pairs':equal,'missing_comparable_country_parquets':missing,'changed_index_name_pairs':changed},
 'no_new_api_builds':True,'no_cache_writes':True,'baseline_is_not_truth':True}
(OUT/'comparison.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:result[k] for k in ['current_warning_patterns','current_patterns_seen_in_prior_union','current_patterns_unmatched_in_prior_union','current_warning_records_in_matched_patterns','warning_category_record_counts']},indent=2))
