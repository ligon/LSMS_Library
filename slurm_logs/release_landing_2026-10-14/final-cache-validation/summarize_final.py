from pathlib import Path
import collections,json,os
P=Path(__file__).parent/'run'
counts=collections.Counter(); records=[]; active=[]; complete=[]; changes=[]; cache_grades=collections.Counter(); final_grades=collections.Counter(); final_nonfresh=[]; issues=[]
wave_grades=collections.Counter(); stale_waves=[]; manifest_gaps=[]; manifest_absent=[]
final_manifest_absent=[]; final_manifest_gaps=[]
inventory=json.loads((P/'inventory.json').read_text())
declared_waves={r['country']:set(r['waves']) for r in inventory['countries']}
shared_country_grades=collections.Counter(); file_totals=collections.Counter()
for f in sorted(P.glob('*/results.jsonl')):
    for line in f.read_text().splitlines():
        d=json.loads(line);records.append(d);counts[d['status']]+=1
        for key in ('measurement_error','cache_measurement_error'):
            if key in d: issues.append({'country':d['country'],'table':d['table'],'kind':key,'error':d[key]})
        cc=d.get('country_cache')
        cache_grades[cc['freshness'] if cc else 'no_country_parquet']+=1
        if cc:
            if cc['footer_waves'] is None: manifest_absent.append({'country':d['country'],'table':d['table']})
            elif not set(d['declared_waves']) <= set(cc['footer_waves']): manifest_gaps.append({'country':d['country'],'table':d['table'],'declared':d['declared_waves'],'footer':cc['footer_waves']})
        for wave, metadata in d.get('wave_caches',{}).items():
            wave_grades[metadata['freshness']]+=1
            if metadata['freshness']=='stale': stale_waves.append({'country':d['country'],'table':d['table'],'wave':wave,'stored_hash':metadata['stored_hash'],'expected_hash':metadata['expected_hash']})
for f in sorted(P.glob('*/status.json')):
    d=json.loads(f.read_text())
    if d['stage']!='complete':active.append(d);continue
    complete.append(d)
    for kind in ['corpus-baseline','private-final-cache']:
        snapshot=json.loads((f.parent/(kind+'.json')).read_text())['files']
        file_totals[kind]+=len(snapshot)
        for key,r in snapshot.items():
            if r.get('error') or not r.get('stable_during_read',True):issues.append({'country':f.parent.name,'kind':kind,'file':key,'error':r.get('error'),'stable':r.get('stable_during_read')})
            if kind=='corpus-baseline' and key.startswith('var/') and key.endswith('.parquet'): shared_country_grades[r.get('freshness','read_error')]+=1
    for key,r in json.loads((f.parent/'private-final-cache.json').read_text())['files'].items():
        if key.startswith('var/') and key.endswith('.parquet'):
            grade=r.get('freshness','read_error');final_grades[grade]+=1
            if grade!='fresh':final_nonfresh.append({'country':f.parent.name,'file':key,'freshness':grade,'country_read_policy':r.get('country_read_policy',grade),'expected_hash':r.get('expected_hash'),'stored_hash':r.get('stored_hash')})
            if r.get('footer_waves') is None:final_manifest_absent.append({'country':f.parent.name,'file':key})
            elif not declared_waves[f.parent.name] <= set(r['footer_waves']):final_manifest_gaps.append({'country':f.parent.name,'file':key,'declared':sorted(declared_waves[f.parent.name]),'footer':r['footer_waves']})
    before=json.loads((f.parent/'corpus-baseline.json').read_text())['files']
    after=json.loads((f.parent/'private-final-cache.json').read_text())['files']
    for path in sorted(set(before)|set(after)):
        a=before.get(path);b=after.get(path)
        if a is None or b is None:
            changes.append({'country':f.parent.name,'path':path,'kind':'new' if a is None else 'not_rebuilt'});continue
        if 'error' in a or 'error' in b:continue
        if path.endswith('.parquet'):
            old=a['frame'];new=b['frame']
            if old['content_hash']!=new['content_hash'] or old['shape']!=new['shape'] or old['dtypes']!=new['dtypes']:
                changes.append({'country':f.parent.name,'path':path,'kind':'changed','before_shape':old['shape'],'after_shape':new['shape'],
                    'before_hash':old['content_hash'],'after_hash':new['content_hash'],
                    'before_dtypes':old['dtypes'],'after_dtypes':new['dtypes'],
                    'numeric_sum_changes':{k:[old.get('numeric_sums',{}).get(k),v] for k,v in new.get('numeric_sums',{}).items() if old.get('numeric_sums',{}).get(k)!=v}})
        elif a.get('sha256')!=b.get('sha256'):
            changes.append({'country':f.parent.name,'path':path,'kind':'json_changed'})
summary={'source_sha':os.environ['LSMS_FINAL_SHA'],'expected_pairs':549,'completed_pairs':len(records),'counts':dict(counts),
         'completed_countries':[d['country'] for d in complete], 'completed_country_count':len(complete),'active':active,'country_cache_grades':dict(cache_grades),'final_country_cache_grades':dict(final_grades),'final_nonfresh':final_nonfresh,
         'wave_cache_grades':dict(wave_grades),'stale_wave_caches':stale_waves,'country_manifest_gaps':manifest_gaps,'country_manifest_absent':manifest_absent,
         'final_country_manifest_absent':final_manifest_absent,'final_country_manifest_gaps':final_manifest_gaps,
         'shared_country_cache_grades':dict(shared_country_grades),'snapshot_file_totals':dict(file_totals),
         'non_success':[{'country':r['country'],'table':r['table'],'status':r['status'],'error':r.get('error')} for r in records if r['status']!='success'],
         'harness_issues':issues, 'worktree_issues':[d for d in complete if d['git_status'] or d['in_tree_parquets']]}
(P/'progress.json').write_text(json.dumps(summary,indent=2)+'\n')
(P/'cache-comparison.json').write_text(json.dumps(changes,indent=2)+'\n')
print(json.dumps({k:summary[k] for k in ['completed_pairs','counts','completed_country_count','active','country_cache_grades','final_country_cache_grades','final_nonfresh','wave_cache_grades','stale_wave_caches','country_manifest_gaps','country_manifest_absent','harness_issues','worktree_issues']},indent=2))
print('KNOWN_UNAVAILABLE_COUNTS',dict(collections.Counter(r['country'] for r in summary['non_success'] if r['country'] in {'Armenia','Nepal'})))
peru_unwired={'individual_education','food_acquired','interview_date','food_expenditures','food_prices','food_quantities'}
print('KNOWN_UNWIRED_COUNTS',dict(collections.Counter(r['country'] for r in summary['non_success'] if r['country']=='Peru' and r['table'] in peru_unwired)))
print('NON_SUCCESS_OTHER', [r for r in summary['non_success'] if r['country'] not in {'Armenia','Nepal'} and not (r['country']=='Peru' and r['table'] in peru_unwired)])
print('CACHE_DIFF_COUNTS',dict(collections.Counter((d['country']+':'+d['kind']) for d in changes)))
