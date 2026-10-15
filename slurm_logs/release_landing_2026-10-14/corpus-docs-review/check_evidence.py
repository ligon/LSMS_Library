from pathlib import Path
import collections,csv,hashlib,json,xml.etree.ElementTree as ET
root=Path('/local/job39919092/release-work/release-docs')
p=root/'slurm_logs/release_landing_2026-10-14'
out=Path(__file__).resolve().parent
summary=json.loads((p/'cache-coverage/cache-coverage-summary.json').read_text())
groups=json.loads((p/'cache-coverage/cache-gap-groups.json').read_text())
original=set().union(*(set(x) for x in groups.values()))
modules={node.split('::')[0].removesuffix('.py').replace('/','.') : node.split('::')[0] for node in original}
modules['tests.test_categorical_mapping_resolution']='tests/test_categorical_mapping_resolution.py'
modules['tests.test_gh866_stale_country_parquet_not_laundered']='tests/test_gh866_stale_country_parquet_not_laundered.py'
counts=collections.Counter()
original_counts=collections.Counter()
node_outcomes={}
evidence=[]
for group in summary:
    for e in group['evidence']:
        data=Path(e['path']).read_bytes()
        assert len(data)==e['bytes']
        assert hashlib.sha256(data).hexdigest()==e['sha256']
        evidence.append({'path':e['path'],'sha256_matches':True})
        if not e['path'].endswith('.xml'): continue
        for case in ET.fromstring(data).findall('.//testcase'):
            name=case.attrib['classname']
            mod=next(m for m in sorted(modules,key=len,reverse=True) if name==m or name.startswith(m+'.'))
            rest=name[len(mod):].lstrip('.')
            node=modules[mod]+('::'+rest.replace('.','::') if rest else '')+'::'+case.attrib['name']
            assert node not in node_outcomes,node
            skip=case.find('skipped')
            outcome='failure' if case.find('failure') is not None or case.find('error') is not None else ('xfail' if skip is not None and skip.get('type')=='pytest.xfail' else 'skipped' if skip is not None else 'passed')
            node_outcomes[node]=outcome
            counts[outcome]+=1
            if node in original: original_counts[outcome]+=1
proof=json.loads((p/'cache-coverage/selection-proof.json').read_text())
assert len(original)==proof['original_unique']==2580
assert len(node_outcomes)==proof['followup_unique']==2590
assert not original-set(node_outcomes)
assert set(node_outcomes)-original==set(proof['additional'])
assert counts==dict(passed=2416,skipped=168,xfail=5,failure=1)
assert original_counts==dict(passed=2406,skipped=168,xfail=5,failure=1)
rows=list(csv.DictReader((p/'corpus/table-summary.tsv').open(),delimiter='\t'))
assert len(rows)==len({(r['country'],r['table']) for r in rows})==549
report={'corpus':{'unique_requests':len(rows),'countries':len({r['country'] for r in rows}),'status':dict(collections.Counter(r['status'] for r in rows)),'error_countries':dict(collections.Counter(r['country'] for r in rows if r['status']=='error'))},'followup':dict(counts),'original_skip_outcomes':dict(original_counts),'missing_original':[],'additional_cases':len(set(node_outcomes)-original),'all_private_log_xml_hashes_match_archive':True,'failed_nodes':[n for n,v in node_outcomes.items() if v=='failure'],'evidence_files':len(evidence)}
(out/'evidence-check.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
