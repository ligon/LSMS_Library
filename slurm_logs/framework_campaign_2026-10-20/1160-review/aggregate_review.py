"""Produce aggregate independent review evidence; never export raw messages."""
from pathlib import Path
import ast,hashlib,json,subprocess,xml.etree.ElementTree as ET
root=Path('/local/job40255479/control/framework-campaign/review-1160')
source=Path('/local/job40255479/worktrees/1160-pooled-rate')
expected='532d58700fa46e52a0b390198604eca1dc4de00e'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==expected
assert not subprocess.check_output(['git','diff','HEAD','--name-only'],cwd=source,text=True).strip()
assert hashlib.sha256((source/'lsms_library/transformations.py').read_bytes()).hexdigest()=='cb973473420759f35b2046e0da618fdbddc278c78708038792b0e85856e1b4d0'
cases=list(ET.parse(root/'focused-private.xml').getroot().iter('testcase'))
skips=[{'test':c.get('classname')+'::'+c.get('name'),'reason':c.find('skipped').get('message')} for c in cases if c.find('skipped') is not None]
assert len(cases)==389 and len(skips)==2
assert not any(c.find('failure') is not None or c.find('error') is not None for c in cases)
names={n.name for n in ast.parse((root/'test_independent_rate.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name.startswith('test_')}
independent=[c for c in cases if c.get('name','').split('[')[0] in names]
assert len(independent)==40
negative=list(ET.parse(root/'negative-private.xml').getroot().iter('testcase'))
assert len(negative)==12 and all(c.find('failure') is not None for c in negative)
assert '389/404 tests collected (15 deselected)' in (root/'collection-private.log').read_text()
live=json.loads((root/'live-rate.json').read_text());cache=json.loads((root/'cache-control.json').read_text());hashes=json.loads((root/'hash-impact.json').read_text())
assert cache['original_source_bytes_inodes_sizes_mtimes_unchanged'] and cache['private_target_all_files_equal_initial_seed']
assert all(c['separate_labeled_Feature_calls_exact'] and c['reverse_rows_exact'] and c['country_columns_exact'] for c in live['cases'].values())
assert all(c['old4cab_function_exact'] for c in live['ordinary_Country_calls'].values())
for case in live['cases'].values():case['rate_column_sum_checksum_by_country']=case.pop('totals_by_country')
(root/'live-controls.json').write_text(json.dumps(live,indent=2)+'\n')
report={'reviewer':'Sue; independent of1160 implementation','verdict':'PASS','target_head':expected,'runtime_commit':'5125d0faa3e8074c289c5dd839cf1690d3e7a570','base':'4cab5f0db7d375ad543e652f61f43bd28fa94ecd','blocking_findings':[],'focused':{'passed':387,'skipped':skips,'failed':0,'errors':0,'seconds':10.03,'producer_selection_passes':347,'additional_independent_passes':40,'requires_s3_deselected':15,'total_collected_before_marker':404,'selected':389},'independent_negative_control':{'base':'4cab5f0db7d375ad543e652f61f43bd28fa94ecd','intentional_failures':12,'passed':0,'deselected':28,'errors':0,'seconds':1.09,'meaning':'New multi-product/duplicate-area/category/nullable arithmetic cases fail on the old country-omitting function.'},'independent_control_families':['multi-product fertilizer math and duplicate positive-area summation','extra country area cannot dilute another numerator','category/nullable country keys including unused categories','one/both shaped empty country inputs','incompatible numeric/text country labels safely refused rather than coerced into a join','empty nullable-integer country axis with text area country','per-country declarations and organic refusal under both nutrient bases','authoritative index with contradictory redundant columns and area local keys as columns','input and attrs immutability'],'actual_warm_controls':{'countries':['Malawi','Uganda'],'pooled_mode_combinations':4,'comparisons_per_combination':['separate labeled Feature calls exact','reversed input/area row order exact','country columns exact'],'ordinary_country_old_function_controls':8,'old_function_counterfactual_scope':'Only exact4cab fertilizer_rate was bound from AST in memory; its dependencies are unchanged in1160. This is not a full historical package replay.','rate_column_sums_are_checksums_not_national_fertilizer_totals':True},'hashes':hashes,'cache_control':cache,'known_limitations':[{'kind':'existing1110 parcel policy/doc contradiction','documentation':'Malawi/_/CONTENTS.org:3035-3042','quote':'fertilizer_rate(Malawi) depends on on. ... This contradicts fertilizer_rate\'s docstring, which says the parcel step is a measured no-op on Malawi.','observed_both_old_and_new':{'Malawi_plot_rows':33402,'Malawi_parcel_rows':11622,'Uganda_plot_rows':0,'Uganda_parcel_rows':509},'disposition':'Not a new1160 regression; no parcel-policy or scientific-share change is authorized. Preserve the limitation rather than claim scientific green.'},'Two Nigeria warm tests skip in this intentionally fresh hermetic root; marked requires_s3 tests are deliberately excluded, not counted as passes.'],'reviewer_probe_correction':'Initial private live probe incorrectly supplied currency to nonmonetary Country.plot_features and stopped with the existing API TypeError. Removed only that unsupported probe kwarg; successful final controls use the supported API. No production change.','source_unchanged':True,'all_review_readers_and_verifiers_stopped':True,'raw_logs_warnings_identifiers_microdata_private':True,'source_sha256':'cb973473420759f35b2046e0da618fdbddc278c78708038792b0e85856e1b4d0'}
(root/'review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'verdict':report['verdict'],'focused':report['focused'],'hashes':hashes},indent=2))
