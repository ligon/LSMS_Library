"""Reconcile exact-case skip/xfail and collection deltas, aggregate only."""
from collections import Counter
import json
from pathlib import Path
root=Path('/local/job40255479/control/framework-campaign/followup-validation')
d=json.loads((root/'suite-final-4cab5f0d/summary.json').read_text())
t=d['transitions'];new=[r for r in t if r['before'] is None];removed=[r for r in t if r['after'] is None];changed=[r for r in t if r['before'] is not None and r['after'] is not None]
assert len(new)==198 and len(removed)==5 and len(changed)==9
assert all(r['after']['outcome']=='passed' for r in new)
assert all(r['before']['outcome']=='passed_or_nonstrict_xpass' for r in removed)
assert all(r['before']['outcome']=='skipped' and r['after']['outcome']=='passed' for r in changed)
assert not any('credential' in reason.lower() for reason in d['skip_reasons'])
assert d['counts']['exitstatus']==0 and d['head_unchanged_after'] and d['tracked_source_unchanged_after']
assert len(d['workers_reporting'])==16
assert d['candidate_xpass_cases']==['tests/test_pakistan_cluster_features.py::test_extraction_is_cluster_grain']
report={'source_commit':d['source_commit'],'baseline_commit':d['baseline']['source_commit'],'counts':d['counts']['terminal_counts'],'baseline_counts':d['baseline']['counts'],'pytest_seconds':765.96,'controller_seconds':d['seconds'],'collection':{'baseline_cases':8857,'candidate_cases':9050,'new_passing_nodeids':198,'old_nodeids_replaced_or_renamed':5,'net_additional_cases':193,'new_by_module':dict(Counter(r['nodeid'].split('::')[0] for r in new)),'removed_nodeids':[r['nodeid'] for r in removed],'replacement_explanation':'One declared-factor/Feature test expands to two colliding/distinct-household cases; four provenance tests are renamed/reworked around approved held-versus-queued release and acquisition-only primary correction contracts.'},'skip_delta':{'baseline':285,'candidate':276,'removed':9,'new':0,'changed_remaining_reasons':0,'skip_to_pass':[{'nodeid':r['nodeid'],'old_reason':r['before']['reason']} for r in changed],'interpretation':'Eight Uganda raw food_acquired wave invariance cases and the Nigeria plot_inputs poisoned-cache control now execute and pass. Initial private corpus/cache state and test ordering differ from852; no unmeasured causal attribution to a particular fix.'},'xfail_delta':{'baseline':9,'candidate':9,'same_nodeids_and_reasons':True,'families':{'existing_602_unharmonized_plot_tenure':5,'source_has_no_sampling_weights':4}},'xpass':{'baseline_count':1,'candidate_count':1,'candidate_directly_recorded_case':d['candidate_xpass_cases'][0],'baseline_identity_limit':d['baseline_xpass_identity_limit'],'scope_limit':'This non-strict expected-failure marker passing verifies the wave artifact exists, omits household i, has300rows and a unique index. It does not certify GH740 resolved, broader source validity or cross-wave cluster identity.'},'import_identity_test':{'nodeid':'tests/test_categorical_mapping_resolution.py::test_import_is_from_the_expected_checkout','outcome':'ordinary PASS, not XPASS','expected_root_set':True},'pass_delta_reconciles':'8764-8562 =198 new passing IDs -5 replaced/renamed old IDs +9 old skips now passing =202','credential_skips':0,'all_16_workers_reported':True,'source_unchanged':True,'raw_survey_records_exported':False}
(root/'suite-reconciliation.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['counts','collection','skip_delta','xfail_delta','xpass']},indent=2))
