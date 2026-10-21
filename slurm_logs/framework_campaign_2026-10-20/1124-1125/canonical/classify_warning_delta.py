"""Review exact warning messages privately; export family-level counts only."""
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import re

root = Path('/local/job40255479/canonical-audit-compare')
labels = ['baseline', 'combined']
def norm(s):
    for value in ['/local/job40255479/worktrees/baseline-976fc840d',
                  '/local/job40255479/worktrees/framework-integration',
                  str(root/'data-baseline'), str(root/'data-combined'),
                  '/local/job40255479/corpus-baseline-data', '/local/job40255479/corpus-combined-data']:
        s = s.replace(value, '<ROOT>')
    return s

rows = {label: list(map(json.loads, (root/f'{label}-raw.jsonl').read_text().splitlines())) for label in labels}
def key(r):
    return (r['phase'],r['feature'],json.dumps(r.get('kwargs',{}),sort_keys=True),
            r['check'],r.get('severity'),norm(r['detail']))
counts = {label: Counter(key(r) for r in records if r['status']=='warn' and r['feature']!='interview_date')
          for label,records in rows.items()}
cold = {label: defaultdict(set) for label in labels}
cold_any_table = {label: defaultdict(set) for label in labels}
for label in labels:
    for path in Path(f'/local/job40255479/corpus-{label}-out').glob('*/*-warnings.json'):
        feature = path.name.removesuffix('-warnings.json')
        for r in json.loads(path.read_text()):
            cold[label][(feature,norm(r['message']))].add((path.parent.name,r['category']))
            cold_any_table[label][norm(r['message'])].add((path.parent.name,r['category']))

def family(message):
    for needle, name in [
        ('Cache miss for ', 'expected_stale_cache_rebuild_notice'),
        ('could not be decoded using utf-8', 'stata_utf8_fallback_warning'),
        ('Makefile not found in ', 'makefile_missing_fallback_warning'),
        ('Issue with categorical mapping:', 'categorical_mapping_warning'),
        ('cluster_features: projected ', 'documented_peru_cluster_projection'),
        ('household rows describe a cluster they no longer live in', 'documented_tanzania_cluster_design'),
        ('/cluster_features: dropping ', 'cluster_missing_key_exclusion'),
        ('grain conflict on ', 'source_grain_conflict'),
        ('reduce_to_agreed(on_conflict=', 'cached_grain_warning_replay'),
        ('Makefile execution failed for ', 'baseline_only_make_failure_warning'),
        ('weight-only row(s)', 'documented_guyana_weight_only_exclusion'),
        ('household_roster: restored B00-ORDEN by cedula row order', 'documented_peru_person_id_repair'),
        ('EthiopiaRHS food_acquired: dropping ', 'rhs_acquisition_source_exclusion'),
        ('food_kg_factors:', 'stated_metric_inference_conflict'),
        ("labels='Aggregate' changed Quantity grand total", 'existing_reaggregation_total_warning'),
    ]:
        if needle in message:
            return name
    return 'unclassified'

def summarize(delta, own, other):
    groups = defaultdict(lambda: {'count':0,'severity_counts':Counter(),'feature_counts':Counter(),
                                  'countries':Counter(),'same_runtime_cold_message_matches':0,
                                  'both_runtime_cold_message_matches':0})
    unmatched = []
    for k,n in delta.items():
        phase,feature,kwargs,check,severity,message = k
        f = family(message)
        g = groups[f]
        g['count'] += n; g['severity_counts'][severity] += n; g['feature_counts'][feature] += n
        same = cold[own].get((feature,message),set()) or cold_any_table[own].get(message,set())
        opposite = cold[other].get((feature,message),set()) or cold_any_table[other].get(message,set())
        if same:
            g['same_runtime_cold_message_matches'] += n
            for country in sorted({c for c,_ in same}): g['countries'][country] += n
        if same and opposite: g['both_runtime_cold_message_matches'] += n
        if f == 'expected_stale_cache_rebuild_notice':
            country = re.match(r'Cache miss for (.*?)/',message).group(1)
            g['countries'][country] += n
        elif not same and f not in {'existing_reaggregation_total_warning','cached_grain_warning_replay',
                                    'baseline_only_make_failure_warning'}:
            unmatched.append({'phase':phase,'feature':feature,'check':check,'family':f,'count':n})
    return {name:{**values,'countries_matching_cold_message_or_cache_notice':sorted(values['countries']),
                  'countries':None} for name,values in groups.items()},unmatched

added = counts['combined']-counts['baseline']; removed=counts['baseline']-counts['combined']
extra,unmatched_added=summarize(added,'combined','baseline')
gone,unmatched_removed=summarize(removed,'baseline','combined')
reaggregation = {}
for label in labels:
    rec = next(r for r in rows[label] if r['feature']=='food_quantities' and r.get('check')=='reaggregate_conservation')
    reaggregation[label] = rec['metrics']
food=json.loads((root/'food-comparison.json').read_text())
local=json.loads((root/'local-cache-controls.json').read_text())
assert local['all_frames_equal_ignoring_row_order']
for country,feature in [('Uganda','cluster_features'),('Nigeria','food_acquired')]:
    control=next(r for r in local['frames'] if r['country']==country and r['feature']==feature)
    assert not control['row_order_only_difference']
assert extra['documented_peru_person_id_repair']['count']==1
assert extra['documented_peru_person_id_repair']['both_runtime_cold_message_matches']==1
expected_delta=math.fsum(w['delta'] for country in ['Senegal','Benin','CotedIvoire']
                         for w in food[country]['quantities']['by_wave'].values())
observed_delta=reaggregation['combined']['baseline']-reaggregation['baseline']['baseline']
assert math.isclose(expected_delta,observed_delta,rel_tol=0,abs_tol=1e-5)
assert reaggregation['combined']['got']==reaggregation['baseline']['got']
report={
 'non_interview_exact_message_added':sum(added.values()),
 'non_interview_exact_message_removed':sum(removed.values()),
 'non_interview_net_warning_increase':sum(added.values())-sum(removed.values()),
 'added_families':extra,'removed_families':gone,
 'messages_not_matched_to_same_runtime_cold_control':unmatched_added+unmatched_removed,
 'country_matching_caveat':'Unscoped runtime messages can match more than one cold country; lists are possible exact-message controls, not inferred row-level attribution.',
 'local_cache_control_frames':len(local['frames']),
 'local_cache_control_rows':sum(r['rows'] for r in local['frames']),
 'all_local_control_values_equal_ignoring_row_order':True,
 'cached_grain_warning_replay_control':{'removed_replay_messages':8,'added_source_grain_conflict_messages':8,
  'country':'Uganda','feature':'cluster_features','local_frames_exactly_equal':True,
  'replay_implementation':'lsms_library/country.py:7784'},
 'baseline_only_make_failure_warning_control':{'country':'Nigeria','feature':'food_expenditures',
  'dependency':'food_acquired','count':1,'eventual_local_food_acquired_frames_exactly_equal':True,
  'warning_removed_from_candidate':True,'cause_not_inferred':True},
 'quantity_reaggregation_warning':{'metrics':reaggregation,'measured_food_quantity_delta':expected_delta,
  'feature_baseline_quantity_delta':observed_delta,'delta_accounts_for_three_parser_countries':True,
  'existing_warning_retained':True,'aggregate_total_unchanged':True},
 'one_added_class_b_runtime_warning':{'country':'Peru','feature':'household_roster',
  'family':'documented_peru_person_id_repair','count':1,
  'documentation':'lsms_library/countries/Peru/_/CONTENTS.org:830-848',
  'same_message_in_both_independent_cold_corpus_controls':True},
 'raw_messages_and_identifiers_private':True,
}
assert 'unclassified' not in extra and 'unclassified' not in gone, (list(extra),list(gone))
assert not report['messages_not_matched_to_same_runtime_cold_control']
(root/'non-interview-warning-classification.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({'added':report['non_interview_exact_message_added'],
                  'removed':report['non_interview_exact_message_removed'],
                  'net':report['non_interview_net_warning_increase'],
                  'unmatched':report['messages_not_matched_to_same_runtime_cold_control'],
                  'added_families':{k:v['count'] for k,v in extra.items()},
                  'removed_families':{k:v['count'] for k,v in gone.items()}},indent=2,sort_keys=True))
