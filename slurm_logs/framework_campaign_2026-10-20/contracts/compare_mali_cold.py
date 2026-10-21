"""Aggregate comparison of new exact-source Mali cold/warm private returns."""
from collections import Counter
import importlib.util
import json
from pathlib import Path
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library.local_tools import data_root
source=Path('/local/job40255479/worktrees/baseline-976fc840d')
assert Path.cwd().resolve()==source
assert Path(ll.__file__).resolve()==source/'lsms_library/__init__.py'
assert countries_root().resolve()==source/'lsms_library/countries'
assert data_root().resolve()==Path('/local/job40255479/mali-cold-review-baseline')
from pandas.testing import assert_frame_equal

out=Path('/local/job40255479/worktrees/framework-contracts/slurm_logs/framework_campaign_2026-10-20/contracts')
roots={name:Path('/local/job40255479/mali-cold-review-'+name) for name in ('baseline','combined')}
frames={name:{mode:pd.read_pickle(root/(mode+'-served.pkl')) for mode in ('cold','warm')} for name,root in roots.items()}
summaries={name:{mode:json.loads((root/(mode+'-summary.json')).read_text()) for mode in ('cold','warm')} for name,root in roots.items()}
spec=importlib.util.spec_from_file_location('fingerprint',Path('/local/job40255479/worktrees/baseline-976fc840d/tests/generate_baseline.py'))
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
for name in roots:
 for mode in ('cold','warm'):
  assert mod.fingerprint(frames[name][mode])==summaries[name][mode]['served']
report={'baseline_sha':summaries['baseline']['cold']['source_sha'],'candidate_sha':summaries['combined']['cold']['source_sha'],
        'fixed_hash_seed':1,'country':'Mali','table':'household_roster','new_source_comparisons':{},'cold_warm_by_source':{},
        'original_corpus_frames_compared':False,'original_corpus_roots_used_for_API':False,'private_snapshot_lossless_roundtrips_verified':True}
for mode in ('cold','warm'):
 a,b=[frames[name][mode] for name in ('baseline','combined')]
 assert_frame_equal(a.sort_index().sort_index(axis=1),b.sort_index().sort_index(axis=1),check_exact=True)
 try:assert_frame_equal(a,b,check_exact=True);original_order_exact=True
 except AssertionError:original_order_exact=False
 report['new_source_comparisons'][mode]={'shape':list(a.shape),'canonical_exact_frame_equal':True,'original_order_exact_frame_equal':original_order_exact,
    'index_order_equal':a.index.equals(b.index),'column_order_equal':a.columns.equals(b.columns),'index_level_dtypes':[str(a.index.get_level_values(i).dtype) for i in range(a.index.nlevels)],
    'fingerprints':{name:summaries[name][mode]['served'] for name in roots}}
for name in roots:
 cold,warm=[frames[name][mode] for mode in ('cold','warm')]
 a,b=[df.sort_index().sort_index(axis=1) for df in (cold,warm)]
 assert_frame_equal(a.drop(columns='MonthsSpent'),b.drop(columns='MonthsSpent'),check_exact=True)
 report['cold_warm_by_source'][name]={'MonthsSpent_dtype':{'cold':str(a['MonthsSpent'].dtype),'warm':str(b['MonthsSpent'].dtype)},
    'MonthsSpent_scalar_types':{mode:summaries[name][mode]['MonthsSpent_scalar_type_counts'] for mode in ('cold','warm')},
    'MonthsSpent_missingness_exact_equal':a['MonthsSpent'].isna().equals(b['MonthsSpent'].isna()),
    'MonthsSpent_string_representations_equal':a['MonthsSpent'].astype('string').equals(b['MonthsSpent'].astype('string')),
    'other_columns_index_labels_dtypes_values_exact_equal':True,'original_index_order_equal':cold.index.equals(warm.index),
    'original_column_order_equal':cold.columns.equals(warm.columns),'warm_cache_bytes_mtime_unchanged':summaries[name]['warm']['country_cache_bytes_mtime_unchanged'],
    'cold_warning_counts':summaries[name]['cold']['warning_counts'],'warm_warning_counts':summaries[name]['warm']['warning_counts'],
    'materialized_parquet_tables':summaries[name]['cold']['materialized_parquet_tables']}
report['limits']=['These are two new fixed-seed builds, not a direct comparison of unavailable original corpus cold returns.',
                 'Mali cold-to-warm MonthsSpent typing is preserved as a separate inherited limitation; no source, fixture, scientific reading or dtype normalization was changed.',
                 'String-representation comparison is reported separately and is not used to normalize either source frame for the exact cold baseline/candidate comparison.']
(out/'mali-cold-review.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ('new_source_comparisons',)},indent=2))
print(json.dumps({k:{x:y for x,y in v.items() if x!='fingerprints'} for k,v in report['new_source_comparisons'].items()},indent=2))
