"""Evidence-only Mali roster cold/warm probe with exact source/cache identity.

Private returned DataFrames are preserved losslessly as pickles outside the
repository; stdout and tracked summaries contain only aggregates/fingerprints.
"""
import argparse
from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import warnings

p=argparse.ArgumentParser()
p.add_argument('--repo',type=Path,required=True)
p.add_argument('--sha',required=True)
p.add_argument('--data',type=Path,required=True)
p.add_argument('--mode',choices=('cold','warm'),required=True)
a=p.parse_args()
import pandas as pd
from pandas.testing import assert_frame_equal
import lsms_library as ll
from lsms_library.diagnostics import load_feature
from lsms_library.paths import countries_root
from lsms_library.local_tools import data_root
assert Path.cwd().resolve()==a.repo.resolve()
assert Path(ll.__file__).resolve()==a.repo/'lsms_library/__init__.py'
assert countries_root().resolve()==a.repo/'lsms_library/countries'
assert data_root().resolve()==a.data.resolve()
assert 'mali-cold-review-' in str(a.data)
assert os.environ['PYTHONHASHSEED']=='1'
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==a.sha
if a.mode=='cold':assert not list(a.data.rglob('*.parquet'))
cache=a.data/'Mali/var/household_roster.parquet'
before=(cache.stat().st_size,cache.stat().st_mtime_ns) if cache.exists() else None
co=ll.Country('Mali',preload_panel_ids=False)
with warnings.catch_warnings(record=True) as captured:
 warnings.simplefilter('always')
 frame=load_feature(co,'household_roster')
# Capture the returned frame before any external serialization/coercion.
returned=frame.copy(deep=True)
spec=importlib.util.spec_from_file_location('fingerprint',a.repo/'tests/generate_baseline.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
summary={'source_sha':a.sha,'mode':a.mode,'seed':1,'served':mod.fingerprint(returned),
         'canonical_served':mod.fingerprint(returned.sort_index().sort_index(axis=1)),
         'index_level_dtypes':[str(returned.index.get_level_values(i).dtype) for i in range(returned.index.nlevels)],
         'wave_rows':{str(k):int(v) for k,v in returned.index.get_level_values('t').value_counts(dropna=False).items()},
         'MonthsSpent_scalar_type_counts':dict(Counter(type(v).__name__ for v in returned['MonthsSpent'])),
         'warning_counts':dict(Counter(w.category.__name__ for w in captured))}
output=a.data/(a.mode+'-served.pkl');returned.to_pickle(output)
reloaded=pd.read_pickle(output)
assert_frame_equal(returned,reloaded,check_exact=True)
assert mod.fingerprint(reloaded)==summary['served']
summary['private_pickle_lossless_exact_roundtrip']=True
if a.mode=='warm':
 assert before==(cache.stat().st_size,cache.stat().st_mtime_ns)
 summary['country_cache_bytes_mtime_unchanged']=True
summary['materialized_parquet_tables']=sorted({f.stem for f in a.data.rglob('*.parquet')})
(a.data/(a.mode+'-summary.json')).write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,sort_keys=True))
