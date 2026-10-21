"""Actual pooled Feature plot-input reducer controls; aggregate outputs only."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import warnings

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--expected-sha',required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.paths import countries_root,data_root
repo=Path.cwd().resolve();private=Path(os.environ['LSMS_DATA_DIR']).resolve()
assert Path(ll.__file__).resolve().parent.parent==repo==Path(os.environ['LSMS_EXPECTED_ROOT']).resolve()
assert countries_root().resolve()==repo/'lsms_library/countries'
assert data_root().resolve()==private and private.is_relative_to(Path('/local'))
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==a.expected_sha
feature=ll.Feature('plot_inputs')
names=[name for name in feature.countries if (private/name/'var/plot_inputs.parquet').exists()]
assert len(names)>=3, names
with warnings.catch_warnings(record=True) as captured:
 warnings.simplefilter('always')
 pooled=feature(names)
 reverse=feature(list(reversed(names)))
 singles={name:feature([name]) for name in names}
 assert 'country' in pooled.index.names
 represented=set(pooled.index.get_level_values('country').unique())
 assert represented=={name for name,df in singles.items() if len(df)},(represented,names)
 reductions={}
 for fn in [T.seed_kg,T.nitrogen_kg]:
  actual=fn(pooled).sort_index()
  expected=pd.concat([fn(df) for df in singles.values()]).sort_index()
  as_column=fn(pooled.reset_index('country')).sort_index()
  swapped=fn(reverse).sort_index()
  assert list(actual.index.names)==['country','t','i','plot_id']
  for label,control in [('separate_country_Feature_calls',expected),('country_column',as_column),('reverse_country_order',swapped)]:
   pd.testing.assert_frame_equal(actual,control,check_exact=True)
  reductions[fn.__name__]={'rows':len(actual),'index_names':list(actual.index.names),'by_country_rows':{str(k):int(v) for k,v in actual.groupby(level='country').size().items()},'column_totals':{str(k):float(v) for k,v in actual.sum().items()},'separate_country_exact':True,'country_column_exact':True,'reverse_order_exact':True}
report={'source_commit':a.expected_sha,'package':str(Path(ll.__file__).resolve()),'countries_root':str(countries_root()),'data_root':str(private),'warm_countries':names,'pooled_rows':len(pooled),'reductions':reductions,'warnings_by_category':dict(Counter(w.category.__name__ for w in captured)),'raw_survey_rows_exported':False}
a.out.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps(report,indent=2,sort_keys=True))
