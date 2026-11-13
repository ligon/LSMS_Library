"""Private warm/Feature preservation check; export aggregate facts only."""
from pathlib import Path
import json,os,warnings
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
root=Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
assert Path(ll.__file__).resolve()==root/'lsms_library/__init__.py'
assert countries_root().resolve()==root/'lsms_library/countries'
out=data_root()
with warnings.catch_warnings():
 warnings.simplefilter('ignore')
 frames={c:ll.Country(c).plot_inputs(currency=None) for c in ('Malawi','Uganda')}
 pooled=ll.Feature('plot_inputs')(['Malawi','Uganda'],currency=None)
pd.testing.assert_frame_equal(pd.read_pickle(out/'cold-served.pkl'),frames['Malawi'],check_exact=True)
assert len(pooled)==sum(map(len,frames.values()))
assert set(pooled.index.get_level_values('country'))==set(frames)
pooled.to_pickle(out/'warm-pooled.pkl')
summary={'country_rows':{c:len(f)for c,f in frames.items()},'pooled_rows':len(pooled),'both_countries_retained':True,'row_conservation':True,'Malawi_cold_warm_exact':True}
(out/'warm-feature-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary))
