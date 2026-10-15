import os,json,sys
from pathlib import Path
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
repo=Path(os.environ['PYTHONPATH']);assert Path(ll.__file__).resolve().parent==repo/'lsms_library';assert countries_root()==repo/'lsms_library/countries';assert 'uganda-assets-' in str(data_root())
result={}
for name in ('Uganda','GhanaSPS'):
 c=ll.Country(name)
 tables=c.data_scheme
 result[name]={'country':{t:c._table_cache_hash(t,c.waves)for t in tables},'waves':{w:{t:c[w]._input_hash(t)for t in tables}for w in c.waves}}
Path(sys.argv[1]).write_text(json.dumps(result,indent=2)+'\n')
