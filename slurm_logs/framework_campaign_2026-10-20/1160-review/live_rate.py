"""Actual warm rate controls on a physically copied private cache.

Only country aggregates leave this process; warning messages stay private.
"""
import ast,hashlib,json,os,subprocess,sys,warnings
from collections import Counter
from pathlib import Path
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.paths import countries_root,data_root
repo=Path.cwd().resolve();private=Path(os.environ['LSMS_DATA_DIR']).resolve()
expected='532d58700fa46e52a0b390198604eca1dc4de00e'
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==expected
assert Path(ll.__file__).resolve().parent.parent==repo
assert countries_root().resolve()==repo/'lsms_library/countries'
assert data_root().resolve()==private
assert private==Path('/local/job40255479/control/framework-campaign/review-1160/live-data')
assert hashlib.sha256(Path(T.__file__).read_bytes()).hexdigest()=='cb973473420759f35b2046e0da618fdbddc278c78708038792b0e85856e1b4d0'
# Bind ONLY the old rate function; every dependency it uses is unchanged4cab.
oldsource=subprocess.check_output(['git','show','4cab5f0db7d375ad543e652f61f43bd28fa94ecd:lsms_library/transformations.py'],text=True)
node=next(n for n in ast.parse(oldsource).body if isinstance(n,ast.FunctionDef) and n.name=='fertilizer_rate')
namespace=dict(vars(T));exec(compile(ast.Module(body=[node],type_ignores=[]),'<frozen4cab-rate>','exec'),namespace);oldrate=namespace['fertilizer_rate']
names=['Malawi','Uganda'];cases={};local={}
with warnings.catch_warnings(record=True) as caught:
 warnings.simplefilter('always')
 pi=ll.Feature('plot_inputs')(names);pf=ll.Feature('plot_features')(names)
 single={name:(ll.Feature('plot_inputs')([name]),ll.Feature('plot_features')([name])) for name in names}
 country={name:(ll.Country(name).plot_inputs(currency='index'),ll.Country(name).plot_features()) for name in names}
 for nutrient in ['N','product']:
  for on in ['plot','parcel']:
   key=nutrient+'/'+on
   actual=T.fertilizer_rate(pi,pf,nutrient=nutrient,on=on).sort_index()
   expected_frame=pd.concat([T.fertilizer_rate(*single[name],nutrient=nutrient,on=on) for name in names]).sort_index()
   pd.testing.assert_frame_equal(actual,expected_frame,check_exact=True)
   reverse=T.fertilizer_rate(pi.iloc[::-1],pf.iloc[::-1],nutrient=nutrient,on=on).sort_index()
   # Reversed within-country floating sums can differ; check exact first.
   pd.testing.assert_frame_equal(actual,reverse,check_exact=True)
   columns=T.fertilizer_rate(pi.reset_index('country'),pf.reset_index('country'),nutrient=nutrient,on=on).sort_index()
   pd.testing.assert_frame_equal(actual,columns,check_exact=True)
   cases[key]={'rows':len(actual),'index_names':list(actual.index.names),'by_country_rows':{name:int((actual.index.get_level_values('country')==name).sum()) for name in names},'totals_by_country':{name:float(actual.loc[actual.index.get_level_values('country')==name].iloc[:,0].sum()) for name in names},'separate_labeled_Feature_calls_exact':True,'reverse_rows_exact':True,'country_columns_exact':True}
   for name in names:
    before=oldrate(*country[name],nutrient=nutrient,on=on).sort_index()
    after=T.fertilizer_rate(*country[name],nutrient=nutrient,on=on).sort_index()
    pd.testing.assert_frame_equal(before,after,check_exact=True)
    local[name+'/'+key]={'rows':len(after),'index_names':list(after.index.names),'old4cab_function_exact':True}
report={'head':expected,'runtime_commit':'5125d0faa3e8074c289c5dd839cf1690d3e7a570','package':ll.__file__,'countries_root':str(countries_root()),'data_root':str(private),'input_rows':{'plot_inputs':len(pi),'plot_features':len(pf)},'cases':cases,'ordinary_Country_calls':local,'warnings_by_category':dict(Counter(w.category.__name__ for w in caught)),'country_guidance':{'Uganda':'Agricultural plot keys carry household-parcel-plot while area keys carry parcel_suffix; plot-mode emptiness is inherited and not waived. Existing WAITING/TODO caveats retained.','Malawi':'Real plot identifiers align; documented historical null-plot drop is distinct from a true duplicate-plot collision. Conversion/FCT TODOs retained.'},'raw_warning_messages_and_identifiers_private':True}
Path(sys.argv[1]).write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'cases':cases,'ordinary_Country_calls':local},indent=2))
