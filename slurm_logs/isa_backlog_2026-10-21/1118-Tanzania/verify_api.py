from pathlib import Path
from dataclasses import asdict
import hashlib,json,os,socket,sys,time,warnings
import pandas as pd
from pandas.testing import assert_frame_equal
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library.local_tools import cache_freshness,read_parquet_cache_hash,read_parquet_cache_waves
from lsms_library.diagnostics import is_this_feature_sane
from lsms_library.transformations import U_UNKNOWN
wt=Path(os.environ['ISA_CHECKOUT']).resolve();ev=Path(os.environ['TASK_EVIDENCE']).resolve();phase=sys.argv[1]
assert Path.cwd().resolve()==wt and Path(ll.__file__).resolve().is_relative_to(wt)
assert not ev.is_relative_to(wt)
assert countries_root().resolve()==wt/'lsms_library/countries' and data_root().resolve().is_relative_to(ev)
try:
 with socket.socket() as s:s.connect(('203.0.113.1',443))
except OSError as e:assert 'forbids network' in str(e)
else:raise AssertionError('offline guard missing')
root=data_root();targets=[root/'Tanzania/var/plot_inputs.parquet',root/'Tanzania/2019-20/_/plot_inputs.parquet',root/'Tanzania/2020-21/_/plot_inputs.parquet']
if phase=='cold':
 assert not any(p.exists() for p in targets)
 assert not list((root/'Tanzania').rglob('*.parquet'))
else:
 assert all(p.exists() for p in targets)
stat_before={str(p.relative_to(root)):[p.stat().st_mtime_ns,hashlib.sha256(p.read_bytes()).hexdigest()] for p in root.rglob('*.parquet')}
c=ll.Country('Tanzania');assert not c.assume_cache_fresh
start=time.perf_counter();out=c.plot_inputs();elapsed=time.perf_counter()-start
before=pd.read_pickle(ev/'private-before-country.pkl')
assert len(out)==len(before)==9698
assert_frame_equal(out.drop(columns='u'),before.drop(columns='u'))
assert out.attrs==before.attrs
missing=before['u'].isna()|before['u'].astype('string').str.strip().eq('').fillna(False)
expected=before.copy();expected.loc[missing,'u']=U_UNKNOWN
assert_frame_equal(out,expected)
assert out.attrs==expected.attrs
if phase=='warm':assert_frame_equal(out,pd.read_pickle(ev/'private-after-cold-country.pkl'))
out.to_pickle(ev/f'private-after-{phase}-country.pkl')
# Save the Country proof before independent Feature verification.
(ev/f'api-{phase}-country-checkpoint.json').write_text(json.dumps({'rows':len(out),'replacements':int(missing.sum()),'ordinary_country':not c.assume_cache_fresh,'seconds':elapsed})+'\n')
rows={str(k):len(g) for k,g in out.groupby(level='t')}
replaced={str(k):int(g.sum()) for k,g in missing.groupby(level='t')}
assert int(missing.sum())==408 and not out['u'].isna().any()
summary={'phase':phase,'rows':len(out),'rows_by_wave':rows,'u_replacements':replaced,'all_nonunit_fields_index_dtypes_attrs_equal':True,'full_frame_expected_equal':True,'seconds':round(elapsed,3),'ordinary_country':not c.assume_cache_fresh,'cache':{str(p.relative_to(root)):{'hash':read_parquet_cache_hash(p),'waves':read_parquet_cache_waves(p)} for p in targets}}
# Comparator must pass a real validated read: do not use trust_cache.
n=ll.Country('Uganda');np=root/'Uganda/var/plot_inputs.parquet'
summary['comparator_pre_read_grade']=cache_freshness(np,n._table_cache_hash('plot_inputs',n.waves))
assert summary['comparator_pre_read_grade']=='fresh'
ng=n.plot_inputs();u=ng['u'] if 'u' in ng else ng.index.get_level_values('u')
assert not u.isna().any() and not pd.Series(u).astype('string').str.strip().eq('').any()
assert len(ng)==84216
assert_frame_equal(ng,pd.read_pickle(ev/'private-comparator-reference.pkl'))
ng.to_pickle(ev/f'private-comparator-{phase}.pkl')
summary['comparator']={'country':'Uganda','rows':len(ng),'unit_missing':int(u.isna().sum()),'unknown':int((u==U_UNKNOWN).sum())}
feature=ll.Feature('plot_inputs')(['Tanzania','Uganda'])
assert len(feature)==len(out)+len(ng)
for name,part in [('Tanzania',out),('Uganda',ng)]:
 assembled=feature.xs(name,level='country')
 part=ll.Country(name).plot_inputs(currency='index')
 # Feature promotes the column u to its canonical index level.
 if 'u' in assembled.index.names and 'u' in part.columns:part=part.set_index('u',append=True)
 flat=part.reset_index().rename(columns={'crop':'j'})
 for level in assembled.index.names:
  if level not in flat:flat[level]={'plot_id':'Farm-level','season':'unknown_season'}[level]
 expected_part=flat.set_index(assembled.index.names).reindex(columns=assembled.columns)
 ak=assembled.reset_index();ek=expected_part.reset_index()
 for level in assembled.index.names:
  ak[level]=ak[level].astype('string');ek[level]=ek[level].astype('string')
 assert_frame_equal(ak.sort_values(assembled.index.names).reset_index(drop=True).convert_dtypes(),ek.sort_values(assembled.index.names).reset_index(drop=True).convert_dtypes(),check_dtype=False)
summary['feature']={'rows':len(feature),'row_conserved':True,'full_country_parts_equal':True,'index':list(feature.index.names)}
summary['sanity']={name:[asdict(x) for x in is_this_feature_sane(df,name,'plot_inputs').checks] for name,df in [('Tanzania',out),('Uganda',ng)]}
if phase=='warm':
 stat_after={str(p.relative_to(root)):[p.stat().st_mtime_ns,hashlib.sha256(p.read_bytes()).hexdigest()] for p in root.rglob('*.parquet')}
 assert stat_before==stat_after
 summary['warm_parquet_writes']=0
summary['parquets_after']=sorted(str(p.relative_to(root)) for p in (root/'Tanzania').rglob('*.parquet'))
(ev/f'api-{phase}-summary.json').write_text(json.dumps(summary,indent=2,default=str)+'\n')
print(json.dumps({k:v for k,v in summary.items() if k not in ('sanity','cache','parquets_after')},indent=2))
