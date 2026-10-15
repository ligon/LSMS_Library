"""Read-only in-memory assembly replay; reports aggregate checksums, never IDs."""
import ast, collections, hashlib, itertools, json, math, os, subprocess, time, warnings
from pathlib import Path
import numpy as np
import pandas as pd
import lsms_library
from lsms_library.feature import _collapse_duplicate_index
from lsms_library.paths import countries_root, data_root
TREE=Path('/local/job39919092/release-work/runtime/corpus')
CONTROL=Path(__file__).resolve().parent
ROOT=Path('/local/job39919092/release-work/runtime/data-corpus/GhanaLSS')
PRIVATE=CONTROL/'empty-data-root'
assert Path(lsms_library.__file__).resolve()==TREE/'lsms_library/__init__.py'
assert countries_root()==TREE/'lsms_library/countries'
assert data_root()==PRIVATE
assert not any(p.is_file() for p in PRIVATE.rglob("*"))
started=time.monotonic()
paths=[ROOT/'var/food_acquired.parquet', ROOT/'var/sample.parquet']
stats={str(p):(p.stat().st_size,p.stat().st_mtime_ns) for p in paths}
raw=pd.read_parquet(paths[0]); sample=pd.read_parquet(paths[1])
# Native IDs on both pre-finalize caches; no Country API or panel rebuilding.
key=pd.MultiIndex.from_arrays([raw.index.get_level_values('t'),raw.index.get_level_values('i')],names=['t','i'])
sample_v=sample['v'].reorder_levels(['t','i'])
assert sample_v.index.is_unique
v=sample_v.reindex(key).to_numpy()
arrays=[raw.index.get_level_values(n) for n in raw.index.names]
arrays.insert(1,v);names=list(raw.index.names);names.insert(1,'v')
raw.index=pd.MultiIndex.from_arrays(arrays,names=names)
levels=['t','v','i','j','u','s']
nulls={n:raw.index.get_level_values(n).isna() for n in levels}
mask=np.logical_or.reduce(list(nulls.values()))
by_level={n:int(m.sum()) for n,m in nulls.items() if m.any()}
assert by_level=={'v':92,'u':13380},by_level
assert int(mask.sum())==13472
# Match served numeric dtypes, without changing values or running finalization.
for c in ['Quantity','Expenditure','Price']:
    raw[c]=raw[c].astype('Float64')
raw=raw.droplevel('visit').reorder_levels(levels)
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    out=_collapse_duplicate_index(raw,'food_acquired',country='GhanaLSS')
assert len(out)==1704880,len(out)
def fsum(x): return math.fsum(x.dropna().to_numpy(dtype=float))
recon={}
for col in ['Quantity','Expenditure']:
    before=fsum(raw[col]); kept=fsum(raw.loc[~mask,col]); lost=fsum(raw.loc[mask,col]); after=fsum(out[col])
    recon[col]={'before_fsum':before,'nonnull_input_fsum':kept,'lost_input_fsum':lost,'after_fsum':after,'before_minus_after':before-after,'nonnull_minus_after':kept-after,'difference_minus_lost':before-after-lost,'all_terms_residual_fsum':math.fsum(itertools.chain(raw[col].dropna().to_numpy(dtype=float),-out[col].dropna().to_numpy(dtype=float),-raw.loc[mask,col].dropna().to_numpy(dtype=float)))}
wave=raw.index.get_level_values('t'); source=raw.index.get_level_values('s')
lost_by_wave={str(w):{'rows':int((mask&(wave==w)).sum()),'checksums':{c:fsum(raw.loc[mask&(wave==w),c]) for c in recon},'source_counts':dict(collections.Counter(str(s) for s in source[mask&(wave==w)]))} for w in sorted(set(wave[mask]))}
refs=['v0.14.0','452302adcfdbf3af538ef06375df1ac6c5aaa8a0','7fa05179c8519f44ea329ba6387f5d6d6441a8ea']
ident={}
for ref in refs:
    get=lambda path:subprocess.check_output(['git','-C',str(TREE),'show',ref+':'+path],text=True)
    entry={'commit':subprocess.check_output(['git','-C',str(TREE),'rev-parse',ref+'^{commit}'],text=True).strip(),'feature_blob':subprocess.check_output(['git','-C',str(TREE),'rev-parse',ref+':lsms_library/feature.py'],text=True).strip()}
    for file, funcs in [('lsms_library/country.py',['most_complete_row','_normalize_dataframe_index']),('lsms_library/derivations.py',['union_keys_by_group'])]:
        txt=get(file)
        for node in ast.walk(ast.parse(txt)):
            if isinstance(node,ast.FunctionDef) and node.name in funcs:
                entry[node.name+'_sha256']=hashlib.sha256(ast.get_source_segment(txt,node).encode()).hexdigest()
    ident[ref]=entry
for k in ['feature_blob','most_complete_row_sha256','_normalize_dataframe_index_sha256','union_keys_by_group_sha256']:
    assert len({r[k] for r in ident.values()})==1,(k,ident)
assert stats=={str(p):(p.stat().st_size,p.stat().st_mtime_ns) for p in paths}
assert not any(p.is_file() for p in PRIVATE.rglob("*"))
report={'input_source':'Frozen corpus pre-finalize country food/sample parquets; native (t,i) v join','source_sha':refs[-1],'country_rows':len(raw),'feature_rows':len(out),'null_key_rows':int(mask.sum()),'null_rows_by_level':by_level,'legitimate_nonnull_visit_row_reduction':len(raw)-int(mask.sum())-len(out),'lost_by_wave':lost_by_wave,'checksums_not_physical_or_comparable_economic_totals':True,'fsum_reconciliation':recon,'source_identity':ident,'pandas_version':pd.__version__,'warning_counts':dict(collections.Counter(type(w.message).__name__ for w in caught)),'read_parquets_size_mtime_unchanged':True,'private_root_has_no_files':True,'seconds':time.monotonic()-started,'limits':['No historical Country input rebuild','No Country/Feature API calls','Replays assembly on current raw values with v joined from same native sample cache; currency omitted because determined by t, already a grouping key','No recommended sentinel or groupby policy change']}
(CONTROL/'replay.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['country_rows','feature_rows','null_key_rows','null_rows_by_level','lost_by_wave','fsum_reconciliation','seconds']},indent=2))
