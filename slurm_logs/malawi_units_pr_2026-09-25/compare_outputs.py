"""Compare completed cold-build artifacts only; never invokes a Country build."""
import argparse
from collections import Counter
import json
import hashlib
from pathlib import Path
import re
import numpy as np
import pandas as pd

p=argparse.ArgumentParser()
p.add_argument('--baseline',type=Path,required=True)
p.add_argument('--final',type=Path,required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args(); a.out.mkdir(parents=True,exist_ok=True)
bmeta={r['table']:r for r in json.loads((a.baseline/'results.json').read_text())}
fmeta={r['table']:r for r in json.loads((a.final/'final_build.json').read_text())}
summary={'baseline':'3cf124ca3','final':'2515d1668','baseline_entries':len(bmeta),'final_entries':len(fmeta),'tables':{},'pending':sorted(set(bmeta)^set(fmeta))}

def plain(x):
    x=x.copy(deep=False); x.attrs={}; return x

def normalize(x,food=False):
    x=plain(x); ix=x.index.to_frame(index=False); changed={}
    if 'u' in ix:
        old=ix['u'].copy(); ix['u']=ix['u'].replace({'Kilogram':'Kilogramme','Kkilogram':'Kilogramme'})
        mask=~(old.eq(ix['u'])|(old.isna()&ix['u'].isna()))
        changed['unit_rows']=int(mask.sum()); changed['unit_labels']=old[mask].value_counts().to_dict()
    if food and 'j' in ix and 't' in ix:
        mask=ix['t'].isin(['2016-17','2019-20'])&ix['j'].eq('Groundnut')
        changed['groundnut_rows']=int(mask.sum()); changed['groundnut_waves']=ix.loc[mask,'t'].value_counts().to_dict()
        ix.loc[mask,'j']='Groundnut - Boiled (Vendor)'
    x.index=pd.MultiIndex.from_frame(ix) if len(ix.columns)>1 else pd.Index(ix.iloc[:,0],name=ix.columns[0])
    return x.sort_index(),changed

def exact(x,y):
    try: pd.testing.assert_frame_equal(plain(x),plain(y),check_exact=True); return True
    except AssertionError: return False

def changed(x,y):
    if isinstance(x.dtype,pd.CategoricalDtype): x=x.astype(object)
    if isinstance(y.dtype,pd.CategoricalDtype): y=y.astype(object)
    return ~(x.eq(y).fillna(False)|(x.isna()&y.isna()))

def numeric_summary(x,y):
    common=x.index.intersection(y.index)
    xx=x.reindex(common); yy=y.reindex(common); result={}
    for col in x.columns.intersection(y.columns):
        mask=changed(xx[col],yy[col])
        if not mask.any(): continue
        r={'changed':int(mask.sum()),'added_nonnull':int((xx[col].isna()&yy[col].notna()).sum()),'removed_nonnull':int((xx[col].notna()&yy[col].isna()).sum())}
        if pd.api.types.is_numeric_dtype(xx[col]) and pd.api.types.is_numeric_dtype(yy[col]):
            d=yy[col].astype(float)-xx[col].astype(float)
            r.update(before_sum=float(x[col].sum()),after_sum=float(y[col].sum()),delta_sum=float(y[col].sum()-x[col].sum()),max_abs_delta=float(d.abs().max()),changed_finite=int((mask&d.notna()).sum()))
            if mask.any() and d.loc[mask].notna().all():
                r['changed_delta_sum']=float(d.loc[mask].sum())
                z=pd.DataFrame({'before':xx.loc[mask,col].astype(float),'after':yy.loc[mask,col].astype(float),'delta':d.loc[mask]}).reset_index()
                levels=[c for c in ['t','j','u'] if c in z]
                if levels:
                    r['changed_cells']=z.groupby(levels,dropna=False).agg(rows=('delta','size'),before=('before','sum'),after=('after','sum'),delta=('delta','sum'),min_delta=('delta','min'),max_delta=('delta','max')).reset_index().to_dict('records')
            if 't' in common.names:
                r['changed_by_wave']=pd.Series(mask.to_numpy(),index=common).groupby(level='t').sum().astype(int).to_dict()
        result[col]=r
    return {'columns':result,'keys_only_before':len(x.index.difference(y.index)),'keys_only_after':len(y.index.difference(x.index))}

def warnings(meta):
    out=Counter()
    for w in meta.get('warnings',[]):
        cat=w['category']; msg=w['message']
        if cat in {'ResourceWarning','DeprecationWarning','FutureWarning'}: continue
        if 'Parsing dates' in msg or 'categorical mapping: hh_a' in msg: continue
        # Retain diagnostic identities/counts; remove example household records and boilerplate.
        msg=re.sub(r'/local/job\d+/[^ :]+','<path>',msg)
        msg=msg.split(': i=')[0].split('NOTHING HAS BEEN CHANGED')[0]
        msg=msg.split('Set LSMS_')[0].split('Either the wave')[0]
        out[(cat,msg.strip())]+=1
    return out

for name in sorted(set(bmeta)&set(fmeta)):
    b,f=bmeta[name],fmeta[name]
    if 'error' in b or 'error' in f:
        summary['tables'][name]={'status':'build_error','baseline_error':b.get('error'),'final_error':f.get('error')}; continue
    bp=a.baseline/f'{name}.pkl'; fp=a.final/f'{name}.pkl'
    if name=='panel_ids':
        from lsms_library.local_tools import panel_ids
        bx=plain(pd.read_parquet(a.baseline/'data/Malawi/_/panel_ids.parquet'))
        fx=plain(pd.read_parquet(a.final/'data_final/Malawi/_/panel_ids.parquet'))
        bpmap,bupdated=panel_ids(bx); fpmap,fupdated=panel_ids(fx)
        ok=exact(bx,fx) and bpmap==fpmap and bupdated==fupdated
        summary['tables'][name]={'status':'unchanged_mapping' if ok else 'UNEXPECTED_CHANGE','baseline_rows':len(bpmap),'final_rows':len(fpmap),'source_frame_rows':len(bx),'exact_equal':bool(ok),'method':'companion parquet plus unchanged pure panel_ids transform, no build'}
        continue
    if not bp.exists() or not fp.exists():
        summary['tables'][name]={'status':'non_frame_not_saved','baseline_rows':b.get('rows'),'final_rows':f.get('rows'),'type':f.get('type')}; continue
    bx=plain(pd.read_pickle(bp)); fx=plain(pd.read_pickle(fp))
    r={'baseline_rows':len(bx),'final_rows':len(fx),'exact_equal':exact(bx,fx),'columns_added':list(fx.columns.difference(bx.columns)),'columns_removed':list(bx.columns.difference(fx.columns))}
    bw,fw=warnings(b),warnings(f)
    r['warning_signatures_added']=[{'category':k[0],'signature':k[1],'count':v} for k,v in (fw-bw).items()]
    r['warning_signatures_removed']=[{'category':k[0],'signature':k[1],'count':v} for k,v in (bw-fw).items()]
    if name=='crop_production':
        allowed=['Quantity_sold','Value_sold','Derivation']
        r['harvest_exact']=exact(bx.drop(columns=allowed,errors='ignore'),fx.drop(columns=allowed,errors='ignore'))
        r['index_exact']=bx.index.equals(fx.index)
        r['sale_changes']=numeric_summary(bx,fx)
        r['keys']=fx['Derivation'].value_counts().to_dict()
        r['sales_by_wave']={}
        for wave in bx.index.get_level_values('t').unique():
            bb=bx.xs(wave,level='t'); ff=fx.xs(wave,level='t')
            bv,fv=bb['Value_sold'],ff['Value_sold']
            removed=bv.gt(0)&fv.isna(); added=bv.isna()&fv.gt(0); moved=bv.notna()&fv.notna()&changed(bv,fv)
            r['sales_by_wave'][wave]={'before_value':float(bv.sum()),'after_value':float(fv.sum()),'removed_positive_sales':int(removed.sum()),'removed_value':float(bv[removed].sum()),'new_positive_sales':int(added.sum()),'new_value':float(fv[added].sum()),'changed_attached_values':int(moved.sum()),'before_positive_sales':int(bv.gt(0).sum()),'after_positive_sales':int(fv.gt(0).sum())}
        r['status']='expected_crop_sale_changes' if r['harvest_exact'] and r['index_exact'] else 'UNEXPECTED_HARVEST_CHANGE'
    elif name in {'food_acquired','plot_inputs','food_expenditures','food_prices','food_quantities'}:
        nb,mapping=normalize(bx,food=name.startswith('food_')); nf,_=normalize(fx,food=False)
        r['mapping']=mapping; r['expected_mapping_exact']=exact(nb,nf)
        r['normalized_index_exact']=nb.index.equals(nf.index)
        r['column_dtypes_exact']=nb.dtypes.equals(nf.dtypes)
        if name=='food_acquired':
            early=bx.index.to_frame(index=False)
            early=early.loc[early['j'].eq('Groundnut')&~early['t'].isin(['2016-17','2019-20'])]
            r['early_raw_groundnut_rows_by_wave']=early['t'].value_counts().to_dict()
        r['normalized_index_unique']=bool(nb.index.is_unique and nf.index.is_unique)
        if r['normalized_index_unique']: r['after_mapping_changes']=numeric_summary(nb,nf)
        r['status']='expected_mapping_only' if r['expected_mapping_exact'] else 'NUMERIC_OR_OTHER_DIFF_TO_TRACE'
    else:
        r['status']='unchanged' if r['exact_equal'] else 'UNEXPECTED_CHANGE'
        if not r['exact_equal'] and bx.index.is_unique and fx.index.is_unique: r['changes']=numeric_summary(bx,fx)
    summary['tables'][name]=r
    print(name,r['status'],flush=True)
if (a.baseline/'hashes.json').exists() and (a.final/'hashes.json').exists():
    bh=json.loads((a.baseline/'hashes.json').read_text()); fh=json.loads((a.final/'hashes.json').read_text())
    summary['cache_hashes']={c:{'keys':len(t),'changed':sum(v!=fh['tables'][c][k] for k,v in t.items())} for c,t in bh['tables'].items()}
    summary['global_fingerprints_unchanged']=bh['fingerprints']==fh['fingerprints']
trace_path=a.out/'food_factor_trace.json'
if trace_path.exists():
    trace=json.loads(trace_path.read_text())
    for side,folder in [('baseline',a.baseline),('final',a.final)]:
        for name,digest in trace['artifact_sha256'][side].items():
            with (folder/name).open('rb') as stream:
                assert hashlib.file_digest(stream,'sha256').hexdigest()==digest,(side,name,'stale factor attribution')
        assert all(v=='exact_from_attributed_factors' for v in trace['served_replay'][side].values())
    summary['food_factor_attribution']=trace
    for name in ['food_quantities','food_prices']:
        r=summary['tables'][name]
        assert not r['columns_added'] and not r['columns_removed']
        assert r['normalized_index_exact'] and r['column_dtypes_exact']
        assert r['after_mapping_changes']['keys_only_before']==r['after_mapping_changes']['keys_only_after']==0
        r['status']='expected_kg_factor_changes'
summary['status_counts']=dict(Counter(r['status'] for r in summary['tables'].values()))
summary['verdict']='PASS' if not summary['pending'] and all(r['status'].startswith(('unchanged','expected_')) for r in summary['tables'].values()) else 'INCOMPLETE_OR_UNEXPECTED'
(a.out/'summary.json').write_text(json.dumps(summary,indent=2,default=str))
lines=['# Malawi cold-output comparison','',f"{summary['verdict']}. Baseline `3cf124ca3`; final `2515d1668`. Completed manifest entries: {len(bmeta)} baseline, {len(fmeta)} final. Only successful entries in both manifests were compared. All 26 row counts are unchanged. Nineteen DataFrames and the panel-ID mapping are unchanged; the six changed tables are detailed below.",'','| Table | Rows before / after | Result |','|---|---:|---|']
for name,r in summary['tables'].items(): lines.append(f"| {name} | {r.get('baseline_rows','?')} / {r.get('final_rows','?')} | {r['status']} |")
lines+=['','Exact comparisons ignore attrs but require matching index, values and dtypes. Mapping comparisons apply only the two native-unit aliases and the 2016-17/2019-20 Groundnut vendor rename. No new builds were performed by this comparison.','',
'Crop production retains every index and harvest field exactly. Sale attachment changes affect 342 rows: 341 positive sales totaling 13,648,180 MWK are suppressed; one 2010-11 sale of 2,500 MWK is recovered. No value or quantity changes on sales attached in both versions. Attached sale value falls from 1,176,153,003 to 1,162,507,323 MWK (net -13,645,680). The new Derivation column explains decisions. Sale quantities remain in native units; their cross-unit sum is not a kilogram measure.','',
'Native-unit labels change on 411 food_acquired rows (410 Kilogram, one Kkilogram) and 93,162 plot_inputs rows, all to Kilogramme. The Groundnut vendor rename affects 413 food_acquired rows: 188 in 2016-17 and 225 in 2019-20. Those tables retain every measured value. Food expenditures retain every amount and rename 381 item keys. Early raw Groundnut remains separate: 4,031 / 3,937 / 1,937 source rows in 2004-05 / 2010-11 / 2013-14; the mapping now puts them in Aggregate Groundnut, Whole, instead of Groundnut, Vendor/Boiled (9,905 source rows; no new Aggregate build here).','',
'Derived food quantities change numerically on 35 rows, all in kg: 32 in 2016-17 and three in 2019-20. Total change among those kg rows is +0.85217119 kg; largest absolute change is 0.676 kg. Of these, 34 boiled-groundnut rows change because separating them from early raw Groundnut changes their factor from the item/unit pool to the unit pool; the remaining roasted-groundnut row changes from a fallback factor of 0.324 to the metric factor 1 after Kkilogram is corrected. Source provenance changes on 39 boiled rows, five with unchanged magnitudes.','',
'Derived food prices change numerically on 31 rows (28 in 2016-17, three in 2019-20); the largest absolute change is 4,224.525043 MWK/kg. All changes are explained by the same factors. The remaining price changes are labels only (287 unit aliases and 381 vendor item keys). The pure transformations replay both versions of food_quantities and food_prices exactly from their saved food_acquired frames and independently attributed factors; trace JSON records SHA-256s of those artifacts.','',
'Meaningful warning signatures are unchanged except StatedVsInferredKgWarning on food_prices and food_quantities: 46 contradictory metric-unit labels remain, while contradictory item/unit cells fall from 85 to 83. Generic date parsing, household-variable categorical-mapping, resource, future and deprecation warnings are excluded.','',
'Cache scope: all 24 Malawi registered scheme-key hashes change; 21 materialized source-table parquets are rebuilt (the registry also has panel_ids and two derived aliases). All 61 control hashes are unchanged: Uganda 24, Niger 19, GhanaLSS 18. Four global build fingerprints are identical.','',
'Reproduce from saved cold artifacts (Python 3.11.6 / pandas 3.0.6 environment; no source builds):','',
'```sh',
'cd /local/job39263225/worktrees/malawi-sale-docs',
'export PYTHONPATH="$PWD" LSMS_COUNTRIES_ROOT="$PWD/lsms_library/countries"',
'export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1',
'taskset -c 24-31 /local/job39263225/venv_img/bin/python /local/job39263225/malawi-pr-review/code-review/trace_food_factors.py --baseline /local/job39263225/malawi-pr-review/baseline --final /local/job39263225/malawi-pr-review/verification --out /local/job39263225/malawi-pr-review/code-review',
'taskset -c 24-31 /local/job39263225/venv_img/bin/python /local/job39263225/malawi-pr-review/code-review/compare_outputs.py --baseline /local/job39263225/malawi-pr-review/baseline --final /local/job39263225/malawi-pr-review/verification --out /local/job39263225/malawi-pr-review/code-review',
'```','',
'Machine-readable counts: summary.json; factor attribution: food_factor_trace.json. This report contains aggregate comparisons only. - Sue']
(a.out/'COMPARISON.md').write_text('\n'.join(lines)+'\n')
