from pathlib import Path
import ast,collections,dataclasses,datetime,hashlib,json,math,os,stat,traceback
import numpy as np,pandas as pd
from pandas.api.extensions import ExtensionArray,ExtensionDtype
P=Path(__file__).resolve().parent;OLD=P.parent/'849';S=OLD/'scenario-v3';ROOT=Path('/local/job40255479/worktrees/isa-Uganda-849')
assert json.loads((P/'protection-before.json').read_text())['original_Uganda_manifest_receipts_exact']==344
PROTOTYPE=Path('/local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution/exact_artifacts.py')
source=PROTOTYPE.read_text();tree=ast.parse(source)
selected=[n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name in {'typed','dtype','index','frame','receipt'}]
assert len(selected)==5
ARGS=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='ARGS'for t in n.targets)))
assert ARGS['check_exact']is True and ARGS['rtol']==ARGS['atol']==0
attrs_types=collections.Counter();exec(compile(ast.Module(body=selected,type_ignores=[]),str(PROTOTYPE)+' [function AST only]','exec'))
control_text=source[source.index('control_count=0\n'):source.index('# Full matched-stage pairs')]
exec(compile(control_text,str(PROTOTYPE)+' [seven comparator self-controls]','exec'));assert control_count==7
inputs={};checks=collections.Counter();failures=[];details=[];cache={}
def track(path):
 path=Path(path).resolve();assert path.is_relative_to(OLD)or path==PROTOTYPE or path==ROOT/'lsms_library/countries/Uganda/_/updated_ids.json'
 if str(path)not in inputs:inputs[str(path)]=receipt(path)
 return path
def load(rel):
 path=track(OLD/rel)
 if str(path)not in cache:cache[str(path)]=pd.read_pickle(path)
 return cache[str(path)]
def document(name):return json.loads(track(OLD/name).read_text())
def check(label,group,fn):
 try:fn();checks[group]+=1
 except Exception:
  entry={'label':label,'group':group,'traceback':traceback.format_exc()};failures.append(entry);(P/f'failure-{len(failures):02d}-private.txt').write_text(entry['traceback'])
def fcheck(a,b,label,group):check(label,group,lambda:frame(a,b,label))
def scheck(a,b,label,group):check(label,group,lambda:typed(a,b,label))
for path in sorted((OLD/'baseline-cold').glob('*-private.pkl')):
 fcheck(load('baseline-cold/'+path.name),load('baseline-warm/'+path.name),'cold/warm/'+path.name,'baseline_cold_warm_full_frames')
actual=load('baseline-cold/public-private.pkl');raw=load('baseline-cold/raw-country-private.pkl');public=load('scenario-v3/projected-public-private.pkl');projected_raw=load('scenario-v3/projected-raw-private.pkl')
fcheck(raw.drop(columns='KgFactor'),projected_raw.drop(columns='KgFactor'),'scenario/raw/non-KgFactor','scenario_nonfactor_full_frames')
fcheck(actual.drop(columns='KgFactor'),public.drop(columns='KgFactor'),'scenario/public/non-KgFactor','scenario_nonfactor_full_frames')
fcheck(load('baseline-cold/factors-private.pkl'),load('scenario-v3/factors-before-private.pkl'),'baseline/factor-before','baseline_factor_positive_control')
waves=list(document('oracle-summary.json')['helper_rows']);keys=['t','i','plot_id','j','u','condition','season'];oracle={};helpers={}
for wave in waves:
 b=load('scenario-v3/'+wave+'-helper-before-private.pkl');a=load('scenario-v3/'+wave+'-helper-proposed-private.pkl');o=load('scenario-v3/'+wave+'-oracle-rows-private.pkl');oracle[wave]=o;helpers[wave]=(b,a)
 fcheck(b.drop(columns='KgFactor'),a.drop(columns='KgFactor'),wave+'/helper/nonfactor','helper_nonfactor_full_frames')
 # Existing independent source-key oracle: group first-nonmissing, key-align,
 # then place expected values on the untouched helper axis as the frozen
 # oracle.py explicitly did. No actual/helper frame is cast or reindexed.
 expected=o.groupby(keys).KgFactor.first().reindex(a.index).astype('Float64')
 assert expected.index.equals(a.index)
 expected=pd.Series(expected.to_numpy(),index=a.index,dtype='Float64',name='KgFactor')
 scheck(a.KgFactor,expected,wave+'/expected-source-factor','reported_factor_source_oracle_series')
 if wave in ['2009-10','2018-19']:
  fcheck(b,a,wave+'/whole-helper-control','unchanged_wave_full_helper_controls')
  fcheck(actual.loc[actual.index.get_level_values('t')==wave],public.loc[public.index.get_level_values('t')==wave],wave+'/whole-public-control','unchanged_wave_full_public_controls')
# Saved, already-decoded Stata frames only. Never invoke get_dataframe or
# read_stata; compare source-position operands for all15 recorded factor slots.
slots=document('source-slots.json')
for row in slots:
 wave=row['wave'];season=row['season'];slot=row['slot'];piece=oracle[wave].loc[(oracle[wave].season==season)&(oracle[wave].source_slot==slot)]
 filename=wave+'-AGSEC5'+season+'-source-private.pkl'if wave!='2019-20'else'Data-agsec5'+season.lower()+'-source-private.pkl'
 src=load('scenario-v3/'+filename);pos=piece.source_position.to_numpy(dtype=int)
 for col,field in [('Quantity',row['quantity_field']),('KgFactor',row['factor_field'])]:
  values=pd.to_numeric(src[field],errors='coerce').iloc[pos].to_numpy(dtype=float,na_value=np.nan)
  if col=='KgFactor':values=np.where(values>0,values,np.nan)
  expected=pd.Series(values,index=piece.index,name=col,dtype='float64')
  scheck(piece[col],expected,f'{wave}/{season}/{slot}/{col}','saved_source_position_operand_series')
# No ladder/helper invocation: independently verify stored screens and factor
# provenance using the frozen formulas/constants and saved factor columns.
from lsms_library import transformations as T
KG_KEYS=T._KG_UNIT_KEYS

def screen(df,reported):
 rep=np.asarray(reported.to_numpy(dtype=float,na_value=np.nan));rep=np.where(np.isfinite(rep)&(rep>0),rep,np.nan);have=~np.isnan(rep)
 text=pd.Series(df.index.get_level_values('u')).astype(str).str.strip().str.lower().to_numpy();is_kg=np.isin(text,sorted(KG_KEYS));lo,hi=T._KG_FACTOR_YEAR_BAND
 bad=(have&is_kg&(np.abs(rep-1.0)>T.KG_UNIT_TOLERANCE))|(have&(rep>T.KG_FACTOR_MAX))|(have&~is_kg&(rep==np.round(rep))&(rep>=lo)&(rep<=hi))
 return np.where(bad,np.nan,rep),bad

def expected_factor_attrs(f,source_frame):
 counts={layer:int((f.KgFactorSource==layer).sum())for layer in T.KG_FACTOR_LAYERS};counts.update(reported_implausible=int(f.kg_reported_rejected.sum()),shipped_implausible=int(f.kg_shipped_rejected.sum()),shipped_matched=0,declared=int((np.isfinite(f.kg_declared)&(f.kg_declared>0)).sum()))
 # Exact default no-shipped/no-unit_kg kwargs are recorded in baseline.py and
 # measure.py. All reported factors on Unknown are excluded from pair audits.
 sentinel=pd.Series(f.index.get_level_values('u')).astype(str).str.strip().str.lower().eq(str(T.U_UNKNOWN).lower()).to_numpy()
 rep=f.kg_reported.to_numpy(dtype=float,na_value=np.nan);rep=np.where(sentinel,np.nan,rep)
 def disagreement(other):
  oth=other.to_numpy(dtype=float,na_value=np.nan);both=np.isfinite(rep)&(rep>0)&np.isfinite(oth)&(oth>0);n=int(both.sum());k=int((np.abs(oth[both]-rep[both])/rep[both]>T.KG_FACTOR_DISAGREEMENT_TOLERANCE).sum())
  return {'both':n,'disagree':k,'share':k/n if n else None}
 return {'kg_factor_sources':counts,'kg_factor_declared_from':'attrs'if'unit_kg'in source_frame.attrs else None,'kg_factor_disagreement':{'reported_vs_shipped':disagreement(f.kg_shipped),'reported_vs_survey_median':disagreement(f.kg_survey_median),'reported_vs_inferred':disagreement(f.kg_inferred),'tolerance':T.KG_FACTOR_DISAGREEMENT_TOLERANCE}}
factors={}
for label,df,f,harvest in [('baseline',actual,load('baseline-cold/factors-private.pkl'),load('baseline-cold/harvest-kg-private.pkl')),('projected',public,load('scenario-v3/factors-proposed-private.pkl'),load('scenario-v3/harvest-proposed-private.pkl'))]:
 factors[label]=f;report,bad=screen(df,df.KgFactor)
 for col,values in [('kg_reported',report),('kg_reported_rejected',bad)]:
  expected=pd.Series(values,index=f.index,name=col);expected.attrs=expected_factor_attrs(f,df)
  scheck(f[col],expected,label+'/'+col,'saved_factor_screen_series')
 check(label+'/factor-full-attrs','saved_factor_full_typed_attrs',lambda f=f,df=df:typed(f.attrs,expected_factor_attrs(f,df),label+'.factor.attrs'))
 # Artifact arithmetic only: source Quantity times already-saved kg_per_unit,
 # the existing zero exclusion, then existing analyst plot-crop reduction.
 kg=pd.to_numeric(df.Quantity,errors='coerce')*f.kg_per_unit
 expected=pd.DataFrame({'Harvest_kg':kg}).replace(0,np.nan).dropna().groupby(['t','i','plot_id','j']).sum()
 ea=expected_factor_attrs(f,df);expected.attrs={name:ea[name]for name in ['kg_factor_sources','kg_factor_disagreement','kg_factor_declared_from']}
 fcheck(harvest,expected,label+'/saved-product-harvest','saved_product_harvest_full_frames')
# Check source identity carrier columns as a complete frame, with every native
# observation and the original RangeIndex order. Native-ID semantics are not
# re-decoded or claimed anew here.
identity=load('scenario-v3/source-identity-locators-private.pkl');combined=pd.concat([oracle[w]for w in waves],ignore_index=True)
fcheck(identity.loc[:,combined.columns],combined,'identity/retained-source-carriers','saved_identity_carrier_full_frame')
# Full saved collision diagnostics: direct source products plus saved public
# factors mapped by the existing config's explicit household lookup. No
# country/helper/finalizer/ladder is called. The actual frames remain intact.
ids=json.loads(track(ROOT/'lsms_library/countries/Uganda/_/updated_ids.json').read_text());carrier_records=[]
for wave in waves:
 b,a=helpers[wave];o=oracle[wave].set_index(keys).copy();o['screened'],o['rejected']=screen(o,o.KgFactor);o['source_kg']=o.Quantity*o.screened
 g=o.groupby(level=keys);conflict=g.KgFactor.nunique(dropna=True);conflict=conflict[conflict>1].index;z=o.loc[o.index.isin(conflict)].copy();zg=z.groupby(level=keys);source_sum=zg.source_kg.sum(min_count=1)
 def helper_factors(kind,helper):
  ff=factors[kind];v=ff.kg_per_unit;axis=v.index.droplevel('v').reorder_levels(keys);lookup=pd.Series(v.to_numpy(),index=axis)
  mapped=helper.rename(index=ids.get(wave,{}),level='i').index
  assert mapped.is_unique and lookup.index.is_unique and bool(mapped.isin(lookup.index).all())
  return pd.Series(lookup.reindex(mapped).to_numpy(),index=helper.index)
 carrier=a.Quantity*helper_factors('projected',a);baseline=b.Quantity*helper_factors('baseline',b)
 finite=z.Quantity.map(np.isfinite);nonnegative=z.Quantity>=0
 complete=(zg.screened.count()==zg.size())&finite.groupby(level=keys).all()&nonnegative.groupby(level=keys).all()
 known=~pd.Series(source_sum.index.get_level_values('u').astype(str).str.strip().str.lower()==str(T.U_UNKNOWN).lower(),index=source_sum.index)
 complete=complete&known&carrier.reindex(complete.index).notna()
 expected=pd.DataFrame({'source_sum':source_sum,'carrier':carrier.reindex(source_sum.index),'baseline':baseline.reindex(source_sum.index),'all_operands_usable_and_unit_comparable':complete})
 diagnostic=load('scenario-v3/'+wave+'-collision-diagnostic-private.pkl')
 fcheck(diagnostic,expected,wave+'/full-collision-diagnostic','source_product_carrier_full_frames')
 good=complete[complete].index;delta=carrier.reindex(good)-source_sum.reindex(good)
 carrier_records.append({'wave':wave,'complete':len(good),'different':int((delta!=0).sum()),'net':float(delta.sum()),'absolute':float(delta.abs().sum())})
 # Canonical exact operands and zero-tolerance differences are separate from
 # preservation checks; never assert projected harvest equals the baseline.
oldcarrier=document('carrier-ambiguity-safe.json')['waves']
for r in carrier_records:
 old=next(x for x in oldcarrier if x['wave']==r['wave'])
 check(r['wave']+'/carrier-counts-and-exact-aggregates','carrier_exact_safe_aggregates',lambda r=r,old=old:typed(r,{'wave':old['wave'],'complete':old['comparable_complete_source_groups'],'different':old['diagnostically_different_carrier_groups'],'net':old['net_carrier_minus_direct_source_kg'],'absolute':old['sum_absolute_carrier_minus_direct_source_kg']}))
assert all(receipt(Path(name))==r for name,r in inputs.items())
(P/'artifact-input-receipts-private.json').write_text(json.dumps(inputs,indent=2)+'\n')
(P/'exact-failures-private.json').write_text(json.dumps(failures,indent=2)+'\n')
prototype={'source':str(PROTOTYPE),'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'functions':['typed','dtype','index','frame','receipt'],'function_AST_sha256':hashlib.sha256(ast.dump(ast.Module(body=selected,type_ignores=[])).encode()).hexdigest(),'function_AST_reused_without_change':True,'old_top_level_checks_executed':False,'seven_self_controls_rejected':control_count}
(P/'comparator-provenance.json').write_text(json.dumps(prototype,indent=2)+'\n')
gaps=['Ordinary default-Country-constructor warm probe saved only ordinary-default-warm.json, not its returned public/harvest frames; no exact artifact pair exists for that constructor comparison.','No separately captured explicit currency=None/Feature frame pair is saved; no new default-versus-None equality claim is made.','The recreated baseline _finalize_result return base_final was not separately saved; only its raw/public operands and projected frame exist. No finalizer/API rerun is performed.','Four preliminary scenario/*.pkl source artifacts belong to the preserved failed initial oracle launch; only final scenario-v3 artifacts are acceptance targets.']
r={'status':'EXACT_ARTIFACT_CHECKS_PASS'if not failures else'EXACT_ARTIFACT_FAILURES_PRESERVED','source_head':'f8d236694c26fe9e709c5548f02c70f74ea72c33','check_exact':True,'rtol':0,'atol':0,'all_axes_levels_codes_order_dtypes_masks_and_recursive_full_typed_attrs':True,'StringDtype_storage_and_na_value_checked':True,'immutable_record_FIELDS_presence_and_attributes_checked':True,'comparisons':dict(checks),'failures_count':len(failures),'failure_labels':[{'label':r['label'],'group':r['group']}for r in failures],'comparator_provenance':prototype,'loaded_inputs':len(inputs),'loaded_old_receipts_before_after_exact':True,'attrs_types':dict(attrs_types),'saved_source_product_carrier_aggregates':carrier_records,'baseline_crop_rows':len(actual),'projected_crop_rows':len(public),'baseline_harvest_rows':len(load('baseline-cold/harvest-kg-private.pkl')),'projected_harvest_rows':len(load('scenario-v3/harvest-proposed-private.pkl')),'projected_factor_changes_are_not_preservation_comparisons':True,'scope_gaps':gaps,'source_decode':False,'Country_Feature_calls':False,'helper_finalizer_ladder_or_measurement_stage_reruns':False,'pytest_or_product_tests':False,'data_cache_payload_files':sum(q.is_file()for q in(P/'data').rglob('*')),'original_tolerant_assertions_relabelled_exact_retroactively':False}
(P/'validation.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps({k:r[k]for k in ['status','comparisons','failures_count','failure_labels','loaded_inputs','baseline_crop_rows','projected_crop_rows']}))
