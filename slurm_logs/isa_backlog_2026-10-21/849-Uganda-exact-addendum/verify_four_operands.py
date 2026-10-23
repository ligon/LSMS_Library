from pathlib import Path
import ast,collections,dataclasses,datetime,hashlib,json,math,os,stat
import numpy as np,pandas as pd
from pandas.api.extensions import ExtensionArray,ExtensionDtype
P=Path(__file__).resolve().parent;OLD=P.parent/'849'
prototype=Path('/local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution/exact_artifacts.py')
source=prototype.read_text();tree=ast.parse(source);nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name in {'typed','dtype','index','frame','receipt'}]
ARGS=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='ARGS'for t in n.targets)));attrs_types=collections.Counter();exec(compile(ast.Module(body=nodes,type_ignores=[]),str(prototype)+' [unchanged function AST]','exec'))
r=json.loads((P/'initial-validation-with-native-dtype-stops.json').read_text());assert len(r['failure_labels'])==4 and {x['group']for x in r['failure_labels']}=={'saved_source_position_operand_series'}
assert hashlib.sha256(source.encode()).hexdigest()==r['comparator_provenance']['source_sha256']
inputs={};proof=[]
def load(rel):
 path=OLD/rel;inputs[str(path)]=receipt(path);return pd.read_pickle(path)
o=load('scenario-v3/2011-12-oracle-rows-private.pkl');slots=json.loads((OLD/'source-slots.json').read_text());original=(OLD/'oracle.py').read_text().splitlines()
for season in ['A','B']:
 row=next(x for x in slots if x['wave']=='2011-12'and x['season']==season and x['slot']==1)
 piece=o.loc[(o.season==season)&(o.source_slot==1)];src=load('scenario-v3/2011-12-AGSEC5'+season+'-source-private.pkl');positions=piece.source_position.to_numpy(dtype=int)
 for column,field in [('Quantity',row['quantity_field']),('KgFactor',row['factor_field'])]:
  # Match frozen oracle.py: numeric source Series, factor.where(>0, NaN),
  # then native to_numpy(). Neither dtype comes from the observed result.
  numeric=pd.to_numeric(src[field],errors='coerce')
  expected_source=numeric.where(numeric>0,np.nan)if column=='KgFactor'else numeric
  values=expected_source.iloc[positions].to_numpy()
  expected=pd.Series(values,index=piece.index,name=column)
  typed(piece[column],expected,'2011-12/'+season+'/'+column)
  proof.append({'wave':'2011-12','season':season,'slot':1,'column':column,'source_field':field,'source_dtype':str(src[field].dtype),'numeric_source_dtype':str(numeric.dtype),'native_numpy_dtype':str(values.dtype),'observed_dtype':str(piece[column].dtype),'rows':len(piece),'exact_zero_tolerance_with_full_axis_dtype_mask_attrs':True,'observed_dtype_not_used_to_construct_expected':True})
assert len(proof)==4 and all(receipt(Path(path))==rec for path,rec in inputs.items())
assert all(x['native_numpy_dtype']==x['source_dtype']==x['observed_dtype']=='float32'for x in proof)
r['status']='EXACT_SAVED_ARTIFACTS_CONFIRMED_WITH_SCOPE_GAPS';r['comparisons']['saved_source_position_operand_series']+=4;r['initial_failures_preserved']='initial-four-source-dtype-stops-private.json';r['initial_failure_kind']='Expected-construction float64 assumption; original source/oracle is float32. Actual artifact frames were never cast.';r['failures_count']=0;r['failure_labels']=[];r['bounded_four_native_dtype_checks']=proof;r['all_saved_factor_slots_source_quantity_and_factor_series']=30;r['unchanged_comparator_function_AST']=True
(P/'validation.json').write_text(json.dumps(r,indent=2)+'\n');(P/'four-native-source-dtype-checks.json').write_text(json.dumps({'checks':proof,'original_oracle_source_sha256':hashlib.sha256((OLD/'oracle.py').read_bytes()).hexdigest(),'source_operation_quotes':[{'line':i+1,'text':line}for i,line in enumerate(original)if 'qty=pd.to_numeric' in line or "rep=pd.to_numeric"in line or "'Quantity':qty.to_numpy(),'KgFactor':rep.where(rep>0,np.nan).to_numpy()"in line]},indent=2)+'\n');(P/'four-input-receipts-private.json').write_text(json.dumps(inputs,indent=2)+'\n')
(P/'prototype-receipt-private.json').write_text(json.dumps({'path':str(prototype),'receipt':receipt(prototype)},indent=2)+'\n')
print(json.dumps({'status':r['status'],'four_native_dtype_checks':4,'exact_total_checks':sum(r['comparisons'].values()),'failures':0,'scope_gaps':len(r['scope_gaps'])}))
