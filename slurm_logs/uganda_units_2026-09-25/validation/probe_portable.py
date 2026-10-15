from pathlib import Path
from itertools import product
from collections import defaultdict
import importlib.util
import argparse
import json
import math
import numpy as np
import pandas as pd
import lsms_library

parser=argparse.ArgumentParser(description="Independent scalar oracle for Uganda food-unit classification.")
parser.add_argument("--output",type=Path,required=True)
args=parser.parse_args()
root=Path.cwd()
assert Path(lsms_library.__file__).resolve().is_relative_to(root),lsms_library.__file__
spec=importlib.util.spec_from_file_location('uganda_review',root/'lsms_library/countries/Uganda/_/uganda.py')
u=importlib.util.module_from_spec(spec)
spec.loader.exec_module(u)

def total(values):
    finite=[v for v in values if not pd.isna(v)]
    return sum(finite) if finite else np.nan

def oracle(wide):
    expected=defaultdict(list)
    for r in wide.reset_index().to_dict('records'):
        for source,parts,price in [('purchased',['home','away'],r['market']),('produced',['own'],r['farmgate']),('inkind',['inkind'],np.nan)]:
            qs=[r['quantity_'+p] for p in parts]
            xs=[r['value_'+p] for p in parts]
            q,x=total(qs),total(xs)
            label=r['u']
            if label=='Unknown' and x>0 and all(pd.isna(v) or v==0 for v in qs):
                label='Value';q=x;price=np.nan
            if q>0 or x>0:
                expected[(r['t'],r['i'],r['j'],label,source)].append((q,x,price))
    records=[]
    for key,group in expected.items():
        qs,xs,ps=zip(*group)
        prices=[p for p in ps if not pd.isna(p)]
        records.append((*key,total(qs),total(xs),sum(prices)/len(prices) if prices else np.nan))
    return pd.DataFrame(records,columns=['t','i','j','u','s','Quantity','Expenditure','Price']).set_index(['t','i','j','u','s']).sort_index()

rows=[]
for n,(unit,qh,qa,xh,xa) in enumerate(product(['Unknown','Kg','136'],[-3,-1,0,1,3,np.nan],[-3,-1,0,1,3,np.nan],[-2,0,5,np.nan],[-2,0,5,np.nan])):
    rows.append(dict(t='2020',i=f'h{n}',j='rice',u=unit,quantity_home=qh,quantity_away=qa,value_home=xh,value_away=xa,
                     quantity_own=qh,value_own=xa,quantity_inkind=qa,value_inkind=xh,market=100.0,farmgate=80.0))
wide=pd.DataFrame(rows).set_index(['t','i','j','u'])
oracle_results={}
for mode in ['unique','duplicates']:
    frame=wide.copy()
    if mode=='duplicates':
        flat=frame.reset_index();flat['i']=[f'h{n%17}' for n in range(len(flat))];frame=flat.set_index(['t','i','j','u'])
    before=frame.copy(deep=True)
    actual=u.food_acquired_to_canonical(frame)
    expected=oracle(frame)
    pd.testing.assert_frame_equal(actual,expected,check_dtype=False)
    pd.testing.assert_frame_equal(frame,before)
    oracle_results[mode]={'input_rows':len(frame),'output_rows':len(actual),'result':'PASS'}
    print(mode,'input',len(frame),'output',len(actual),'PASS',flush=True)

results={}
for dtype in ['float64','Float64','Int64','object']:
    frame=wide.iloc[[8,12,45]].copy().astype(dtype)
    try:
        actual=u.food_acquired_to_canonical(frame)
        pd.testing.assert_frame_equal(actual.astype(float),oracle(frame).astype(float))
        results[dtype]='PASS'
    except Exception as e:
        results[dtype]=type(e).__name__+': '+str(e)
# Categorical axes are a dtype boundary, distinct from the raw-reader test.
frame=wide.iloc[[8,12,45]].reset_index()
frame['u']=pd.Categorical(frame['u'])
frame=frame.set_index(['t','i','j','u'])
try:
    result=u.food_acquired_to_canonical(frame)
    results['categorical_u']='PASS'
except Exception as e:
    results['categorical_u']=type(e).__name__+': '+str(e)
print(json.dumps(results,indent=2),flush=True)
summary={'oracle':oracle_results,'dtype_checks':results}
args.output.parent.mkdir(parents=True,exist_ok=True)
args.output.write_text(json.dumps(summary,indent=2))
assert all(value=='PASS' for value in results.values()),results
