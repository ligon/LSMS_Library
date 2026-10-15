"""Attribute derived-food differences using saved frames and pure transforms."""
from pathlib import Path
import argparse
import json
import hashlib
import warnings
import pandas as pd
import numpy as np
import lsms_library as ll
from lsms_library.transformations import food_kg_factors
p=argparse.ArgumentParser();p.add_argument('--baseline',type=Path,required=True);p.add_argument('--final',type=Path,required=True);p.add_argument('--out',type=Path,required=True);args=p.parse_args()
base=pd.read_pickle(args.baseline/'food_acquired.pkl'); final=pd.read_pickle(args.final/'food_acquired.pkl')
base.attrs={};final.attrs={}
# Map labels only for matching rows, never quantities or candidate factors.
def canonical_index(x,unit=True,food=True):
    ix=x.index.to_frame(index=False)
    if unit: ix['u']=ix['u'].replace({'Kilogram':'Kilogramme','Kkilogram':'Kilogramme'})
    if food: ix.loc[ix['t'].isin(['2016-17','2019-20'])&ix['j'].eq('Groundnut'),'j']='Groundnut - Boiled (Vendor)'
    x=x.copy(deep=False); x.attrs={};x.index=pd.MultiIndex.from_frame(ix);return x.sort_index()
# The country unit-factor composition is config-only, with no table builds.
unit_kg=ll.Country('Malawi')._unit_kg_factors()
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    bf=canonical_index(food_kg_factors(base,unit_kg=unit_kg))
    ff=canonical_index(food_kg_factors(final,unit_kg=unit_kg))
    # Counterfactual: aliases applied, old mixed raw/vendor j vocabulary retained.
    alias_only=canonical_index(base,food=False)
    af=canonical_index(food_kg_factors(alias_only,unit_kg=unit_kg))
x=canonical_index(base); y=canonical_index(final)
pd.testing.assert_frame_equal(x,y,check_exact=True)
mask=~(bf['kg_per_unit'].eq(ff['kg_per_unit'])|(bf['kg_per_unit'].isna()&ff['kg_per_unit'].isna()))
changed=ff[mask].copy();changed['before_factor']=bf.loc[mask,'kg_per_unit'];changed['before_source']=bf.loc[mask,'KgFactorSource'];changed['native_quantity']=y.loc[mask,'Quantity'];changed['before_kg']=changed['native_quantity']*changed['before_factor'];changed['after_kg']=changed['native_quantity']*changed['kg_per_unit']
changed['alias_factor']=af.loc[mask,'kg_per_unit']; changed['alias_source']=af.loc[mask,'KgFactorSource']
# Report only aggregated cells, never household identifiers.
cols=['t','j','u','before_source','KgFactorSource','before_factor','kg_per_unit','alias_factor']
cells=changed.reset_index().groupby(cols,dropna=False).agg(rows=('native_quantity','size'),native_quantity=('native_quantity','sum'),before_kg=('before_kg','sum'),after_kg=('after_kg','sum')).reset_index()
summary={'changed_input_factors':int(mask.sum()),'cells':cells.to_dict('records'),'source_counts_before':bf['KgFactorSource'].value_counts().to_dict(),'source_counts_after':ff['KgFactorSource'].value_counts().to_dict(),'alias_only_factor_changes':int((~(bf['kg_per_unit'].eq(af['kg_per_unit'])|(bf['kg_per_unit'].isna()&af['kg_per_unit'].isna()))).sum()),'item_split_factor_changes':int((~(af['kg_per_unit'].eq(ff['kg_per_unit'])|(af['kg_per_unit'].isna()&ff['kg_per_unit'].isna()))).sum())}
# Source Quantity_kg overrides apply; these changed factors are all non-survey layers.
assert not changed['KgFactorSource'].eq('survey_kg').any()
source_changed=bf['KgFactorSource'].ne(ff['KgFactorSource'])
st=ff.loc[source_changed,['KgFactorSource']].copy();st['before_source']=bf.loc[source_changed,'KgFactorSource'];st['magnitude_changed']=mask.loc[source_changed]
summary['source_transitions']=st.reset_index().groupby(['t','j','before_source','KgFactorSource'],dropna=False).agg(rows=('magnitude_changed','size'),magnitudes_changed=('magnitude_changed','sum')).reset_index().to_dict('records')
# Reproduce served derived-food magnitudes from these very factors. Finalizers
# only add v and reorder levels here, so compare on the other served keys.
import lsms_library.transformations as tr
real_factors=tr.food_kg_factors
replay={}
for side,frame,factors,folder in [('baseline',base,bf,args.baseline),('final',final,ff,args.final)]:
    # bf/ff were normalized for comparison. Obtain the original-index factors
    # by assigning the canonical lookup back onto original input row order.
    canonical=frame.index.to_frame(index=False)
    canonical['u']=canonical['u'].replace({'Kilogram':'Kilogramme','Kkilogram':'Kilogramme'})
    canonical.loc[canonical['t'].isin(['2016-17','2019-20'])&canonical['j'].eq('Groundnut'),'j']='Groundnut - Boiled (Vendor)'
    selected=factors.reindex(pd.MultiIndex.from_frame(canonical));selected.index=frame.index
    # Preserve original factor-summary attrs for the transformation return.
    selected.attrs={'kg_factor_sources':{},'kg_factor_wave_spread':{},'kg_factor_baseline_gate':{}}
    tr.food_kg_factors=lambda *a,**kw:selected
    replay[side]={}
    for name,fn in [('food_quantities',tr.food_quantities_from_acquired),('food_prices',tr.food_prices_from_acquired)]:
        got=fn(frame,unit_kg=unit_kg); got.attrs={}
        expected=pd.read_pickle(folder/f'{name}.pkl');expected.attrs={}
        if 'v' in expected.index.names:expected=expected.droplevel('v')
        expected=expected.reorder_levels(got.index.names).sort_index();got=got.sort_index()
        # Country spelling normalization changes kilogram aliases on price
        # indices; quantities are already tagged kg. Compare mapped keys.
        got=canonical_index(got,food=False);expected=canonical_index(expected,food=False)
        pd.testing.assert_frame_equal(got,expected,check_exact=True)
        replay[side][name]='exact_from_attributed_factors'
tr.food_kg_factors=real_factors
summary['served_replay']=replay
summary['artifact_sha256']={side:{f:hashlib.file_digest((folder/f).open('rb'),'sha256').hexdigest() for f in ['food_acquired.pkl','food_quantities.pkl','food_prices.pkl']} for side,folder in [('baseline',args.baseline),('final',args.final)]}
(args.out/'food_factor_trace.json').write_text(json.dumps(summary,indent=2,default=str))
print(cells.to_string(index=False));print('totals', {k:v for k,v in summary.items() if k!='cells'})
