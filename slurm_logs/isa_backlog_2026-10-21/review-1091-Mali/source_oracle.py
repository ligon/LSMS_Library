from identity import *
from exact import equal
import pandas as pd
from lsms_library import local_tools as lt
assert json.loads((P/'PREPARATION.json').read_text())['terminal_success']
os.chdir(ROOT/'lsms_library/countries/Mali/2014-15/_')
assert Path.cwd().resolve()==ROOT/'lsms_library/countries/Mali/2014-15/_'
cm='categorical_mapping.org'
foods=lt.get_categorical_mapping(str(cm),tablename='harmonize_food',idxvars='Code',**{'Preferred Label':'Preferred Label'})
units=lt.get_categorical_mapping(str(cm),tablename='u',idxvars='Code',**{'Preferred Label':'Preferred Label'})
data={};cats={}
for visit in (1,2):
 fn=ROOT/f'lsms_library/countries/Mali/2014-15/Data/EACIS04_p{visit}.dta'
 data[visit]=(lt.get_dataframe(fn),lt.get_dataframe(fn,convert_categoricals=False))
 cats[visit]=lt.get_dataframe(fn,categories_only=True)
dictionary=cats[1]['s04q03']
assert dictionary==cats[1]['s04q06']==cats[1]['s04q09']==cats[2]['s04q09']
assert not cats[2].get('s04q03')and not cats[2].get('s04q06')
pieces=[];unresolved=0;raw_positive=0;rows=[]
for visit,(decoded,codes)in data.items():
 for obs,(uf,qf,pf)in enumerate([('s04q03','s04q04','s04q05'),('s04q06','s04q07','s04q08'),('s04q09','s04q10','s04q11')],1):
  unit_labels=codes[uf].map(dictionary).astype('string')
  j=decoded.s04q01.astype('string').str.strip().map(foods).astype('string')
  u=unit_labels.str.strip().map(units).astype('string')
  price=pd.to_numeric(codes[pf],errors='coerce');quantity=pd.to_numeric(codes[qf],errors='coerce')
  eligible=price.notna()&(price>0)&j.notna()&u.notna()
  raw_positive+=int((price>0).sum());unresolved+=int(((price>0)&u.isna()).sum())
  v=codes.grappe.astype('Int64').astype('string')
  expected=pd.DataFrame({'t':'2014-15','v':v,'i':v,'j':j,'u':u,'variety':codes.s04q02.astype('Int64').astype('string'),'visit':visit,'obs':obs,'Price':price,'Quantity':quantity.mask(quantity.isin([999,9999])),'Description':decoded.s04q02.astype('string').str.strip()})
  pieces.append(expected.loc[eligible]);rows.append({'visit':visit,'obs':obs,'raw_rows':len(codes),'positive_price':int((price>0).sum()),'eligible':int(eligible.sum())})
oracle=pd.concat(pieces,ignore_index=True).set_index(['t','v','i','j','u','variety','visit','obs'])[['Price','Quantity','Description']].sort_index()
assert len(oracle)==155931 and oracle.index.is_unique and not oracle.index.to_frame(index=False).isna().any().any()
assert oracle.groupby(level='obs').size().to_dict()=={1:66699,2:49014,3:40218}
assert oracle.groupby(level='visit').size().to_dict()=={1:66159,2:89772}
assert unresolved==129
oracle.to_pickle(P/'source-oracle-prewrite-private.pkl')
# Native reference is post-Parquet, so use the actual unchanged writer control.
lt.to_parquet(oracle,P/'oracle-native-writer-control.parquet')
serialized=lt.get_dataframe(P/'oracle-native-writer-control.parquet')
serialized.to_pickle(P/'source-oracle-native-private.pkl')
# Preserved reviewed822 native reference provides the complete actual dtypes/axes/order/attrs target.
equal(serialized,pd.read_pickle(P/'references/default-all-cold-native.pkl'))
(P/'source-oracle.json').write_text(json.dumps({'status':'PASS','source_files':2,'eligible_rows':len(oracle),'obs_counts':{'1':66699,'2':49014,'3':40218},'visit_counts':{'1':66159,'2':89772},'positive_unresolved_unit_entries_excluded':unresolved,'raw_positive_slots':raw_positive,'slots':rows,'all_native_values_keys_masks_dtypes_order_axes_full_typed_attrs_exact':True,'candidate_wave_helper_or_finalizer_called_for_expected_rows':False,'source_oracle_and_native_serialization_layers_separate':True,'postread_oracle_uses_actual_unchanged_writer_control':True,'initial_strict_prewrite_vs_postread_dtype_stop_preserved':True,'missing_passage2_unit_dictionaries_confirmed':True,'exact_known_community_dictionary_correspondence':True},indent=2))
print('Independent raw-source oracle PASS:155931 full native rows;129 unresolved-unit entries remain excluded.')
