"""Credential-free smoke of the published v0.15.0 wheel, outside every checkout."""
from pathlib import Path
import importlib,importlib.metadata,json,os,subprocess
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from lsms_library import config
from lsms_library.catalog_relations import same_study_aliases,derived_from,evidence_for
from lsms_library.provenance import read_provenance
from lsms_library.cli import _git_tracked_files,_intree_source_files
from lsms_library import transformations as T
out=Path.cwd();envroot=Path('/local/job39919092/release-work/published-install-env')
pkg=Path(ll.__file__).resolve().parent;croot=countries_root()
assert pkg.is_relative_to(envroot),pkg
assert 'PYTHONPATH' not in os.environ and 'LSMS_COUNTRIES_ROOT' not in os.environ
assert croot==pkg/'countries',(croot,pkg)
assert not subprocess.run(['git','-C',str(pkg),'rev-parse','--is-inside-work-tree'],capture_output=True).returncode==0
before={str(p.relative_to(pkg)):(p.stat().st_size,p.stat().st_mtime_ns) for p in pkg.rglob('*') if p.is_file() and '__pycache__' not in p.parts}
version=importlib.metadata.version('LSMS_Library');assert version=='0.15.0',version
public=['Country','Wave','Feature','countries','features','coverage','currency_for','convert','PopulationRecord','population_records','RecallRecord','recall_records','recall','DerivationRecord','derivation_records','derivations','population_pyramid','coordinate_map','lorenz_curve','authenticate']
for name in public:assert callable(getattr(ll,name)),name
registered=ll.countries();features=ll.features();print('CATALOG_DIAGNOSTIC',len(registered),registered,features,flush=True);assert 'Uganda' in registered and 'household_roster' in features
metadata={}
for name in ['Uganda','Malawi','Tanzania']:
 c=ll.Country(name,preload_panel_ids=False);assert c.waves and c.data_scheme
 metadata[name]={'waves':list(c.waves),'tables':sorted(c.data_scheme)}
 assert 'food_acquired' in c.data_scheme
assert len(metadata['Uganda']['waves'])==8
assert 'Uganda' in ll.countries(feature='food_acquired')
assert 'food_quantities' in ll.features(country='Uganda')
f=ll.Feature('household_roster');assert 'Uganda' in f.countries
aliases=same_study_aliases();derived=derived_from();assert aliases['902']==aliases['297']=='297'
assert len(derived['5835'])==4 and evidence_for('5835')
provenance={}
for country,wave,cid in [('Malawi','2016-17','2939'),('Malawi','2019-20','3819')]:
 p=read_provenance(croot,country,wave);assert cid in p.catalog_ids;provenance[country+'/'+wave]={'catalog_ids':p.catalog_ids,'covers':p.covers}
p=read_provenance(croot,'Tanzania','2008-15');assert set(p.covers)=={'76','1050','2252','2862'};provenance['Tanzania/2008-15']={'catalog_ids':p.catalog_ids,'covers':p.covers}
tracked=_git_tracked_files(croot);assert tracked is None
strays=_intree_source_files(croot,registered,tracked=None);assert strays['git_consulted'] is False
assert not strays['with_sidecar'] and not strays['no_sidecar'] and not strays['stray_dirs'],strays
# Exercise a data-free derived API, not only import names.
index=pd.MultiIndex.from_tuples([('2020','h','Rice','kg','purchased')],names=['t','i','j','u','s'])
fa=pd.DataFrame({'Quantity':[2.0],'Expenditure':[6.0],'Price':[3.0]},index=index)
q=T.food_quantities_from_acquired(fa);assert q.Quantity.iloc[0]==2 and q.index.get_level_values('u')[0]=='kg'
assert not config.s3_creds_path().exists()
assert not (croot/'.dvc/s3_creds').exists()
after={str(p.relative_to(pkg)):(p.stat().st_size,p.stat().st_mtime_ns) for p in pkg.rglob('*') if p.is_file() and '__pycache__' not in p.parts}
assert before==after,'Runtime modified installed source/config tree'
record={'version':version,'package':str(pkg),'countries_root':str(croot),'data_root':str(data_root()),'cwd':str(out),'public_callables_checked':public,'registered_countries':len(registered),'declared_features':len(features),'country_config_reads':metadata,'provenance':provenance,'relations_file':str(pkg/'catalog_relations.yml'),'same_study_aliases':aliases,'derived_5835':derived['5835'],'git_tracked_files':tracked,'no_git_stray_counts':{k:len(v) for k,v in strays.items() if isinstance(v,list)},'source_tree_unchanged_excluding_bytecode':True,'synthetic_quantity_kg':float(q.Quantity.iloc[0])}
(out/'installed-smoke.json').write_text(json.dumps(record,indent=2));print(json.dumps(record,indent=2))
