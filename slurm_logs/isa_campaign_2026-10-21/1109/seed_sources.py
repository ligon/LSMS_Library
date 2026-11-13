from pathlib import Path
import json,re,shutil,hashlib
import yaml
import argparse
ap=argparse.ArgumentParser()
ap.add_argument('--checkout',required=True,type=Path)
ap.add_argument('--shared-data',required=True,type=Path)
ap.add_argument('--comparator-data',required=True,type=Path)
ap.add_argument('--out-root',required=True,type=Path)
args=ap.parse_args()
ROOT=args.checkout.resolve()
import lsms_library as ll
import pyarrow.parquet as pq
from lsms_library.paths import countries_root
assert Path(ll.__file__).resolve()==ROOT/'lsms_library/__init__.py'
assert countries_root().resolve()==ROOT/'lsms_library/countries'
RAW=args.shared_data.resolve()
DEST=args.out_root.resolve()
country=ROOT/'lsms_library/countries/Malawi'
sources=set()
for p in country.glob('*/Data/**/*.dta.dvc'):
    if re.match(r'(?i)ag_mod_[dgh](?:_\d+)?\.dta\.dvc$',p.name):sources.add(p)
def file_specs(node):
    if isinstance(node,yaml.ScalarNode):yield node.value
    elif isinstance(node,yaml.SequenceNode):
        for n in node.value:yield from file_specs(n)
    elif isinstance(node,yaml.MappingNode):
        for k,v in node.value:yield k.value
for wave in sorted(country.glob('*/_/data_info.yml')):
    tree=yaml.compose(wave.read_text())
    for k,v in tree.value:
        if k.value not in ('sample','panel_ids'):continue
        def files(node):
            if isinstance(node,yaml.MappingNode):
                for a,b in node.value:
                    if a.value=='file':yield from file_specs(b)
                    else:yield from files(b)
            elif isinstance(node,yaml.SequenceNode):
                for n in node.value:yield from files(n)
        for f in files(v):sources.add(wave.parent.parent/'Data'/f'{f}.dvc')
manifest=[]
for p in sorted(sources):
    assert p.is_file(),p
    data=yaml.safe_load(p.read_text())['outs'][0];h=data['md5']
    choices=[RAW/'dvc-cache'/h[:2]/h[2:],RAW/'dvc-cache/files/md5'/h[:2]/h[2:]]
    blob=next((b for b in choices if b.is_file()),None)
    assert blob is not None, f'No cached blob for {p.relative_to(ROOT)}'
    assert blob.stat().st_size==data['size'],p
    for variant in ('baseline','fixed'):
        out=DEST/variant/'dvc-cache'/h[:2]/h[2:];out.parent.mkdir(parents=True,exist_ok=True)
        if not out.exists():shutil.copyfile(blob,out)
        assert out.stat().st_ino!=blob.stat().st_ino
    manifest.append({'source':str(p.relative_to(ROOT)),'md5':h,'bytes':data['size']})
for variant in ('baseline','fixed'):
    for table in ('plot_inputs','sample'):
        p=args.comparator_data.resolve()/'Uganda/var'/f'{table}.parquet';assert p.is_file(),p
        comparator=ll.Country('Uganda')
        expected=comparator._table_cache_hash(table,comparator.waves)
        assert pq.read_metadata(p).metadata[b'lsms_cache_hash'].decode()==expected,(p,expected)
        out=DEST/variant/'Uganda/var'/p.name;out.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,out)
        assert p.stat().st_ino!=out.stat().st_ino
(DEST/'seed-manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps({'unique_sources':len(manifest),'copied_bytes_per_root':sum(x['bytes']for x in manifest),'private_L2':'Uganda plot_inputs/sample only; Malawi cold','all_L1_and_L2_physically_copied':True}))
