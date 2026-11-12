"""Root-granted ten-source capture, ledger1497§5; no ID policy or build."""
import hashlib
import json
import os
from pathlib import Path
import pickle
import shutil
import subprocess
import sys
from unittest.mock import patch


def digest(path,algorithm='sha256'):
    h=hashlib.new(algorithm)
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(4*1024*1024),b''):h.update(block)
    return h.hexdigest()


def attach_build_tripwires(stack,library,local,pandas,forbidden,parquet_probe=None):
    """Actual public construction/writer targets, not the nested loader closure."""
    stack.enter_context(patch.object(library.Country,'__init__',forbidden))
    stack.enter_context(patch.object(library.Feature,'__init__',forbidden))
    stack.enter_context(patch.object(local,'to_parquet',forbidden))
    stack.enter_context(patch.object(pandas,'read_parquet',parquet_probe or forbidden))
    stack.enter_context(patch.object(pandas.DataFrame,'to_parquet',forbidden))


def confined_dta_probe(original,allowed,current,records,is_stata):
    """Retain the sanctioned reader's format probe only on bound DTA streams."""
    def probe(stream,*args,**kwargs):
        assert hasattr(stream,'read') and hasattr(stream,'seek'),'parquet/cache path prohibited'
        name=getattr(stream,'name',None)
        assert name is not None,'parquet/cache path prohibited'
        path=Path(name).resolve();binding=allowed.get(path)
        assert binding is not None,'unbound parquet/cache stream prohibited'
        assert current.get('role') in ('numeric','categories') and current.get('source')==binding['path']
        cursor=stream.tell();head=stream.read(64);stream.seek(cursor)
        assert is_stata(head),'only a bound Stata format probe is permitted'
        records.append({'source':binding['path'],'role':current['role'],'private_stream':str(path),'md5_binding':binding['md5']})
        return original(stream,*args,**kwargs)
    return probe


def main():
    os.umask(0o077)
    w=Path(os.environ['W']).resolve();assert Path.cwd()==w
    head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    assert head==os.environ['TANZANIA1497_EXPECTED_HEAD']
    spec=json.loads((w/'bench/research/tanzania_1497/inputs.json').read_text())
    assert spec['production_base']=='c780562b299bb100cdd006037ba424c8ad3264e9'
    assert subprocess.check_output(['git','rev-parse','HEAD:lsms_library'],text=True)==subprocess.check_output(['git','rev-parse',spec['production_base']+':lsms_library'],text=True)
    assert os.environ['PYTHONPATH']==str(w)
    assert os.environ['LSMS_COUNTRIES_ROOT']==str(w/'lsms_library/countries')
    assert all(os.environ[n]=='1' for n in ['OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS'])
    os.environ['LSMS_BUILD_WORKERS']='1'
    for key in ['LSMS_SKIP_AUTH','LSMS_NO_CACHE','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN','MICRODATA_API_KEY','LSMS_MICRODATA_API_KEY']:os.environ.pop(key,None)
    data=Path(os.environ['LSMS_DATA_DIR']).resolve()
    assert data.is_relative_to(Path('/local/job'+os.environ['SLURM_JOB_ID']))
    l1=data/'dvc-cache';assert l1.is_symlink()
    shared=l1.resolve();l1.unlink();l1.mkdir(mode=0o700)
    config=data.parent/'tanzania1497-config';config.mkdir(mode=0o700,exist_ok=False)
    os.environ['LSMS_CONFIG_DIR']=str(config)
    credential=config/'s3_creds'
    shutil.copyfile('/global/home/users/ligon/.config/lsms_library/s3_creds',credential);credential.chmod(0o600)
    os.environ['LSMS_S3_CREDS']=str(credential)
    (config/'config.yml').write_text('data_dir: '+str(data)+'\n')
    report=Path(os.environ['TANZANIA1497_REPORT']).resolve();report.mkdir(mode=0o700,parents=True,exist_ok=False)
    result={'head':head,'production_base':spec['production_base'],'job':os.environ['SLURM_JOB_ID'],
            'staging':[],'sources':[],'errors':[],'reader_roles':[],'stata_read_returns':[],
            'natural_dta_parquet_probes':[],'new_source_projections':0}
    try:
        import yaml
        recovery=Path(os.environ['TANZANIA1497_RECOVER']).resolve() if os.environ.get('TANZANIA1497_RECOVER') else None
        old=None
        if recovery:
            old=json.loads(Path(str(recovery)+'.terminal.json').read_text())
            assert old['head']=='5130f40980312002edb4fb70db09a3a386a72ee4' and old['job']=='41017803' and old['exit_code']==1
            assert old['credential_copy_absent']
            result['recovery']={'closed_job':old['job'],'head':old['head'],'no_new_projections':True}
        for n,item in enumerate(spec['sources']):
            p=w/'lsms_library/countries'/item['path']
            side=yaml.safe_load(Path(str(p)+'.dvc').read_text())['outs'][0]
            assert side['md5']==item['md5'] and side['size']==item['size'],item['path']
            relative=Path(item['md5'][:2])/item['md5'][2:]
            if recovery:
                rel='private-originals/'+f'{n:02d}-'+Path(item['path']).name
                origin=recovery/rel
                assert digest(origin)==old['files'][rel]['sha256']
            else:origin=next((p for p in [shared/relative,shared/'files/md5'/relative] if p.is_file()),None)
            entry={'source':item['path'],'md5':item['md5'],'bytes':item['size'],
                   'shared_blob_present':None if recovery else origin is not None,
                   'origin_kind':'terminal-hashed-private-original' if recovery else 'sharedL1'}
            if origin:
                before=(origin.stat().st_size,origin.stat().st_mtime_ns,digest(origin,'md5'))
                assert before[0]==item['size'] and before[2]==item['md5']
                target=l1/relative;target.parent.mkdir(mode=0o700,exist_ok=True)
                shutil.copyfile(origin,target);target.chmod(0o444)
                assert digest(target,'md5')==item['md5']
                assert before==(origin.stat().st_size,origin.stat().st_mtime_ns,digest(origin,'md5'))
                entry['physical_private_copy']=True
                entry['closed_original_unchanged' if recovery else 'shared_unchanged']=True
            result['staging'].append(entry)
        import lsms_library
        from lsms_library import local_tools, data_access
        from lsms_library.paths import countries_root,data_root
        assert Path(lsms_library.__file__).resolve().is_relative_to(w)
        assert countries_root().resolve()==w/'lsms_library/countries'
        assert data_root().resolve()==data
        assert Path(local_tools._DVC_CACHE_DIR).resolve()==l1
        assert Path(sys.prefix).resolve()==Path('/local/job'+os.environ['SLURM_JOB_ID']+'/venv_img')
        result['identity']={'cwd':str(w),'import':lsms_library.__file__,'prefix_real':str(Path(sys.prefix).resolve()),
                            'data_root':str(data),'countries_root':str(countries_root()),'config':str(config)}
        def forbidden(*args,**kwargs):raise AssertionError('WB/acquisition/API/Make/parquet forbidden in1497 capture')
        current={}
        allowed={}
        for item in spec['sources']:
            if item['kind']=='dta':
                rel=Path(item['md5'][:2])/item['md5'][2:]
                allowed[(l1/rel).resolve()]=item
                allowed[(l1/'files/md5'/rel).resolve()]=item
        previous=sys.getprofile()
        def observe(frame,event,arg):
            if previous:previous(frame,event,arg)
            if event=='return' and frame.f_code.co_name=='read' and frame.f_globals.get('__name__')=='pandas.io.stata':
                reader=frame.f_locals.get('self')
                result['stata_read_returns'].append({'source':current.get('source'),'role':current.get('role'),
                    'variable_labels':reader.variable_labels() if reader else None})
        sys.setprofile(observe)
        providers=['_get_catalog_idno','_find_stata_zip_url','_download_and_extract','_extract_all_from_zip']
        from contextlib import ExitStack
        with ExitStack() as stack:
            for name in providers:
                if hasattr(data_access,name):stack.enter_context(patch.object(data_access,name,forbidden))
            import pandas as pd
            probe=confined_dta_probe(pd.read_parquet,allowed,current,result['natural_dta_parquet_probes'],local_tools._is_stata_dta)
            attach_build_tripwires(stack,lsms_library,local_tools,pd,forbidden,probe)
            for n,item in enumerate(spec['sources']):
                current.clear();current.update(source=item['path'],role='projection')
                entry=dict(item);slug=f'{n:02d}-'+Path(item['path']).name
                try:
                    if recovery:projected=recovery/'private-originals'/slug
                    else:
                        projected=data_access.get_data_file(item['path'])
                        result['new_source_projections']+=1
                    assert projected is not None,'sanctioned source projection unavailable'
                    projected=Path(projected).resolve()
                    assert projected.is_relative_to(recovery if recovery else data)
                    assert projected.stat().st_size==item['size'] and digest(projected,'md5')==item['md5']
                    original=report/'private-originals'/slug;original.parent.mkdir(mode=0o700,exist_ok=True)
                    shutil.copyfile(projected,original);original.chmod(0o400)
                    entry['retained_original']=str(original);entry['sha256']=digest(original)
                    if item['kind']=='pdf':
                        with original.open('rb') as stream:assert stream.read(5)==b'%PDF-'
                        entry['pdf_magic']=True
                    else:
                        path=w/'lsms_library/countries'/item['path']
                        current['role']='numeric';numeric=local_tools.get_dataframe(path,convert_categoricals=False)
                        result['reader_roles'].append(dict(current))
                        current['role']='categories';categories=local_tools.get_dataframe(path,categories_only=True)
                        result['reader_roles'].append(dict(current))
                        output=report/'private-captures';output.mkdir(mode=0o700,exist_ok=True)
                        numeric.to_pickle(output/(slug+'.numeric.pkl'))
                        with (output/(slug+'.categories.pkl')).open('wb') as f:pickle.dump(categories,f)
                        entry.update(rows=len(numeric),columns=list(numeric.columns),dtypes={k:str(v) for k,v in numeric.dtypes.items()},
                            categories_role_internal_full_parse=True)
                    result['sources'].append(entry)
                except Exception as e:result['errors'].append({'source':item['path'],'type':type(e).__name__,'message':str(e)})
                (report/'inventory.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
        sys.setprofile(previous)
        assert len(result['reader_roles'])==12 and len(result['stata_read_returns'])==12, 'reader roles/actual full parses incomplete'
        tool=Path('/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/private/existing-pdf-tool/pypdf-6.21.4.zip')
        assert digest(tool)=='5998ebb4d7f1f6b59cf718425bd7337165496a4e025c55bc8a91dd7b87035039'
        sys.dont_write_bytecode=True;sys.path.insert(0,str(tool));import pypdf
        assert pypdf.__version__=='6.21.4'
        output=report/'pdf-text';output.mkdir(mode=0o700)
        for entry in result['sources']:
            if entry['kind']!='pdf':continue
            reader=pypdf.PdfReader(entry['retained_original'])
            pages=[{'pdf_page':i+1,'text':page.extract_text(orientations=(0,90,180,270))} for i,page in enumerate(reader.pages)]
            entry['pdf_pages']=len(pages)
            (output/(Path(entry['path']).name+'.json')).write_text(json.dumps({'source':entry['path'],'md5':entry['md5'],'pages':pages},ensure_ascii=False,indent=2)+'\n')
        return 1 if result['errors'] else 0
    finally:
        result['credential_removed']=False
        if credential.is_file():credential.unlink();result['credential_removed']=True
        (report/'inventory.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
        result['artifact_hashes']={p.relative_to(report).as_posix():{'sha256':digest(p),'bytes':p.stat().st_size} for p in report.rglob('*') if p.is_file()}
        (report/'terminal-custody.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
        print(json.dumps({'job':result['job'],'head':head,'sources':len(result['sources']),'errors':result['errors'],'credential_removed':result['credential_removed'],'reader_roles':len(result['reader_roles']),'actual_stata_full_parses':len(result['stata_read_returns'])}))


if __name__=='__main__':raise SystemExit(main())
