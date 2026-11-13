"""GH1051(B): all-source healing, actual Wave/CSV/private DVC/footer paths."""
from pathlib import Path
from types import SimpleNamespace
import copy, hashlib, io, json, os, socket, urllib.request, warnings
import pandas as pd
import pytest
from lsms_library import country as C, local_tools as LT, paths, data_access as DA

TABLE='tiny'; YEAR='w0'
class TinyWave:
    column_mapping=C.Wave.column_mapping
    _cartesian_keys=C.Wave._cartesian_keys
    _merge_subframes=C.Wave._merge_subframes
    def __init__(self,h,files,required=False):
        self.h=h;self.year=YEAR;self.name='Invented1051B/w0';self.file_path=h.root/'Invented1051B'/YEAR
        self.country=SimpleNamespace(name='Invented1051B',resources={'Data Scheme':{TABLE:{'index':'(t, v)','Region':'str','Latitude':'float' if required else {'type':'float','optional':True}}}})
        self.data_scheme=[TABLE];self.formatting_functions={}
        self.resources={TABLE:{'dfs':['main','enrichment'],'merge_on':['v'],'final_index':['t','v'],
          'main':{'file':'main.csv','idxvars':{'v':'ea'},'myvars':{'Region':'reg'}},
          'enrichment':{'file':files,'idxvars':{'v':'ea'},'myvars':{'Latitude':'lat'}}}}
    def _read_l2_wave_cache(self,request,path):
        assert request==TABLE and path==self.h.output
        return None
    def _input_hash(self,request):
        assert request==TABLE
        return 'invented-1051B-stable'
    def _required_scheme_columns(self,request):
        assert request==TABLE
        return C._required_scheme_columns(self.country.resources['Data Scheme'][TABLE])

@pytest.fixture
def h(tmp_path,monkeypatch,request):
    root=tmp_path/'countries';data=tmp_path/'data';cache=data/'dvc-cache'
    source=root/'Invented1051B'/YEAR/'Data';source.mkdir(parents=True);cache.mkdir(parents=True)
    h=SimpleNamespace(root=root,data=data,cache=cache,source=source,output=data/'Invented1051B'/YEAR/'_'/f'{TABLE}.parquet',events=[],available={'main.csv':True,'one.csv':False,'two.csv':False},payloads={'main.csv':b'ea,reg\nc1,N\nc2,S\n','one.csv':b'ea,lat\nc1,1\n','two.csv':b'ea,lat\nc2,2\n'},forbidden=[])
    h.md5={n:hashlib.md5(b).hexdigest() for n,b in h.payloads.items()}
    h.keys={f'memory://invented/{suffix}':n for n,md in h.md5.items() for suffix in [md[:2]+'/'+md[2:],'files/md5/'+md[:2]+'/'+md[2:]]}
    for n,md in h.md5.items():(source/(n+'.dvc')).write_text(f'outs:\n- md5: {md}\n  path: {n}\n')
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT',str(root));monkeypatch.setenv('LSMS_DATA_DIR',str(data))
    paths.countries_root.cache_clear();paths.data_root.cache_clear();monkeypatch.setattr(LT,'_COUNTRIES_DIR',root);monkeypatch.setattr(LT,'_DVC_CACHE_DIR',cache);monkeypatch.setattr(LT,'_FETCH_ATTEMPTS',1)
    for state in [C._BUILD_DROP_LEDGER,C._SOURCE_PROBES,C._DEGRADED_ATTEMPTED,C._REBUILD_HISTORY]:state.clear()
    def normalized(fn):
        p=Path(fn)
        candidates=[p] if p.is_absolute() else [Path.cwd()/p,root/p]
        for candidate in candidates:
            q=Path(os.path.normpath(candidate))
            if q.parent==source and q.name in h.payloads:return q
        raise AssertionError('foreign invented reader path')
    def get_file(src,dst):
        assert src in h.keys
        n=h.keys[src];expected=cache/h.md5[n][:2]/h.md5[n][2:]
        assert Path(dst)==expected.with_suffix(f'.tmp.{os.getpid()}')
        h.events.append(['get_file',n])
        if not h.available[n]:raise FileNotFoundError(n)
        Path(dst).write_bytes(h.payloads[n])
    def exists(src):
        assert src in h.keys;h.events.append(['exists',h.keys[src]]);return h.available[h.keys[src]]
    def stream(fn,mode=None):
        p=normalized(fn);assert mode in (None,'rb');h.events.append(['stream',p.name])
        if not h.available[p.name]:raise FileNotFoundError(p.name)
        return io.BytesIO(h.payloads[p.name])
    def remote(*,name):
        assert name=='invented';return SimpleNamespace(path='memory://invented',fs=SimpleNamespace(get_file=get_file,exists=exists))
    monkeypatch.setattr(LT,'DVCFS',SimpleNamespace(repo=SimpleNamespace(config={'core':{'remote':'invented'}},cloud=SimpleNamespace(get_remote=remote)),open=stream))
    def forbidden(*a,**k):h.forbidden.append('external leaf');raise AssertionError('external leaf forbidden')
    for n in ['connect','connect_ex']:monkeypatch.setattr(socket.socket,n,forbidden)
    monkeypatch.setattr(socket,'create_connection',forbidden);monkeypatch.setattr(urllib.request,'urlopen',forbidden);monkeypatch.setattr(DA,'get_data_file',forbidden);monkeypatch.setattr(LT,'_try_get_data_file',forbidden)
    reader=LT.get_dataframe
    def finite_reader(fn,*a,**k):
        p=normalized(fn)
        if not (p.parent/(p.name+'.dvc')).exists():
            h.events.append(['invented-no-source',p.name]);raise FileNotFoundError(p.name)
        return reader(fn,*a,**k)
    monkeypatch.setattr(LT,'get_dataframe',finite_reader)
    def build(files=None,required=False):
        files=['one.csv','two.csv'] if files is None else files
        wave=TinyWave(h,files,required)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always');out=C.Wave.grab_data(wave,TABLE)
        records=copy.deepcopy(C.build_drop_reports('Invented1051B',TABLE))
        stamp=LT.read_parquet_build_drops(h.output)
        assert stamp==(records or None)
        h.build={'records':records,'stamp':stamp,'warnings':[str(w.message) for w in caught],'columns':list(out.columns),'rows':len(out),'reader_events':copy.deepcopy(h.events)}
        return out,records,wave
    h.build_frame=build
    def fresh_probe(available,records):
        h.available.update(available);C._SOURCE_PROBES.clear();C._DEGRADED_ATTEMPTED.clear();h.events.clear()
        verdict=C._degraded_cache_verdict(records,('Invented1051B',TABLE));h.probe={'available':dict(h.available),'verdict':verdict,'memo':dict(C._SOURCE_PROBES),'attempted':[list(x) for x in C._DEGRADED_ATTEMPTED],'events':copy.deepcopy(h.events)};return verdict
    h.fresh_probe=fresh_probe
    yield h
    assert not h.forbidden
    request.node.user_properties.append(('diagnosis',json.dumps({'build':getattr(h,'build',None),'probe':getattr(h,'probe',None),'forbidden':h.forbidden})))
    if os.environ.get('DIAG_CASE_OUTPUT'):
        E=Path(os.environ['DIAG_CASE_OUTPUT']);E.mkdir(mode=0o700,exist_ok=True);(E/(request.node.name+'.json')).write_text(json.dumps({'build':getattr(h,'build',None),'probe':getattr(h,'probe',None),'forbidden':h.forbidden},indent=2)+'\n')
    for state in [C._BUILD_DROP_LEDGER,C._SOURCE_PROBES,C._DEGRADED_ATTEMPTED,C._REBUILD_HISTORY]:state.clear()
    paths.countries_root.cache_clear();paths.data_root.cache_clear()


@pytest.mark.parametrize('files',[['one.csv','two.csv'],['two.csv','one.csv']])
@pytest.mark.parametrize('one,two',[(False,False),(True,False),(False,True),(True,True)])
def test_all_sources_needed_for_one_subframe(h,files,one,two):
    out,recs,_=h.build_frame(files=files)
    assert len(recs)==1 and recs[0]['file'].endswith('/'+files[0])
    assert recs[0]['files']==['Invented1051B/w0/Data/'+n for n in files]
    assert recs[0]['kind']=='unavailable' and recs[0]['missing_optional']==['Latitude']
    assert 'Latitude' not in out and out.Region.tolist()==['N','S']
    verdict,reason=h.fresh_probe({'one.csv':one,'two.csv':two},recs)
    assert verdict==('rebuild' if one and two else 'serve')
    assert set(C._SOURCE_PROBES)<=set(recs[0]['files'])
    if one and two:
        assert reason.startswith('degraded:') and all(n in reason for n in files)
        before=copy.deepcopy(h.events)
        assert C._degraded_cache_verdict(recs,('Invented1051B',TABLE))==('serve',None)
        assert h.events==before

@pytest.mark.parametrize('one,two',[(True,False),(False,True)])
def test_absent_blob_inventory_includes_unattempted_files(h,one,two):
    h.available.update({'one.csv':one,'two.csv':two});out,recs,_=h.build_frame()
    assert 'Latitude' not in out and recs[0]['file'].endswith('/one.csv')
    expected=['two.csv'] if one else ['one.csv','two.csv']
    assert recs[0]['files']==['Invented1051B/w0/Data/'+n for n in expected]
    # Available remote bytes are not assumed locally present if never read.
    assert h.fresh_probe({},recs)==('serve',None)

@pytest.mark.parametrize('typo',['one.csv','two.csv'])
@pytest.mark.parametrize('required',[False,True])
def test_mixed_no_source_configuration_uses_existing_column_policy(h,typo,required):
    (h.source/(typo+'.dvc')).unlink();h.available.update({'one.csv':True,'two.csv':True})
    if required:
        with pytest.raises(RuntimeError,match='no such source.*required declared column'):
            h.build_frame(required=True)
        assert not h.output.exists() and C.build_drop_reports('Invented1051B',TABLE)==[]
    else:
        out,recs,_=h.build_frame();assert 'Latitude' not in out and len(recs)==1
        assert (recs[0]['kind'],recs[0]['cause'])==('config','no-such-source')
        assert recs[0]['file'].endswith('/'+typo) and recs[0]['missing_optional']==['Latitude']
        assert h.fresh_probe({},recs)==('complete',None) and C._SOURCE_PROBES=={}
        assert 'no such source' in ' '.join(h.build['warnings'])


def test_missing_paths_are_ordered_normalized_and_deduplicated(h):
    a=h.source/'one.csv';b=h.source/'two.csv'
    before=list(h.events)
    r=C._drop_record(YEAR,'enrichment',FileNotFoundError('invented'),[b,a,h.source/'x/../two.csv',a],{'Latitude':'lat'})
    assert r['files']==['Invented1051B/w0/Data/two.csv','Invented1051B/w0/Data/one.csv']
    assert h.events==before  # no fetch/exists/stream to construct metadata

@pytest.mark.parametrize('malformed',[None,[], 'one.csv',('one.csv',),[''],[None],['one.csv',7]])
def test_malformed_new_source_list_never_claims_healing(h,malformed):
    _,recs,_=h.build_frame();recs[0]['files']=malformed
    assert h.fresh_probe({'one.csv':True,'two.csv':True},recs)==('serve',None)
    assert C._SOURCE_PROBES=={} and h.events==[] and C._DEGRADED_ATTEMPTED==set()

@pytest.mark.parametrize('available',[False,True])
def test_legacy_single_file_keeps_shape_and_retry(h,available):
    _,recs,_=h.build_frame(files='one.csv');assert recs[0]['file'].endswith('/one.csv') and 'files' not in recs[0]
    assert h.fresh_probe({'one.csv':available},recs)[0]==('rebuild' if available else 'serve')


def test_legacy_ambiguous_stamp_is_not_inferred_or_migrated(h):
    _,recs,_=h.build_frame();recs[0].pop('files')
    assert h.fresh_probe({'one.csv':True,'two.csv':False},recs)[0]=='rebuild'
    assert 'files' not in recs[0] and list(C._SOURCE_PROBES)==[recs[0]['file']]


def test_independent_subframes_can_heal_without_all_drops_ready(h):
    _,recs,_=h.build_frame()
    independent={**copy.deepcopy(recs[0]),'sub':'independent','file':'Invented1051B/w0/Data/main.csv','missing_optional':['Other']}
    independent.pop('files')
    assert h.fresh_probe({'one.csv':True,'two.csv':False},recs+[independent])[0]=='rebuild'
    assert C._SOURCE_PROBES['Invented1051B/w0/Data/two.csv'] is False
    assert C._SOURCE_PROBES['Invented1051B/w0/Data/main.csv'] is True


def test_actual_readgate_memo_and_wave_rebuild_heal_private_footer(h):
    from types import MethodType
    _,recs,_=h.build_frame();assert LT.read_parquet_build_drops(h.output)==recs
    receiver=SimpleNamespace(name='Invented1051B',waves=[YEAR],wave_folder_map={})
    receiver._table_cache_hash=lambda table,waves:'invented-1051B-stable'
    for name in ['_cache_covers_waves','_restrict_to_waves','_grain_reports_for_waves']:
        setattr(receiver,name,MethodType(getattr(C.Country,name),receiver))
    C._SOURCE_PROBES.clear();C._DEGRADED_ATTEMPTED.clear();h.available['one.csv']=True
    with pytest.warns(C.DegradedCacheWarning) as caught:
        hit,covered,reason=C.Country._read_l2_country_cache(receiver,TABLE,h.output,[YEAR])
    assert 'Latitude' not in hit and covered==[YEAR] and reason is None
    w=caught[0].message;assert w.missing_optional==('Latitude',) and w.files==tuple(recs[0]['files'])
    before=copy.deepcopy(h.events);h.available['two.csv']=True
    with pytest.warns(C.DegradedCacheWarning):
        still,_,_=C.Country._read_l2_country_cache(receiver,TABLE,h.output,[YEAR])
    assert 'Latitude' not in still and h.events==before  # false memo lasts this process
    C._SOURCE_PROBES.clear();C._DEGRADED_ATTEMPTED.clear() # explicit new-process simulation
    miss,covered,reason=C.Country._read_l2_country_cache(receiver,TABLE,h.output,[YEAR])
    assert miss is None and covered==[YEAR] and reason.startswith('degraded:')
    restored,newrecs,_=h.build_frame();assert newrecs==[] and restored.Latitude.tolist()==[1,2]
    assert LT.read_parquet_build_drops(h.output) is None
    before_events=copy.deepcopy(h.events);before_bytes=h.output.read_bytes();before_mtime=h.output.stat().st_mtime_ns
    for _ in range(2):
        hit,_,reason=C.Country._read_l2_country_cache(receiver,TABLE,h.output,[YEAR])
        assert reason is None;pd.testing.assert_frame_equal(hit,restored,check_exact=True)
    assert h.events==before_events and h.output.read_bytes()==before_bytes and h.output.stat().st_mtime_ns==before_mtime


def test_malformed_sidecar_still_has_existing_unavailable_policy(h):
    (h.source/'one.csv.dvc').write_text('outs: []\n')
    _,recs,_=h.build_frame();assert recs[0]['kind']=='unavailable' and recs[0]['files']==['Invented1051B/w0/Data/one.csv','Invented1051B/w0/Data/two.csv']
    assert h.fresh_probe({},recs)==('serve',None)


def test_malformed_declaration_remains_loud_before_writer(h):
    with pytest.raises(AttributeError):C.Wave.grab_data(TinyWave(h,None),TABLE)
    assert C.build_drop_reports('Invented1051B',TABLE)==[] and not h.output.exists()


def test_untracked_local_sources_retain_legacy_representative_fallback(h):
    a=h.source/'local-a.csv';b=h.source/'local-b.csv';a.write_bytes(b'a\n1\n');b.write_bytes(b'a\n2\n')
    r=C._drop_record(YEAR,'local',FileNotFoundError('invented'),[a,b],{'Latitude':'lat'})
    assert r['kind']=='unavailable' and 'files' not in r and r['file'].endswith('/local-a.csv')


def test_missing_column_keeps_precedence_over_no_source(h):
    r=C._drop_record(YEAR,'enrichment',KeyError('bad column'),[h.source/'typo.csv',h.source/'two.csv'],{'Latitude':'lat'})
    assert (r['kind'],r['cause'])==('config','missing-column') and 'files' not in r


def test_all_available_build_preserves_original_columns(h):
    h.available.update({'one.csv':True,'two.csv':True});out,recs,_=h.build_frame()
    assert recs==[] and out.Region.tolist()==['N','S'] and out.Latitude.tolist()==[1,2]
    assert C._degraded_cache_verdict(None,('Invented1051B',TABLE))==('complete',None)
