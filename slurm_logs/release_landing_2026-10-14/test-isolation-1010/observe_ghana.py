"""Private event observer; leaves hashed production functions untouched."""
import os,json,time,subprocess
from pathlib import Path
import lsms_library as ll
from lsms_library.paths import data_root,countries_root
from lsms_library.local_tools import read_parquet_cache_hash,read_parquet_cache_waves,cache_freshness
ROOT=Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
assert Path(ll.__file__).resolve().parent==ROOT/'lsms_library',ll.__file__
assert countries_root()==ROOT/'lsms_library/countries'
OUT=Path(os.environ['GHANA_EVENTS'])
_C=ll.Country('GhanaLSS')
def snapshot():
 p=data_root('GhanaLSS')/'var/food_acquired.parquet'
 expected=_C._table_cache_hash('food_acquired',_C.waves)
 return {'NO_CACHE':os.environ.get('LSMS_NO_CACHE'),'root':str(data_root()),'country_root':str(data_root('GhanaLSS')),'expected':expected,'stored':read_parquet_cache_hash(p),'manifest':read_parquet_cache_waves(p),'freshness':cache_freshness(p,expected)}
def emit(event,**kw):
 with OUT.open('a') as f: f.write(json.dumps({'event':event,'time':time.time(),**kw})+'\n')
_before=snapshot()
_run=subprocess.run

def observe_run(*args,**kw):
 cmd=args[0] if args else kw.get('args')
 # Never record environment values, raw data, or authentication commands.
 is_build=isinstance(cmd,(list,tuple)) and str(cmd[0]) in ('make',os.sys.executable) and 'GhanaLSS' in str(kw.get('cwd',''))
 if not is_build:return _run(*args,**kw)
 started=time.monotonic();emit('build_start',cmd=[str(x) for x in cmd],cwd=str(kw.get('cwd')),snapshot=snapshot())
 try:return _run(*args,**kw)
 finally:emit('build_end',seconds=time.monotonic()-started,snapshot=snapshot())
subprocess.run=observe_run
assert snapshot()==_before,'Observer changed cache identity'
emit('observer_ready',snapshot=_before,package=ll.__file__)

def pytest_runtest_setup(item):emit('test_setup',node=item.nodeid,snapshot=snapshot())
def pytest_runtest_logreport(report):emit('test_report',node=report.nodeid,when=report.when,outcome=report.outcome,duration=report.duration,snapshot=snapshot())
def pytest_sessionfinish(session,exitstatus):emit('finish',exitstatus=exitstatus,snapshot=snapshot())
