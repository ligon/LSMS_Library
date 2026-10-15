import json,time,importlib.util,os
from pathlib import Path
import pytest
import observe_ghana as obs
root=Path(os.environ['PYTHONPATH'].split(os.pathsep)[0])
assert os.environ.get('LSMS_NO_CACHE') is None
obs.emit('before_preceding_suite',snapshot=obs.snapshot())
status=pytest.main(['tests/test_collectonly_no_purge.py','-q','--timeout=600','--timeout-method=thread'])
assert status==0,status
obs.emit('after_preceding_suite',snapshot=obs.snapshot())
spec=importlib.util.spec_from_file_location('actual_currency_tests',root/'tests/test_currency_denominated_units.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
started=time.monotonic()
obs.emit('api_start',api='_country',snapshot=obs.snapshot())
c,food=m._country('GhanaLSS')
obs.emit('api_end',api='_country',seconds=time.monotonic()-started,rows=len(food),snapshot=obs.snapshot())
del food
for method,mode,expected in [('food_prices','unitvalue',1676093),('food_prices','unitprice',189656),('food_quantities','units',1694623)]:
 started=time.monotonic();obs.emit('api_start',api=method,units=mode,snapshot=obs.snapshot())
 frame=getattr(c,method)(units=mode)
 assert len(frame)==expected,(method,mode,len(frame),expected)
 obs.emit('api_end',api=method,units=mode,seconds=time.monotonic()-started,rows=len(frame),snapshot=obs.snapshot())
 del frame
obs.emit('replay_passed',snapshot=obs.snapshot())
