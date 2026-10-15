import os,importlib.util,json
from pathlib import Path
import pytest
root=Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
spec=importlib.util.spec_from_file_location('actual_collectonly_tests',root/'tests/test_collectonly_no_purge.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
rows=[]
for initial in (None,'0','1'):
 if initial is None:os.environ.pop('LSMS_NO_CACHE',None)
 else:os.environ['LSMS_NO_CACHE']=initial
 r={'initial':initial}
 with pytest.MonkeyPatch.context() as mp:
  m.test_real_run_with_rebuild_caches_still_purges_everything(mp)
  r['inside']=os.environ.get('LSMS_NO_CACHE')
 r['after_rebuild_teardown']=os.environ.get('LSMS_NO_CACHE')
 with pytest.MonkeyPatch.context() as mp:
  m.test_no_purge_flag_is_accepted_and_inert(mp)
  r['inside_following_test']=os.environ.get('LSMS_NO_CACHE')
 r['after_following_teardown']=os.environ.get('LSMS_NO_CACHE')
 rows.append(r)
print(json.dumps({'source':str(root),'actual_tests':True,'purge_spies_only':True,'rows':rows},indent=2))
