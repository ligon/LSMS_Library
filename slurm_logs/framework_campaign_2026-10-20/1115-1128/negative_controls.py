"""Reproduce fixes against exact baseline or exact prior function definitions.

No checkout/source mutation: base imports the actual 852 code; prior-functions
AST-loads the unchanged 613 definitions over current helpers in a pytest fixture.
The latter isolates completion/routing safeguards, not a whole prior build.
"""
import argparse
import ast
from pathlib import Path
import subprocess
import sys
from types import FunctionType
import pytest
import lsms_library as ll
from lsms_library import provenance as pv
from lsms_library.paths import countries_root, data_root

p=argparse.ArgumentParser(); p.add_argument('mode',choices=['base','prior-functions']); a=p.parse_args()
root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert Path.cwd().resolve()==root
assert Path(ll.__file__).resolve()==root/'lsms_library/__init__.py'
assert countries_root().resolve()==root/'lsms_library/countries'
assert '1115-1128-negative-' in str(data_root())
current=Path('/local/job40255479/worktrees/1115-1128-provenance')
testfile=current/'tests/test_wave_provenance.py'
if a.mode=='base':
    assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()=='852ac2bfcd5f798533f56e7efa1f77db80c0138f'
    # Collection-only compatibility constant; no baseline function changes.
    pv.URL_PLACEHOLDER='(source URL not recorded)'
    selection=' or '.join(['test_unknown_keys_in_the_record_are_rendered',
        'test_a_repeated_unknown_key_keeps_every_line',
        'test_an_empty_valued_key_does_not_swallow_the_next_line',
        'test_the_url_comes_from_the_body_only',
        'test_keyword_lines_in_the_notes_stay_where_they_were_written',
        'test_the_placeholder_is_not_reingested',
        'test_round_trip_keeps_each_known_scalar_occurrence',
        'test_secondary_catalog_link_is_never_acquisition_routing',
        'test_additional_bare_body_urls_are_preserved_as_notes',
        'test_explicit_primary_does_not_discard_a_different_body_url'])
    plugins=[]
else:
    assert root==current
    def original(path,name,namespace):
        source=subprocess.check_output(['git','show','613611578:'+path],text=True)
        node=next(n for n in ast.parse(source).body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==name)
        decorators=node.decorator_list
        node.decorator_list=[]
        code=compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])), '<613611578:'+path+':'+name+'>','exec')
        temporary=dict(namespace);exec(code,temporary)
        raw=temporary[name]
        function=FunctionType(raw.__code__,namespace,name,raw.__defaults__,raw.__closure__)
        function.__kwdefaults__=raw.__kwdefaults__
        for decorator in reversed(decorators):
            assert isinstance(decorator,ast.Name)
            function=namespace[decorator.id](function)
        return function
    class PriorFunctions:
        @pytest.fixture(autouse=True)
        def replay(self,monkeypatch,request):
            from lsms_library import data_access as da
            if request.node.name.startswith('test_a_stamped_primary_correction_requires_acquisition'):
                bf=request.getfixturevalue('bf')
                monkeypatch.setattr(bf,'main',original('scripts/backfill_wave_provenance.py','main',vars(bf)))
            else:
                for name in ['add_wave','populate_and_push','_read_source_url']:
                    monkeypatch.setattr(da,name,original('lsms_library/data_access.py',name,vars(da)))
    plugins=[PriorFunctions()]
    selection=' or '.join(['test_push_must_confirm_every_requested_file',
        'test_successful_push_records_only_after_the_push',
        'test_no_push_records_after_a_complete_download',
        'test_failed_cross_entry_acquisition_preserves_existing_record',
        'test_secondary_catalog_link_is_never_acquisition_routing',
        'test_a_stamped_primary_correction_requires_acquisition'])
raise SystemExit(pytest.main(['-q','-o','addopts=',str(testfile),'-k',selection],plugins=plugins))
