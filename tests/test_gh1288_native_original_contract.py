"""Native synthetic original-contract regression controls for GH1288.

Normal pytest, invented Alpha/Beta files and native Make/Python producers only.
All24 controls require genuine Make availability; Linux release CI must execute
all24. No external report/grant/guard or source-data dependency is required.
The two late-after-consumption cases remain observations, not wrong-value proofs.
"""
from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import shutil
import numpy as np
import pandas as pd
import pytest
from lsms_library import country as C, local_tools as L
from tests.test_gh1288_zero_exit_freshness import model  # fixture infrastructure only

# Same genuine capability convention as existing native Make regressions.
pytestmark = pytest.mark.skipif(shutil.which('make') is None,
                                reason='GNU make not on PATH')

REAL_RUN = subprocess.run
# Capture the original owned helpers before the imported model fixture runs.
ORIGINAL_ACCESS_HELPERS = {
    name: getattr(L, name) for name in (
        '_dvc_cache_path', '_warm_dvc_cache_for_feature',
        '_ensure_dvc_pulled', '_try_get_data_file')
}


@pytest.fixture(autouse=True)
def restore_original_access_helpers(model, monkeypatch):
    # The original model installs IO stubs. Restore owned function identity
    # before any test body/producer installation/seed/fingerprint computation.
    # External provider stubs and the actual-operation guard remain unchanged.
    for name, original in ORIGINAL_ACCESS_HELPERS.items():
        monkeypatch.setattr(L, name, original)
    assert all(getattr(L, name) is original
               for name, original in ORIGINAL_ACCESS_HELPERS.items())

BASE_FINALIZE = C.Country._finalize_result  # unchanged dd40 AST, metadata only
TABLE, OTHER = 'facts', 'support'
WAVES = ['a', 'b']
ATTRS = {'producer': {'integer': 3, 'flag': True, 'missing': None,
                      'labels': ['independent', 'native']}}


def _frame(values=(17.0, 29.0)):
    out = pd.DataFrame({'t': pd.array(WAVES[:len(values)], dtype='str'),
                        'i': [f'h{k}' for k in range(len(values))],
                        'x': pd.array(values, dtype='Float64')}).set_index(['t', 'i'])
    out.attrs = copy.deepcopy(ATTRS)
    return out


def _state(path):
    if not path.exists():
        return None
    s = path.lstat()
    assert not path.is_symlink()
    return (s.st_ino, s.st_size, s.st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest())


def _put_frame(path, frame, **metadata):
    path.parent.mkdir(parents=True, exist_ok=True)
    L.to_parquet(frame, path, absolute_path=True, **metadata)


def _install(model, tmp_path, *, mode='healthy', computed=False,
             missing_other=False, grouped_output=None, no_country_rule=False,
             wave_mode=None):
    name = model.country.name
    proof = tmp_path/'independent-native-proof'
    proof.mkdir()
    for filename in ['panel_ids.json', 'updated_ids.json']:
        (model.source/'_'/filename).write_text('{}\n')
    for table, values in [(TABLE, [2.0, 4.0]), (OTHER, [1.0, 2.0])]:
        for w, value, household in zip(WAVES, values, ['h0', 'h1']):
            setup = ('import os,json\nfrom pathlib import Path\nimport pandas as pd\n'
                     'from lsms_library.local_tools import to_parquet\n'
                     f'p=Path(os.environ["LSMS_DATA_DIR"])/{name!r}/{w!r}/"_"/{(table+".parquet")!r}\n'
                     'p.parent.mkdir(parents=True,exist_ok=True)\n')
            if table == TABLE and w == 'a' and wave_mode:
                body = (f'Path({str(proof/"wave-a.json")!r}).write_text(json.dumps('
                        f'{{"mode":{wave_mode!r},"output_existed_before":p.exists()}}))\n')
                if wave_mode == 'touch':
                    body += 'p.touch()\n'
            else:
                body = (f'f=pd.DataFrame({{"t":[{w!r}],"i":[{household!r}],'
                        f'"x":pd.array([{value!r}],dtype="Float64")}}).set_index(["t","i"])\n'
                        f'f.attrs={ATTRS!r}\n'
                        'to_parquet(f,p,absolute_path=True)\n')
            (model.source/w/'_'/f'{table}.py').write_text(setup+body)
    (model.source/'_/support.py').write_text(
        'import os,json\nfrom pathlib import Path\nimport pandas as pd\n'
        'from lsms_library.local_tools import get_dataframe,to_parquet\n'
        f'r=Path(os.environ["LSMS_DATA_DIR"])/{name!r}\n'
        'a=get_dataframe(r/"a/_/support.parquet")\n'
        'b=get_dataframe(r/"b/_/support.parquet")\n'
        'f=a.copy(deep=True)\nf["x"]=pd.array([a["x"].iloc[0]+b["x"].iloc[0]+2.0],dtype="Float64")\n'
        f'f.attrs={ATTRS!r}\n'
        '(r/"var").mkdir(parents=True,exist_ok=True)\n'
        'to_parquet(f,r/"var/support.parquet",absolute_path=True)\n'
        f'Path({str(proof/"other.json")!r}).write_text(json.dumps({{"native_other":True,"value":float(f["x"].iloc[0])}}))\n')
    if grouped_output is not None:
        read_support = 'before=None\nconsumed=None\naddend=5.0\n'
    else:
        reference = ('table="support"\nsupport_path="../var/"+table+".parquet"\n'
                     if computed else 'support_path="../var/support.parquet"\n')
        read_support = reference+'support=get_dataframe(support_path)\nbefore=float(support["x"].iloc[0])\n'
        if mode == 'intentional105':
            read_support += ('mut=support.copy(deep=True)\nmut["x"]=105.0\n'
                             'to_parquet(mut,r/"var"/"support.parquet",absolute_path=True)\n')
        elif mode == 'identical':
            read_support += ('p=r/"var"/"support.parquet"\nq=p.with_name(p.name+".identical")\n'
                             'q.write_bytes(p.read_bytes())\nos.replace(q,p)\n')
        read_support += ('support=get_dataframe(support_path)\n'
                         'consumed=float(support["x"].iloc[0])\naddend=consumed\n')
    cooutput = ''
    if grouped_output is not None:
        cooutput = ('companion=f.copy(deep=True)\n'
                    f'companion["x"]=pd.array({list(grouped_output)!r},dtype="Float64")\n'
                    'to_parquet(companion,r/"var"/"support.parquet",absolute_path=True)\n')
    (model.source/'_/facts.py').write_text(
        'import os,json\nfrom pathlib import Path\nimport pandas as pd\n'
        'from lsms_library.local_tools import get_dataframe,to_parquet\n'
        f'r=Path(os.environ["LSMS_DATA_DIR"])/{name!r}\n'
        +read_support+
        'f=pd.concat([get_dataframe(r/"a/_/facts.parquet"),get_dataframe(r/"b/_/facts.parquet")])\n'
        'f["x"]=6.0*f["x"]+addend\n'
        f'f.attrs={ATTRS!r}\n'
        '(r/"var").mkdir(parents=True,exist_ok=True)\n'
        'to_parquet(f,r/"var/facts.parquet",absolute_path=True)\n'
        +cooutput+
        f'Path({str(proof/"main.json")!r}).write_text(json.dumps({{"native_main":True,"before_support":before,"consumed_support":consumed,"values":f["x"].astype(float).tolist(),"grouped_output":{list(grouped_output) if grouped_output is not None else None!r}}}))\n')
    rules = ('LSMS_DATA_ROOT ?= $(LSMS_DATA_DIR)\n'
             f'VAR_DIR ?= $(LSMS_DATA_ROOT)/{name}/var\n')
    for table in [TABLE, OTHER]:
        if missing_other and table == OTHER:
            continue
        rules += (f'$(LSMS_DATA_ROOT)/{name}/%/_/{table}.parquet: ../%/_/{table}.py\n'
                  '\t$(PYTHON) $<\n')
    if not missing_other and grouped_output is None:
        rules += (f'$(VAR_DIR)/support.parquet: support.py '
                  f'$(LSMS_DATA_ROOT)/{name}/a/_/support.parquet '
                  f'$(LSMS_DATA_ROOT)/{name}/b/_/support.parquet\n'
                  '\t$(PYTHON) support.py\n')
    if not no_country_rule:
        targets = '$(VAR_DIR)/facts.parquet'
        if grouped_output is not None:
            targets += ' $(VAR_DIR)/support.parquet'
        rules += (targets+': facts.py '
                  f'$(LSMS_DATA_ROOT)/{name}/a/_/facts.parquet '
                  f'$(LSMS_DATA_ROOT)/{name}/b/_/facts.parquet')
        if grouped_output is None:
            rules += ' $(VAR_DIR)/support.parquet'
        rules += '\n\t$(PYTHON) facts.py\n'
    if missing_other:
        (model.source/'_/support.py').unlink()
        for w in WAVES:
            (model.source/w/'_/support.py').unlink()
            (model.source/w/'_/data_info.yml').write_text('facts: null\n')
    (model.source/'_/Makefile').write_text(rules)
    return proof


def _seed_waves(model, table, values, *, current):
    frame = _frame(values)
    for k, w in enumerate(WAVES):
        path = model.wave_path(w, table)
        metadata = {'cache_hash':model.country[w]._input_hash(table)} if current else {}
        _put_frame(path, frame.iloc[[k]].copy(deep=True), **metadata)


def _seed_live(model, *, current_other=True, other_values=(5.0,), seed_other_waves=True):
    if current_other and seed_other_waves:
        _seed_waves(model, OTHER, [1.0,2.0], current=True)
    other = model.data/model.country.name/'var/support.parquet'
    model.seed(other, _frame(other_values), current=current_other, table=OTHER, waves=WAVES)
    if current_other:
        witness = model.country._make_declared_inputs(OTHER)  # metadata fixture setup only
        assert L.stamp_parquet_hash(other, model.country._table_cache_hash(OTHER,WAVES),
                                   cache_waves=WAVES, make_inputs=witness)
    model.seed(model.selected, _frame([801.0,802.0]))
    paths = [model.selected,other,*[model.wave_path(w,t) for t in [TABLE,OTHER] for w in WAVES],
             model.source/'_/panel_ids.json',model.source/'_/updated_ids.json']
    return other,{p:_state(p) for p in paths}


def _assert_prerequisites(before, selected):
    assert {p:_state(p) for p in before if p!=selected} == {p:v for p,v in before.items() if p!=selected}


def _assert_all_live(before):
    assert {p:_state(p) for p in before} == before


def _assert_native_result(model, actual, values=(17.0,29.0)):
    literal = _frame(values)
    expected = BASE_FINALIZE(model.country,literal.copy(deep=True),
                             model.scheme_doc['Data Scheme'][TABLE],TABLE)
    pd.testing.assert_frame_equal(actual,expected,check_exact=True)
    assert actual.attrs==expected.attrs
    np.testing.assert_array_equal(actual['x'].to_numpy(dtype=float),list(values))
    stored=L.get_dataframe(model.selected)
    pd.testing.assert_frame_equal(stored,literal,check_exact=True)
    assert stored.attrs==ATTRS


@pytest.mark.parametrize('mode',['no-output','touch'])
def test_old_hashless_wave_not_used_after_zero_exit(model,tmp_path,monkeypatch,mode):
    proof=_install(model,tmp_path,wave_mode=mode)
    _seed_waves(model,TABLE,[901.0,902.0],current=False)
    other,before=_seed_live(model)
    model.selected.unlink()  # isolate old-wave/new-country mechanism
    before[model.selected]=None
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    with pytest.raises((RuntimeError,L.ParquetFormatError)):
        model.load()
    event=json.loads((proof/'wave-a.json').read_text())
    assert event=={'mode':mode,'output_existed_before':False}
    assert not (proof/'main.json').exists()
    _assert_all_live(before)


@pytest.mark.parametrize('routing',['mtime-target','no-rule-with-script'])
def test_old_country_target_requires_actual_native_builder(model,tmp_path,monkeypatch,routing):
    proof=_install(model,tmp_path,no_country_rule=routing=='no-rule-with-script')
    _seed_waves(model,TABLE,[2.0,4.0],current=True)
    other,before=_seed_live(model)
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    actual=model.load()
    _assert_native_result(model,actual)
    assert json.loads((proof/'main.json').read_text())['values']==[17.0,29.0]
    assert _state(model.selected)!=before[model.selected]
    _assert_prerequisites(before,model.selected)


def test_private_other_rebuild_and_native_transform_have_literal_17_29(model,tmp_path,monkeypatch):
    proof=_install(model,tmp_path)
    other,before=_seed_live(model,current_other=False,other_values=(901.0,902.0))
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    actual=model.load()
    _assert_native_result(model,actual)
    assert json.loads((proof/'other.json').read_text())=={'native_other':True,'value':5.0}
    assert json.loads((proof/'main.json').read_text())['values']==[17.0,29.0]
    _assert_prerequisites(before,model.selected)


@pytest.mark.parametrize('companion',[(8.0,9.0),(10.0,20.0)])
def test_unread_grouped_cooutput_may_change_privately(model,tmp_path,monkeypatch,companion):
    proof=_install(model,tmp_path,grouped_output=companion)
    other,before=_seed_live(model,other_values=(51.0,52.0))
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    actual=model.load()
    _assert_native_result(model,actual)
    event=json.loads((proof/'main.json').read_text())
    assert event['before_support'] is None and event['consumed_support'] is None
    assert event['grouped_output']==list(companion)
    assert not (proof/'other.json').exists()
    _assert_prerequisites(before,model.selected)


def test_current_computed_other_is_available_and_warm_reusable(model,tmp_path,monkeypatch):
    proof=_install(model,tmp_path,computed=True)
    other,before=_seed_live(model)
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    first=model.load()
    _assert_native_result(model,first)
    assert not (proof/'other.json').exists(),'current computed Other stays available without forced rebuild'
    assert L.read_parquet_make_inputs(model.selected)['tables'][OTHER]==model.country._table_cache_hash(OTHER,WAVES)
    selected_state,main_state=_state(model.selected),_state(proof/'main.json')
    second=model.load()
    _assert_native_result(model,second)
    assert _state(model.selected)==selected_state and _state(proof/'main.json')==main_state
    _assert_prerequisites(before,model.selected)


def test_identical_current_replacement_stays_productive(model,tmp_path,monkeypatch):
    proof=_install(model,tmp_path,mode='identical')
    other,before=_seed_live(model)
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    actual=model.load()
    _assert_native_result(model,actual)
    event=json.loads((proof/'main.json').read_text())
    assert event['before_support']==event['consumed_support']==5.0
    assert not (proof/'other.json').exists()
    _assert_prerequisites(before,model.selected)


def test_intentional_current_intermediate_105_returns_117_129(model,tmp_path,monkeypatch):
    proof=_install(model,tmp_path,mode='intentional105')
    other,before=_seed_live(model)
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    actual=model.load()
    _assert_native_result(model,actual,[117.0,129.0])
    event=json.loads((proof/'main.json').read_text())
    assert event['before_support']==5.0 and event['consumed_support']==105.0
    assert event['values']==[117.0,129.0]
    _assert_prerequisites(before,model.selected)


def test_late_after_consumption_mutation_is_observation_not_wrong_value_proof(model,tmp_path,monkeypatch):
    proof=_install(model,tmp_path)
    other,before=_seed_live(model)
    reached=[]
    def late(self,frame,entry,method_name,**kwargs):
        if self is model.country and method_name==TABLE:
            roots=list(model.data.glob('.lsms_make_*'))
            assert len(roots)==1
            path=roots[0]/self.name/'a/_/facts.parquet'
            changed=L.get_dataframe(path).copy(deep=True)
            np.testing.assert_array_equal(changed['x'].to_numpy(dtype=float),[2.0])
            changed['x']=902.0
            _put_frame(path,changed)
            reached.append(True)
        return BASE_FINALIZE(self,frame,entry,method_name,**kwargs)
    monkeypatch.setattr(C.Country,'_finalize_result',late)
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    try:
        actual=model.load()
    except RuntimeError as exc:
        # Observation only; a refusal here is NOT a reproduced wrong-value bug
        # or claimed complete acceptance. Require actual late phase and no promotion.
        assert reached==[True]
        _assert_all_live(before)
        behavior={'behavior':'refused-after-consumption','error_type':type(exc).__name__}
    else:
        assert reached==[True]
        _assert_native_result(model,actual)
        _assert_prerequisites(before,model.selected)
        behavior={'behavior':'returned-correct-17-29'}
    assert json.loads((proof/'main.json').read_text())['values']==[17.0,29.0]
    (proof/'late-observation.json').write_text(json.dumps(behavior,sort_keys=True))


def test_computed_hashless_no_producer_keeps_availability_residual(model,tmp_path,monkeypatch):
    proof=_install(model,tmp_path,computed=True,missing_other=True)
    other,before=_seed_live(model,current_other=False,other_values=(5.0,))
    monkeypatch.setattr(C.subprocess,'run',REAL_RUN)
    with pytest.raises(RuntimeError) as caught:
        model.load()
    assert isinstance(caught.value.__cause__,subprocess.CalledProcessError)
    assert b'support.parquet' in caught.value.__cause__.stderr
    assert not (proof/'main.json').exists()
    _assert_all_live(before)
