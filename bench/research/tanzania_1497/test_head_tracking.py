"""Existing-selector controls, not a new household continuation policy."""
import importlib.util
from pathlib import Path
import warnings

import pandas as pd
import pytest
from lsms_library import local_tools as LT
SANCTIONED_GETTER=LT.get_dataframe


@pytest.fixture(scope='module')
def native():
    root=Path(__file__).resolve().parents[3]
    path=root/'lsms_library/countries/Tanzania/_/tanzania.py'
    spec=importlib.util.spec_from_file_location('tanzania1497_native_control',path)
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def refuse_sources(monkeypatch):
    from lsms_library import local_tools,data_access
    def refuse(*a,**k):raise AssertionError('No source/API IO permitted in invented head controls')
    monkeypatch.setattr(local_tools,'get_dataframe',refuse)
    monkeypatch.setattr(data_access,'get_data_file',refuse)


def cover(order=('frame','split')):
    return pd.DataFrame({'sdd_hhid':list(order),'y4_hhid':['previous']*len(order)})


def roster(rows):
    return pd.DataFrame(rows,columns=['sdd_hhid','hh_b05','hh_b06'])


def select(native,c,r):
    return native._map_with_head_tracking(c,r,'sdd_hhid','y4_hhid')


def test_current_head_with_previous_member_one_selects_split_over_frame(native):
    r=roster([('frame','HEAD',2),('split','HEAD',1)])
    out=select(native,cover(),r)
    assert out.to_dict('records')==[{'i':'split','previous_i':'previous'}]


def test_previous_head_non_head_presence_is_distinct_from_current_selector(native):
    # The previous head lives in split, but now a spouse is its current head.
    r=roster([('frame','HEAD',3),('split','HEAD',2),('split','PARENT',1)])
    out=select(native,cover(),r)
    assert out.i.tolist()==['frame']  # existing first-cover-row fallback
    assert select(native,cover(('split','frame')),r).i.tolist()==['split']


def test_no_previous_member_one_current_head_fallback_depends_on_cover_order(native):
    r=roster([('frame','HEAD',4),('split','HEAD',5)])
    assert select(native,cover(),r).i.tolist()==['frame']
    assert select(native,cover(('split','frame')),r).i.tolist()==['split']


def test_multiple_eligible_current_heads_are_not_silently_classified_unique(native):
    out=select(native,cover(),roster([('frame','HEAD',1),('split','HEAD',1)]))
    assert out.i.tolist()==['frame','split']
    assert out.previous_i.nunique()==1


def test_numeric_head_code_requires_own_label_dictionary_before_selector(native):
    r=roster([('frame',1,2),('split',1,1)])
    assert select(native,cover(),r).i.tolist()==['frame']
    labelled=r.copy();labelled.hh_b05=labelled.hh_b05.replace({1:'HEAD'})
    assert select(native,cover(),labelled).i.tolist()==['split']


def test_non_split_keeps_simple_link_without_inventing_head_evidence(native):
    out=select(native,cover(('only',)),roster([]))
    assert out.to_dict('records')==[{'i':'only','previous_i':'previous'}]


def test_missing_previous_link_is_excluded_without_invented_history(native):
    c=cover();c['y4_hhid']=pd.NA
    assert select(native,c,roster([('frame','HEAD',1)])).empty


def test_native_selector_conserves_input_payloads_and_roster_order_irrelevant(native):
    c=cover();r=roster([('frame','HEAD',2),('split','HEAD',1),('frame','SPOUSE',1)])
    c.attrs['private']='preserve';r.attrs['private']='preserve'
    before_c=c.copy(deep=True);before_r=r.copy(deep=True)
    a=select(native,c,r);b=select(native,c,r.iloc[::-1])
    pd.testing.assert_frame_equal(a,b,check_exact=True)
    pd.testing.assert_frame_equal(c,before_c,check_exact=True)
    pd.testing.assert_frame_equal(r,before_r,check_exact=True)
    assert c.attrs==before_c.attrs and r.attrs==before_r.attrs


def test_capture_tripwires_attach_to_actual_public_targets_without_source_call():
    from contextlib import ExitStack
    import lsms_library
    from lsms_library import local_tools
    from bench.research.tanzania_1497.capture import attach_build_tripwires
    def refuse(*a,**k):raise AssertionError('build-writer-tripwire')
    with ExitStack() as stack:
        attach_build_tripwires(stack,lsms_library,local_tools,pd,refuse)
        for action in [lambda:lsms_library.Country('Invented'),lambda:lsms_library.Feature('invented'),
                       lambda:local_tools.to_parquet(pd.DataFrame(),'invented.parquet'),
                       lambda:pd.read_parquet('invented.parquet'),
                       lambda:pd.DataFrame().to_parquet('invented.parquet')]:
            with pytest.raises(AssertionError,match='build-writer-tripwire'):action()


def test_sanctioned_dta_probe_is_real_and_confined_to_bound_stream(tmp_path):
    from contextlib import ExitStack
    import lsms_library
    from lsms_library import local_tools
    from bench.research.tanzania_1497.capture import attach_build_tripwires,confined_dta_probe,digest
    path=tmp_path/'invented.dta';pd.DataFrame({'value':[1,2]}).to_stata(path,write_index=False)
    item={'path':'Invented/only.dta','md5':digest(path,'md5')};current={'source':item['path'],'role':'numeric'};records=[]
    probe=confined_dta_probe(pd.read_parquet,{path.resolve():item},current,records,local_tools._is_stata_dta)
    def refuse(*a,**k):raise AssertionError('unrelated IO/build forbidden')
    with ExitStack() as stack:
        attach_build_tripwires(stack,lsms_library,local_tools,pd,refuse,probe)
        out=SANCTIONED_GETTER(path,convert_categoricals=False)
        assert out.value.tolist()==[1,2]
        assert len(records)==1 and records[0]['md5_binding']==item['md5']
        with pytest.raises(AssertionError,match='parquet/cache path'):pd.read_parquet(tmp_path/'cache.parquet')
        other=tmp_path/'other.dta';other.write_bytes(path.read_bytes())
        with other.open('rb') as stream:
            with pytest.raises(AssertionError,match='unbound parquet/cache'):pd.read_parquet(stream)
