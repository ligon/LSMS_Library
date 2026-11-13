"""Invented-key checks for the diagnostic classifier, not an ID policy."""
import pandas as pd
import pytest
from bench.research.tanzania_1497.classify import nonblank,link_kind,location_kind


def test_absent_wave_empty_household_is_not_a_person_key():
    households=pd.Series(['a','',None,'  ','b','c'])
    persons=pd.Series([1,None,2,3,None,4])
    assert (nonblank(households)&persons.notna()).tolist()==[True,False,False,False,False,True]
    assert (nonblank(households)!=persons.notna()).sum()==3


@pytest.mark.parametrize('places,expected',[
    ([], 'not_found'),(['f'],'frame'),(['s'],'split'),(['z'],'elsewhere'),
    (['f','s'],'both'),(['f','z'],'frame_and_elsewhere'),(['s','z'],'split_and_elsewhere')])
def test_full_person_location_classes(places,expected):
    assert location_kind(places,'f','s')==expected


@pytest.mark.parametrize('linked,expected',[
    (set(),'neither'),({'f'},'frame'),({'s'},'split'),({'s','f'},'both'),({'other'},'neither')])
def test_link_class_never_assumes_frame_wins(linked,expected):
    assert link_kind('f','s',linked)==expected


def test_full_previous_person_key_separates_equal_person_numbers_in_merged_family():
    roster=pd.DataFrame({'current':['f','f','s'],'previous':['oldA','oldB','oldA'],'person':[2,1,1]})
    target=roster.loc[roster.previous.eq('oldA')&roster.person.eq(1),'current']
    assert target.tolist()==['s']
    assert set(roster.loc[roster.person.eq(1),'current'])=={'f','s'}
    # Cover previous-household alone would incorrectly assign oldA/person1 to f.
    assert set(roster.assign(previous='oldA').loc[lambda d:d.person.eq(1),'current'])=={'f','s'}
