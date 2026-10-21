"""Independent #1160 math and identity probes, synthetic data only.

Prepared without execution; run only after the complete worker freeze.
"""
from copy import deepcopy
import warnings
import numpy as np
import pandas as pd
import pytest
from lsms_library import transformations as T


def frames(on='plot'):
    # A/B share all local keys but have different fertilizer mixes and areas.
    # C has another parcel; D supplies area only and must never dilute A/B.
    land=lambda k:f'h-{k}-child' if on=='parcel' else k
    area_land=lambda k:f'{k}_owned' if on=='parcel' else k
    pi=pd.DataFrame([
        ('A','2020','h',land('x'),'Urea','kg',10.),
        ('A','2020','h',land('x'),'DAP','kg',4.),
        ('A','2020','h',land('x'),'Seed','kg',100.),
        ('B','2020','h',land('x'),'Urea','kg',3.),
        ('B','2020','h',land('x'),'DAP','kg',2.),
        ('C','2020','h',land('y'),'Urea','kg',5.),
    ],columns=['country','t','i','plot_id','input','u','Quantity']).set_index(
        ['country','t','i','plot_id','input','u'])
    pf=pd.DataFrame([
        ('A','2020','h',area_land('x'),1.),
        ('A','2020','h',area_land('x'),3.),
        ('B','2020','h',area_land('x'),2.),
        ('C','2020','h',area_land('y'),5.),
        ('D','2020','h',area_land('x'),100.),
    ],columns=['country','t','i','plot_id','Area']).set_index(
        ['country','t','i','plot_id'])
    return pi,pf


def labels(frame,kind,*,area=False):
    work=frame.reset_index('country')
    if kind=='category':
        order=['D','C','B','A','unused'] if area else ['A','C','B','unused']
        work['country']=pd.Categorical(work['country'],categories=order,ordered=True)
    elif kind=='nullable':work['country']=pd.array(work['country'],dtype='string')
    return work.set_index('country',append=True)


@pytest.mark.parametrize('on',['plot','parcel'])
@pytest.mark.parametrize('nutrient,expected',[('N',{'A':1.33,'B':.87,'C':.46}),
                                           ('product',{'A':3.5,'B':2.5,'C':1.})])
@pytest.mark.parametrize('kind',['ordinary','category','nullable'])
def test_independent_mixed_products_duplicate_area_and_unmatched_country(on,nutrient,expected,kind):
    pi,pf=frames(on)
    pi,pf=labels(pi,kind),labels(pf,kind,area=True)
    # Independent row permutation and duplicate local identities exercise labels.
    pi=pi.iloc[[4,0,5,2,1,3]].reset_index('country')
    pf=pf.iloc[[4,1,3,0,2]]
    pi.attrs['country']='ignored attrs'
    pf.attrs['country']='ignored attrs'
    before_pi,before_pf=pi.copy(deep=True),pf.copy(deep=True)
    before_attrs=deepcopy((pi.attrs,pf.attrs))
    out=T.fertilizer_rate(pi,pf,nutrient=nutrient,on=on)
    assert list(out.index.names)==['country','t','i','parcel' if on=='parcel' else 'plot_id']
    assert len(out)==3
    assert set(out.index.get_level_values('country'))==set(expected)
    for country,value in expected.items():
        assert out.xs(country,level='country').iloc[0,0]==pytest.approx(value)
    pd.testing.assert_frame_equal(pi,before_pi)
    pd.testing.assert_frame_equal(pf,before_pf)
    assert (pi.attrs,pf.attrs)==before_attrs


@pytest.mark.parametrize('on',['plot','parcel'])
@pytest.mark.parametrize('nutrient',['N','product'])
def test_country_without_positive_area_never_borrows_another_countrys_land(on,nutrient):
    pi,pf=frames(on)
    pf=pf.reset_index('country')
    # A's matching local land exists only with B/D positive areas.
    pf.loc[pf['country']=='A','Area']=0.
    out=T.fertilizer_rate(pi,pf,nutrient=nutrient,on=on)
    assert set(out.index.get_level_values('country'))=={'B','C'}


@pytest.mark.parametrize('side',['numerator','area','both'])
@pytest.mark.parametrize('kind',['category','nullable'])
@pytest.mark.parametrize('on',['plot','parcel'])
def test_typed_empty_country_keys_preserve_the_country_axis(side,kind,on):
    pi,pf=frames(on);pi,pf=labels(pi,kind),labels(pf,kind,area=True)
    if side in ['numerator','both']:pi=pi.iloc[:0]
    if side in ['area','both']:pf=pf.iloc[:0]
    out=T.fertilizer_rate(pi,pf,on=on)
    assert out.empty
    assert list(out.index.names)==['country','t','i','parcel' if on=='parcel' else 'plot_id']
    assert list(out.columns)==['Nitrogen_kg_per_ha']


@pytest.mark.parametrize('on',['plot','parcel'])
@pytest.mark.parametrize('nutrient,share',[('N',.46),('product',1.)])
def test_country_declarations_and_unknown_or_organic_rows_keep_existing_policy(on,nutrient,share):
    pi,pf=frames(on)
    pi=pi[pi.index.get_level_values('input')=='Urea'].reset_index(['u','country'])
    pi['u']='review basket'
    pi.attrs['unit_kg']={'A':{'review basket':3.},'B':{'review basket':7.},'C':{'review basket':11.}}
    before=deepcopy(pi.attrs)
    out=T.fertilizer_rate(pi,pf,nutrient=nutrient,on=on)
    expected={'A':10*3*share/4,'B':3*7*share/2,'C':5*11*share/5}
    for country,value in expected.items():assert out.xs(country,level='country').iloc[0,0]==pytest.approx(value)
    assert pi.attrs==before
    refused=pi.reset_index('input')
    refused['input']='Organic Fertilizer'
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        empty=T.fertilizer_rate(refused,pf,nutrient=nutrient,on=on)
    assert empty.empty
    assert 'country' in empty.index.names
    assert any(issubclass(w.category,T.NutrientCoverageWarning) for w in caught)


@pytest.mark.parametrize('nutrient',['N','product'])
@pytest.mark.parametrize('on',['plot','parcel'])
def test_area_local_columns_and_authoritative_index_survive_conflicting_metadata(on,nutrient):
    pi,pf=frames(on)
    pi['country']='wrong redundant value'
    pf['country']=pd.NA
    pi.attrs['country']='wrong attrs'
    pf=pf.reset_index(['t','i','plot_id'])
    before_pi,before_pf=pi.copy(deep=True),pf.copy(deep=True)
    out=T.fertilizer_rate(pi,pf,nutrient=nutrient,on=on)
    assert set(out.index.get_level_values('country'))=={'A','B','C'}
    pd.testing.assert_frame_equal(pi,before_pi)
    pd.testing.assert_frame_equal(pf,before_pf)


@pytest.mark.parametrize('on',['plot','parcel'])
def test_country_numeric_and_text_keys_never_join_by_coercion(on):
    pi,pf=frames(on)
    pi=pi.reset_index('country');pf=pf.reset_index('country')
    pi['country']=pi['country'].map({'A':1,'B':2,'C':3}).astype('int64')
    pf['country']=pd.array(pf['country'].map({'A':'1','B':'2','C':'3','D':'4'}),dtype='string')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:out=T.fertilizer_rate(pi,pf,on=on)
        except ValueError as error:
            # Pandas may explicitly refuse incompatible merge dtypes. This
            # is a safe refusal; converting labels to borrow area is not.
            assert 'country' in str(error)
        else:
            assert out.empty
            assert any(issubclass(w.category,T.PlotGrainMismatchWarning) for w in caught)


@pytest.mark.parametrize('on',['plot','parcel'])
def test_empty_numeric_country_axis_with_text_area_country_has_shaped_empty(on):
    pi,pf=frames(on)
    pi=pi.reset_index('country').iloc[:0]
    pi['country']=pd.array([],dtype='Int64')
    out=T.fertilizer_rate(pi,pf,on=on)
    assert out.empty
    assert list(out.index.names)==['country','t','i','parcel' if on=='parcel' else 'plot_id']
