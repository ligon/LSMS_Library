"""Composed D1 controls; expectations use independent source operands."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def eth():
    path = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Ethiopia/_/ethiopia.py'
    spec = importlib.util.spec_from_file_location('_ethiopia1120', path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def exact(a, b):
    pd.testing.assert_frame_equal(a, b, check_exact=True)
    assert a.attrs == b.attrs
    for c in a:
        assert a[c].dtype == b[c].dtype
        np.testing.assert_array_equal(a[c].isna().to_numpy(), b[c].isna().to_numpy())
        mask = a[c].notna()
        np.testing.assert_array_equal(a.loc[mask, c].to_numpy(), b.loc[mask, c].to_numpy())


def operands():
    hh = pd.DataFrame({'hh': ['primary-b', 'primary-a'], 'ea': ['old-b', 'old-a'],
                       'pw': pd.array([2., 6.], dtype='Float64'),
                       'region': [4, 3], 'rural': [1, 2]})
    labels = hh.copy(); labels['region'] = ['Original Four', 'Original Three']
    labels['rural'] = ['rural', 'urban']
    pp = pd.DataFrame({'hh': ['new-b', 'primary-a', 'new-b', 'new-a'],
                       'ea': ['new-ea', 'wrong-ea', 'new-ea', 'old-a'],
                       'pw': pd.array([0., 999., 0., None], dtype='Float64'),
                       'region': [4, 99, 4, 3], 'rural': [1, 99, 1, 2]})
    sample = pd.DataFrame({'i': hh.hh, 'v': hh.ea,
        'weight': hh.pw.astype(float), 'Rural': ['Rural','Urban'],
        'strata': ['Original Four Rural', 'Original Three Urban'],
        'panel_weight': hh.pw.astype(float), 't': '2018-19'})
    sample.attrs={'provenance':{'stage':'native','ordered':[2,1]}}
    return hh, labels, pp, sample


C = dict(hhid='hh', ea='ea', weight='pw', region='region', rural='rural')
R = {'rural':'Rural','urban':'Urban'}


def test_priority_order_raw_weights_masks_attrs_and_complete_denominator(eth):
    from lsms_library.country import _normalise_sample_weights
    hh, labels, pp, sample = operands()
    originals=[x.copy(deep=True) for x in (hh,labels,pp,sample)]
    out=eth.with_agriculture_cover_households(sample,'2018-19',hh,labels,pp,C,R)
    assert out.i.tolist()==['primary-b','primary-a','new-b','new-a']
    exact(out.iloc[:2],sample)
    np.testing.assert_array_equal(out.weight.notna(),[True,True,True,False])
    np.testing.assert_array_equal(out.weight.dropna().to_numpy(),[2.,6.,0.])
    assert out.v.tolist()==['old-b','old-a','new-ea','old-a']
    assert out.strata.tolist()==['Original Four Rural','Original Three Urban']*2
    norm=_normalise_sample_weights(out.set_index(['i','t']),country='Ethiopia')
    # Independent raw operands, including zero in the denominator, excluding NA.
    expected=np.array([2.,6.,0.])/(8./3.)
    np.testing.assert_array_equal(norm.weight.dropna().to_numpy(),expected)
    assert norm.weight.iloc[1]/norm.weight.iloc[0]==3.
    for before,after in zip(originals,(hh,labels,pp,sample)):exact(after,before)


@pytest.mark.parametrize('column,value',[('ea','other'),('ea',None),('region',3),
    ('region',None),('rural',2),('rural',None),('pw',None),('pw',np.nextafter(0.,1.)),
    ('pw',1.e-300)])
def test_actual_holder_comparator_refuses_conflicts_before_reduction(eth,column,value):
    hh,labels,pp,sample=operands();pp.loc[2,column]=value
    with pytest.raises(ValueError,match='holder rows'):
        eth.with_agriculture_cover_households(sample,'2018-19',hh,labels,pp,C,R)


def test_actual_nullable_float64_nextafter_at_nonzero_is_not_close_enough(eth):
    hh,labels,pp,sample=operands()
    pp.loc[[0,2],'pw']=1.;pp.loc[2,'pw']=np.nextafter(1.,2.)
    assert pp.pw.dtype==pd.Float64Dtype()
    with pytest.raises(ValueError,match='holder rows'):
        eth.with_agriculture_cover_households(sample,'2018-19',hh,labels,pp,C,R)


@pytest.mark.parametrize('field',['region','rural'])
def test_unknown_label_authority_refused(eth,field):
    hh,labels,pp,sample=operands();pp.loc[[0,2],field]=77
    with pytest.raises(ValueError,match='never uses'):
        eth.with_agriculture_cover_households(sample,'2018-19',hh,labels,pp,C,R)


def test_no_additions_returns_original_object(eth):
    hh,labels,pp,sample=operands();pp=pp.iloc[[1]]
    assert eth.with_agriculture_cover_households(sample,'2018-19',hh,labels,pp,C,R) is sample


def test_recorded_bridge_uses_both_covers_and_refuses_ambiguity(eth):
    old='12345678901';new='123456789012345'
    a=pd.DataFrame({'ea_id':[old],'ea_id2':[new]})
    b=pd.DataFrame({'ea_id':['other'],'ea_id2':['other-new']})
    hh=pd.DataFrame({'household_id2':['primary']})
    pp=pd.DataFrame({'household_id2':[new+'001'], 'ea_id':[old]})
    c=dict(hhid='household_id2',ea='ea_id2',pp_ea='ea_id')
    xw=eth.ea_id2_crosswalk(b,a)
    out=eth._agriculture_only_households('2015-16',hh,pp,c,[],xw)
    assert out.v.tolist()==[new]
    b.loc[0]=[old,'different']
    with pytest.raises(ValueError,match='no EA'):
        eth._agriculture_only_households('2015-16',hh,pp,c,[],eth.ea_id2_crosswalk(a,b))
    with pytest.raises(ValueError,match='prefix'):
        eth._agriculture_only_households('2015-16',hh,pp,c,[],{old:'different'})


@pytest.mark.parametrize('baseline',[None,'other'])
def test_w2_baseline_holder_agreement_precedes_reduction(eth,baseline):
    new='123456789012345';hh=pd.DataFrame({'household_id2':['primary']})
    pp=pd.DataFrame({'household_id2':[new+'001']*2,'ea_id2':[new]*2,
                     'ea_id':['old']*2,'household_id':['baseline',baseline]})
    with pytest.raises(ValueError,match='holder rows'):
        eth._agriculture_only_households('2013-14',hh,pp,
                                        dict(hhid='household_id2',ea='ea_id2'),[])


def test_w3_pp_baseline_is_not_authority(eth):
    new='123456789012345';hh=pd.DataFrame({'household_id2':['primary']})
    pp=pd.DataFrame({'household_id2':[new+'001']*2,'ea_id':['old']*2,
                     'household_id':['irrelevant',None]})
    out=eth._agriculture_only_households('2015-16',hh,pp,
        dict(hhid='household_id2',ea='ea_id2',pp_ea='ea_id'),[],{'old':new})
    assert out.i.tolist()==[new+'001']


def test_cluster_only_adds_missing_eas_and_joins_geo_by_household(eth,monkeypatch):
    hh=pd.DataFrame({'household_id':['a','b'],'ea_id':['E','E'],
                     'saq01':[4,4],'saq02':[1,1],'saq14':[1,1],'pw_w4':[2.,3.]})
    pp=pd.DataFrame({'household_id':['c','d','f'],'ea_id':['E','N','N'],
                     'saq01':[4]*3,'saq02':[1]*3,'saq14':[1]*3,'pw_w4':[2.,2.,2.]})
    geo=pd.DataFrame({'household_id':['c','d'],'lat_mod':[999.,12.], 'lon_mod':[888.,13.]})
    df=pd.DataFrame({'t':['2018-19']*2,'v':['E']*2,'i':['a','b'],
                     'Region':['Four']*2,'District':['One']*2,'Rural':['Rural']*2,
                     'Latitude':[1.,2.],'Longitude':[3.,4.]}).set_index(['t','v','i'])
    df.attrs={'selection':'observed rows'}
    def read(path,**kw):
        name=Path(path).name
        return {'sect_cover_hh_w4.dta':hh,'sect_cover_pp_w4.dta':pp,
                'ETH_HouseholdGeovariables_Y4.dta':geo}[name].copy(deep=True)
    monkeypatch.setattr(eth,'get_dataframe',read)
    out=eth.cluster_features(df)
    exact(out.iloc[:2],df)
    assert out.index.tolist()==list(df.index)+[('2018-19','N','d'),('2018-19','N','f')]
    assert out.Latitude.tolist()[:3]==[1.,2.,12.]
    assert pd.isna(out.Latitude.iloc[3]) and pd.isna(out.Longitude.iloc[3])
