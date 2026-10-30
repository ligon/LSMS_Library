"""W5 v02 fixes through the real cover compiler and unchanged Site2 selector.

Only source-reader leaves are invented. No source/cache/provider reads.
Ledger978-nigeria-w5-geography sections2–4.
"""
import importlib.util
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import country as framework
from lsms_library import local_tools as L
from lsms_library.paths import countries_root


@pytest.fixture
def w5(monkeypatch):
    path=countries_root()/'Nigeria/_/nigeria.py'
    spec=importlib.util.spec_from_file_location('_nigeria_w5_geography',path)
    ng=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ng)
    config=ng.ROUND_COVERS['2023-24']
    def raw(ids,eas):
        return pd.DataFrame({'hhid':ids,'ea':eas,'state':1,'lga':101,'zone':1,
            'sector':2,'strata':1,'wt_cross_wave5':np.arange(len(ids),dtype=float),
            'wt_longpanel_wave5':np.nan})
    pp=raw([21,11,31,41,51,61],[3,1,1,0,5,6])
    ph=raw([11,31,41,51],[0,1,0,5])
    ph['hh_moved']=[2,2,3,2]
    def decoded(frame):
        result=frame.assign(zone='North Central',state='Kogi',sector='2. RURAL')
        if 'hh_moved' in result:
            result['hh_moved']=['2. NO, IN ORIGINAL DWELLING',
                '2. NO, IN ORIGINAL DWELLING','3. MOVED','2. NO, IN ORIGINAL DWELLING']
        return result
    frames={}
    for rnd,frame in (('pp',pp),('ph',ph)):
        for label,data in ((False,frame),(True,decoded(frame))):
            frames[(ng._data_path('2023-24',config[rnd]),label)]=data
            frames[(ng._data_path('2023-24',config[rnd+'_roster']),label)]=data.copy(deep=True)
    geo_path=ng._data_path('2023-24','nga_householdgeovariables_y5.dta')
    frames[(geo_path,True)]=pd.DataFrame({'hhid':[31,21,51,999],
        'lat_mod':[7.4,9.1,8.2,1.0],'lon_mod':[4.2,5.1,6.3,1.0]})
    originals=dict(frames)
    before={key:frame.copy(deep=True) for key,frame in frames.items()}
    seen=[]
    def reader(filename,convert_categoricals=True,**kwargs):
        key=(str(filename),convert_categoricals)
        seen.append(key)
        return frames[key].copy(deep=True)
    monkeypatch.setattr(L,'get_dataframe',reader)
    yield ng,frames,geo_path,seen
    for key,frame in before.items():
        pd.testing.assert_frame_equal(originals[key],frame,check_exact=True)


def test_w5_uses_verified_root_level_binding(w5):
    ng,_,_,_=w5
    assert ng.ROUND_COVERS['2023-24']['geo']==(
        'nga_householdgeovariables_y5.dta','lat_mod','lon_mod')


def test_household_coordinates_left_join_own_round_without_changing_sample(w5,monkeypatch):
    ng,_,geo_path,seen=w5
    expected_sample=ng.sample_for_wave('2023-24')
    candidate=ng.cluster_features_for_wave('2023-24')
    with monkeypatch.context() as context:
        context.setitem(ng.ROUND_COVERS['2023-24'],'geo',None)
        baseline=ng.cluster_features_for_wave('2023-24')
        baseline_sample=ng.sample_for_wave('2023-24')
    pd.testing.assert_frame_equal(expected_sample,baseline_sample,check_exact=True)
    pd.testing.assert_index_equal(candidate.index,baseline.index)
    other=[column for column in baseline if column not in ('Latitude','Longitude')]
    pd.testing.assert_frame_equal(candidate[other],baseline[other],check_exact=True)
    assert set(candidate.xs('2023Q3',level='t').index.get_level_values('i'))=={'11','21','31','41','51','61'}
    assert set(candidate.xs('2024Q1',level='t').index.get_level_values('i'))=={'11','31','41','51'}
    assert '999' not in candidate.index.get_level_values('i')
    assert (geo_path,True) in seen


def test_coordinates_and_same_location_key_belong_to_each_household(w5):
    ng,frames,geo_path,_=w5
    got=ng.cluster_features_for_wave('2023-24')
    source=frames[(geo_path,True)].set_index('hhid')[['lat_mod','lon_mod']]
    source.index=source.index.map(str)
    expected=source.reindex(got.index.get_level_values('i'))
    expected.index=got.index
    expected.columns=['Latitude','Longitude']
    pd.testing.assert_frame_equal(got[expected.columns],expected,check_exact=True)
    ph=got.xs('2024Q1',level='t').reset_index().set_index('i')
    assert ph.loc['11','v']=='1/101/1'
    assert ph.loc['41','v']=='moved-41'
    assert ph.loc['11','Derivation']=='Nigeria::cluster_features::ph-same-location'


def test_site2_keeps_one_observed_pair_and_reports_disagreement(w5):
    ng,frames,geo_path,_=w5
    frames[(geo_path,True)]=pd.DataFrame({'hhid':[11,31,999],
        'lat_mod':[7.1,7.4,1.0],'lon_mod':[4.8,4.2,1.0]})
    source=ng.cluster_features_for_wave('2023-24')
    with warnings.catch_warnings(record=True) as observed:
        warnings.simplefilter('always')
        got=framework._collapse_to_cluster_grain(source,['t','v'],country='Nigeria',wave='2023-24')
    pair=tuple(got.loc[('2024Q1','1/101/1'),['Latitude','Longitude']])
    assert pair in {(7.1,4.8),(7.4,4.2)}
    assert pair!=(7.25,4.5)
    assert any(issubclass(item.category,framework.GrainCollapseWarning) for item in observed)


def test_completeness_can_change_selected_provenance_without_moving_household_provenance(w5,monkeypatch):
    ng,_,_,_=w5
    candidate=ng.cluster_features_for_wave('2023-24')
    with monkeypatch.context() as context:
        context.setitem(ng.ROUND_COVERS['2023-24'],'geo',None)
        baseline=ng.cluster_features_for_wave('2023-24')
    # PH11's complete-PP repair carries provenance but no geo fix. PH31
    # carries a fix and no repair. Derivation does not vote on completeness.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore',framework.GrainCollapseWarning)
        old=framework._collapse_to_cluster_grain(baseline,['t','v'])
        new=framework._collapse_to_cluster_grain(candidate,['t','v'])
    key=('2024Q1','1/101/1')
    assert old.loc[key,'Derivation']=='Nigeria::cluster_features::ph-same-location'
    assert pd.isna(new.loc[key,'Derivation'])
    assert tuple(new.loc[key,['Latitude','Longitude']])==(7.4,4.2)
    pd.testing.assert_series_equal(candidate['Derivation'],baseline['Derivation'],check_exact=True)


def test_duplicate_household_fixes_are_refused(w5):
    ng,frames,geo_path,_=w5
    frame=frames[(geo_path,True)]
    frames[(geo_path,True)]=pd.concat([frame,frame.iloc[[0]]],ignore_index=True)
    with pytest.raises(AssertionError,match='household has two fixes'):
        ng.cluster_features_for_wave('2023-24')
