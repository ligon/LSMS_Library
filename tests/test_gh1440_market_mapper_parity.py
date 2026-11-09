"""Invented GH1440 mapping/route controls preserved from independent review.

The prior numeric cold/warm failure is retained externally; this positive
regression requires the same existing mapper exactly once on either route.
No Country/Wave constructor, source reader, config or cache is used.
"""
from copy import deepcopy
import pytest
import pandas as pd
from botocore.exceptions import EndpointConnectionError
from lsms_library.country import Country



def cf(t, value):
    frame=pd.DataFrame({'Region':pd.array([value],dtype='string')},
        index=pd.MultiIndex.from_tuples([(t,'c')],names=['t','v']))
    frame.attrs={'native': {'wave':t}}
    return frame


class Wave:
    data_scheme=['cluster_features']
    resources={}
    def __init__(self, country, t): self.country,self.t=country,t
    def cluster_features(self):
        self.country.direct.append(self.t)
        if self.t=='c': raise AssertionError('must break before scanning third wave')
        return self.country.raw[self.t].copy(deep=True)


class Receiver:
    _market_lookup=Country._market_lookup
    _apply_categorical_mappings=Country._apply_categorical_mappings
    _add_market_index=Country._add_market_index
    name='Invented'
    waves=['a','b','c']
    data_scheme=['sample']
    categorical_mapping={}
    def __init__(self):
        self.raw={'a':cf('a','RAW'), 'b':cf('b','RAW').iloc[:0]}
        self.finalized=pd.concat([cf(t,'FINAL_'+t.upper()) for t in self.waves])
        self.direct=[];self.loads=0;self.error=None
    def __getitem__(self,t): return Wave(self,t)
    def cluster_features(self):
        self.loads+=1
        if self.error is not None: raise self.error
        return self.finalized.copy(deep=True)
    def sample(self): return self.households.copy(deep=True)


def test_three_wave_fallback_is_once_discards_partial_raw_and_preserves_operands():
    c=Receiver();c.categorical_mapping={'Region':pd.DataFrame({'Raw':['RAW','FINAL_A'],'Preferred Label':['WRONG','DOUBLE']})}
    before={t:x.copy(deep=True) for t,x in c.raw.items()}; final=c.finalized.copy(deep=True)
    out=c._market_lookup()
    assert out.to_dict('records')==[{'t':t,'v':'c','m':'FINAL_'+t.upper()} for t in c.waves]
    assert c.direct==['a','b'] and c.loads==1
    assert c._market_lookup() is out and c.loads==1 and c.direct==['a','b']
    for t,x in before.items():
        pd.testing.assert_frame_equal(c.raw[t],x,check_exact=True);assert c.raw[t].attrs==x.attrs
    pd.testing.assert_frame_equal(c.finalized,final,check_exact=True);assert c.finalized.attrs==final.attrs


def test_partial_household_priority_and_nullable_payload_survive_cold_fallback():
    c=Receiver();c.households=pd.DataFrame({'Region':pd.array(['HOUSEHOLD',pd.NA,pd.NA],dtype='string')},
        index=pd.MultiIndex.from_tuples([('h1','a'),('h2','a'),('h1','b')],names=['i','t']))
    frame=pd.DataFrame({'x':pd.array([1.,pd.NA,3.],dtype='Float64')},
        index=pd.MultiIndex.from_tuples([('a','c','h1','j1'),('a','c','h2','j2'),('b','c','h1','j1')],names=['t','v','i','j']))
    frame.attrs={'id_converted':True,'detail':{'keep':[1,None]}}
    old=frame.copy(deep=True);hh=c.households.copy(deep=True)
    out=c._add_market_index(frame,'Region',table='facts')
    expected=old.reset_index().drop(columns='v');expected['m']=pd.Series(['HOUSEHOLD','FINAL_A','FINAL_B'],dtype=object)
    expected=expected.set_index(['t','m','i','j']);expected.attrs=deepcopy(old.attrs)
    # Baseline warm lookup: unchanged original add-market method, independent known labels.
    class WarmBaseline(Receiver):
        def _market_lookup(self,column='Region'):
            return pd.DataFrame({'t':['a','b','c'],'v':['c','c','c'],'m':['FINAL_A','FINAL_B','FINAL_C']})
    baseline=WarmBaseline();baseline.households=hh.copy(deep=True)
    old_result=baseline._add_market_index(old.copy(deep=True),'Region',table='facts')
    pd.testing.assert_frame_equal(old_result,expected,check_exact=True)
    pd.testing.assert_frame_equal(out,expected,check_exact=True);assert out.attrs==old.attrs
    pd.testing.assert_frame_equal(frame,old,check_exact=True)
    pd.testing.assert_frame_equal(c.households,hh,check_exact=True)
    assert c.loads==1 and c.direct==['a','b']


def test_nonempty_direct_wave_never_queries_declaration_or_country_loader():
    c=Receiver();c.waves=['a']
    class Warm:
        def cluster_features(self): return cf('a','RAW')
        @property
        def data_scheme(self): raise AssertionError('unnecessary declaration read')
        @property
        def resources(self): raise AssertionError('unnecessary resource read')
    class WarmReceiver(Receiver):
        def __getitem__(self,t): return Warm()
    c.__class__=WarmReceiver
    c.categorical_mapping={'Region':pd.DataFrame({'Raw':['RAW'],'Preferred Label':['MAPPED']})}
    assert c._market_lookup().to_dict('records')==[{'t':'a','v':'c','m':'MAPPED'}]
    assert c.loads==0


@pytest.mark.parametrize('error',[EndpointConnectionError(endpoint_url='https://invented.invalid'),KeyboardInterrupt('stop'),SystemExit(7)],ids=['endpoint','interrupt','systemexit'])
def test_loader_errors_outside_wave_skip_boundary_are_exact_and_uncached(error):
    c=Receiver();c.error=error
    with pytest.raises(type(error)) as raised:c._market_lookup()
    assert raised.value is error and c.loads==1
    assert not hasattr(c,'_market_lookup_cache_Region')


def test_finalized_reset_index_contract_error_is_not_swallowed_or_retried():
    c=Receiver();c.finalized['t']='conflicting-column';old=c.finalized.copy(deep=True)
    with pytest.raises(ValueError,match='cannot insert t, already exists'):c._market_lookup()
    assert c.loads==1 and c.direct==['a','b']
    assert not hasattr(c,'_market_lookup_cache_Region')
    pd.testing.assert_frame_equal(c.finalized,old,check_exact=True)


def test_numeric_code_labels_are_identical_on_cold_country_and_warm_direct_routes():
    class Numeric(Receiver):
        waves=['a','b']
        categorical_mapping={'Region':pd.DataFrame({'Code':[1,2],'Preferred Label':['One','Two']})}
        def __init__(self,ready):
            self.ready=ready;self.loads=0
            self.raw={'a':cf('a','1'),'b':cf('b','2')}
        def __getitem__(self,t):
            country=self
            class NumericWave:
                data_scheme=['cluster_features']
                resources={}
                def cluster_features(self):
                    return country.raw[t].copy(deep=True) if country.ready else country.raw[t].iloc[:0].copy()
            return NumericWave()
        def cluster_features(self):
            self.loads+=1
            # The existing actual Country mapper, not a guessed finalized label.
            return Country._apply_categorical_mappings(self,pd.concat(list(self.raw.values())))
    cold,warm=Numeric(False),Numeric(True)
    a,b=cold._market_lookup(),warm._market_lookup()
    assert a['m'].tolist()==['One','Two']
    assert cold.loads==1 and warm.loads==0
    pd.testing.assert_frame_equal(a,b,check_exact=True)


def test_raw_mapper_projection_cannot_relabel_identity_or_other_columns():
    class Projected(Receiver):
        waves=['a']
        categorical_mapping={name:pd.DataFrame({'Code':[raw],'Preferred Label':[target]})
            for name,raw,target in [('Region','RAW','MAPPED'),('t','a','WRONG_T'),
                                    ('v','c','WRONG_V'),('District','KEEP','WRONG_DISTRICT')]}
        def __getitem__(self,t):
            country=self
            class Direct:
                def cluster_features(self): return country.raw[t].copy(deep=True)
            return Direct()
        def _apply_categorical_mappings(self,frame,*args,**kwargs):
            assert list(frame.columns)==['Region']
            assert isinstance(frame.index,pd.RangeIndex) and frame.index.name is None
            return Country._apply_categorical_mappings(self,frame,*args,**kwargs)
    country=Projected();country.raw['a']['District']='KEEP'
    original=country.raw['a'].copy(deep=True)
    assert country._market_lookup().to_dict('records')==[{'t':'a','v':'c','m':'MAPPED'}]
    assert country.loads==0
    pd.testing.assert_frame_equal(country.raw['a'],original,check_exact=True)
