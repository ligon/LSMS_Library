"""Source-semantic regressions for Uganda #1119; no survey/network required."""
import numpy as np
import pandas as pd
import pytest
from lsms_library.countries.Uganda._ import uganda as ug
import importlib.util
from pathlib import Path
_spec = importlib.util.spec_from_file_location(
    'nigeria_round_oracle', Path(__file__).with_name('nigeria_round_oracle.py'))
_oracle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_oracle)
assert_numeric_exact = _oracle.assert_numeric_exact


@pytest.fixture(autouse=True)
def native_mapping_directory(monkeypatch):
    monkeypatch.chdir(Path(ug.__file__).parent)


def assert_typed(a, b):
    assert type(a) is type(b)
    if isinstance(a, dict):
        assert a.keys() == b.keys()
        for k in a:
            assert_typed(a[k], b[k])
    elif isinstance(a, (list, tuple)):
        assert len(a) == len(b)
        for x, y in zip(a, b):
            assert_typed(x, y)
    else:
        assert a == b


def assert_strict(a, b):
    pd.testing.assert_frame_equal(a, b, check_exact=True)
    pd.testing.assert_index_equal(a.index, b.index, exact=True)
    pd.testing.assert_index_equal(a.columns, b.columns, exact=True)
    assert a.dtypes.equals(b.dtypes)
    assert_typed(a.attrs, b.attrs)
    for c in a:
        assert a[c].isna().equals(b[c].isna())
        assert a[c].array.equals(b[c].array)
        if pd.api.types.is_numeric_dtype(a[c]):
            assert_numeric_exact(a[c], b[c])


CM = {'A': {'inputs': {'hhid': 'hh', 'parcel': 'parcel', 'plot': 'plot',
      'organic': {'used': 'used', 'qty': 'q'},
      'pesticide': {'used': 'pused', 'qty': 'pq'}}},
      'seed': {'hhid': 'hh', 'parcel': 'parcel', 'plot': 'plot', 'crop': 'crop',
               'qty': 'q', 'seed_type': 'type', 'purchased': 'buy',
               'purchased_qty': 'bq'}}


def seed(crops, **kw):
    n = len(crops)
    d = dict(hh=['100']*n, parcel=[1]*n, plot=list(range(1,n+1)),
             crop=crops, q=[1.0]*n, type=[np.nan]*n, buy=[np.nan]*n,
             bq=[np.nan]*n)
    d.update(kw)
    return pd.DataFrame(d)


def build(s, f=None):
    return ug.plot_inputs_for_wave('2011-12', f, None, s, CM,
                                   source_refs={'A': 'synthetic:3A', 'seed': 'synthetic:4A'})


def test_branch_identity_and_seed_labels():
    code, known = next(iter(ug._crop_label_map().items()))
    s = seed([code, None, 98765, 0, 98765.25, 'Literal crop', 'n/a',
              '(not crop-specific)', 'unknown_crop', ' '])
    f = pd.DataFrame(dict(hh=['200'],parcel=[1],plot=[1],used=[1],q=[0.0],
                          pused=[1],pq=[np.nan]))
    d = build(s, f).reset_index()
    assert d.loc[d.input!='Seed','j'].tolist()==['(not crop-specific)']*2
    assert d.loc[d.input=='Seed','j'].tolist()==[
        known,'unknown_crop','98765','unknown_crop','98765.25','Literal crop','n/a',
        '(not crop-specific)','unknown_crop','unknown_crop']
    assert d.season.eq('A').all()
    assert d.loc[d.input!='Seed','Quantity'].tolist()[0] == 0


def test_eligibility_frozen_before_classification():
    code, known = next(iter(ug._crop_label_map().items()))
    s = seed([None, 98765, 'literal', None, None, None, None, code],
             q=[np.nan]*3+[0.0]+[np.nan]*4,
             type=[np.nan]*4+[1]+[np.nan]*3,
             buy=[np.nan]*5+[2]+[np.nan]*2,
             bq=[np.nan]*6+[2]+[np.nan])
    d=build(s).reset_index()
    # Purchased quantity alone was not an eligibility condition; False and
    # zero were, as was a mapped crop even without any seed measure.
    assert d.plot_id.tolist()==['100-1-4','100-1-5','100-1-6','100-1-8']
    assert d.j.tolist()==['unknown_crop']*3+[known]
    assert d.Improved.iloc[1] == False
    assert d.Purchased.iloc[2] == False


def test_missing_plot_retained_and_known_duplicate_policy_unchanged():
    code=next(iter(ug._crop_label_map()))
    single=build(seed([code],plot=[np.nan]))
    assert len(single)==1
    assert single.index.get_level_values('plot_id').tolist()==['unknown_plot']
    assert single.index.get_level_values('input_obs').tolist()==['row:synthetic:4A:0']
    d=build(seed([code,code,code],plot=[1,1,np.nan],q=[2.,3.,7.]))
    assert len(d)==2
    known=d.loc[d.index.get_level_values('plot_id')!='unknown_plot']
    missing=d.loc[d.index.get_level_values('plot_id')=='unknown_plot']
    assert known.Quantity.iloc[0]==5.0  # existing known-key reducer retained
    assert missing.Quantity.iloc[0]==7.0


def test_raw_fraction_does_not_alias_mapped_integral():
    code=next(iter(ug._crop_label_map()))
    d=build(seed([code+0.25]))
    assert d.index.get_level_values('j').tolist()==[str(code+0.25)]


@pytest.mark.parametrize('delta',[np.nextafter(1.,2.),1.+1e-12])
def test_exact_oracle_rejects_nullable_float_false_pass(delta):
    a=pd.DataFrame({'x':pd.array([1.,None],dtype='Float64')})
    b=a.copy(); b.loc[0,'x']=delta
    with pytest.raises(AssertionError): assert_strict(a,b)


def test_typed_attrs_oracle():
    a=pd.DataFrame({'x':pd.array([1.,None],dtype='Float64')})
    b=a.copy(); a.attrs={'nested':[{'x':True}]}; b.attrs={'nested':[{'x':1}]}
    with pytest.raises(AssertionError): assert_strict(a,b)


def test_legacy_sort_preserves_payload_order_after_relabel():
    code=next(iter(ug._crop_label_map()))
    # Duplicate known crop triggers the legacy sorted reducer. 'n/a' used to
    # sort after its mapped name; new '98765' sorts before it, but must not move q.
    d=build(seed([code,code,98765],plot=[1,1,1],q=[2.,3.,7.]))
    assert d.Quantity.tolist()==[5.,7.]
    assert d.index.get_level_values('j').tolist()==[ug._crop_label_map()[code],'98765']


def test_conflicting_legacy_crop_group_is_loud():
    # Both were previously n/a and their quantities were irreversibly summed.
    # Choosing either label, or changing the reducer to split them, is outside
    # a vocabulary-only fix.
    with pytest.raises(ValueError,match='conflates distinct raw crop'):
        build(seed([98765,98766],plot=[1,1],q=[2.,3.]))


def test_duplicate_missing_crop_keeps_existing_sum():
    d=build(seed([None,None],plot=[1,1],q=[2.,3.]))
    assert len(d)==1
    assert d.Quantity.iloc[0]==5.
    assert d.index.get_level_values('j').tolist()==['unknown_crop']


def test_distinct_missing_plot_crop_records_are_preserved():
    code=next(iter(ug._crop_label_map()))
    d=build(seed([98765,98766,code],plot=[np.nan,np.nan,1],q=[2.,3.,7.]))
    assert len(d)==3 and d.index.is_unique
    np.testing.assert_array_equal(d.Quantity.to_numpy(float),[2.,3.,7.])
    assert d.index.get_level_values('j').tolist()==['98765','98766',ug._crop_label_map()[code]]


def test_entirely_empty_seed_payload_still_excluded():
    assert len(build(seed([None],q=[np.nan])))==0


def test_missing_plot_rows_never_pool_and_keep_typed_payloads():
    code=next(iter(ug._crop_label_map()))
    s=seed([code, 0, 98765.25], plot=[np.nan]*3, q=[2., 0., np.nan])
    s['buy']=[1,2,1]
    got=build(s)
    assert len(got)==3 and got.index.is_unique
    assert got.index.get_level_values('input_obs').tolist()==[
        'row:synthetic:4A:0','row:synthetic:4A:1','row:synthetic:4A:2']
    assert got.index.get_level_values('j').tolist()[1:]==['unknown_crop','98765.25']
    pd.testing.assert_extension_array_equal(got.Quantity.array,pd.array([2.,0.,None],dtype='Float64'))
    np.testing.assert_array_equal(got.Quantity.dropna().to_numpy(float),[2.,0.])
    with pytest.raises(ValueError, match='verified source reference'):
        ug.plot_inputs_for_wave('2011-12',None,None,s,CM)
