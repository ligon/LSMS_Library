"""Selected H carrier: invented independent row/field oracle; no source IO."""
import ast
import importlib.util
from pathlib import Path
import sys
import warnings

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
import yaml

from lsms_library.paths import countries_root

ROOT = Path(__file__).resolve().parents[1]
MW = countries_root() / 'Malawi'
BASE = 'cf5637114ed513b774ebae929e57f8252e8f4de2'
KEYS = ['t', 'i', 'plot_id', 'input', 'crop', 'u', 'input_obs']
HALVES = [('2010-11', 'Full_Sample'), ('2013-14', 'single'),
          ('2016-17', 'Cross_Sectional'), ('2016-17', 'Panel'),
          ('2019-20', 'Cross_Sectional'), ('2019-20', 'Panel')]
# Independent literal own field oracle, not generated from production bindings.
SLOTS = {'Full_Sample': [('ag_h07a', 'ag_h07b', 'ag_h10', 'purchase:first'),
                         ('ag_h16a', 'ag_h16b', 'ag_h19', 'purchase:first'),
                         ('ag_h26a', 'ag_h26b', 'ag_h29', 'purchase:second')],
         'single': [('ag_h16a', 'ag_h16b', 'ag_h19', 'purchase:first'),
                    ('ag_h26a', 'ag_h26b', 'ag_h29', 'purchase:second')],
         'Cross_Sectional': [('ag_h07a', 'ag_h07b', 'ag_h10', 'purchase:first')],
         'Panel': [('ag_h16a', 'ag_h16b', 'ag_h19', 'purchase:first'),
                   ('ag_h26a', 'ag_h26b', 'ag_h29', 'purchase:second')]}
UNITS = {1: 'Gram', 2: 'Kilogramme', 5: '3.7 kg Bag', 6: '5 kg Bag',
         8: '50 kg Bag', 9: 'Other (Specify)', 77: 'Seed unit code 77'}


@pytest.fixture(scope='module')
def mw():
    spec = importlib.util.spec_from_file_location('malawi_1190_selected', MW / '_/malawi.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source(mw, wave, half):
    s = next(s for s in mw._SEED_LINE_SOURCES[wave] if s['half'] == half)
    return mw._seed_source_reference(MW / wave / s['h'])


def hframe(n=9):
    d = pd.DataFrame({'hhid': ['h'] * n, 'ag_h0b': [1] * n,
        'ag_h0c': [1] * n, 'crop_code': [1] * n, 'ag_h05': [1] * n})
    for q, u, v in [('ag_h07a', 'ag_h07b', 'ag_h10'),
                    ('ag_h16a', 'ag_h16b', 'ag_h19'),
                    ('ag_h26a', 'ag_h26b', 'ag_h29')]:
        d[q] = [0., -2., 1.25, 2.5, 5., np.nan, 7., 8., 9.][:n]
        d[u] = [1, 2, 5, 6, 8, 9, 77, np.nan, 2][:n]
        d[v] = [4., -6., np.nan, 0., 10., 20., 30., 40., 50.][:n]
    for q, u, f in [('ag_h36a', 'ag_h36b', 'ag_h34'),
                    ('ag_h38a', 'ag_h38b', 'ag_h37')]:
        d[q] = [0., -3., 1.5, 2.5, 4., np.nan, 7., 8., 9.][:n]
        d[u] = [2, 5, 6, 8, 9, 77, np.nan, 77, 1][:n]
        d[f] = [1, 2, np.nan, 2, 1, 1, np.nan, 1, 2][:n]
    # Native blanks and conflicting source answers, plus forbidden value decoys.
    d['ag_h35'] = ''; d['ag_h35__0'] = 7
    d['ag_h39a'] = 4; d['ag_h06'] = 'source literal'
    d['ag_h09'] = 111.; d['ag_h40'] = 222.
    d['ag_h36_1a'] = 333.; d['ag_h36_1b'] = 444.
    if n > 1:
        d.loc[n-2, ['ag_h0b', 'ag_h0c', 'crop_code']] = 999
    d.loc[n-1, ['ag_h0b', 'ag_h0c', 'crop_code']] = np.nan
    d.index = [44] * n  # DataFrame labels are not release positions.
    return d


def components(mw, h, wave='2013-14', half='single', positions=None, ref=None):
    return mw._seed_acquisition_block(h, t=wave, source_half=half,
        source_ref=source(mw, wave, half) if ref is None else ref,
        row_positions=list(range(len(h))) if positions is None else positions)


def oracle(h, wave, half, positions, ref):
    cropcol = 'ag_h0b' if wave == '2010-11' else 'ag_h0c' if wave == '2013-14' else 'crop_code'
    slots = list(SLOTS[half])
    if half != 'Cross_Sectional':
        slots += [('ag_h36a', 'ag_h36b', None, 'purchase:third')]
    slots += [('ag_h38a', 'ag_h38b', None, 'gift')]
    expected = []
    for pos, (_, r) in zip(positions, h.iterrows()):
        if pd.isna(r[cropcol]):
            continue
        for q, u, v, kind in slots:
            if q not in h or pd.isna(r[q]):
                continue
            if v and (v not in h or pd.isna(r[v]) or r[v] == 0):
                continue
            unit = 'Unknown' if pd.isna(r[u]) else UNITS[r[u]]
            expected.append((f'row:{ref}:{pos}/field:{q}', r[q], unit,
                             r[v] if v else pd.NA, kind, pos, q))
    return expected


def exact(a, b):
    assert_frame_equal(a, b, check_exact=True)
    assert a.attrs == b.attrs
    assert a.dtypes.equals(b.dtypes)
    assert a.isna().equals(b.isna())
    for col in a.select_dtypes(include='number'):
        keep = a[col].notna()
        np.testing.assert_array_equal(a.loc[keep, col].to_numpy(), b.loc[keep, col].to_numpy())


@pytest.mark.parametrize('wave,half', HALVES)
def test_independent_source_row_field_oracle(mw, wave, half):
    h = hframe(); before = h.copy(deep=True)
    positions = [0, 2, 3, 6, 9, 12, 15, 20, 22]
    got = components(mw, h, wave, half, positions)
    expected = oracle(h, wave, half, positions, source(mw, wave, half))
    assert got.index.get_level_values('input_obs').tolist() == [x[0] for x in expected]
    assert got.Acquisition_quantity.tolist() == [x[1] for x in expected]
    assert got.Acquisition_unit.tolist() == [x[2] for x in expected]
    assert got.Acquisition_kind.tolist() == [x[4] for x in expected]
    assert got.Source_row.tolist() == [x[5] for x in expected]
    assert got.Component_field.tolist() == [x[6] for x in expected]
    for (_, r), e in zip(got.iterrows(), expected):
        assert pd.isna(r.Acquisition_value) if pd.isna(e[3]) else r.Acquisition_value == e[3]
        native = h.iloc[positions.index(e[5])]
        for col in h:
            if col.startswith('ag_h') or col == ('crop_code' if wave in ['2016-17','2019-20'] else 'ag_h0b' if wave == '2010-11' else 'ag_h0c'):
                assert pd.isna(r[col]) if pd.isna(native[col]) else r[col] == native[col]
    assert set(got.index.get_level_values('plot_id')) == {'Farm-level'}
    assert set(got.index.get_level_values('u')) == {'Unknown'}
    assert got.Acquisition_quantity.dtype == pd.Float64Dtype()
    assert got.Acquisition_flag.dtype == pd.BooleanDtype()
    assert got.Source_row.dtype == pd.Int64Dtype()
    assert got.Carrier_role.eq('reported_H_component').all()
    exact(h, before)


@pytest.mark.parametrize('flag', [1, 2, None])
@pytest.mark.parametrize('quantity', [0., -2., 1.5, None])
def test_literal_third_and_gift_flags_decoys(mw, flag, quantity):
    h = hframe(1)
    h['ag_h16a'] = np.nan; h['ag_h26a'] = np.nan
    h['ag_h0c'] = 1
    for q, f in [('ag_h36a','ag_h34'), ('ag_h38a','ag_h37')]:
        h[q] = quantity; h[f] = flag
    out = components(mw, h)
    assert len(out) == (0 if quantity is None else 2)
    if quantity is None:
        return
    assert out.Acquisition_quantity.tolist() == [quantity, quantity]
    assert out.Acquisition_value.isna().all()
    assert out.Acquisition_flag.isna().all() if flag is None else out.Acquisition_flag.eq(flag == 1).all()
    assert out.iloc[0].Acquisition_source == ''
    assert out.iloc[0].Source_answer_field == 'ag_h35'
    assert out.iloc[0].ag_h36_1a == 333.


def legacy(mw, wave='2013-14', half='single'):
    g = pd.DataFrame({'hhid':['h','h','z','x'], 'plotkey':['p','p2',None,'p'],
        'code':[1,1,1,1], 'q':[3.,pd.NA,pd.NA,2.], 'u':[1,2,2,2]})
    seeds = mw._seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='code',
        qty='q', unit='u', source_ref='invented-G', source_half=half, row_positions=[0,1,2,3])
    h = hframe(1).assign(hhid='x', crop_code=1)
    purchase = mw._seed_purchase_block(h, hhid='hhid', cropcode='crop_code',
        qty_cols=['ag_h16a'], unit_cols=['ag_h16b'], value_cols=['ag_h19'], source_half=half)
    gift = mw._seed_gift_block(h, hhid='hhid', cropcode='crop_code', source_half=half)
    d = pd.DataFrame({'i':['x','x'], 'plot_id':['p','p'], '_input_code':[4,4],
        'crop':[pd.NA,pd.NA], 'Quantity':pd.array([4.,5.],dtype='Float64'),
        'u':['Kilogramme','Kilogramme'], 'Improved':pd.array([pd.NA]*2,dtype='boolean'),
        'Applied':pd.array([True,True],dtype='boolean')})
    return mw.assemble_plot_inputs(wave, [seeds, d], [purchase], [gift])


def test_append_full_legacy_payload_attrs_and_application_consumers(mw):
    from lsms_library.transformations import seed_kg, nitrogen_kg, fertilizer_rate
    old = legacy(mw); old.attrs['country'] = 'Malawi'; old.attrs['proof'] = {'typed': (1,False)}
    h = hframe().assign(hhid=['h','x','only-H','h','z','z','only-H','z','z'])
    new = mw._append_seed_acquisitions(old, [components(mw,h)])
    exact(new.loc[old.index, old.columns], old)
    extra = new.iloc[len(old):]
    assert extra[list(old)].isna().all().all()
    assert new.loc[old.index, extra.columns.difference(old.columns)].isna().all().all()
    area = pd.DataFrame({'Area':[2.,3.,4.]}, index=pd.MultiIndex.from_tuples(
        [('2013-14','x','p'),('2013-14','h','p'),('2013-14','h','p2')], names=['t','i','plot_id']))
    area.attrs['country'] = 'Malawi'
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        for fn in (seed_kg, nitrogen_kg):
            a,b = fn(old),fn(new)
            # Numerical application rows, masks/types/attrs must all remain exact.
            exact(a,b)
            assert 'Farm-level' not in b.index.get_level_values('plot_id')
        for nutrient in ('N','product'):
            exact(fertilizer_rate(old, area, nutrient=nutrient),
                  fertilizer_rate(new, area, nutrient=nutrient))
    assert len(extra) > 0 and extra.Quantity.isna().all() and extra.Applied.isna().all()


@pytest.mark.parametrize('positions', [[0,0],[-1,2],[True,1],[0],[None,2],[0.5,2]])
def test_malformed_positions_refuse(mw, positions):
    with pytest.raises((ValueError,TypeError)):
        components(mw,hframe(2),positions=positions)


@pytest.mark.parametrize('bad', ['', 'Malawi/path', 'Malawi/2013-14/Data/AG_MOD_H_13.dta@dvc:AG_MOD_H_13.dta:'+'0'*64])
def test_source_or_hash_mismatch_refuses(mw,bad):
    with pytest.raises(ValueError, match='source/sidecar'):
        components(mw,hframe(2),ref=bad)


def test_colliding_components_and_legacy_identity_refuse(mw):
    h = hframe(2); extra = components(mw,h); old = legacy(mw)
    with pytest.raises(ValueError,match='colliding'):
        mw._append_seed_acquisitions(old,[extra,extra])
    collision=extra.copy(); collision.index=collision.index.set_levels(
        [old.index.get_level_values('input_obs')[0]]+list(collision.index.levels[-1][1:]),level='input_obs')
    with pytest.raises(ValueError,match='colliding'):
        mw._append_seed_acquisitions(old,[collision])
    h['hhid']=None
    with pytest.raises(ValueError,match='household'):
        components(mw,h)


def test_equal_records_slots_and_binding_limitations(mw):
    h=hframe(2).assign(ag_h0c=1,ag_h16a=2.,ag_h19=10.,ag_h26a=2.,ag_h29=10.)
    h['ag_h36a']=np.nan; h['ag_h38a']=np.nan
    out=components(mw,h,positions=[7,9])
    assert out.Acquisition_quantity.tolist()==[2.,2.,2.,2.]
    assert out.index.get_level_values('input_obs').is_unique
    assert out.Binding_status.str.contains('truncated').all()
    out=components(mw,h,'2016-17','Cross_Sectional')
    assert out.Binding_status.str.contains('Other_Specify').all()
    assert not out.Acquisition_kind.eq('purchase:third').any()
    h['ag_h38a']=1.
    out=components(mw,h,'2016-17','Panel')
    assert out.loc[out.Acquisition_kind.eq('gift'),'Binding_status'].str.contains('OTHER_UNIT').all()


def old_source(rel):
    # Pinned source operands also work on the project's shallow CI checkout.
    if rel == 'lsms_library/transformations.py':
        name = 'nitrogen_kg.py.txt'
    elif rel.endswith('/malawi.py'):
        name = 'malawi.py.txt'
    else:
        name = rel.split('/')[3] + '-plot_inputs.py.txt'
    return (ROOT / 'tests/fixtures/malawi1190_source_baseline' / name).read_text()


def test_old_helper_and_r1_r3_source_inverse_exact():
    rel='lsms_library/countries/Malawi/_/malawi.py'
    old=ast.parse(old_source(rel));new=ast.parse((ROOT/rel).read_text())
    added={'SEED_ACQUISITION_ROLE','_seed_acquisition_block','_append_seed_acquisitions'}
    def owned(n):
        return getattr(n,'name',None) in added or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in added for t in n.targets)
    new.body=[n for n in new.body if not owned(n)]
    assert ast.dump(new,include_attributes=False)==ast.dump(old,include_attributes=False)


def driver_inputs(wave,half):
    hh='case_id' if half in ('Full_Sample','Cross_Sectional') else 'y2_hhid' if wave=='2013-14' else 'y3_hhid' if wave=='2016-17' else 'y4_hhid'
    d=pd.DataFrame({hh:['h'], 'ag_d00':['p'], 'gardenid':['g'], 'plotid':['p'],
                    'ag_d39a':[4], 'ag_d39d':[4.], 'ag_d39f':[4], 'ag_d39i':[5.],
                    'ag_d39g':[4], 'ag_d39j':[5.], 'ag_d36':[2], 'ag_d38':[1], 'ag_d40':[2]})
    g=pd.DataFrame({hh:['h','h','h','z'], 'ag_g0b':['p','p','p2',None] if wave=='2010-11' else [1,1,1,1],
        'ag_g0d':[1,1,1,1], 'ag_g00':['p','p','p2',None], 'gardenid':['g']*4,
        'plotid':['p','p','p2',None], 'crop_code':[1]*4, 'ag_g04a':[3.,3.,np.nan,np.nan],
        'ag_g04b':[1]*4, 'ag_g0f':[2]*4, 'ag_g0d_os':[None]*4, 'other':['a','b','c','d']})
    h=hframe(3).assign(**{hh:['h','h','only-H']})
    # row0 repeats at1; row2 equal projected payload but differs in unused field.
    h=h.iloc[[0,0,0,2]].reset_index(drop=True);h['unused']=['a','a','b','c']
    h['ag_h0b']=1;h['ag_h0c']=1;h['crop_code']=1
    return d,g,h


def run_driver(mw,monkeypatch,wave,baseline=False):
    import lsms_library.local_tools as lt
    script=MW/wave/'_/plot_inputs.py'
    text=old_source(str(script.relative_to(ROOT))) if baseline else script.read_text()
    tree=ast.parse(text)
    tree.body=[n for n in tree.body if not isinstance(n,ast.FunctionDef) or n.name!='_read_usecols']
    seen=[];written=[];original={}
    def invented_reader(path,**kwargs):
        assert kwargs=={'convert_categoricals':False}
        part='Cross_Sectional' if 'Cross_Sectional' in path else 'Panel' if 'Panel/' in path else 'Full_Sample' if wave=='2010-11' else 'single'
        d,g,h=driver_inputs(wave,part)
        kind='d' if 'mod_d' in path.lower() else 'g' if 'mod_g' in path.lower() else 'h'
        result={'d':d,'g':g,'h':h}[kind]
        seen.append((part,kind));original[part]=h.copy(deep=True)
        return result.copy(deep=True)
    def invented_restricted(path,usecols):
        assert path=='Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta'
        return driver_inputs(wave,'Cross_Sectional')[0].copy(deep=True)
    def capture_write(frame,path):
        assert path=='plot_inputs.parquet';written.append(frame.copy(deep=True))
    monkeypatch.setattr(lt,'get_dataframe',invented_reader)
    monkeypatch.setattr(lt,'to_parquet',capture_write)
    monkeypatch.setitem(sys.modules,'malawi',mw)
    ns={'__file__':str(script),'__name__':'invented_actual_driver','_read_usecols':invented_restricted}
    with warnings.catch_warnings():
        warnings.simplefilter('ignore');exec(compile(tree,str(script),'exec'),ns)
    assert len(written)==1
    return written[0], original, seen


@pytest.mark.parametrize('wave',['2010-11','2013-14','2016-17','2019-20'])
def test_actual_extracted_driver_assembly_and_write_boundary(mw,monkeypatch,wave):
    old,raw,reads=run_driver(mw,monkeypatch,wave,baseline=True)
    new,raw2,reads2=run_driver(mw,monkeypatch,wave)
    assert reads==reads2
    exact(new.loc[old.index,old.columns],old)
    extra=new.iloc[len(old):]
    assert extra[list(old)].isna().all().all()
    expected=[]
    for half,h in raw2.items():
        positions=np.arange(len(h))
        if wave=='2010-11':
            keep=~h.duplicated().to_numpy();positions=positions[keep];h=h.loc[keep]
        expected += oracle(h,wave,half,positions,source(mw,wave,half))
    assert extra.index.get_level_values('input_obs').tolist()==[e[0] for e in expected]
    assert extra.Acquisition_quantity.tolist()==[e[1] for e in expected]
    if wave=='2010-11':
        assert 1 not in extra.Source_row.tolist() and 2 in extra.Source_row.tolist()
    if wave in ['2016-17','2019-20']:
        xs=extra[extra.Source_half.eq('Cross_Sectional')]
        assert xs.index.get_level_values('i').str.startswith('cs-'+('17-' if wave=='2016-17' else '19-')).all()


def test_country_finalizer_feature_preserve_all_h_and_old_eligibility(mw,monkeypatch):
    from lsms_library.country import Country
    from lsms_library.feature import (_harmonize_country_frame,_canonical_index_levels,
                                      _missing_level_sentinels,_level_aliases)
    old=legacy(mw)
    h=hframe(2).assign(ag_h0c=1)
    new=mw._append_seed_acquisitions(old,[components(mw,h)])
    country=Country('Malawi')
    country._updated_ids_cache={'2013-14': {'h':'canonical-h', 'x':'canonical-x', 'z':'canonical-z'}}
    country._panel_ids_attempted=True
    # Real finalizer and canonical machinery, with no sample/table/API read.
    monkeypatch.setattr(Country,'_join_v_from_sample',lambda self,frame,*a,**k:frame)
    # Existing ID machinery executes against an explicitly invented map provider.
    scheme=yaml.safe_load((MW/'_/data_scheme.yml').read_text())['Data Scheme']['plot_inputs']
    a=country._finalize_result(old.copy(),scheme,'plot_inputs')
    b=country._finalize_result(new.copy(),scheme,'plot_inputs')
    role=b.Carrier_role.eq('reported_H_component').fillna(False)
    exact(b.loc[~role,a.columns],a)
    assert int(role.sum())==len(new)-len(old)
    assert b.attrs['id_converted']
    assert 'canonical-h' in b.index.get_level_values('i')
    # The real fast path does not walk a finalized frame twice.
    exact(country._finalize_result(b.copy(),scheme,'plot_inputs'),b)
    assert b.loc[role,'Quantity'].isna().all()
    flat=b.reset_index();flat['v']='cluster'
    aligned=_harmonize_country_frame(flat.set_index(list(b.index.names)+['v']),
        _canonical_index_levels('plot_inputs'),'Malawi','plot_inputs',
        aliases=_level_aliases('plot_inputs'),sentinels=_missing_level_sentinels('plot_inputs'))
    assert set(aligned.index.get_level_values('input_obs'))==set(b.index.get_level_values('input_obs'))
    assert len(aligned)==len(b)
    assert 'unknown_season' in aligned.index.get_level_values('season')
    # A nextafter mutation is rejected by the exact numerical comparator.
    changed=aligned.copy();idx=changed.Acquisition_quantity.first_valid_index()
    changed.loc[idx,'Acquisition_quantity']=np.nextafter(float(changed.loc[idx,'Acquisition_quantity']),np.inf)
    with pytest.raises(AssertionError):exact(changed,aligned)


@pytest.mark.parametrize('role',[None, 'different_role', 'reported_H_component'])
def test_nitrogen_exact_old_role_absent_and_other_country_behavior(mw,role):
    import lsms_library.transformations as tr
    tree=ast.parse(old_source('lsms_library/transformations.py'))
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='nitrogen_kg')
    ns=dict(tr.__dict__)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<baseline-nitrogen>','exec'),ns)
    frame=legacy(mw)
    frame=pd.concat({'InventedOtherCountry':frame},names=['country'])
    if role is not None: frame['Carrier_role']=role
    expected=frame if role!='reported_H_component' else frame.iloc[:0]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        exact(tr.nitrogen_kg(frame),ns['nitrogen_kg'](expected))
