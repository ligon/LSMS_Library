"""Invented-only public bridge controls: no closed W3 cohort or source calls."""
from copy import deepcopy
import json
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import nigeria_harvest as nh
from lsms_library import transformations as tr
from lsms_library.countries.Nigeria._ import nigeria as ng


CARD = {1: '1. KILOGRAM (KG)', 2: '2. GRAMS (G)', 3: '3. LITRE (L)',
        130: '130. SACK/BAG SMALL', 131: '131. SACK/BAG MEDIUM',
        160: '160. BUNDLE SMALL', 190: '190. JERRY CAN/RUBBER 10 LITRES'}
PROVENANCE = {'harvest_md5': '0' * 32, 'conversion_md5': '1' * 32,
              'updated_ids_sha256': '2' * 64, 'crop_labels_sha256': '3' * 64,
              'kind': 'invented; not source admission'}


def table(rows=None):
    if rows is None:
        rows = [(1120, 130, 20.), (1120, 131, 40.), (1121, 130, 30.),
                (1120, 1, 1.), (1120, 2, .001)]
    out = pd.DataFrame(rows, columns=['crop_cd', 'unit_cd', 'conv_national'])
    out['crop_name'] = 'INVENTED'
    out['unit_name'] = out.unit_cd.map(CARD).str.replace(r'^\d+\.\s+', '', regex=True)
    out['note'] = pd.Series(['invented; construction unvalidated'] * len(out), dtype='string')
    for zone, column in nh._ZONES.items():
        # Zone choices differ only on non-metric containers.
        out[column] = out.conv_national.where(out.unit_cd.isin([1, 2]), out.conv_national * zone)
    return out


def native(rows=None):
    if rows is None:
        rows = [(10, 1, 1120, 1, 130, 1, 2.),
                (10, 1, 1121, 2, 130, 1, 3.)]
    out = pd.DataFrame(rows, columns=['hhid', 'plotid', 'cropcode', 'cropid',
                                      'sa3iq6ii', 'zone', 'sa3iq6i'])
    out['sa3iq6a'] = pd.Series([17.] * len(out), dtype='float32')
    out['sa3iq6ii_os'] = pd.Series([pd.NA] * len(out), dtype='string')
    out['harvested'] = 2  # Deliberately no: current producer still keeps lines.
    return out


def run(h=None, f=None, **kwargs):
    options = dict(harvest_unit_card=CARD, conversion_unit_card=CARD,
                   crop_labels={1120: 'Yam', 1121: 'Yam'},
                   updated_ids={'2016Q1': {'10': 'old10'}},
                   provenance=PROVENANCE, geography='national')
    options.update(kwargs)
    return nh.native_w3_harvest_kg(native() if h is None else h,
                                  table() if f is None else f, **options)


def test_full_release_universe_and_literal_mass():
    h = native([(10, 1, 1120, 1, 130, 1, 2.),
                (10, 1, 1121, 2, 130, 1, 3.),
                (10, 1, 1120, 3, 131, 1, np.nan),
                (10, 1, 1120, 4, 130, 1, 0.),
                (np.nan, 1, 1120, 5, 130, 1, 9.),
                (10, 1, 9999, 6, 130, 1, 11.),
                (10, np.nan, 1120, 7, np.nan, np.nan, 7.)])
    h.index = pd.Index([3, 3, 1, 1, 0, 0, -1], name='raw_position')
    f = table([(1120,130,20.), (1120,131,40.), (1121,130,30.), (9999,130,70.)])
    h0, f0 = h.copy(deep=True), f.copy(deep=True)
    result, audit = run(h, f)
    pd.testing.assert_frame_equal(audit.native_lines[h.columns], h0)
    pd.testing.assert_frame_equal(audit.conversion_rows[f.columns], f0)
    pd.testing.assert_frame_equal(h, h0)
    pd.testing.assert_frame_equal(f, f0)
    assert list(audit.native_lines.W3_SourceRow) == list(range(7))
    assert list(audit.native_lines.W3_Eligible) == [True]*4 + [False,False,True]
    assert audit.metadata['unserved_rows'] == 2
    assert audit.metadata['delegated_rows'] == 5
    assert list(audit.native_lines.W3_CanonicalI[:4]) == ['old10']*4
    assert list(audit.native_lines.W3_ProductionObs[:4]) == [f'reported:crop_id:{i}' for i in range(1,5)]
    assert result.index.names == ['country','t','i','plot_id','j']
    assert result.Harvest_kg.sum() == 130.
    assert result.attrs['kg_factor_sources']['shipped'] == 4
    assert audit.native_lines.W3_kg_per_unit.iloc[2] == 40.
    assert audit.native_lines.W3_MassStatus.iloc[4] == 'unserved'


@pytest.mark.parametrize('geography,expected', [('national',130.), ('zone',390.)])
def test_geography_literal_and_sizes(geography, expected):
    h = native([(10,1,1120,1,130,3,2.),(10,1,1120,2,131,3,1.),(10,1,1121,3,130,3,5/3)])
    result, audit = run(h, geography=geography)
    assert result.Harvest_kg.sum() == expected
    assert list(audit.native_lines.W3_LookupUnit) == ['SACK/BAG SMALL','SACK/BAG MEDIUM','SACK/BAG SMALL']
    assert len(set(audit.native_lines.W3_ServedUnit)) == 1


def test_missing_zone_does_not_go_national():
    with pytest.warns(tr.ShippedFactorWarning, match='matched 0'):
        result, audit = run(native([(10,1,1120,1,130,np.nan,2.)]), geography='zone')
    assert result.empty
    assert audit.native_lines.W3_kg_per_unit.isna().all()
    assert audit.native_lines.W3_Region.isna().all()


@pytest.mark.parametrize('kwargs', [{'geography':None},{'geography':'mean'},
                                  {'updated_ids':{}}, {'provenance':{}},
                                  {'provenance':{'harvest_md5':object()}}])
def test_required_explicit_contract(kwargs):
    with pytest.raises(ValueError):
        run(**kwargs)


def test_geography_has_no_default():
    with pytest.raises(TypeError, match='geography'):
        nh.native_w3_harvest_kg(native(), table(), harvest_unit_card=CARD,
            conversion_unit_card=CARD, crop_labels={}, updated_ids={}, provenance={})


@pytest.mark.parametrize('drop', ['zone', 'cropcode', 'cropid', 'hhid', 'sa3iq6ii'])
def test_required_native_dimensions(drop):
    with pytest.raises(ValueError, match='source dimensions'):
        run(native().drop(columns=drop))


@pytest.mark.parametrize('rows', [[], [(10,1,9999,1,130,1,2.)],
                                 [(10,1,1120,1,130,1,np.nan)]])
def test_empty_unserved_and_missing_quantity(rows):
    result, audit = run(native(rows))
    assert len(audit.native_lines) == len(rows)
    assert result.empty
    assert audit.metadata['native_rows'] == len(rows)


def test_holds_precede_all_layers_and_no_extra_inputs(monkeypatch):
    h = native([(10,1,1120,1,3,1,2.), (10,1,1120,2,160,1,3.),
                (10,1,1120,3,190,1,4.), (10,1,1120,4,130,1,5.),
                (10,1,9998,5,3,1,6.)])
    # Extra fields/attrs must never activate survey/reported/metric/price layers.
    h['Quantity_kg'] = 999.; h['KgFactor'] = 777.; h['Expenditure'] = 555.
    h.attrs['unit_kg'] = {'LITRE (L)': 88., 'SACK/BAG SMALL': 99.}
    f = table([(1120,3,1.), (1120,160,9.), (1120,190,10.), (1120,130,20.)])
    f.loc[f.unit_cd == 160, 'unit_name'] = 'DIFFERENT BUNDLE SIZE'
    h0, f0 = h.copy(deep=True), f.copy(deep=True)
    calls = []
    for name in ['harvest_kg','harvest_kg_factors']:
        original = getattr(tr,name)
        def checked(df, _name=name, _original=original, **kw):
            assert kw['volume_as_mass'] is False
            assert len(df) == 1
            assert set(df.columns) <= {'Quantity','u','crop_variety','region'}
            assert df.u.iloc[0] == 'SACK/BAG SMALL'
            assert 'unit_kg' not in df.attrs
            assert kw['unit_kg'] == {'kg':1.,'g':.001}
            calls.append(_name)
            return _original(df, **kw)
        monkeypatch.setattr(tr,name,checked)
    with pytest.warns(nh.HarvestFactorHoldWarning):
        result, audit = run(h, f, crop_labels={1120:'Yam',9998:'Other'})
    assert result.Harvest_kg.sum() == 100.
    assert len(calls) == 3  # diagnostics + consumer + its internal diagnostics
    assert list(audit.native_lines.W3_Held) == [True,True,True,False,True]
    assert audit.metadata['factor_inventory']['unit_identity_held'] == 1
    assert audit.metadata['factor_inventory']['fluid_basis_held'] == 2
    assert audit.native_lines.loc[[0,1,2,4], 'W3_kg_per_unit'].isna().all()
    pd.testing.assert_frame_equal(h,h0); pd.testing.assert_frame_equal(f,f0)


@pytest.mark.parametrize('body', ['1/2 KG BOWL','30.0 G LOAF','1–2 KG BOWL','½ KG BOWL'])
def test_proven_code_prefix_preserves_numeric_body(body):
    card = {**CARD, 7:'7. '+body}
    f = table([(1120,7,4.)]); f['unit_name'] = body
    result, audit = run(native([(10,1,1120,1,7,1,2.)]), f,
                        harvest_unit_card=card, conversion_unit_card=card)
    assert result.Harvest_kg.sum() == 8.
    assert audit.native_lines.W3_LookupUnit.iloc[0] == body


@pytest.mark.parametrize('card', [{}, {130:'131. SACK/BAG SMALL'}, {130:'SACK/BAG SMALL'}])
def test_missing_malformed_harvest_card_stays_audited(card):
    result, audit = run(native([(10,1,1120,1,130,1,2.)]), harvest_unit_card=card)
    assert result.empty
    assert audit.metadata['held_eligible_rows'] == 1
    assert audit.native_lines.W3_InvalidCard.iloc[0]


def test_own_card_disagreement_is_not_alias():
    result, audit = run(native([(10,1,1120,1,130,1,2.)]),
                        harvest_unit_card={**CARD,130:'130. SACK/BAG LARGE'})
    assert result.empty
    assert audit.native_lines.W3_CardConflict.iloc[0]


@pytest.mark.parametrize('mutation', ['duplicate','missing-line','fractional-line','mapped-collision'])
def test_current_and_panel_errors_are_not_filtered(mutation):
    h = native([(10,1,1120,1,130,1,2.), (11,1,1120,2,130,1,3.)])
    maps = {'2016Q1':{'10':'old10'}}
    if mutation == 'duplicate': h.loc[1,['hhid','cropid']] = [10,1]
    if mutation == 'missing-line': h.loc[1,'cropid'] = np.nan
    if mutation == 'fractional-line':
        h['cropid'] = h.cropid.astype(float)
        h.loc[1,'cropid'] = 1.5
    if mutation == 'mapped-collision': maps = {'2016Q1':{'10':'same','11':'same'}}
    before = h.copy(deep=True)
    with pytest.raises((ValueError,TypeError)):
        run(h, updated_ids=maps)
    pd.testing.assert_frame_equal(h,before)


def test_actual_raw_walk_once_with_chain_and_subset():
    # Raw chained-only T must move once to U, not closure beyond that.
    h = native([(20,1,1120,1,130,1,2.)])
    maps = {'2016Q1':{'10':'20','20':'30','30':'40'}}
    before = deepcopy(maps)
    result, audit = run(h,updated_ids=maps)
    assert result.index.get_level_values('i').tolist() == ['30']
    assert audit.native_lines.W3_RawFormattedI.tolist() == ['20']
    assert audit.metadata['id_converted'] is True
    assert maps == before


def test_normalized_factor_alias_refused():
    card = {**CARD,132:'132. sack/bag small'}
    f = table([(1120,130,20.),(1120,132,30.)]); f.loc[1,'unit_name']='sack/bag small'
    with pytest.raises(ValueError,match='normalized factor keys'):
        run(f=f, conversion_unit_card=card)


def test_arrow_audits_and_plain_metadata():
    import pyarrow as pa
    result, audit = run()
    assert isinstance(audit,nh.NativeHarvestAudit)
    for frame in [audit.native_lines,audit.conversion_rows]:
        pd.testing.assert_frame_equal(pa.Table.from_pandas(frame).to_pandas(),frame)
    # The unchanged shared aggregate's StringDtype index roundtrips as pandas
    # str with this Arrow version; do not change the kernel to normalize it.
    pd.testing.assert_frame_equal(pa.Table.from_pandas(result).to_pandas(),result,
                                  check_index_type=False)
    json.dumps(audit.metadata,allow_nan=False)
    assert audit.metadata['provenance'] == PROVENANCE
    assert audit.metadata['kernel_counts_scope'] == 'delegated rows only'


def test_exact_existing_kernel_output_and_arrow_index_limit():
    import pyarrow as pa
    h = native()
    result, audit = run(h)
    index = pd.MultiIndex.from_arrays([
        ['Nigeria']*2, ['2016Q1']*2, ['old10']*2,
        ['1']*2, pd.array(['Yam']*2,dtype='string'),
        pd.array(['reported:crop_id:1','reported:crop_id:2'],dtype='string')],
        names=['country','t','i','plot_id','crop','production_obs'])
    operand = pd.DataFrame({'Quantity':[2.,3.], 'u':['SACK/BAG SMALL']*2,
                             'crop_variety':['1120','1121']},index=index)
    factors = pd.DataFrame({'country':['Nigeria']*2,'t':['2016Q1']*2,
        'crop_variety':['1120','1121'],'u':['SACK/BAG SMALL']*2,
        'KgFactor':[20.,30.]}).set_index(['country','t','crop_variety','u'])
    expected = tr.harvest_kg(operand,shipped_factors=factors,
                             volume_as_mass=False,unit_kg={'kg':1.,'g':.001})
    pd.testing.assert_frame_equal(result,expected)
    actual_attrs = deepcopy(result.attrs)
    actual_attrs.pop('nigeria_w3_native_audit')
    assert actual_attrs == expected.attrs
    serialized = pa.Table.from_pandas(expected).to_pandas()
    assert serialized.index.get_level_values('j').dtype != expected.index.get_level_values('j').dtype
    pd.testing.assert_frame_equal(serialized,expected,
                                  check_index_type=False)


def test_nullable_audit_serialization_and_repeat_call():
    import pyarrow as pa
    h = native([(10,1,1120,1,np.nan,np.nan,np.nan),
                (np.nan,np.nan,9999,np.nan,np.nan,np.nan,0.)])
    first, audit = run(h)
    second, again = run(h)
    pd.testing.assert_frame_equal(first,second)
    for frame in [audit.native_lines,audit.conversion_rows]:
        pd.testing.assert_frame_equal(pa.Table.from_pandas(frame).to_pandas(),frame)
    pd.testing.assert_frame_equal(audit.native_lines,again.native_lines)
    assert audit.native_lines.W3_Eligible.tolist() == [True,False]
    assert audit.native_lines.W3_kg_per_unit.isna().all()


def test_all_fluid_factor_table_and_no_delegated_zero():
    h = native([(10,1,1120,1,3,1,2.)])
    f = table([(1120,3,1.)])
    with pytest.warns(nh.HarvestFactorHoldWarning):
        result, audit = run(h,f)
    assert result.empty
    assert audit.metadata['delegated_rows'] == 0
    assert audit.metadata['held_eligible_rows'] == 1
    assert audit.conversion_rows.W3_Held.all()
    assert audit.native_lines.W3_kg_per_unit.isna().all()


def test_negative_native_quantity_is_not_filtered_or_clipped():
    h = native([(10,1,1120,1,130,1,-2.)])
    result, audit = run(h)
    assert result.Harvest_kg.sum() == -40.
    assert audit.native_lines.sa3iq6i.iloc[0] == -2.
    assert audit.metadata['delegated_rows'] == 1


@pytest.mark.parametrize('change', ['duplicate-factor','missing-factor-key','bad-metric-definition'])
def test_unsafe_factor_input_refusal(change):
    f, card = table(), CARD.copy()
    if change == 'duplicate-factor': f = pd.concat([f,f.iloc[[0]]],ignore_index=True)
    if change == 'missing-factor-key': f.loc[0,'unit_cd'] = np.nan
    if change == 'bad-metric-definition':
        card[1] = '1. HUGE CONTAINER'
        f.loc[f.unit_cd == 1,'unit_name'] = 'HUGE CONTAINER'
    before = f.copy(deep=True)
    with pytest.raises(ValueError):
        run(f=f,conversion_unit_card=card)
    pd.testing.assert_frame_equal(f,before)


def test_public_hold_warning_callsite_and_promotion_preserve_inputs():
    h = native([(10,1,1120,1,3,1,2.)])
    f = table([(1120,3,1.)])
    h0, f0 = h.copy(deep=True), f.copy(deep=True)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        run(h,f)
    holds = [w for w in caught if w.category is nh.HarvestFactorHoldWarning]
    assert len(holds) == 1
    assert holds[0].filename == __file__  # Public caller, not private bridge.
    with warnings.catch_warnings():
        warnings.simplefilter('error',nh.HarvestFactorHoldWarning)
        with pytest.raises(nh.HarvestFactorHoldWarning):
            run(h,f)
    pd.testing.assert_frame_equal(h,h0)
    pd.testing.assert_frame_equal(f,f0)


def test_raw_extension_payload_index_and_metadata_immutability():
    import pyarrow as pa
    h, f = native(), table()
    h.index = pd.MultiIndex.from_arrays([[1,1],['x','x']],names=['original_a','original_b'])
    h['extra_answer'] = pd.array([pd.NA,True],dtype='boolean')
    h['extra_code'] = pd.Categorical(['missing','answer'])
    h.attrs['unit_kg'] = {'SACK/BAG SMALL':123.}
    f['extra_float'] = pd.Series([pd.NA]*len(f),dtype='Float32')
    f.attrs['unit_kg'] = {'SACK/BAG SMALL':456.}
    h0,f0 = h.copy(deep=True),f.copy(deep=True)
    cards, maps, provenance = deepcopy(CARD),{'2016Q1':{'10':'old10'}},deepcopy(PROVENANCE)
    before = deepcopy((cards,maps,provenance))
    result,audit=run(h,f,harvest_unit_card=cards,conversion_unit_card=cards,
                     updated_ids=maps,provenance=provenance)
    pd.testing.assert_frame_equal(audit.native_lines[h.columns],h0)
    pd.testing.assert_frame_equal(audit.conversion_rows[f.columns],f0)
    for frame in [audit.native_lines,audit.conversion_rows]:
        pd.testing.assert_frame_equal(pa.Table.from_pandas(frame).to_pandas(),frame)
    pd.testing.assert_frame_equal(h,h0); pd.testing.assert_frame_equal(f,f0)
    assert h.attrs == h0.attrs and f.attrs == f0.attrs
    assert (cards,maps,provenance) == before
    assert result.Harvest_kg.sum() == 130.


def test_no_sales_synthesis_and_current_producer_witness():
    h = native([(10,1,1120,1,130,1,2.),(10,2,1120,2,130,1,3.)])
    h['Quantity_sold'] = 500.; h['Unit_sold'] = 'SACK'
    result, audit = run(h)
    assert result.Harvest_kg.sum() == 100.
    assert not (result.index.get_level_values('plot_id') == 'Farm-level').any()
    dec=h.copy(); dec['sa3iq6ii']=h.sa3iq6ii.map(CARD)
    expected=ng.crop_production_for_wave('2016Q1',[dict(df=h,dec=dec,hhid='hhid',plot='plotid',
        crop='cropcode',line='cropid',qty='sa3iq6i',unit='sa3iq6ii',perennial=False)],{1120:'Yam'})
    assert audit.native_lines.W3_ProductionObs.tolist() == expected.index.get_level_values('production_obs').tolist()


@pytest.mark.parametrize('geography,column', [('national','conv_national'),('zone','conv_NE_2')])
@pytest.mark.parametrize('kind', ['nullable', 'float-nan'])
def test_selected_missing_factor_refuses_regardless_of_nullable_dtype(geography,column,kind):
    f = table()
    f[column] = f[column].astype('Float64' if kind == 'nullable' else 'float64')
    f.loc[0,column] = pd.NA if kind == 'nullable' else np.nan
    before = f.copy(deep=True)
    with pytest.raises(ValueError,match='finite and positive'):
        run(f=f,geography=geography)
    pd.testing.assert_frame_equal(f,before,check_exact=True)


@pytest.mark.parametrize('geography', ['national','zone'])
def test_positive_nullable_factors_retain_raw_payload_and_kernel_output(geography):
    f = table()
    columns = ['conv_national'] if geography == 'national' else list(nh._ZONES.values())
    ordinary = run(f=f,geography=geography)
    for column in columns:
        f[column] = f[column].astype('Float64')
    before=f.copy(deep=True)
    result,audit=run(f=f,geography=geography)
    pd.testing.assert_frame_equal(f,before,check_exact=True)
    pd.testing.assert_frame_equal(audit.conversion_rows[f.columns],before,check_exact=True)
    pd.testing.assert_frame_equal(result,ordinary[0],check_exact=True)
    assert result.attrs == ordinary[0].attrs
    pd.testing.assert_frame_equal(audit.native_lines,ordinary[1].native_lines,check_exact=True)


@pytest.mark.parametrize('reason', ['missing-household','unlabelled-crop','missing-crop'])
def test_all_unserved_new_unit_field_retains_nullable_string_arrow(reason):
    import pyarrow as pa
    h = native([(10,1,1120,1,130,1,2.)])
    for column in ['hhid','plotid','cropcode','cropid','sa3iq6ii','zone']:
        h[column] = h[column].astype('Int64')
    if reason == 'missing-household':h.loc[0,'hhid']=pd.NA
    if reason == 'unlabelled-crop':h.loc[0,'cropcode']=9999
    if reason == 'missing-crop':h.loc[0,'cropcode']=pd.NA
    f=table()
    before_h,before_f=h.copy(deep=True),f.copy(deep=True)
    # First prove that these invented raw types support the strict carrier.
    for raw in [h,f]:
        pd.testing.assert_frame_equal(pa.Table.from_pandas(raw).to_pandas(),raw,check_exact=True)
    result,audit=run(h,f)
    assert result.empty
    assert audit.metadata['eligible_rows']==audit.metadata['delegated_rows']==0
    assert isinstance(audit.native_lines.W3_ServedUnit.dtype,pd.StringDtype)
    assert audit.native_lines.W3_ServedUnit.isna().all()
    pd.testing.assert_frame_equal(audit.native_lines[h.columns],before_h,check_exact=True)
    pd.testing.assert_frame_equal(audit.conversion_rows[f.columns],before_f,check_exact=True)
    for frame in [audit.native_lines,audit.conversion_rows]:
        pd.testing.assert_frame_equal(pa.Table.from_pandas(frame).to_pandas(),frame,check_exact=True)
    pd.testing.assert_frame_equal(h,before_h,check_exact=True)
    pd.testing.assert_frame_equal(f,before_f,check_exact=True)
