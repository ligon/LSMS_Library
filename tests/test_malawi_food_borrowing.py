"""Controls for the approved native-key borrowing and complete-quantity gate."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope='module')
def borrowing():
    path = (Path(__file__).resolve().parents[1] / 'lsms_library/countries/Malawi'
            / '2024-25/_/food_borrowing.py')
    spec = importlib.util.spec_from_file_location('malawi_food_borrowing_control', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def factors(rows=None):
    return pd.DataFrame(rows or [
        [1, 'MAIZE UFA MGAIWA (NORMAL FLOUR)', 101, 'PAIL SMALL', '', '4A', 2., 2],
        [1, 'MAIZE UFA MGAIWA (NORMAL FLOUR)', 101, 'OTHER (SPECIFY)', 'BASIN', '23', 3., 4],
        [1, 'MAIZE UFA MGAIWA (NORMAL FLOUR)', 101, 'OTHER (SPECIFY)', 'CUP', '23', .4, 1]],
        columns=['region', 'item_name', 'item_code', 'unit_name', 'Otherunit',
                 'unit_code', 'factor', 'source'])


def wide(n=1):
    frame = pd.DataFrame({'j': ['household']*n, 't': ['2024-25']*n,
                          'i': ['Maize ufa mgaiwa (normal flour)']*n,
                          'hh_g02': [101]*n, '_borrow_region': [1]*n,
                          'expenditure': [10.]*n})
    for source in ['consumed', 'bought', 'produced', 'gifted']:
        frame[f'quantity_{source}'] = [2. if source == 'bought' else np.nan]*n
        frame[f'unitcode_{source}'] = ['4A']*n
        frame[f'unitsdetail_{source}'] = ['']*n
        frame[f'u_{source}'] = ['Pail Small']*n
    return frame.set_index(['j', 't', 'i'])


def test_full_otherunit_key_selects_own_factor(borrowing):
    data = wide(2)
    data['unitcode_bought'] = '23'
    data['unitsdetail_bought'] = ['basin', 'cup']
    data['u_bought'] = ['Basin', 'Cup']
    matched = borrowing.match_ihs5_factors(data, factors())
    assert matched.cfactor_bought.tolist() == [3., .4]
    assert matched.ihs5_bought_source.tolist() == [4, 1]


def test_factor_raw_keys_survive_lookup_normalization(borrowing):
    source = factors().iloc[[1]].copy()
    source['Otherunit'] = '  basin '
    data = wide()
    data['unitcode_bought'] = '23'
    data['unitsdetail_bought'] = 'BASIN'
    data['u_bought'] = 'Basin'
    matched = borrowing.match_ihs5_factors(data, source)
    assert matched.cfactor_bought.iloc[0] == 3.
    assert matched.ihs5_bought_Otherunit.iloc[0] == '  basin '
    assert matched.ihs5_bought_item_code.iloc[0] == 101


def test_all_raw_sidecars_are_declared_for_wave_cache_hash(borrowing):
    from lsms_library.local_tools import scan_script_data_refs
    references = set(scan_script_data_refs(borrowing.HERE / 'food_acquired.py'))
    expected = {
        '../Data/Cross_Sectional/hh_mod_g1.dta',
        '../Data/Panel/hh_mod_g1_24.dta',
        '../Data/Cross_Sectional/hh_mod_a_filt.dta',
        '../Data/Panel/hh_mod_a_filt_24.dta',
        '../../2019-20/Data/Cross_Sectional/ihs_foodconversion_factor_2020.dta',
    }
    assert expected <= references
    for ref in expected:
        source = borrowing.HERE.parent / 'Data' / ref
        assert Path(str(source.resolve()) + '.dvc').is_file()


def test_duplicate_full_factor_key_is_not_averaged(borrowing):
    source = factors()
    with pytest.raises(ValueError, match='unique'):
        borrowing.prepare_ihs5_factors(pd.concat([source, source.iloc[[0]]]))


def test_no_unknown_or_generic_other_match(borrowing):
    data = wide(3)
    data['unitcode_bought'] = '23'
    data['unitsdetail_bought'] = ['', 'N/A', 'BASIN']
    data['u_bought'] = ['Unknown', 'Unknown', 'Basin']
    result = borrowing.match_ihs5_factors(data, factors())
    assert result.cfactor_bought.isna().tolist() == [True, True, False]


@pytest.mark.parametrize('code,item,unit', [
    (119, 'MAIZE, YELLOW FLOUR', 'PAIL SMALL'),
    (101, 'MAIZE UFA MGAIWA (NORMAL FLOUR)', 'TIN 1 KG'),
    (120, 'POPCORN', 'PAIL SMALL'),
    (112, 'BUNS, SCONES', 'PAIL SMALL')])
def test_changed_or_unproved_item_unit_is_refused(borrowing, code, item, unit):
    source = factors().iloc[[0]].copy()
    source['item_code'] = code
    source['item_name'] = item
    source['unit_name'] = unit
    if unit == 'TIN 1 KG':
        source['unit_code'] = '74'
    assert not borrowing.prepare_ihs5_factors(source).borrow_compatible.any()


def test_metric_rows_keep_the_original_single_conversion(borrowing):
    data = wide()
    data['u_bought'] = '500 g packet'
    data['unitcode_bought'] = '36'
    source = factors().iloc[[0]].copy()
    source['unit_code'] = '36'
    source['unit_name'] = 'PACKET (500G)'
    baseline = borrowing.canonical_food(data)
    candidate = borrowing.derive_borrowed_food(data, source)
    pd.testing.assert_frame_equal(candidate[baseline.columns], baseline)
    assert candidate.Quantity_kg.isna().all()
    assert candidate.Derivation.isna().all()


def _bucket(data, borrowing):
    """Unit code 26, the instrument's '5 Litre Bucket(Chigoba)' (PR #1604 F1)."""
    data['unitcode_bought'] = '26'
    data['u_bought'] = '5 Litre Bucket(Chigoba)'
    source = factors().iloc[[0]].copy()
    source['unit_code'] = '26'
    source['unit_name'] = '5 LITRE BUCKET(CHIGOBA)'
    source['factor'] = 3.88
    assert borrowing._metric('5 Litre Bucket(Chigoba)')  # parses as 5 l
    return data, source


def test_volume_bucket_borrows_its_released_factor_natively(borrowing):
    data, source = _bucket(wide(), borrowing)
    result = borrowing.derive_borrowed_food(data, source)
    assert result.index.get_level_values('u').tolist() == ['5 Litre Bucket(Chigoba)']
    assert result.Quantity.tolist() == [2.]
    assert result.Quantity_kg.iloc[0] == pytest.approx(2. * 3.88)
    assert result.Derivation.tolist() == [borrowing.BORROW_KEY]


def test_unmatched_volume_bucket_keeps_the_shared_choke_point(borrowing):
    data, source = _bucket(wide(), borrowing)
    data['_borrow_region'] = 2  # no released factor for this region
    baseline = borrowing.canonical_food(data)
    result = borrowing.derive_borrowed_food(data, source)
    pd.testing.assert_frame_equal(result[baseline.columns], baseline)
    assert result.index.get_level_values('u').tolist() == ['kg']
    assert result.Quantity.tolist() == [10.]
    assert result.Quantity_kg.isna().all()
    assert result.Derivation.isna().all()


def test_mixed_bucket_group_routes_each_contributor_to_its_served_key(borrowing):
    """A matched and an unmatched bucket under one (t, i, j, s) are two served
    rows; the unmatched one must not refuse the matched one's kilograms."""
    data, source = _bucket(wide(2), borrowing)
    data['_borrow_region'] = [1, 2]
    result = borrowing.derive_borrowed_food(data, source)
    by_u = result.reset_index().set_index('u')
    assert sorted(by_u.index) == ['5 Litre Bucket(Chigoba)', 'kg']
    assert by_u.loc['5 Litre Bucket(Chigoba)', 'Quantity_kg'] == pytest.approx(2. * 3.88)
    assert by_u.loc['5 Litre Bucket(Chigoba)', 'Derivation'] == borrowing.BORROW_KEY
    assert by_u.loc['kg', 'Quantity'] == 10.
    assert pd.isna(by_u.loc['kg', 'Quantity_kg'])
    assert pd.isna(by_u.loc['kg', 'Derivation'])
    matched = borrowing.match_ihs5_factors(data, source)
    assert borrowing._served_unit(matched, 'bought').tolist() == [
        '5 Litre Bucket(Chigoba)', 'kg']


def test_mass_label_stays_refused_beside_the_bucket_exemption(borrowing):
    data = wide()
    data['unitcode_bought'] = '41'
    data['u_bought'] = 'Satchet/Tube (25G)'
    assert borrowing._metric_refused(data, 'bought').all()
    assert borrowing.BORROW_DESPITE_METRIC_LABEL == frozenset({'26'})


def test_partial_collapsed_quantity_stays_missing(borrowing):
    data = wide(2)
    data['_borrow_region'] = [1, 2]
    result = borrowing.derive_borrowed_food(data, factors())
    assert len(result) == 1
    assert result.Quantity.iloc[0] == 4.
    assert result.Quantity_kg.isna().all()
    assert result.Derivation.isna().all()


def test_active_unknown_quantity_cannot_certify_partial_kg(borrowing):
    data = wide(2)
    data['quantity_bought'] = [2., np.nan]
    data['expenditure'] = [10., 30.]
    data['_borrow_region'] = [1, 2]
    baseline = borrowing.canonical_food(data)
    result = borrowing.derive_borrowed_food(data, factors())
    pd.testing.assert_frame_equal(result[baseline.columns], baseline, check_exact=True)
    assert result.Quantity.iloc[0] == 2.
    assert result.Expenditure.iloc[0] == 40.
    assert result.Quantity_kg.isna().all()
    assert result.Derivation.isna().all()


@pytest.mark.parametrize('left,right', [
    ('No 1/2 PLATE', 'No 12 PLATE'),
    ('LOAF (30.0G)', 'LOAF (300G)'),
    ('No 1-2 PLATE', 'No 12 PLATE'),
    ('No 1–2 PLATE', 'No 1/2 PLATE'),
    ('No ½ PLATE', 'No 12 PLATE'),
    ('No ⅓ PLATE', 'No ⅔ PLATE'),
    ('.5 kg', '5 kg'), ('-5 kg', '5 kg'),
])
def test_numeric_meaning_is_preserved_in_label_compatibility(borrowing, left, right):
    assert borrowing._label(left) != borrowing._label(right)


def test_printed_ordinal_case_and_spacing_alias(borrowing):
    assert borrowing._label('No. 12 PLATE') == borrowing._label('no 12 plate')


def test_fraction_alias_cannot_reach_actual_derivation(borrowing):
    source = factors().iloc[[0]].copy()
    source['unit_code'] = '7'
    source['unit_name'] = 'No 1/2 PLATE'
    data = wide()
    data['unitcode_bought'] = '7'
    data['u_bought'] = 'No 12 Plate'
    result = borrowing.derive_borrowed_food(data, source)
    assert result.Quantity_kg.isna().all()
    assert result.Derivation.isna().all()


def test_complete_collapsed_quantities_and_unrelated_payload(borrowing):
    data = wide(2)
    baseline = borrowing.canonical_food(data)
    candidate = borrowing.derive_borrowed_food(data, factors())
    pd.testing.assert_frame_equal(candidate[baseline.columns], baseline)
    assert candidate.Quantity_kg.iloc[0] == 8.
    assert candidate.Derivation.iloc[0] == borrowing.BORROW_KEY


def test_expenditure_only_row_does_not_invent_quantity(borrowing):
    data = wide()
    data['quantity_bought'] = np.nan
    result = borrowing.derive_borrowed_food(data, factors())
    assert result.Quantity.isna().all()
    assert result.Quantity_kg.isna().all()
    assert result.Expenditure.iloc[0] == 10.


def test_reader_contract_and_literal_raw_operands(borrowing, monkeypatch):
    """Exercise production read assembly with the actual reader's narrow API."""
    def reader(path, convert_categoricals=True, encoding=None, categories_only=False):
        panel = '/Panel/' in str(path)
        identity = {'y5_hhid': ['panel']} if panel else {'case_id': ['cross']}
        if 'a_filt' in str(path):
            return pd.DataFrame(identity | {'region': [1]})
        raw = identity | {'hh_g02': [101], 'hh_g05': [10.]}
        for stem in borrowing.SOURCES.values():
            raw.update({stem+'a': ['a'], stem+'b': ['4A'],
                        stem+'b_label': [4], stem+'b_oth': ['']})
        if convert_categoricals:
            raw['hh_g02'] = ['Maize ufa mgaiwa (normal flour)']
            for stem in borrowing.SOURCES.values():
                raw[stem+'b_label'] = ['PAIL']
        return pd.DataFrame(raw)
    monkeypatch.setattr(borrowing, 'get_dataframe', reader)
    monkeypatch.setattr(borrowing, 'drop_ihps24_refreshed', lambda frame, column: frame)
    out = borrowing.read_food_wide()
    assert len(out) == 2
    assert out.hh_g04b_oth.tolist() == ['', '']
    assert out.hh_g04a.tolist() == ['a', 'a']
    assert out.quantity_bought.isna().all()
    assert out.hh_g02.tolist() == [101, 101]
    assert set(out.index.get_level_values('j')) == {'cs-24-cross', 'panel'}
