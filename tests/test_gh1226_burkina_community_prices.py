"""Own Burkina PRICE instrument, literal inputs only; no source/API builds."""
import importlib.util
import runpy
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library import local_tools as tools
from lsms_library.ehcvm import community_price_slots
from lsms_library.paths import countries_root

KEY = 'Burkina_Faso::community_prices::quoted-native-unit-count'


@pytest.fixture
def mapping(monkeypatch):
    path = countries_root() / 'Burkina_Faso' / '2018-19' / '_'
    spec = importlib.util.spec_from_file_location('mapping', path / 'mapping.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'mapping', module)
    spec.loader.exec_module(module)
    monkeypatch.chdir(path)
    return module


def source():
    return pd.DataFrame({
        'grappe': [12, 12, 12], 'vague': [1, 1, 1], 'Existe': [1, 1, 2],
        's05q01': [5, 6, 5], 's05q02': [1000, 1030, 1000],
        's05q03': [np.nextafter(100., 101.), 0., np.nextafter(100., 101.)],
        's05q04': [200.25, -1., pd.NA],
    })


COMMUNITY = {'s05q01': {5: 'Maïs en épi', 6: 'Maïs en grain'},
             's05q02': {1000: ' Kg ', 1030: 'Avec os au Kg'}}
HOUSEHOLD = {'s07bq01': {5: 'Maïs en épi', 6: 'Maïs en grain'}}


def exact_numbers(actual, values):
    expected = pd.Series(values, dtype='Float64')
    pd.testing.assert_extension_array_equal(actual.array, expected.array)
    keep = expected.notna().to_numpy()
    np.testing.assert_array_equal(actual.to_numpy(dtype=float, na_value=np.nan)[keep],
                                  expected.to_numpy(dtype=float, na_value=np.nan)[keep], strict=True)


def test_all_slots_repeated_equal_rows_and_original_values_survive(mapping):
    codes = source()
    before = codes.copy(deep=True)
    out = mapping.community_prices_s05(codes, COMMUNITY, HOUSEHOLD)
    assert len(out) == 6 and out.index.is_unique
    assert out.index.names == ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    repeated = out.xs('s05q01=5;s05q02=1000', level='variety')
    assert repeated.index.get_level_values('obs').tolist() == [1, 2, 3, 4]
    exact_numbers(repeated.Price, [np.nextafter(100., 101.), 200.25,
                                   np.nextafter(100., 101.), pd.NA])
    assert repeated.SourceRow.tolist() == [0, 0, 2, 2]
    exact_numbers(out.xs('s05q01=6;s05q02=1030', level='variety').Price, [0., -1.])
    assert 'Quantity' not in out and 'Derivation' not in out and 'i' not in out.index.names
    pd.testing.assert_frame_equal(codes, before)


def test_native_coordinates_protect_coalescing_product_and_unit_labels(mapping):
    codes = pd.concat([source(), source().iloc[[0]]], ignore_index=True)
    codes.loc[3, 's05q02'] = 1030
    hh = {'s07bq01': {5: 'Riz importé 3', 6: 'Riz importé long grain'}}
    out = mapping.community_prices_s05(codes, COMMUNITY, hh)
    assert set(out.index.get_level_values('j')) == {'Riz importé'}
    # Actual post-read u mapping may coalesce these labels. The complete
    # native unit code in constructed variety keeps distinct observations.
    frame = out.reset_index()
    frame['u'] = frame['u'].replace({'Avec os au Kg': 'Kg'})
    keys = out.index.names
    assert not frame.duplicated(keys).any()
    assert set(frame.SourceUnitCode) == {'1000', '1030'}
    assert set(frame.SourceProductCode) == {'5', '6'}


@pytest.mark.parametrize('code,label', [(140, 'Sorgho rouge'), (141, 'Macoroni'),
                                        (143, 'Sucre en carreaux')])
def test_missing_product_dictionary_uses_exact_own_capi_attestation(mapping, code, label):
    codes = source().iloc[:1].copy()
    codes['s05q01'] = code
    out = mapping.community_prices_s05(codes, COMMUNITY, HOUSEHOLD)
    assert set(out.Description) == {label}
    assert set(out.SourceProductCode) == {str(code)}
    assert 'Unknown' not in out.index.get_level_values('j')[0]


def test_source_product_dictionary_precedes_capi_and_uses_household_map(mapping):
    codes = source().iloc[:1].copy()
    codes['s05q01'] = 140
    cats = {**COMMUNITY, 's05q01': {140: 'own community label'}}
    hh = {'s07bq01': {140: 'Maïs en épi'}}
    out = mapping.community_prices_s05(codes, cats, hh)
    assert set(out.Description) == {'own community label'}
    assert set(out.index.get_level_values('j')) == {'Maïs en épi'}


@pytest.mark.parametrize('code,expected', [
    (1161, 'Canari-petit'),
    (1001, 'Unknown [Burkina_Faso 2018-19 s05q02=1001]'),
    (9998, 'Unknown [Burkina_Faso 2018-19 s05q02=9998]'),
])
def test_unit_fallbacks_do_not_infer_digits_or_mass(mapping, code, expected):
    codes = source().iloc[:1].copy()
    codes['s05q02'] = code
    out = mapping.community_prices_s05(codes, COMMUNITY, HOUSEHOLD)
    assert set(out.index.get_level_values('u')) == {expected}
    assert out.SourceUnitCode.tolist() == [str(code)] * 2


@pytest.mark.parametrize('code,label', [(2540, 'Demi'), (2550, 'Entier'),
                                        (1161, 'Source full-size label')])
def test_reported_complete_unit_label_wins_without_base_substitution(mapping, code, label):
    codes = source().iloc[:1].copy()
    codes['s05q02'] = code
    cats = {**COMMUNITY, 's05q02': {code: label}}
    out = mapping.community_prices_s05(codes, cats, HOUSEHOLD)
    assert set(out.index.get_level_values('u')) == {label}


def test_unknown_recorded_product_remains_opaque_and_identifiable(mapping):
    codes = source().iloc[:1].copy()
    codes['s05q01'] = 999
    out = mapping.community_prices_s05(codes, COMMUNITY, HOUSEHOLD)
    assert set(out.index.get_level_values('j')) == {
        'Unknown [Burkina_Faso 2018-19 s05q01=999]'}
    assert out.index.get_level_values('variety').tolist() == ['s05q01=999;s05q02=1000'] * 2


@pytest.mark.parametrize('column', ['grappe', 'vague', 's05q01', 's05q02'])
def test_missing_native_key_refuses_instead_of_discarding_source_row(mapping, column):
    codes = source().astype({'grappe': 'Float64', 'vague': 'Float64',
                              's05q01': 'Float64', 's05q02': 'Float64'})
    codes.loc[0, column] = pd.NA
    with pytest.raises(ValueError, match='missing native observation keys'):
        mapping.community_prices_s05(codes, COMMUNITY, HOUSEHOLD)


def test_quantity_is_separate_registered_count_and_never_changes_prices(mapping):
    from lsms_library.derivations import derivation_records, resolve_callable
    base = mapping.community_prices_s05(source(), COMMUNITY, HOUSEHOLD)
    before = base.copy(deep=True)
    record = derivation_records('Burkina_Faso')[KEY]
    out = resolve_callable(record.function)(base)
    pd.testing.assert_frame_equal(out.drop(columns=['Quantity', 'Derivation']), before)
    pd.testing.assert_frame_equal(base, before)
    exact_numbers(out.Quantity, [1.] * len(out))
    assert set(out.Derivation) == {KEY}
    assert 'modelling-choice' in {a['basis'] for a in record.assumptions}
    assert set(record.raw_variables) == set(source().columns)


def test_raw_inputs_preserve_original_fields_and_position_without_quantity(mapping, monkeypatch):
    codes = source()
    calls = []
    def read(path, **kwargs):
        calls.append((str(path), kwargs))
        if not kwargs.get('categories_only'):
            return codes
        return COMMUNITY if str(path).endswith('s05_co_bfa2018.dta') else HOUSEHOLD
    monkeypatch.setattr(tools, 'get_dataframe', read)
    out = mapping.inputs_community_prices()
    pd.testing.assert_frame_equal(out.reset_index()[list(codes)], codes)
    assert out.SourceRow.tolist() == [0, 1, 2]
    assert 'Quantity' not in out and 'obs' not in out.index.names
    assert len(calls) == 3 and calls[0][1] == {'convert_categoricals': False}
    assert 'i' not in out.index.names
    with pytest.raises(ValueError, match='only2018-19'):
        mapping.inputs_community_prices('2021-22')


def test_actual_script_reads_only_declared_sources_and_uses_standard_writer(mapping, monkeypatch):
    calls, writes = [], []
    def read(path, **kwargs):
        calls.append((path, kwargs))
        if not kwargs.get('categories_only'):
            return source()
        return COMMUNITY if path.endswith('s05_co_bfa2018.dta') else HOUSEHOLD
    monkeypatch.setattr(tools, 'get_dataframe', read)
    monkeypatch.setattr(tools, 'to_parquet', lambda df, path: writes.append((df, path)))
    runpy.run_path('community_prices.py')
    assert calls == [('../Data/s05_co_bfa2018.dta', {'convert_categoricals': False}),
                     ('../Data/s05_co_bfa2018.dta', {'categories_only': True}),
                     ('../Data/s07b_me_bfa2018.dta', {'categories_only': True})]
    assert len(writes) == 1 and writes[0][1] == 'community_prices.parquet'
    assert len(writes[0][0]) == 6 and set(writes[0][0].Derivation) == {KEY}


def test_shared_mechanics_do_not_apply_country_selection_policy():
    base = pd.DataFrame({'key': ['a', 'a', 'a']}, index=[1, 2, 3])
    raw = pd.DataFrame({'first': [pd.NA, 0, -1], 'second': [9, 9, 9]}, index=base.index)
    ranks = pd.Series([0, 1, 2], index=base.index)
    out = community_price_slots(base, raw, ('first', 'second'), ranks)
    assert out.obs.tolist() == [1, 3, 5, 2, 4, 6]
    exact_numbers(out.Price, [pd.NA, 0, -1, 9, 9, 9])
    with pytest.raises(ValueError, match='align'):
        community_price_slots(base, raw.reset_index(), ('first', 'second'), ranks)


def test_actual_finalizer_preserves_native_coordinates_after_u_coalescence(mapping, monkeypatch):
    from lsms_library import Country
    from lsms_library.derivations import derivation_records, resolve_callable
    from lsms_library.feature import _harmonize_country_frame, _canonical_index_levels

    codes = source().iloc[:2].copy()
    codes['s05q01'] = 5  # SAME product, two different native unit codes.
    native = mapping.derive_quoted_native_unit_count(
        mapping.community_prices_s05(codes, COMMUNITY, HOUSEHOLD))
    country = Country('Burkina_Faso', preload_panel_ids=False)
    monkeypatch.setattr(country, '_join_v_from_sample',
                        lambda *_: pytest.fail('cluster prices must not build sample'))
    public = country._finalize_result(native,
        country._materialization_entry('community_prices'), 'community_prices')
    assert len(public) == 4 and public.index.is_unique
    assert set(public.index.get_level_values('u')) == {'Kg'}
    assert set(public.index.get_level_values('variety')) == {
        's05q01=5;s05q02=1000', 's05q01=5;s05q02=1030'}
    assert set(public.SourceUnitCode) == {'1000', '1030'}
    pd.testing.assert_extension_array_equal(public.Price.array, native.Price.array)
    exact_numbers(public.Quantity, [1.] * 4)
    assert set(public.Derivation) == {KEY} and KEY in public.attrs['derivations']['Burkina_Faso']
    feature = _harmonize_country_frame(public,
        _canonical_index_levels('community_prices'), 'Burkina_Faso', 'community_prices')
    assert feature.index.equals(public.index) and feature.index.is_unique
    pd.testing.assert_extension_array_equal(feature.Price.array, public.Price.array)
    assert set(feature.Derivation) == {KEY}
    # Resolve the registry's actual inputs callable by path, then inject only
    # its reader leaf; this is not merely a direct same-module call.
    record = derivation_records('Burkina_Faso')[KEY]
    def read(path, **kwargs):
        if not kwargs.get('categories_only'):
            return codes
        return COMMUNITY if str(path).endswith('s05_co_bfa2018.dta') else HOUSEHOLD
    monkeypatch.setattr(tools, 'get_dataframe', read)
    inputs = resolve_callable(record.inputs)()
    pd.testing.assert_frame_equal(inputs.reset_index()[list(codes)], codes)
    assert 'Quantity' not in inputs and 'i' not in inputs.index.names
