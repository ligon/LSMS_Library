"""Mali s05 preserves both source slots and never invents a quantity (#821)."""
import importlib.util
import runpy
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.paths import countries_root


@pytest.fixture
def mapping(monkeypatch):
    path = countries_root() / 'Mali' / '2018-19' / '_'
    spec = importlib.util.spec_from_file_location('mapping', path / 'mapping.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'mapping', module)
    spec.loader.exec_module(module)
    monkeypatch.chdir(path)
    return module


def sources():
    codes = pd.DataFrame({
        'grappe': [12, 12, 12, 13], 'vague': [1, 1, 1, 2],
        'Existe': [1, 1, 1, 2], 's05q01': [5, 6, 5, 7],
        's05q02': [1071, 1071, 1071, 1433],
        's05q03': [100.125, 0, 100.125, pd.NA],
        's05q04': [200.25, -1, 250.5, pd.NA],
    })
    labels = codes.copy()
    labels['s05q01'] = ['Maïs en épi', 'Maïs en grain', 'Maïs en épi', 'Mil']
    labels['s05q02'] = ['Boite - Petit'] * 3 + ['Tas - Grand']
    return codes, labels


def exact_numbers(actual, expected):
    expected = pd.Series(expected, dtype='Float64')
    np.testing.assert_array_equal(actual.isna().to_numpy(), expected.isna().to_numpy())
    mask = expected.notna().to_numpy()
    np.testing.assert_array_equal(actual.to_numpy(dtype=float, na_value=np.nan)[mask],
                                  expected.to_numpy(dtype=float, na_value=np.nan)[mask])


def test_slots_duplicates_sizes_and_missing_prices_are_retained(mapping):
    codes, labels = sources()
    out = mapping.community_prices_s05(codes, labels)
    assert len(out) == 2 * len(codes) and out.index.is_unique
    assert out.index.names == ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    assert set(out.index.get_level_values('v')) == {'12', '13'}
    assert 'i' not in out.index.names
    assert set(out.index.get_level_values('u')) == {'Boite - Petit', 'Tas - Grand'}
    # Both source product codes survive their shared harmonized j='Maïs'.
    maize = out.xs('Maïs', level='j')
    assert set(maize.index.get_level_values('variety')) == {'5', '6'}
    repeated = maize.xs('5', level='variety')
    assert repeated.index.get_level_values('obs').tolist() == [1, 2, 3, 4]
    exact_numbers(repeated.Price, [100.125, 200.25, 100.125, 250.5])
    exact_numbers(maize.xs('6', level='variety').Price, [0, -1])
    assert out.xs('13', level='v').Price.isna().all()  # Existe=2 is not an exclusion
    assert out.Quantity.isna().all() and out.Quantity.dtype == pd.Float64Dtype()
    assert 'NumberOfUnits' not in out and 'Derivation' not in out


@pytest.mark.parametrize('unreported_weight', [0, pd.NA, 999, 2.5])
def test_no_weight_or_count_inferred_from_unselected_fields(mapping, unreported_weight):
    codes, labels = sources()
    # No such field is declared in this module. In particular, a kg unit
    # label or a hypothetical extra field must not make Quantity=1/weight.
    codes['poids'] = unreported_weight
    labels['s05q02'] = 'Kg'
    out = mapping.community_prices_s05(codes, labels)
    assert out.Quantity.isna().all()
    exact_numbers(out.xs(('12', '5'), level=('v', 'variety')).Price,
                  [100.125, 200.25, 100.125, 250.5])


def test_script_uses_selected_source_and_shared_writer(mapping, monkeypatch):
    codes, labels = sources()
    reads, writes = [], []
    def read(path, convert_categoricals=True):
        assert path == '../Data/s05_co_mli2018.dta'
        reads.append(convert_categoricals)
        return (labels if convert_categoricals else codes).copy()
    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.setattr(local_tools, 'to_parquet', lambda df, path: writes.append((df, path)))
    runpy.run_path('community_prices.py')
    assert reads == [True, False] and len(writes) == 1
    assert writes[0][1] == 'community_prices.parquet'
    pd.testing.assert_frame_equal(writes[0][0], mapping.community_prices_s05(codes, labels))


def test_absent_source_is_not_silently_replaced(mapping, monkeypatch):
    def absent(*args, **kwargs):
        raise FileNotFoundError('selected s05 source unavailable')
    monkeypatch.setattr(local_tools, 'get_dataframe', absent)
    monkeypatch.setattr(local_tools, 'to_parquet', lambda *_: pytest.fail('must not write a substitute'))
    with pytest.raises(FileNotFoundError, match='selected s05 source'):
        runpy.run_path('community_prices.py')


def test_missing_key_is_loud_instead_of_dropped(mapping):
    codes, labels = sources()
    codes.loc[0, 'vague'] = pd.NA
    with pytest.raises(ValueError, match='missing observation key'):
        mapping.community_prices_s05(codes, labels)


def test_public_transform_preserves_sizes_and_feature_omits_unknown_quantity(mapping):
    from lsms_library import Country
    from lsms_library.feature import _harmonize_country_frame, _canonical_index_levels

    codes, labels = sources()
    # Existing u-table spelling rule removes only this space; it must not
    # strip the fixed-mass size or collapse the small/large container labels.
    labels.loc[3, 's05q02'] = 'Seau (5 Kg )'
    native = mapping.community_prices_s05(codes, labels)
    country = Country('Mali', preload_panel_ids=False)
    public = country._finalize_result(
        native, country._materialization_entry('community_prices'), 'community_prices')
    assert len(public) == len(native) and public.index.is_unique
    assert public.index.names == ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    assert set(public.index.get_level_values('u')) == {'Boite - Petit', 'Seau (5 Kg)'}
    assert public.Quantity.isna().all()
    feature = _harmonize_country_frame(
        public, _canonical_index_levels('community_prices'), 'Mali', 'community_prices')
    assert list(feature.columns) == ['Price', 'Description']
    assert feature.index.equals(public.index)
    exact_numbers(feature.Price, public.Price.tolist())
