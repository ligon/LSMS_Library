"""Mali #1121: exact labor-source labels through the real curation boundary."""
import importlib.util

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library.paths import countries_root
from lsms_library.transformations import (
    total_family_labor_days, total_hired_labor_days, total_labor_days,
)


@pytest.fixture
def mali(monkeypatch):
    directory = countries_root() / 'Mali' / '_'
    spec = importlib.util.spec_from_file_location('_mali_1121', directory / 'mali.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(directory)
    return module


@pytest.mark.parametrize('source', ['family', 'hired', 'other'])
def test_real_curation_binds_each_source_to_lowercase(mali, source):
    assert mali._labor_source_labels(pd.Series([source])).tolist() == [source]


def test_mapping_preserves_exact_code_eligibility(mali):
    result = mali._labor_source_labels(pd.Series([
        ' family ', 'hired', 'other', 'Family', 'FAMILY', 'unknown', None,
    ]))
    assert result.iloc[:3].tolist() == ['family', 'hired', 'other']
    assert result.iloc[3:].isna().all()


@pytest.fixture
def splits():
    # Repeated sources represent passage/demographic contributions. A wage-only
    # hired split is content; unknown sources and all-zero/sentinel splits are not.
    return pd.DataFrame({
        't': ['synthetic'] * 9, 'i': ['synthetic'] * 9,
        'plot_id': ['synthetic'] * 9,
        'source': ['family', 'family', 'hired', 'hired', 'other',
                   'unknown', 'family', 'hired', 'other'],
        'PersonDays': [2, 3, 7, pd.NA, 4, 50, 0, 999, pd.NA],
        'Wage': [pd.NA, pd.NA, 11, 10, pd.NA, 100, 0, 0, pd.NA],
    })


def test_real_finalizer_retains_split_sums_and_amount_masks(mali, splits):
    original = splits.copy(deep=True)
    result = mali.plot_labor_finalize(splits)
    assert_frame_equal(splits, original, check_exact=True)
    assert list(result.index.names) == ['t', 'i', 'plot_id', 'source']
    assert result.index.is_unique and len(result) == 3
    assert result.index.get_level_values('source').tolist() == ['family', 'hired', 'other']
    assert list(result.columns) == ['PersonDays', 'Wage']
    assert result.PersonDays.tolist() == [5, 7, 4]
    assert result.xs('hired', level='source').Wage.tolist() == [21]
    assert result.loc[result.index.get_level_values('source') != 'hired', 'Wage'].isna().all()


def test_existing_family_and_hired_selectors_accept_actual_labels(mali, splits):
    result = mali.plot_labor_finalize(splits)
    assert total_family_labor_days(result).Total_family_labor_days.tolist() == [5]
    assert total_hired_labor_days(result).Total_hired_labor_days.tolist() == [7]
    assert total_labor_days(result).Total_labor_days.tolist() == [16]


def test_wage_only_hired_observation_keeps_missing_person_days(mali):
    raw = pd.DataFrame({
        't': ['synthetic'], 'i': ['synthetic'], 'plot_id': ['synthetic'],
        'source': ['hired'], 'PersonDays': [pd.NA], 'Wage': [10],
    })
    result = mali.plot_labor_finalize(raw)
    assert len(result) == 1 and result.PersonDays.isna().all()
    assert result.Wage.tolist() == [10]
    assert result.index.get_level_values('source').tolist() == ['hired']


@pytest.mark.parametrize('days', [0, pd.NA, 999])
def test_no_content_does_not_gain_a_row(mali, days):
    raw = pd.DataFrame({
        't': ['synthetic'], 'i': ['synthetic'], 'plot_id': ['synthetic'],
        'source': ['family'], 'PersonDays': [days], 'Wage': [pd.NA],
    })
    result = mali.plot_labor_finalize(raw)
    assert result.empty
    assert list(result.index.names) == ['t', 'i', 'plot_id', 'source']
