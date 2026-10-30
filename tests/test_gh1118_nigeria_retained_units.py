"""Normalize retained units after channel reduction; do not infer eligibility."""
import importlib.util
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from lsms_library.transformations import U_UNKNOWN

@pytest.fixture
def nigeria():
    path = Path(__file__).parents[1] / 'lsms_library/countries/Nigeria/_/nigeria.py'
    spec = importlib.util.spec_from_file_location('nigeria_1118', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

@pytest.mark.parametrize('unit', [float('nan'), pd.NA, None, '', '  ', 'nan', '<NA>'])
@pytest.mark.parametrize('quantity', [None, 0.0, 2.0])
def test_retained_missing_unit(nigeria, unit, quantity):
    part = pd.DataFrame({'i':['synthetic'], 'plot_id':['p'], 'crop':['Maize'],
                         'input':['Seed'], 'Quantity':pd.Series([quantity], dtype='Float64'),
                         'u':[unit], 'Purchased':pd.Series([False], dtype='boolean')})
    out = nigeria.assemble_plot_inputs('2018Q3', [part])
    assert len(out) == 1
    assert out.u.iloc[0] == U_UNKNOWN
    assert out.Quantity.isna().iloc[0] == pd.isna(quantity)
    assert out.Purchased.tolist() == [False]
    if quantity is not None:
        assert out.Quantity.iloc[0] == quantity

@pytest.mark.parametrize('unit', ['Kg', 'Stem', 'Metric Cup', '65', '  recorded  '])
def test_recorded_unit_without_quantity_is_preserved(nigeria, unit):
    part = pd.DataFrame({'i':['synthetic'], 'plot_id':['p'], 'crop':['Maize'],
                         'input':['Seed'], 'Quantity':[np.nan], 'u':[unit]})
    out = nigeria.assemble_plot_inputs('2018Q3', [part])
    assert len(out) == 1 and out.u.iloc[0] == unit
    assert pd.isna(out.Quantity.iloc[0])

def test_channel_reduction_precedes_fill(nigeria):
    # The existing mixed missing/recorded rule sums 2 + 3. A per-channel fill
    # would introduce a second unit and instead keep only the larger channel.
    q, u = nigeria._pick_channel([
        {'qty':pd.Series([2., np.nan]), 'u':pd.Series([pd.NA, pd.NA], dtype='string')},
        {'qty':pd.Series([3., np.nan]), 'u':pd.Series(['Kg', 'Stem'], dtype='string')},
    ])
    part = pd.DataFrame({'i':['a','b'], 'plot_id':['p','p'], 'crop':['Maize','Maize'],
                         'input':['Seed','Seed'], 'Quantity':q, 'u':u})
    out = nigeria.assemble_plot_inputs('2018Q3', [part])
    np.testing.assert_array_equal(out.Quantity.dropna().to_numpy(), [5.])
    assert out.u.tolist() == ['Kg', 'Stem']
    assert out.Quantity.isna().tolist() == [False, True]


def _finalize_invented(out):
    """Run the real finalizer with the existing injected sample-cache pattern.

    Only source-independent already-harmonized labels and sample rows are
    supplied; the all-null filter and declared/canonical dtypes are real.
    """
    from lsms_library import Country

    country = Country('Nigeria', preload_panel_ids=False)
    country._updated_ids_cache = {}
    country._panel_ids_attempted = True
    country._categorical_mapping_cache = {}
    country._sample_v_cache = pd.DataFrame(
        {'v': ['invented-cluster']},
        index=pd.MultiIndex.from_tuples(
            [('synthetic', '2018Q3')], names=['i', 't']))
    def no_source_reads(*args, **kwargs):
        raise AssertionError('Synthetic finalizer test attempted source/cache IO')

    from lsms_library import country as country_module, local_tools
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(country_module, 'get_dataframe', no_source_reads)
        patch.setattr(local_tools, 'get_dataframe', no_source_reads)
        return country._finalize_result(
            out, country._materialization_entry('plot_inputs'), 'plot_inputs')


def _part(**payload):
    return pd.DataFrame({'i': ['synthetic'], 'plot_id': ['p'],
                         'crop': ['Maize'], 'input': ['Seed'], **payload})


@pytest.mark.parametrize('unit', [None, pd.NA, float('nan')])
def test_hollow_native_record_stays_hollow_through_real_finalizer(nigeria, unit):
    out = nigeria.assemble_plot_inputs('2018Q3', [_part(u=unit)])
    assert len(out) == 1  # preserve native eligibility/dedup position
    assert out.isna().all(axis=None)
    served = _finalize_invented(out)
    assert served.empty


@pytest.mark.parametrize('observed', [
    {'Purchased': False}, {'Improved': False}, {'Quantity': 0.0},
    {'Quantity_purchased': 0.0},
])
def test_observed_false_or_zero_retains_missing_unit_record(nigeria, observed):
    out = nigeria.assemble_plot_inputs('2018Q3', [_part(u=None, **observed)])
    served = _finalize_invented(out)
    assert len(served) == 1
    assert served.u.tolist() == [U_UNKNOWN]
    column, value = next(iter(observed.items()))
    assert served[column].tolist() == [value]


@pytest.mark.parametrize('unit', ['', '  ', 'nan', '<NA>'])
def test_literal_missing_string_was_observed_before_normalization(nigeria, unit):
    # A non-NA source string alone already survives baseline dropna(how=all).
    out = nigeria.assemble_plot_inputs('2018Q3', [_part(u=unit)])
    served = _finalize_invented(out)
    assert len(served) == 1 and served.u.tolist() == [U_UNKNOWN]


def test_ignored_extra_payload_cannot_rescue_a_hollow_returned_record(nigeria):
    out = nigeria.assemble_plot_inputs(
        '2018Q3', [_part(u=None, ignored_source_annotation='recorded')])
    assert out.isna().all(axis=None)
    assert _finalize_invented(out).empty


@pytest.mark.parametrize('hollow_first', [True, False])
def test_keep_first_boundary_precedes_hollow_drop(nigeria, hollow_first):
    hollow = _part(u=None)
    observed = _part(u=None, Purchased=False)
    parts = [hollow, observed] if hollow_first else [observed, hollow]
    out = nigeria.assemble_plot_inputs('2018Q3', parts)
    assert len(out) == 1
    served = _finalize_invented(out)
    assert len(served) == (0 if hollow_first else 1)
    if not hollow_first:
        assert served.u.tolist() == [U_UNKNOWN]
        assert served.Purchased.tolist() == [False]
