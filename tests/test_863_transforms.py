"""The remaining GH #863 EPAR-parity transforms.

``gross_crop_revenue``, ``livestock_income``, ``hdds``, ``fcs``,
``fertilizer_rate``, ``crop_diversity`` -- ``rcsi`` / ``rcsi_phase`` landed
earlier and live in ``tests/test_rcsi.py``.

Pattern follows ``tests/test_rcsi.py``: hand-computed cases on minimal
frames for the pure-function behaviour, an error path per guard, and
data-gated smoke tests against the warm corpus that skip cleanly when a
country's cache is not present.

The guards being pinned are the ones GH #863 says must not be copied from
EPAR: a food-group mapping that is not a PARTITION must raise (EPAR's own
Tanzania mapping puts itemcode 704 in two groups and Stata silently kept
the first), an ``area``-weighted Shannon index must refuse rather than
approximate a column the schema does not carry, and no reduction may
zero-fill a household that reported nothing.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    FCS_WFP_WEIGHTS,
    HDDS_GROUPS,
    crop_diversity,
    fcs,
    fertilizer_rate,
    gross_crop_revenue,
    hdds,
    livestock_income,
)


# ---------------------------------------------------------------------------
# Frame builders
# ---------------------------------------------------------------------------

def _crop(rows, crop_level='j', extra=()):
    """(t, i, plot, <crop>, u, season) frame with Quantity_sold/Value_sold."""
    cols = ['t', 'i', 'plot', crop_level, 'u', 'season',
            'Quantity_sold', 'Value_sold']
    df = pd.DataFrame(rows, columns=cols)
    return df.set_index(['t', 'i', 'plot', crop_level, 'u', 'season'])


def _livestock(rows, cols=('HeadCount', 'HeadSold', 'ValuePerAnimal')):
    df = pd.DataFrame(rows, columns=['t', 'i', 'animal', *cols])
    return df.set_index(['t', 'i', 'animal'])


def _food(rows, cols=('Quantity', 'Expenditure')):
    df = pd.DataFrame(rows, columns=['t', 'i', 'j', 'u', 's', *cols])
    return df.set_index(['t', 'i', 'j', 'u', 's'])


# ===========================================================================
# gross_crop_revenue
# ===========================================================================

def test_gross_crop_revenue_sums_reported_sales_per_household():
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 100.0, 5000.0),
        ('2019-20', 'hh1', 'p2', 'Maize', 'kg', 'A', 50.0, 2500.0),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', 10.0, 900.0),
        ('2019-20', 'hh2', 'p1', 'Maize', 'kg', 'A', 20.0, 1000.0),
    ])
    out = gross_crop_revenue(cp)
    assert list(out.columns) == ['Gross_crop_revenue']
    assert out.loc[('2019-20', 'hh1'), 'Gross_crop_revenue'] == 8400.0
    assert out.loc[('2019-20', 'hh2'), 'Gross_crop_revenue'] == 1000.0


def test_gross_crop_revenue_by_crop_keeps_the_crop_level():
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 100.0, 5000.0),
        ('2019-20', 'hh1', 'p2', 'Maize', 'kg', 'A', 50.0, 2500.0),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', 10.0, 900.0),
    ])
    out = gross_crop_revenue(cp, by='j')
    assert list(out.index.names) == ['t', 'i', 'j']
    assert out.loc[('2019-20', 'hh1', 'Maize'), 'Gross_crop_revenue'] == 7500.0
    assert out.loc[('2019-20', 'hh1', 'Beans'), 'Gross_crop_revenue'] == 900.0


def test_gross_crop_revenue_by_j_resolves_malawis_crop_level():
    """Malawi names the crop level ``crop``, Uganda names it ``j``; ``by='j'``
    means "keep the crop level", not "keep a level literally called j"."""
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 100.0, 5000.0),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', 10.0, 900.0),
    ], crop_level='crop')
    out = gross_crop_revenue(cp, by='j')
    assert list(out.index.names) == ['t', 'i', 'crop']
    assert out.loc[('2019-20', 'hh1', 'Maize'), 'Gross_crop_revenue'] == 5000.0


def test_gross_crop_revenue_never_zero_fills_a_non_seller():
    """A household present in crop_production with no reported sale is
    ABSENT, not 0 -- pandas ``sum()`` of all-NaN would give 0."""
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 100.0, 5000.0),
        ('2019-20', 'hh2', 'p1', 'Maize', 'kg', 'A', np.nan, np.nan),
    ])
    out = gross_crop_revenue(cp)
    assert ('2019-20', 'hh1') in out.index
    assert ('2019-20', 'hh2') not in out.index


def test_gross_crop_revenue_missing_value_column_raises():
    cp = _crop([('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 1.0, 1.0)])
    with pytest.raises(ValueError, match='Value_sold'):
        gross_crop_revenue(cp.drop(columns=['Value_sold']))


def test_gross_crop_revenue_rejects_a_bad_by():
    cp = _crop([('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 1.0, 1.0)])
    with pytest.raises(ValueError, match='by='):
        gross_crop_revenue(cp, by='plot')


# ===========================================================================
# livestock_income
# ===========================================================================

def test_livestock_income_reported_per_head_basis():
    ls = _livestock([
        ('2019-20', 'hh1', 'Cattle', 3.0, 2.0, 150000.0),
        ('2019-20', 'hh1', 'Goats', 8.0, 4.0, 20000.0),
    ])
    out = livestock_income(ls)
    assert out.loc[('2019-20', 'hh1', 'Cattle'),
                   'Livestock_sales_value'] == 300000.0
    assert out.loc[('2019-20', 'hh1', 'Goats'),
                   'Livestock_sales_value'] == 80000.0
    assert set(out['ValuationSource']) == {'reported_per_head'}
    # Additive over `animal`: the household total is a groupby away.
    total = out.groupby(['t', 'i'])['Livestock_sales_value'].sum()
    assert total.loc[('2019-20', 'hh1')] == 380000.0


def test_livestock_income_keeps_a_genuine_zero_and_drops_a_missing_price():
    ls = _livestock([
        ('2019-20', 'hh1', 'Cattle', 3.0, 0.0, 150000.0),   # sold nothing
        ('2019-20', 'hh1', 'Goats', 8.0, 4.0, np.nan),      # price unknown
    ])
    out = livestock_income(ls)
    assert out.loc[('2019-20', 'hh1', 'Cattle'),
                   'Livestock_sales_value'] == 0.0
    assert ('2019-20', 'hh1', 'Goats') not in out.index


def test_livestock_income_sales_value_basis_is_reported_not_constructed():
    ls = _livestock(
        [('2019-20', 'hh1', 'Cattle', 3.0, 2.0, 275000.0)],
        cols=('HeadCount', 'HeadSold', 'SalesValue'))
    out = livestock_income(ls, price='sales_value')
    assert out.loc[('2019-20', 'hh1', 'Cattle'),
                   'Livestock_sales_value'] == 275000.0
    assert set(out['ValuationSource']) == {'reported_sales_value'}


def test_livestock_income_sales_value_absent_raises():
    ls = _livestock([('2019-20', 'hh1', 'Cattle', 3.0, 2.0, 150000.0)])
    with pytest.raises(ValueError, match='SalesValue'):
        livestock_income(ls, price='sales_value')


def test_livestock_income_supplied_price_series_by_t_and_animal():
    ls = _livestock([
        ('2019-20', 'hh1', 'Cattle', 3.0, 2.0, np.nan),
        ('2019-20', 'hh2', 'Cattle', 1.0, 1.0, np.nan),
    ])
    price = pd.Series(
        [100.0],
        index=pd.MultiIndex.from_tuples([('2019-20', 'Cattle')],
                                        names=['t', 'animal']))
    out = livestock_income(ls, price=price)
    assert out.loc[('2019-20', 'hh1', 'Cattle'),
                   'Livestock_sales_value'] == 200.0
    assert out.loc[('2019-20', 'hh2', 'Cattle'),
                   'Livestock_sales_value'] == 100.0
    assert set(out['ValuationSource']) == {'supplied_price'}


def test_livestock_income_supplied_price_by_animal_alone():
    ls = _livestock([('2019-20', 'hh1', 'Goats', 8.0, 4.0, np.nan)])
    out = livestock_income(ls, price=pd.Series({'Goats': 25.0}))
    assert out.loc[('2019-20', 'hh1', 'Goats'),
                   'Livestock_sales_value'] == 100.0


def test_livestock_income_unpriced_sale_raises_rather_than_under_reporting():
    ls = _livestock([
        ('2019-20', 'hh1', 'Cattle', 3.0, 2.0, np.nan),
        ('2019-20', 'hh1', 'Camels', 1.0, 1.0, np.nan),
    ])
    with pytest.raises(ValueError, match='no supplied price'):
        livestock_income(ls, price=pd.Series({'Cattle': 100.0}))


def test_livestock_income_missing_head_sold_raises():
    ls = _livestock([('2019-20', 'hh1', 'Cattle', 3.0, 2.0, 1.0)])
    with pytest.raises(ValueError, match='HeadSold'):
        livestock_income(ls.drop(columns=['HeadSold']))


def test_livestock_income_bad_price_basis_raises():
    ls = _livestock([('2019-20', 'hh1', 'Cattle', 3.0, 2.0, 1.0)])
    with pytest.raises(ValueError, match='price='):
        livestock_income(ls, price='median')


# ===========================================================================
# hdds
# ===========================================================================

def _full_hdds_groups(**assigned):
    """A complete 12-group mapping, empty except where named."""
    groups = {g: [] for g in HDDS_GROUPS}
    for key, items in assigned.items():
        groups[key.replace('_', ' ')] = items
    return groups


def test_hdds_counts_groups_with_any_acquisition():
    fa = _food([
        ('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 5.0, 500.0),
        ('2019-20', 'hh1', 'Rice', 'kg', 'produced', 2.0, np.nan),
        ('2019-20', 'hh1', 'Beans', 'kg', 'purchased', 1.0, 200.0),
        ('2019-20', 'hh2', 'Maize', 'kg', 'purchased', 5.0, 500.0),
    ])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = ['Maize', 'Rice']
    groups['Legumes, nuts and seeds'] = ['Beans']
    out = hdds(fa, groups=groups)
    assert list(out.columns) == ['HDDS']
    # hh1 acquired from two of the twelve groups; hh2 from one.
    assert out.loc[('2019-20', 'hh1'), 'HDDS'] == 2
    assert out.loc[('2019-20', 'hh2'), 'HDDS'] == 1


def test_hdds_zero_quantity_and_zero_expenditure_is_not_consumption():
    fa = _food([
        ('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 5.0, 500.0),
        ('2019-20', 'hh1', 'Beans', 'kg', 'purchased', 0.0, 0.0),
    ])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = ['Maize']
    groups['Legumes, nuts and seeds'] = ['Beans']
    assert hdds(fa, groups=groups).loc[('2019-20', 'hh1'), 'HDDS'] == 1


def test_hdds_expenditure_alone_counts():
    """A purchase whose quantity the survey did not record is still food."""
    fa = _food([('2019-20', 'hh1', 'Maize', 'kg', 'purchased',
                 np.nan, 500.0)])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = ['Maize']
    assert hdds(fa, groups=groups).loc[('2019-20', 'hh1'), 'HDDS'] == 1


def test_hdds_non_partition_mapping_raises_the_epar_704_case():
    """EPAR's Tanzania recode assigns itemcode 704 to FRUITS and to SWEETS
    (W5.do:2538-2560); Stata silently kept the first.  We refuse."""
    fa = _food([('2019-20', 'hh1', 'Sugarcane', 'kg', 'purchased', 1.0, 1.0)])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Fruits'] = ['Sugarcane']
    groups['Sweets'] = ['Sugarcane']
    with pytest.raises(ValueError, match='not a partition'):
        hdds(fa, groups=groups)


def test_hdds_unmapped_item_raises():
    fa = _food([
        ('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0),
        ('2019-20', 'hh1', 'Cassava', 'kg', 'purchased', 1.0, 1.0),
    ])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = ['Maize']
    with pytest.raises(ValueError, match='not mapped'):
        hdds(fa, groups=groups)


def test_hdds_missing_group_key_raises_and_empty_list_is_the_way_to_say_none():
    fa = _food([('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0)])
    partial = {'Cereals': ['Maize']}
    with pytest.raises(ValueError, match='Not named'):
        hdds(fa, groups=partial)
    full = {g: [] for g in HDDS_GROUPS}
    full['Cereals'] = ['Maize']
    assert hdds(fa, groups=full).loc[('2019-20', 'hh1'), 'HDDS'] == 1


def test_hdds_unknown_group_key_raises():
    fa = _food([('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0)])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = ['Maize']
    groups['Ultra-processed'] = []
    with pytest.raises(ValueError, match='Unrecognised'):
        hdds(fa, groups=groups)


def test_hdds_string_instead_of_list_raises():
    fa = _food([('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0)])
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = 'Maize'
    with pytest.raises(TypeError, match='sequence of j labels'):
        hdds(fa, groups=groups)


# ===========================================================================
# fcs
# ===========================================================================

def _fcs_groups(**assigned):
    groups = {g: [] for g in FCS_WFP_WEIGHTS}
    groups.update(assigned)
    return groups


def test_fcs_hand_computed_with_the_standard_weights():
    fa = _food([
        ('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 5.0, 500.0, 7),
        ('2019-20', 'hh1', 'Beans', 'kg', 'purchased', 1.0, 200.0, 3),
        ('2019-20', 'hh1', 'Oil', 'l', 'purchased', 1.0, 100.0, 6),
    ], cols=('Quantity', 'Expenditure', 'DaysConsumed'))
    groups = _fcs_groups()
    groups['Main staples'] = ['Maize']
    groups['Pulses'] = ['Beans']
    groups['Oil'] = ['Oil']
    out = fcs(fa, groups=groups, days='DaysConsumed')
    # 2*7 + 3*3 + 0.5*6 = 14 + 9 + 3 = 26
    assert out.loc[('2019-20', 'hh1'), 'FCS'] == pytest.approx(26.0)


def test_fcs_caps_group_days_at_seven():
    """Rice on 5 days and maize on 4 is 7 staple-days, not 9."""
    fa = _food([
        ('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0, 4),
        ('2019-20', 'hh1', 'Rice', 'kg', 'purchased', 1.0, 1.0, 5),
    ], cols=('Quantity', 'Expenditure', 'DaysConsumed'))
    groups = _fcs_groups()
    groups['Main staples'] = ['Maize', 'Rice']
    out = fcs(fa, groups=groups, days='DaysConsumed')
    assert out.loc[('2019-20', 'hh1'), 'FCS'] == pytest.approx(14.0)


def test_fcs_absent_group_scores_zero_days_not_a_dropped_household():
    fa = _food([('2019-20', 'hh1', 'Maize', 'kg', 'purchased',
                 1.0, 1.0, 7)], cols=('Quantity', 'Expenditure', 'DaysConsumed'))
    groups = _fcs_groups()
    groups['Main staples'] = ['Maize']
    out = fcs(fa, groups=groups, days='DaysConsumed')
    assert out.loc[('2019-20', 'hh1'), 'FCS'] == pytest.approx(14.0)


def test_fcs_custom_weights_change_the_group_vocabulary_symmetrically():
    fa = _food([
        ('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0, 7),
        ('2019-20', 'hh1', 'Salt', 'kg', 'purchased', 1.0, 1.0, 7),
    ], cols=('Quantity', 'Expenditure', 'DaysConsumed'))
    weights = {**FCS_WFP_WEIGHTS, 'Condiments': 0.0}
    groups = {g: [] for g in weights}
    groups['Main staples'] = ['Maize']
    groups['Condiments'] = ['Salt']
    out = fcs(fa, groups=groups, days='DaysConsumed', weights=weights)
    assert out.loc[('2019-20', 'hh1'), 'FCS'] == pytest.approx(14.0)
    # Without declaring the ninth group, Salt is an unmapped item -> raise.
    short = {g: [] for g in FCS_WFP_WEIGHTS}
    short['Main staples'] = ['Maize']
    with pytest.raises(ValueError, match='not mapped'):
        fcs(fa, groups=short, days='DaysConsumed')


def test_fcs_missing_days_column_raises_and_says_why():
    fa = _food([('2019-20', 'hh1', 'Maize', 'kg', 'purchased', 1.0, 1.0)])
    groups = _fcs_groups()
    groups['Main staples'] = ['Maize']
    with pytest.raises(ValueError, match='CONSUMPTION-FREQUENCY'):
        fcs(fa, groups=groups, days='DaysConsumed')


# ===========================================================================
# fertilizer_rate
# ===========================================================================

def _plot_inputs(rows):
    df = pd.DataFrame(
        rows, columns=['t', 'i', 'plot', 'input', 'j', 'u', 'Quantity'])
    return df.set_index(['t', 'i', 'plot', 'input', 'j', 'u'])


def _plot_features(rows, key='plot_id'):
    df = pd.DataFrame(rows, columns=['t', 'i', key, 'Area'])
    return df.set_index(['t', 'i', key])


def test_fertilizer_rate_nitrogen_per_hectare():
    pi = _plot_inputs([
        ('2019-20', 'hh1', 'p1', 'Urea', 'Maize', 'kg', 100.0),
    ])
    pf = _plot_features([('2019-20', 'hh1', 'p1', 2.0)])
    out = fertilizer_rate(pi, pf)
    # 100 kg urea x 0.46 N = 46 kg N over 2 ha = 23 kg N/ha
    assert list(out.columns) == ['Nitrogen_kg_per_ha']
    assert out.loc[('2019-20', 'hh1', 'p1'),
                   'Nitrogen_kg_per_ha'] == pytest.approx(23.0)


def test_fertilizer_rate_product_basis_is_the_raw_kilograms():
    pi = _plot_inputs([
        ('2019-20', 'hh1', 'p1', 'Urea', 'Maize', 'kg', 100.0),
    ])
    pf = _plot_features([('2019-20', 'hh1', 'p1', 2.0)])
    out = fertilizer_rate(pi, pf, nutrient='product')
    assert list(out.columns) == ['Fertilizer_kg_per_ha']
    assert out.loc[('2019-20', 'hh1', 'p1'),
                   'Fertilizer_kg_per_ha'] == pytest.approx(50.0)


def test_fertilizer_rate_sums_inputs_and_areas_to_the_same_land_grain():
    pi = _plot_inputs([
        ('2019-20', 'hh1', 'p1', 'Urea', 'Maize', 'kg', 100.0),
        ('2019-20', 'hh1', 'p1', 'DAP', 'Maize', 'kg', 50.0),
    ])
    pf = _plot_features([('2019-20', 'hh1', 'p1', 2.0)])
    out = fertilizer_rate(pi, pf)
    # (100*0.46 + 50*0.18) / 2 = (46 + 9) / 2 = 27.5
    assert out.loc[('2019-20', 'hh1', 'p1'),
                   'Nitrogen_kg_per_ha'] == pytest.approx(27.5)


def test_fertilizer_rate_zero_area_plot_is_dropped_not_infinite():
    pi = _plot_inputs([
        ('2019-20', 'hh1', 'p1', 'Urea', 'Maize', 'kg', 100.0),
        ('2019-20', 'hh1', 'p2', 'Urea', 'Maize', 'kg', 100.0),
    ])
    pf = _plot_features([('2019-20', 'hh1', 'p1', 2.0),
                         ('2019-20', 'hh1', 'p2', 0.0)])
    out = fertilizer_rate(pi, pf)
    assert ('2019-20', 'hh1', 'p2') not in out.index
    assert np.isfinite(out['Nitrogen_kg_per_ha']).all()


def test_fertilizer_rate_bad_nutrient_and_bad_on_raise():
    pi = _plot_inputs([('2019-20', 'hh1', 'p1', 'Urea', 'Maize', 'kg', 1.0)])
    pf = _plot_features([('2019-20', 'hh1', 'p1', 1.0)])
    with pytest.raises(ValueError, match='nutrient='):
        fertilizer_rate(pi, pf, nutrient='P')
    with pytest.raises(ValueError, match='on='):
        fertilizer_rate(pi, pf, on='household')


def test_fertilizer_rate_missing_area_column_raises():
    pi = _plot_inputs([('2019-20', 'hh1', 'p1', 'Urea', 'Maize', 'kg', 1.0)])
    pf = _plot_features([('2019-20', 'hh1', 'p1', 1.0)])
    with pytest.raises(ValueError, match='Area'):
        fertilizer_rate(pi, pf.drop(columns=['Area']))


# ===========================================================================
# crop_diversity
# ===========================================================================

def test_crop_diversity_count_basis_is_ln_n_for_equal_occurrences():
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p2', 'Beans', 'kg', 'A', np.nan, np.nan),
    ])
    out = crop_diversity(cp)
    assert list(out.columns) == ['Crop_diversity']
    assert out.loc[('2019-20', 'hh1'),
                   'Crop_diversity'] == pytest.approx(math.log(2))


def test_crop_diversity_single_crop_scores_exactly_zero():
    cp = _crop([('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 1.0, 1.0)])
    out = crop_diversity(cp)
    value = out.loc[('2019-20', 'hh1'), 'Crop_diversity']
    assert value == 0.0
    assert not math.copysign(1.0, value) < 0    # not -0.0


def test_crop_diversity_count_dedups_the_u_and_condition_rows():
    """One maize crop reported in two units on one plot is ONE occurrence --
    counting raw rows would score it as twice the maize."""
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p1', 'Maize', 'sack', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', np.nan, np.nan),
    ])
    out = crop_diversity(cp)
    assert out.loc[('2019-20', 'hh1'),
                   'Crop_diversity'] == pytest.approx(math.log(2))


def test_crop_diversity_count_separates_plots_and_seasons():
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p2', 'Maize', 'kg', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'B', np.nan, np.nan),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', np.nan, np.nan),
    ])
    out = crop_diversity(cp)
    # shares 3/4 maize, 1/4 beans
    expected = -(0.75 * math.log(0.75) + 0.25 * math.log(0.25))
    assert out.loc[('2019-20', 'hh1'),
                   'Crop_diversity'] == pytest.approx(expected)


def test_crop_diversity_value_basis_uses_reported_sales():
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 100.0, 750.0),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', 10.0, 250.0),
    ])
    out = crop_diversity(cp, weight='value')
    expected = -(0.75 * math.log(0.75) + 0.25 * math.log(0.25))
    assert out.loc[('2019-20', 'hh1'),
                   'Crop_diversity'] == pytest.approx(expected)


def test_crop_diversity_value_basis_covers_sold_crops_only():
    """A household that sold one of its three crops scores 0 on the value
    basis -- documented, and the reason 'count' is the default."""
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 100.0, 750.0),
        ('2019-20', 'hh1', 'p1', 'Beans', 'kg', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p1', 'Cassava', 'kg', 'A', np.nan, np.nan),
    ])
    assert crop_diversity(cp, weight='value').loc[
        ('2019-20', 'hh1'), 'Crop_diversity'] == 0.0
    assert crop_diversity(cp).loc[
        ('2019-20', 'hh1'), 'Crop_diversity'] == pytest.approx(math.log(3))


def test_crop_diversity_area_weight_raises_naming_the_missing_column():
    cp = _crop([('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 1.0, 1.0)])
    with pytest.raises(NotImplementedError) as exc:
        crop_diversity(cp, weight='area')
    message = str(exc.value)
    assert 'AreaShare' in message and 'planted' in message


def test_crop_diversity_bad_weight_raises():
    cp = _crop([('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', 1.0, 1.0)])
    with pytest.raises(ValueError, match='weight='):
        crop_diversity(cp, weight='harvest')


def test_crop_diversity_resolves_malawis_crop_level():
    cp = _crop([
        ('2019-20', 'hh1', 'p1', 'Maize', 'kg', 'A', np.nan, np.nan),
        ('2019-20', 'hh1', 'p2', 'Beans', 'kg', 'A', np.nan, np.nan),
    ], crop_level='crop')
    out = crop_diversity(cp)
    assert out.loc[('2019-20', 'hh1'),
                   'Crop_diversity'] == pytest.approx(math.log(2))


# ===========================================================================
# Data-gated smoke tests against the warm corpus
# ===========================================================================

def _warm(country, table):
    """Load a country table THAT IS ALREADY WARM, or skip.

    TWO gates, because existence is not freshness.  The L2-country parquet
    must exist (a missing one is a clean skip, like missing credentials),
    AND the Country is constructed with ``assume_cache_fresh=True`` so a
    present-but-stale hash serves from the parquet instead of rebuilding.
    Existence alone is NOT enough: a stale-hash parquet rebuilds and WRITES
    to a cache other agents share, and takes minutes.  ``assume_cache_fresh``
    still runs ``_finalize_result`` (kinship, spellings, the v-join), which
    is exactly the smoke-test contract -- see CLAUDE.md §"Cache Behavior".
    """
    try:
        import lsms_library as ll
        from lsms_library.local_tools import data_root
    except Exception as exc:                      # pragma: no cover
        pytest.skip(f"lsms_library unavailable: {exc}")
    parquet = data_root() / country / 'var' / f'{table}.parquet'
    if not parquet.exists():
        pytest.skip(f"{country} {table} is not warm here ({parquet})")
    try:
        df = getattr(ll.Country(country, assume_cache_fresh=True), table)()
    except Exception as exc:
        pytest.skip(f"{country} {table} unavailable here: {exc}")
    if df is None or df.empty:
        pytest.skip(f"{country} {table} empty -- no warm cache here")
    return df


def test_malawi_warm_livestock_income():
    ls = _warm('Malawi', 'livestock')
    if 'ValuePerAnimal' not in ls.columns or 'HeadSold' not in ls.columns:
        pytest.skip('Malawi livestock lacks the sale columns here')
    out = livestock_income(ls)
    assert list(out.columns) == ['Livestock_sales_value', 'ValuationSource']
    assert set(out['ValuationSource']) == {'reported_per_head'}
    assert (out['Livestock_sales_value'] >= 0).all()
    assert len(out) <= len(ls)


def test_malawi_warm_food_acquired_hdds_partition_guard_bites():
    """The real Malawi j vocabulary is large; a mapping that does not cover
    it must raise rather than silently deflate every household's score."""
    fa = _warm('Malawi', 'food_acquired')
    groups = {g: [] for g in HDDS_GROUPS}
    groups['Cereals'] = ['Maize grain (not as ufa)']
    with pytest.raises(ValueError, match='not mapped'):
        hdds(fa, groups=groups)


def test_malawi_warm_food_acquired_hdds_scores_in_range():
    fa = _warm('Malawi', 'food_acquired')
    items = sorted({str(x) for x in fa.index.get_level_values('j')})
    # Not a curation -- a mechanical round-robin, purely to exercise the
    # reduction at corpus scale.  The real mapping is per-country work.
    groups = {g: [] for g in HDDS_GROUPS}
    for n, item in enumerate(items):
        groups[HDDS_GROUPS[n % len(HDDS_GROUPS)]].append(item)
    out = hdds(fa, groups=groups)
    assert out['HDDS'].between(0, len(HDDS_GROUPS)).all()
    assert list(out.index.names) == ['t', 'i']
    assert out['HDDS'].max() > 1


def test_malawi_warm_fertilizer_rate():
    pi = _warm('Malawi', 'plot_inputs')
    pf = _warm('Malawi', 'plot_features')
    out = fertilizer_rate(pi, pf)
    assert list(out.columns) == ['Nitrogen_kg_per_ha']
    assert len(out) > 0
    assert np.isfinite(out['Nitrogen_kg_per_ha']).all()
    assert (out['Nitrogen_kg_per_ha'] >= 0).all()


def test_uganda_warm_crop_production_revenue_and_diversity():
    cp = _warm('Uganda', 'crop_production')
    rev = gross_crop_revenue(cp)
    assert list(rev.columns) == ['Gross_crop_revenue']
    assert (rev['Gross_crop_revenue'] > 0).any()
    # by='j' must sum back to the household total.
    by_crop = gross_crop_revenue(cp, by='j')
    regrouped = by_crop.groupby(['t', 'i'])['Gross_crop_revenue'].sum()
    assert regrouped.reindex(rev.index).round(6).equals(
        rev['Gross_crop_revenue'].round(6))

    div = crop_diversity(cp)
    assert (div['Crop_diversity'] >= 0).all()
    assert div['Crop_diversity'].max() < math.log(500)


def test_malawi_warm_crop_revenue_by_crop_sums_back_to_the_household():
    cp = _warm('Malawi', 'crop_production')
    rev = gross_crop_revenue(cp)
    assert list(rev.columns) == ['Gross_crop_revenue']
    assert (rev['Gross_crop_revenue'] > 0).any()
    # Malawi names its crop level ``crop``; by='j' must still resolve it and
    # the per-crop frame must sum back to the household total.
    by_crop = gross_crop_revenue(cp, by='j')
    assert list(by_crop.index.names) == ['t', 'i', 'crop']
    regrouped = by_crop.groupby(['t', 'i'])['Gross_crop_revenue'].sum()
    assert regrouped.round(4).equals(
        rev['Gross_crop_revenue'].reindex(regrouped.index).round(4))


def test_malawi_warm_crop_diversity():
    cp = _warm('Malawi', 'crop_production')
    div = crop_diversity(cp)
    assert (div['Crop_diversity'] >= 0).all()
    val = crop_diversity(cp, weight='value')
    assert (val['Crop_diversity'] >= 0).all()
    # The value basis sees only sellers, so it covers fewer households.
    assert len(val) <= len(div)
