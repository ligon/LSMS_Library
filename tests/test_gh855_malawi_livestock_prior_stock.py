"""Malawi livestock: the 12-months-ago stock ag_r07 (GH #855).

Module R asks "How many [LIVESTOCK] did your household own exactly 12 months
ago?" (``ag_r07``) in all four IHS3+ waves.  It is served as
``HeadCount12MonthsAgo``: a second reported STOCK, carried as answered.  It
must not replace or fill ``HeadCount``, must not be differenced into a flow,
and does not widen the row universe -- measured 2026-11-03, no source row in
any wave or half has ``ag_r07 > 0`` without already meeting the keep-filter.
No microdata needed.
"""
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'


@pytest.fixture(scope='module')
def malawi():
    path = MALAWI / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh855', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _roster(**cols):
    base = {'hhid': ['1', '2', '3', '4'], 'ag_r0a': [301, 301, 301, 301],
            'ag_r01': [1, 1, 2, 2], 'ag_r02': [4.0, pd.NA, 0.0, 0.0],
            'ag_r10': [0.0] * 4, 'ag_r16': [0.0] * 4, 'ag_r04': [1.0] * 4,
            'ag_r07': [6.0, pd.NA, 3.0, pd.NA]}
    base.update(cols)
    return pd.DataFrame(base)


def test_prior_stock_is_carried_as_answered(malawi):
    block = malawi._livestock_block(_roster(), hhid='hhid', animalcode='ag_r0a',
                                    t='2010-11')
    out = malawi.assemble_livestock('2010-11', [block])
    by_i = out.reset_index().set_index('i')
    assert by_i.loc['1', 'HeadCount12MonthsAgo'] == 6.0
    # a blank prior stock stays blank and never fills a blank current count
    assert pd.isna(by_i.loc['2', 'HeadCount12MonthsAgo'])
    assert by_i.loc['1', 'HeadCount'] == 4.0
    assert out['HeadCount12MonthsAgo'].dtype == pd.Float64Dtype()


def test_prior_stock_does_not_widen_the_row_universe(malawi):
    # household 3 says it does not own the species now, holds 0, moved none,
    # but had 3 a year ago: the keep-filter is unchanged, so it is not served
    block = malawi._livestock_block(_roster(), hhid='hhid', animalcode='ag_r0a',
                                    t='2010-11')
    assert sorted(block['i']) == ['1', '2']


def test_missing_prior_stock_column_is_tolerated(malawi):
    block = malawi._livestock_block(_roster().drop(columns='ag_r07'), hhid='hhid',
                                    animalcode='ag_r0a', t='2010-11')
    out = malawi.assemble_livestock('2010-11', [block])
    assert out['HeadCount12MonthsAgo'].isna().all()


def test_two_lines_of_one_species_add_their_prior_stock(malawi):
    roster = _roster(hhid=['1', '1', '2', '2'], ag_r07=[2.0, 3.0, pd.NA, pd.NA],
                     ag_r01=[1, 1, 1, 1])
    block = malawi._livestock_block(roster, hhid='hhid', animalcode='ag_r0a',
                                    t='2010-11')
    out = malawi.assemble_livestock('2010-11', [block]).reset_index().set_index('i')
    assert out.loc['1', 'HeadCount12MonthsAgo'] == 5.0
    assert pd.isna(out.loc['2', 'HeadCount12MonthsAgo'])


def test_scheme_declares_the_column():
    text = (MALAWI / '_' / 'data_scheme.yml').read_text()
    assert 'HeadCount12MonthsAgo: float' in text
