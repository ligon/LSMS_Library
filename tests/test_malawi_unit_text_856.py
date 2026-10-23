"""GH #856: harvest/sale unit OTHER (SPECIFY) text that restates a coded unit.

The 2010-11 crop-harvest unit code list offers 3 = 90 KG BAG and 11 = BASKET
(DENGU).  The IHS4/IHS5 (2016-17, 2019-20) Module G / I / Q value labels drop
both codes and those files carry zero rows on either; respondents wrote them
under 13 = OTHER (SPECIFY) plus text.  `malawi._UNIT_TEXT_RECODE` recodes a
13 row whose own specify text is an exact, measured restatement back to 3 /
11, so the row is served the same `u` label a 2010-11 code-3 / code-11 row
is.  These tests drive synthetic frames through the real `_harvest_block` /
`_sale_block`; they need no data.
"""
from __future__ import annotations

import importlib.util
import sys

import pandas as pd
import pytest

from lsms_library.paths import countries_root

MALAWI_ = countries_root() / 'Malawi' / '_'


@pytest.fixture(scope='module')
def malawi_mod():
    """The Malawi country module, imported by path (as the wave scripts do)."""
    path = MALAWI_ / 'malawi.py'
    if not path.exists():                                   # pragma: no cover
        pytest.skip(f'{path} not present')
    sys.path.insert(0, str(MALAWI_))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh856', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f'Malawi module unavailable: {type(exc).__name__}: {exc}')
    finally:
        sys.path.remove(str(MALAWI_))
    return mod


#: (unit code, specify text, expected u) -- one row each.
G_ROWS = [
    (13, '90 KG BAG', '90 kg Bag'),               # mapped
    (13, '90KG', '90 kg Bag'),                    # mapped spelling
    (13, ' basket  ( dengu ) ', 'Basket (Dengu)'),  # normalised, then mapped
    (13, 'DENGU', 'Basket (Dengu)'),              # mapped
    (13, 'BASKET (LARGE)', 'Other (Specify)'),    # size-qualified: left
    (13, 'BASKET (UNSPECIFIED SIZE)', 'Other (Specify)'),  # left
    (13, '70 KG BAG', 'Other (Specify)'),         # no code for it: left
    (13, '90 KG BAD', 'Other (Specify)'),         # typo: left
    (13, '', 'Other (Specify)'),                  # no text
    (2, '90 KG BAG', '50 kg Bag'),                # text on a non-13 code: inert
    (3, '', '90 kg Bag'),                         # code 3 unchanged
    (11, '', 'Basket (Dengu)'),                   # code 11 unchanged
]


def _g_frame():
    n = len(G_ROWS)
    return pd.DataFrame({
        'hhid': [f'h{k}' for k in range(n)],
        'plotkey': ['p1'] * n,
        'crop_code': [1] * n,                     # maize
        'ag_g13a': [float(10 + k) for k in range(n)],
        'ag_g13b': [r[0] for r in G_ROWS],
        'ag_g13c': [1] * n,
        'ag_g13b_oth': [r[1] for r in G_ROWS],
    })


def _harvest(malawi_mod, df, **kw):
    return malawi_mod._harvest_block(
        df, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
        qty='ag_g13a', unit='ag_g13b', condition='ag_g13c',
        perennial=False, t='2019-20', **kw)


class TestHarvestBlock:

    def test_restated_units_are_served_the_coded_label(self, malawi_mod):
        out = _harvest(malawi_mod, _g_frame(), unit_text='ag_g13b_oth')
        assert list(out['u']) == [r[2] for r in G_ROWS]

    def test_quantity_never_moves(self, malawi_mod):
        df = _g_frame()
        out = _harvest(malawi_mod, df, unit_text='ag_g13b_oth')
        assert list(out['Quantity'].astype(float)) == list(df['ag_g13a'])

    def test_recovered_rows_are_counted(self, malawi_mod):
        out = _harvest(malawi_mod, _g_frame(), unit_text='ag_g13b_oth')
        assert out.attrs['unit_text_recovered'] == {3: 2, 11: 2}

    def test_without_unit_text_nothing_changes(self, malawi_mod):
        """`unit_text=None` is the pre-#856 behaviour, row for row."""
        out = _harvest(malawi_mod, _g_frame())
        expected = ['Other (Specify)' if r[0] == 13 else r[2] for r in G_ROWS]
        expected = ['50 kg Bag' if r[0] == 2 else e
                    for r, e in zip(G_ROWS, expected)]
        assert list(out['u']) == expected
        assert out.attrs['unit_text_recovered'] == {3: 0, 11: 0}

    def test_coded_rows_match_a_2010_11_coded_row(self, malawi_mod):
        """A recovered row's `u` is byte-identical to a code-3 / code-11 row's."""
        out = _harvest(malawi_mod, _g_frame(), unit_text='ag_g13b_oth')
        u = list(out['u'])
        assert u[0] == u[10] and u[2] == u[11]

    def test_a_misnamed_text_column_is_loud(self, malawi_mod):
        with pytest.raises(KeyError):
            _harvest(malawi_mod, _g_frame(), unit_text='ag_g13b_os')


class TestWhitelist:

    def test_every_entry_is_already_normalised(self, malawi_mod):
        """An entry not in normal form could never match: it would be dead."""
        for spellings in malawi_mod._UNIT_TEXT_RECODE.values():
            for s in spellings:
                got = malawi_mod._normalise_unit_text(pd.Series([s])).iloc[0]
                assert got == s, (s, got)

    def test_targets_are_disjoint_and_coded(self, malawi_mod):
        a, b = (malawi_mod._UNIT_TEXT_RECODE[3],
                malawi_mod._UNIT_TEXT_RECODE[11])
        assert not (a & b)
        cmap = malawi_mod._malawi_code_map('harmonize_crop_unit')
        assert cmap[3] == '90 kg Bag' and cmap[11] == 'Basket (Dengu)'

    @pytest.mark.parametrize('left', [
        '1 90 KG', '90 KG BAD', '90KG BAY', 'BASKET (UNSPECIFIED SIZE)',
        'BASKET (LARGE)', 'BASKET (SMALL)', 'BASKET (BAMBOO)', 'LARGE BASKET',
        'LARGE BASKETS', 'SMALL BASKET', 'MEDIAM BASKET', 'WEAVING BASKET',
        'MEDIUM WEAVING BASKET', 'LARGE DENGU', 'DENGU(LARGE)',
        'DENGU(GOGODA)', '70 KG BAG', '100 KG BAG', '50 KG BAG',
    ])
    def test_deliberately_left_strings_are_not_recovered(self, malawi_mod,
                                                         left):
        for spellings in malawi_mod._UNIT_TEXT_RECODE.values():
            assert left not in spellings


#: (sale unit code, specify text, expected u)
Q_ROWS = [
    (13, 'DENGU', 'Basket (Dengu)'),
    (13, 'BASKET (DENGUE)', 'Basket (Dengu)'),
    (13, '90KG BAG', '90 kg Bag'),
    (13, 'BASKET (UNSPECIFIED SIZE)', 'Other (Specify)'),
    (13, 'CLUSTER', 'Other (Specify)'),
    (9, 'DENGU', 'Piece'),                        # non-13 code: inert
]


def _q_frame():
    n = len(Q_ROWS)
    return pd.DataFrame({
        'hhid': [f'h{k}' for k in range(n)],       # one line per household
        'crop_code': [7] * n,                      # banana (perennial 1007)
        'ag_q01': [1] * n,
        'ag_q02a': [float(1 + k) for k in range(n)],
        'ag_q02b': [r[0] for r in Q_ROWS],
        'ag_q02b_oth': [r[1] for r in Q_ROWS],
        'ag_q03': [float(100 * (k + 1)) for k in range(n)],
    })


def _sale(malawi_mod, df, **kw):
    return malawi_mod._sale_block(
        df, hhid='hhid', cropcode='crop_code', sold_flag='ag_q01',
        qty_sold='ag_q02a', value_sold='ag_q03', unit_sold='ag_q02b',
        perennial=True, **kw)


class TestSaleBlock:

    def test_restated_sale_units_are_served_the_coded_label(self, malawi_mod):
        out = _sale(malawi_mod, _q_frame(),
                    unit_sold_text='ag_q02b_oth').set_index('i')
        got = [out.loc[f'h{k}', 'u'] for k in range(len(Q_ROWS))]
        assert got == [r[2] for r in Q_ROWS]
        assert out.attrs['unit_text_recovered'] == {3: 1, 11: 2}

    def test_sale_quantity_and_value_never_move(self, malawi_mod):
        df = _q_frame()
        out = _sale(malawi_mod, df, unit_sold_text='ag_q02b_oth').set_index('i')
        for k in range(len(Q_ROWS)):
            assert float(out.loc[f'h{k}', 'Quantity_sold']) == df['ag_q02a'][k]
            assert float(out.loc[f'h{k}', 'Value_sold']) == df['ag_q03'][k]

    def test_without_unit_sold_text_nothing_changes(self, malawi_mod):
        out = _sale(malawi_mod, _q_frame()).set_index('i')
        got = [out.loc[f'h{k}', 'u'] for k in range(len(Q_ROWS))]
        assert got == ['Other (Specify)'] * 5 + ['Piece']


class TestAttachIsSymmetric:
    """Both sides recovered: a 13 + "90 KG BAG" sale still meets its
    13 + "90 KG BAG" harvest row -- now on '90 kg Bag'."""

    def test_recovered_harvest_and_sale_still_attach(self, malawi_mod):
        g = pd.DataFrame({
            'hhid': ['h1'], 'plotkey': ['p1'], 'crop_code': [1],
            'ag_g13a': [10.0], 'ag_g13b': [13], 'ag_g13c': [1],
            'ag_g13b_oth': ['90 KG BAG'],
        })
        i_mod = pd.DataFrame({
            'hhid': ['h1'], 'crop_code': [1], 'ag_i01': [1],
            'ag_i02a': [4.0], 'ag_i02b': [13], 'ag_i02c': [1],
            'ag_i02b_oth': ['90KG BAG'], 'ag_i03': [50000.0],
        })
        harvest = [_harvest(malawi_mod, g, unit_text='ag_g13b_oth')]
        sale = [malawi_mod._sale_block(
            i_mod, hhid='hhid', cropcode='crop_code', sold_flag='ag_i01',
            qty_sold='ag_i02a', value_sold='ag_i03', unit_sold='ag_i02b',
            condition='ag_i02c', perennial=False,
            unit_sold_text='ag_i02b_oth')]
        out = malawi_mod.assemble_crop_production('2019-20', harvest, sale)
        assert list(out['u']) == ['90 kg Bag']
        assert float(out['Quantity'].iloc[0]) == 10.0
        assert float(out['Quantity_sold'].iloc[0]) == 4.0
        assert float(out['Value_sold'].iloc[0]) == 50000.0
        assert out.attrs['unit_text_recovered'] == {
            'harvest': {3: 1, 11: 0}, 'sale': {3: 1, 11: 0}}
