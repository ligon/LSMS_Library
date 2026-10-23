"""GH #827: Malawi community_prices served 38,506 of 82,635 rows with no unit.

They were never blank at the API: ``local_tools.map_index`` renames a null
``u`` to the literal ``'unit'`` on every read, so the served table carried
38,506 rows of ``u == 'unit'``.  Measured causes (``Malawi/_/CONTENTS.org``,
"community_prices: what Module CK actually asks"):

* 28,616 rows on the 17 items whose NUMBER OF UNITS / UNIT CODES cells the
  Module CK form STRIKES OUT (IHS3 Community questionnaire p. 20) -- the item
  fixes the unit ("Coca-Cola, Fanta (bottle)", "Ganyu wage, 1 day").  They
  now carry that unit (``malawi._CK_ITEM_FIXED_UNIT``; precedent Nigeria
  W1/W2 ``_W1W2_PRICE_UNIT``).  ``NumberOfUnits`` stays NA: not asked.
* 9,875 rows on items whose unit IS asked, with a blank code or a code on no
  Module CK list (9,640 not-available rows the skip pattern jumps past, 61
  priced with a blank code, 158 priced with an off-list code, 16 other) --
  now the missing-unit sentinel ``Unknown`` (#842/#1041; Malawi's #1042 rule
  for codes "on no instrument's list").

Two tiers: static (synthetic frames through ``_price_block`` /
``assemble_community_prices``; no microdata) and data-gated (the delivered
table).
"""
from __future__ import annotations

import importlib.util
import sys

import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN

COUNTRY = 'Malawi'
MALAWI_ = countries_root() / COUNTRY / '_'

#: The items whose unit cells the IHS3 CK form strikes out (printed p. 20),
#: and the unit each item's own text fixes.
STRUCK = {3: '50 kg Bag', 4: '50 kg Bag', 30: 'Piece', 31: 'Bottle',
          32: 'Bottle', 33: 'Packet', 34: 'Piece', 35: 'Piece',
          42: '50 kg per Kilometre', 44: 'Piece', 45: 'Piece', 46: 'Piece',
          47: 'Piece', 48: 'Piece', 49: 'Piece', 50: 'Piece', 51: 'Day'}


@pytest.fixture(scope='module')
def malawi_mod():
    """The Malawi country module, imported by path (it is not a package)."""
    path = MALAWI_ / 'malawi.py'
    if not path.exists():                                   # pragma: no cover
        pytest.skip(f'{path} not present')
    sys.path.insert(0, str(MALAWI_))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh827', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(MALAWI_))
    return mod


def _ck(rows):
    """A synthetic Module CK file: (ea, item, avail, price, n, unit)."""
    return pd.DataFrame(rows, columns=['ea_id', 'item', 'avail', 'price',
                                       'n', 'unit'])


def _block(mod, rows, t='2010-11'):
    return mod._price_block(_ck(rows), ea_col='ea_id', item_col='item',
                            price_col='price', nunits_col='n',
                            unit_col='unit', avail_col='avail', t=t)


class TestFixedUnitTable:
    def test_the_struck_items_and_their_units(self, malawi_mod):
        assert malawi_mod._CK_ITEM_FIXED_UNIT == STRUCK

    def test_no_struck_item_is_a_unit_asked_food(self, malawi_mod):
        """The 19 unit-asked foods (codes 1-2, 5-21) are never overridden."""
        asked_foods = {1, 2} | set(range(5, 22))
        assert not asked_foods & set(malawi_mod._CK_ITEM_FIXED_UNIT)


class TestPriceBlock:
    def test_struck_item_carries_the_unit_its_item_fixes(self, malawi_mod):
        out = _block(malawi_mod, [
            ('101', 31, 1, 200.0, None, None),      # Coca-Cola (bottle)
            ('101', 33, 1, 1500.0, None, None),     # Cigarettes (packet)
            ('101', 3, 1, 300.0, None, None),       # Cost to grind 50 kg bag
            ('101', 42, 1, 25.0, None, None),       # transport per 50kg per km
            ('101', 44, 1, 900.0, None, None),      # Hand hoe
            ('101', 51, 1, 400.0, None, None),      # Ganyu wage, 1 day
        ])
        assert out['u'].tolist() == ['Bottle', 'Packet', '50 kg Bag',
                                     '50 kg per Kilometre', 'Piece', 'Day']

    def test_struck_item_number_of_units_is_not_invented(self, malawi_mod):
        out = _block(malawi_mod, [('101', 31, 1, 200.0, None, None),
                                  ('101', 48, 1, 45000.0, None, None)])
        assert out['NumberOfUnits'].isna().all()
        assert out['Price'].tolist() == [200.0, 45000.0]

    def test_struck_item_not_available_still_carries_the_item_unit(self, malawi_mod):
        out = _block(malawi_mod, [('101', 32, 2, None, None, None)])
        assert out['u'].tolist() == ['Bottle']
        assert out['Available'].tolist() == [False]

    @pytest.mark.parametrize('code', [None, 0, 22, 23, 25, 48, 50, 69, 200, 500])
    def test_blank_or_off_list_code_is_the_missing_unit_sentinel(self, malawi_mod, code):
        out = _block(malawi_mod, [('101', 27, 1, 30.0, 2.0, code)], t='2013-14')
        assert out['u'].tolist() == [U_UNKNOWN]
        assert out['NumberOfUnits'].tolist() == [2.0]       # reported, kept

    def test_unit_asked_item_not_available_is_unknown(self, malawi_mod):
        """The skip pattern ('NO...2 >> NEXT ITEM') jumps past price and
        unit, so no unit was recorded -- `Unknown`, never a null on the
        declared `u` level (which map_index serves as 'unit')."""
        out = _block(malawi_mod, [('101', 1, 2, None, None, None)])
        assert out['u'].tolist() == [U_UNKNOWN]

    def test_recorded_unit_codes_still_decode(self, malawi_mod):
        out = _block(malawi_mod, [('101', 1, 1, 9000.0, 1.0, 2),
                                  ('101', 31, 1, 200.0, None, None),
                                  ('101', 24, 1, 250.0, 1.0, 20)])
        assert out['u'].tolist() == ['50 kg Bag', 'Bottle', 'Satchet/Tube']

    def test_a_recorded_unit_on_a_struck_item_is_refused(self, malawi_mod):
        """A future wave that asks the unit for a struck item must not have
        its answer silently overwritten by the fixed unit."""
        with pytest.raises(ValueError, match='struck out'):
            _block(malawi_mod, [('101', 31, 1, 200.0, 1.0, 9)])


class TestAssemble:
    def test_no_null_and_no_placeholder_on_u(self, malawi_mod):
        piece = _block(malawi_mod, [
            ('101', 1, 1, 9000.0, 1.0, 2), ('101', 5, 2, None, None, None),
            ('101', 31, 1, 200.0, None, None), ('102', 27, 1, 30.0, None, 23)])
        out = malawi_mod.assemble_community_prices('2010-11', [piece])
        u = out.index.get_level_values('u')
        assert not pd.isna(u).any()
        assert 'unit' not in set(u)
        assert len(out) == 4
        assert sorted(out['Price'].dropna().tolist()) == [30.0, 200.0, 9000.0]


# --- data-gated: the delivered table ---------------------------------------

#: Rows per wave -- unchanged by #827 (the fix relabels, it adds and drops
#: nothing).  Measured on the cold build of 2026-10-23 (development
#: 9a0ed5f3c and this branch alike).
ROWS = {'2010-11': 39_166, '2013-14': 6_511, '2019-20': 36_958}
#: `u == 'Unknown'` per wave after #827 (was 0; those rows read 'unit').
UNKNOWN = {'2010-11': 4_555, '2013-14': 1_156, '2019-20': 4_164}


@pytest.fixture(scope='module')
def delivered():
    import lsms_library as ll
    try:
        return ll.Country(COUNTRY).community_prices()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} community_prices not buildable here: {e}')


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    def test_no_row_is_served_without_a_unit(self, delivered):
        u = pd.Series(delivered.index.get_level_values('u'))
        assert u.isna().sum() == 0
        assert (u == 'unit').sum() == 0     # the map_index placeholder

    @pytest.mark.parametrize('wave', sorted(ROWS))
    def test_rows_and_unknown_per_wave(self, delivered, wave):
        x = delivered.xs(wave, level='t', drop_level=False)
        assert len(x) == ROWS[wave]
        u = pd.Series(x.index.get_level_values('u'))
        assert int((u == U_UNKNOWN).sum()) == UNKNOWN[wave]

    def test_struck_items_carry_their_fixed_unit_and_no_count(self, delivered, malawi_mod):
        labels = malawi_mod._price_item_code_map()
        want = {labels[c]: u for c, u in STRUCK.items()}
        d = delivered.reset_index()
        d = d[d['j'].isin(want)]
        assert len(d) == 28_631
        assert (d['u'] == d['j'].map(want)).all()
        assert d['NumberOfUnits'].isna().all()
