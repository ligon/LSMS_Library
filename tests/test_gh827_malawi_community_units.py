"""GH #827: Malawi community_prices served 38,506 of 82,635 rows with no unit.

They were never blank at the API: ``local_tools.map_index`` renames a null
``u`` to the literal ``'unit'`` on every read, so the served table carried
38,506 rows of ``u == 'unit'``.  Measured causes (``Malawi/_/CONTENTS.org``,
"community_prices: what Module CK actually asks"):

* 28,631 rows on the 17 items whose NUMBER OF UNITS / UNIT CODES cells the
  Module CK form STRIKES OUT (IHS3 Community questionnaire p. 20, IHPS
  Market questionnaire p. 12, IHS5 Community questionnaire p. 20) -- the item
  fixes the unit ("Coca-Cola, Fanta (bottle)", "Ganyu wage, 1 day").  They
  now carry that unit (``malawi._CK_ITEM_FIXED_UNIT``; precedent Nigeria
  W1/W2 ``_W1W2_PRICE_UNIT``).  The form strikes the count cell too, so no
  row records a count; the 21,641 PRICED rows of these items are served
  ``NumberOfUnits = 1`` as the registered derivation
  ``Malawi::community_prices::struck-item-one-unit`` (ruling @ligon
  2026-10-23; precedent Niger ``one-item-per-price``).  Unpriced rows keep
  NA.
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

#: The items whose unit cells the CK form strikes out (IHS3 Community p. 20,
#: IHPS Market p. 12, IHS5 Community p. 20),
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

    def test_price_block_serves_the_recorded_count_only(self, malawi_mod):
        """`_price_block` reads; it does not derive.  The 1 on a struck item
        is added after the collapse by `derive_ck_struck_one_unit`
        (TestStruckItemOneUnit), never here."""
        out = _block(malawi_mod, [('101', 31, 1, 200.0, None, None),
                                  ('101', 48, 1, 45000.0, None, None)])
        assert out['NumberOfUnits'].isna().all()
        assert 'Derivation' not in out.columns
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
        """Code 21 is the CK list's own OTHER (SPECIFY) -- on the list, so
        it decodes to its label; it is NOT an off-list code and must not
        fall to `Unknown` (#827 leaves Other (Specify) untouched)."""
        out = _block(malawi_mod, [('101', 1, 1, 9000.0, 1.0, 2),
                                  ('101', 31, 1, 200.0, None, None),
                                  ('101', 24, 1, 250.0, 1.0, 20),
                                  ('101', 29, 1, 600.0, 2.0, 21)])
        assert out['u'].tolist() == ['50 kg Bag', 'Bottle', 'Satchet/Tube',
                                     'Other (Specify)']

    @pytest.mark.parametrize('code', [9, 23])
    def test_a_recorded_unit_on_a_struck_item_is_refused(self, malawi_mod, code):
        """A future wave that asks the unit for a struck item must not have
        its answer silently overwritten by the fixed unit.  The guard is on
        the RAW code: an off-list code (23) decodes to no label but is still
        a recorded answer, so a guard on the decoded label would miss it."""
        with pytest.raises(ValueError, match='struck out'):
            _block(malawi_mod, [('101', 31, 1, 200.0, 1.0, code)])


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


#: The registry key of the ruling (@ligon, 2026-10-23).
KEY = 'Malawi::community_prices::struck-item-one-unit'


def _assembled(mod, rows, t='2010-11'):
    out = mod.assemble_community_prices(t, [_block(mod, rows, t=t)])
    return out.reset_index().set_index(['v', 'j'])


class TestStruckItemOneUnit:
    """Ruling (@ligon, 2026-10-23): on the struck items, NumberOfUnits = 1 as
    a registered derivation -- only where a price is recorded and no count."""

    ROWS = [
        ('101', 31, 1, 200.0, None, None),     # Coca-Cola, priced -> 1
        ('101', 51, 1, 400.0, None, None),     # Ganyu wage, priced -> 1
        ('101', 50, 1, 0.0, None, None),       # mosquito net at 0 MK -> 1
        ('101', 32, 2, None, None, None),      # Beer, not available -> NA
        ('101', 44, None, None, None, None),   # Hand hoe, no answer -> NA
        ('101', 1, 1, 9000.0, None, 2),        # unit-asked, no count -> NA
        ('101', 27, 1, 30.0, 2.0, 23),         # unit-asked, recorded 2 -> 2
        ('101', 5, 2, None, None, None),       # unit-asked, not available
    ]

    def test_priced_struck_rows_are_served_one_and_carry_the_key(self, malawi_mod):
        out = _assembled(malawi_mod, self.ROWS)
        labels = malawi_mod._price_item_code_map()
        for code in (31, 51, 50):
            row = out.loc[('101', labels[code])]
            assert pd.notna(row['NumberOfUnits']) and row['NumberOfUnits'] == 1.0, code
            assert pd.notna(row['Derivation']) and row['Derivation'] == KEY, code
        assert int((out['Derivation'] == KEY).sum()) == 3

    def test_unpriced_struck_rows_get_no_count_and_no_key(self, malawi_mod):
        out = _assembled(malawi_mod, self.ROWS)
        labels = malawi_mod._price_item_code_map()
        for code in (32, 44):
            row = out.loc[('101', labels[code])]
            assert pd.isna(row['NumberOfUnits']), code
            assert pd.isna(row['Derivation']), code

    def test_unit_asked_items_are_untouched(self, malawi_mod):
        out = _assembled(malawi_mod, self.ROWS)
        labels = malawi_mod._price_item_code_map()
        assert pd.isna(out.loc[('101', labels[1]), 'NumberOfUnits'])
        assert out.loc[('101', labels[27]), 'NumberOfUnits'] == 2.0
        asked = out[~out.index.get_level_values('j').isin(
            [labels[c] for c in STRUCK])]
        assert asked['Derivation'].isna().all()
        assert len(asked) == 3

    def test_a_recorded_count_is_never_overwritten(self, malawi_mod):
        """No held file records a count on a struck item; if one ever does,
        the derivation leaves it alone (the unit guard in `_price_block`
        refuses only a recorded UNIT code)."""
        out = _assembled(malawi_mod, [('101', 31, 1, 600.0, 3.0, None)])
        assert out['NumberOfUnits'].tolist() == [3.0]
        assert out['Derivation'].isna().all()

    def test_derivation_is_a_string_column_on_every_assembled_frame(self, malawi_mod):
        out = malawi_mod.assemble_community_prices(
            '2010-11', [_block(malawi_mod, [('101', 1, 1, 9000.0, 1.0, 2)])])
        assert list(out.columns) == ['Price', 'NumberOfUnits', 'Available',
                                     'Derivation']
        assert str(out['Derivation'].dtype) == 'string'
        assert out['Derivation'].isna().all()

    def test_the_function_refuses_a_frame_already_keyed(self, malawi_mod):
        d = pd.DataFrame({'_item_code': [31], 'Price': [1.0],
                          'NumberOfUnits': [None], 'Derivation': ['x']})
        with pytest.raises(ValueError, match='already carries'):
            malawi_mod.derive_ck_struck_one_unit(d)

    def test_the_key_is_registered(self, malawi_mod):
        from lsms_library.derivations import derivation_records, resolve_callable
        rec = derivation_records(COUNTRY)[KEY]
        assert malawi_mod.CK_STRUCK_ONE_UNIT == KEY
        assert rec.table == 'community_prices'
        assert rec.columns == ['NumberOfUnits']
        assert list(rec.waves) == ['2010-11', '2013-14', '2019-20']
        assert rec.function.endswith('Malawi._.malawi:derive_ck_struck_one_unit')
        assert rec.inputs.endswith(
            'Malawi._.malawi:inputs_community_prices_struck_one_unit')
        assert resolve_callable(rec.function).__name__ == 'derive_ck_struck_one_unit'
        assert {a['basis'] for a in rec.assumptions} >= {'questionnaire'}

    def test_inputs_sources_cover_the_registered_waves(self, malawi_mod):
        assert sorted(malawi_mod._CK_PRICE_SOURCES) == ['2010-11', '2013-14',
                                                        '2019-20']


# --- data-gated: the delivered table ---------------------------------------

#: Rows per wave -- unchanged by #827 (the fix relabels, it adds and drops
#: nothing).  Measured on the cold build of 2026-10-23 (development
#: 9a0ed5f3c and this branch alike).
ROWS = {'2010-11': 39_166, '2013-14': 6_511, '2019-20': 36_958}
#: `u == 'Unknown'` per wave after #827 (was 0; those rows read 'unit').
UNKNOWN = {'2010-11': 4_555, '2013-14': 1_156, '2019-20': 4_164}
#: Rows served NumberOfUnits = 1 under KEY, per wave: the struck items' rows
#: with a recorded price and no count, counted on the RAW Module CK files
#: (r2 oracle, 2026-10-23; 2019-20 = Cross_Sectional 8,641 + Panel 1,015).
STAMPED = {'2010-11': 9_784, '2013-14': 2_201, '2019-20': 9_656}
STAMPED_TOTAL = 21_641


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

    def test_struck_items_carry_their_fixed_unit_and_one_when_priced(self, delivered, malawi_mod):
        """Ruling (@ligon, 2026-10-23): the 21,641 priced struck rows are
        served NumberOfUnits = 1 under the registry key; the 6,990 unpriced
        ones (6,975 "No" + 15 with neither a price nor a "No") keep NA."""
        labels = malawi_mod._price_item_code_map()
        want = {labels[c]: u for c, u in STRUCK.items()}
        d = delivered.reset_index()
        d = d[d['j'].isin(want)]
        assert len(d) == 28_631
        assert d['u'].eq(d['j'].map(want)).fillna(False).all()
        priced = d['Price'].notna()
        assert int(priced.sum()) == STAMPED_TOTAL
        # NA-strict: on a nullable column `(x == v).all()` SKIPS NA, so an
        # all-NA column would pass it vacuously (it did, on the PR head).
        assert d.loc[priced, 'NumberOfUnits'].eq(1.0).fillna(False).all()
        assert d.loc[priced, 'Derivation'].eq(KEY).fillna(False).all()
        assert d.loc[~priced, 'NumberOfUnits'].isna().all()
        assert d.loc[~priced, 'Derivation'].isna().all()
        assert int((~priced).sum()) == 6_990

    @pytest.mark.parametrize('wave', sorted(STAMPED))
    def test_derivation_rows_per_wave(self, delivered, wave):
        x = delivered.xs(wave, level='t')
        assert int((x['Derivation'] == KEY).sum()) == STAMPED[wave]
        assert set(x['Derivation'].dropna()) == {KEY}

    def test_the_key_is_on_no_other_row(self, delivered):
        assert int(delivered['Derivation'].notna().sum()) == STAMPED_TOTAL
        assert str(delivered['Derivation'].dtype) == 'string'

    def test_attrs_summary(self, delivered):
        summ = delivered.attrs['derivations'][COUNTRY][KEY]
        assert summ['rows'] == STAMPED_TOTAL
        assert summ['columns'] == ['NumberOfUnits']
        assert summ['in'] == 'community_prices'

    def test_inputs_join_the_stamped_rows(self, delivered):
        """`derivation_inputs` returns one raw Module CK row per stamped
        served row, keyed by the served (t, v, j, u), under the original
        variable names: a recorded price and a blank count on every one."""
        import lsms_library as ll
        raw = ll.Country(COUNTRY).derivation_inputs(KEY)
        assert raw.index.is_unique
        stamped = delivered[delivered['Derivation'].eq(KEY).fillna(False)].index
        assert set(raw.index) == set(stamped)
        assert len(raw) == STAMPED_TOTAL
        for wave, (price, count) in {'2010-11': ('com_ck00b1', 'com_ck00b2'),
                                     '2013-14': ('com_ck00b1', 'com_ck00b2'),
                                     '2019-20': ('ckb', 'ckc')}.items():
            r = raw.xs(wave, level='t')
            assert pd.to_numeric(r[price], errors='coerce').notna().all(), wave
            assert pd.to_numeric(r[count], errors='coerce').isna().all(), wave
        served_price = delivered.loc[raw.index, 'Price'].astype('Float64')
        raw_price = pd.concat([
            pd.to_numeric(raw.xs(w, level='t', drop_level=False)[c], errors='coerce')
            for w, c in (('2010-11', 'com_ck00b1'), ('2013-14', 'com_ck00b1'),
                         ('2019-20', 'ckb'))]).astype('Float64')
        assert served_price.loc[raw_price.index].eq(raw_price).fillna(False).all()
