"""An unsized unit label pools its sizes in the kg inference (GH #1450).

#1224 serves EHCVM ``u`` as ``<unit> (<Size>)``.  A delivered kg factor has no
``t`` axis, so an older wave that records no size used to borrow an
``(item, unit)`` estimate from EHCVM's bare labels; #1224 left its ``Tas`` nearly
alone in its cell.  The repair is to the pool behind both inferred arms: an
unsized label reads every report of its base unit, a sized label reads only
itself.  Rule: ``transformations._UNIT_SIZE_WORDS``.

Synthetic frames and the real ``u`` tables from the tree; the delivered checks
at the end are data-gated, and carry only what the issue already states.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lsms_library.ehcvm import size_label
from lsms_library.paths import countries_root
from lsms_library.transformations import (
    FOOD_KG_BASELINE_MAX_SPREAD, FOOD_KG_FACTOR_LAYERS, _size_pooled_frame,
    _unit_size_base, _UNIT_SIZE_WORDS, food_kg_factors,
    food_prices_from_acquired, food_quantities_from_acquired,
)

from tests.test_ehcvm_7day import SIZES

#: Sizes a delivered cell serves that `transformations._UNIT_SIZE_WORDS` does not yet name.
#: Guinea-Bissau 2021-22 serves `Inteiro` (whole; 26 consumed rows, e.g. `Corda (Inteiro)`).
#: Adding it is a `transformations.py` edit that re-warms GhanaLSS, Uganda and Ethiopia, which
#: the 2026-11-13 ruling for this stream forbids; until it lands, an `X (Inteiro)` label is read
#: as a unit of its own, so it neither pools with bare `X` nor is size-pooled (residual drafted
#: in the gnb stream report).
SIZES_NOT_YET_IN_THE_VOCABULARY = frozenset({"Inteiro"})

IDX = ['t', 'i', 'j', 'u']
READ_BACK = ['kg_per_unit', 'KgFactorSource', 'kg_item_unit', 'kg_unit',
             'kg_item_n_waves', 'kg_item_wave_spread', 'baseline_refused_spread']


def frame(rows):
    """``(t, i, j, u)``-indexed frame from ``(t, i, j, u, Quantity, Expenditure)``."""
    idx = pd.MultiIndex.from_tuples([r[:4] for r in rows], names=IDX)
    return pd.DataFrame({'Quantity': [r[4] for r in rows],
                         'Expenditure': [r[5] for r in rows]}, index=idx)


def kg(t, item, n=10, price=100.0, start=0):
    """*n* households buying *item* by the kilogram at *price* per kg."""
    return [(t, f'{t}h{k}', item, 'Kg', 1.0, price) for k in range(start, start + n)]


def units(t, item, u, n, price, start=100):
    """*n* households buying one *u* of *item* at *price*."""
    return [(t, f'{t}h{k}', item, u, 1.0, price) for k in range(start, start + n)]


def strip_sizes(df):
    """The frame as it was before #1224: the size is not in the label."""
    stripped = [(_unit_size_base(u) or u) for u in df.index.get_level_values('u')]
    return df.set_axis(pd.MultiIndex.from_arrays(
        [stripped if n == 'u' else df.index.get_level_values(n)
         for n in df.index.names], names=df.index.names))


def rows_of(f, *, u=None, t=None, j=None):
    m = np.ones(len(f), dtype=bool)
    for name, value in (('u', u), ('t', t), ('j', j)):
        if value is not None:
            m &= (f.index.get_level_values(name) == value)
    return f[m]


#: An older wave with no baseline for the item and no size in its labels, beside
#: an EHCVM-shaped wave that has both.  One Tas (Petit) costs 50 per kg-priced
#: 100 (0.5 kg), a Tas (Grand) 200 (2.0 kg), and no other size is in the pool.
def older_and_sized(*, extra=()):
    return frame(
        kg('T2', 'A', 10)
        + units('T1', 'A', 'Tas', 6, 80.0)
        + units('T2', 'A', 'Tas (Petit)', 7, 50.0)
        + units('T2', 'A', 'Tas (Grand)', 6, 200.0)
        + list(extra))


class TestWhatASizeIs:
    @pytest.mark.parametrize('label,base', [
        ('Tas (Petit)', 'Tas'),
        ('Tiya (Entier)', 'Tiya'),
        ('Unidade (Pequeno)', 'Unidade'),
        ('Tas (Très petite)', 'Tas'),
        ('Bouquet (Avec 2 brassées)', 'Bouquet'),
        ('Bouquet (98)', 'Bouquet'),       # Togo's unlabelled size code
        ('662 (Moyen)', '662'),            # a residual code keeps its size
        ('Sac (50 Kg) (Moyen)', 'Sac (50 Kg)'),   # the quantity stays in the base
        ('Bidon de 1,5 L (Autre taille)', 'Bidon de 1,5 L'),
    ])
    def test_a_size_suffix(self, label, base):
        assert _unit_size_base(label) == base

    @pytest.mark.parametrize('label', [
        'Sac (50 Kg)', 'Paquet (250 g)', 'Sac (0,9 Kg)',   # a stated quantity
        'Heap (Small)', 'Heap (unspecified)', 'Piece (Big)',   # another survey's own
        'Tablette (30)', 'Grass Hoppers (25)', 'Mililitres (500)',   # a count
        'Motte (boule)', 'Tin (Debe)', 'Other (Specify)',   # a gloss
        'Whole Animal (Sheep)',            # a content
        'Tas',
        'Tas(Petit)', 'Tas (petit)',       # not the form the hook writes
        '(Petit)',
        'Kg (Grand)', 'Litre (Quart)',     # a metric base: never minted, refused
        None, np.nan, pd.NA,
    ])
    def test_not_a_size_suffix(self, label):
        assert _unit_size_base(label) is None

    def test_the_vocabulary_is_the_delivered_sizes(self):
        """The chain that stops drift: a delivered cell serving a new size fails
        ``test_every_size_suffix_is_the_countrys_own`` until it joins ``SIZES``,
        and this fails until it joins the vocabulary -- or is named in
        ``SIZES_NOT_YET_IN_THE_VOCABULARY``, with its reason."""
        delivered = frozenset().union(*SIZES.values())
        assert _UNIT_SIZE_WORDS == delivered - SIZES_NOT_YET_IN_THE_VOCABULARY
        assert not _UNIT_SIZE_WORDS & SIZES_NOT_YET_IN_THE_VOCABULARY

    @pytest.mark.parametrize('word', sorted(_UNIT_SIZE_WORDS))
    def test_each_word_is_spelled_as_the_hook_serves_it(self, word):
        assert size_label(word) == word

    def test_no_unit_table_in_the_tree_mints_a_size_of_its_own(self):
        """A size is what `ehcvm.carry_unit_size` writes, never a label a country
        table already holds -- so no country's own vocabulary is read as sized."""
        from lsms_library.local_tools import all_dfs_from_orgfile
        checked = 0
        for org in sorted(countries_root().glob('*/_/categorical_mapping.org')):
            table = all_dfs_from_orgfile(org).get('u')
            if table is None:
                continue
            checked += 1
            cells = pd.unique(table.astype(object).to_numpy().ravel())
            sized = sorted(str(c) for c in cells if _unit_size_base(c) is not None)
            assert not sized, f'{org.parent.parent.name}: {sized}'
        assert checked >= 5


class TestUnsizedRowsPool:
    def test_an_unsized_row_reads_its_bases_sized_reports(self):
        """T1 has no baseline of its own: the factor is the sized wave's pool."""
        df = older_and_sized()
        got = rows_of(food_kg_factors(df), t='T1', u='Tas')
        assert set(got['KgFactorSource']) == {'item_unit'}
        # the median of 7 x 0.5 and 6 x 2.0 is 0.5; with the T2 reports only
        assert got['kg_per_unit'].iloc[0] == pytest.approx(0.5)
        assert got['kg_size_pooled'].all()

    def test_it_is_what_the_row_read_with_no_size_in_any_label(self):
        """The identity the issue asks for, column by column."""
        df = older_and_sized(extra=(
            units('T2', 'A', 'Tas', 5, 400.0, start=300)        # size-free reports
            + units('T1', 'B', 'Tas', 5, 80.0, start=400)       # no baseline anywhere
            + units('T2', 'B', 'Tas (Moyen)', 5, 90.0, start=500)
            + kg('T1', 'C', 10, start=600) + kg('T2', 'C', 10, start=600)
            + units('T1', 'C', 'Tas', 5, 70.0, start=700)
            + units('T2', 'C', 'Tas (Grand)', 5, 250.0, start=800)))
        got = food_kg_factors(df)
        want = food_kg_factors(strip_sizes(df))
        unsized = np.array([_unit_size_base(u) is None
                            for u in df.index.get_level_values('u')])
        pd.testing.assert_frame_equal(
            got.loc[unsized, READ_BACK].reset_index(drop=True),
            want.loc[unsized, READ_BACK].reset_index(drop=True))

    def test_the_u_pooled_rung_pools_too(self):
        """B has no baseline in any wave, so the bare row can only reach `unit`,
        and that estimate must read the sized households as well."""
        rows = []
        for k in range(10):
            rows += [('T2', f'h{k}', 'A', 'Kg', 1.0, 100.0),
                     ('T2', f'h{k}', 'B', 'Tas (Petit)', 1.0, 50.0)]
        rows += [('T1', f'old{k}', 'B', 'Tas', 1.0, 80.0) for k in range(5)]
        df = frame(rows)
        f = food_kg_factors(df)
        bare = rows_of(f, u='Tas')
        assert set(bare['KgFactorSource']) == {'unit'}
        assert bare['kg_per_unit'].iloc[0] == pytest.approx(0.5)
        assert bare['kg_size_pooled'].all()
        unsized = df.index.get_level_values('u') == 'Tas'
        np.testing.assert_allclose(
            bare['kg_per_unit'], food_kg_factors(strip_sizes(df)).loc[unsized, 'kg_per_unit'])

    def test_every_rung_reads_the_same_estimate(self):
        """Factor, wave provenance and tightness come from one estimate."""
        f = food_kg_factors(older_and_sized())
        bare = rows_of(f, u='Tas').iloc[0]
        assert bare['kg_item_n_waves'] == 1      # T1 has no baseline: T2 alone
        assert bare['kg_item_wave_spread'] == pytest.approx(1.0)
        assert f.attrs['kg_factor_wave_spread']['rows_served_by_item_rung'] \
            == int(f['KgFactorSource'].isin(['item_unit', 'item_unit_tight']).sum())

    def test_a_bare_row_with_no_sized_sibling_reads_only_itself(self):
        rows = kg('T1', 'A') + units('T1', 'A', 'Calebasse', 6, 300.0) \
            + units('T1', 'A', 'Tas (Petit)', 6, 50.0)
        f = food_kg_factors(frame(rows))
        assert not rows_of(f, u='Calebasse')['kg_size_pooled'].any()
        assert rows_of(f, u='Calebasse')['kg_per_unit'].iloc[0] == pytest.approx(3.0)


class TestSizedRowsDoNotMove:
    def test_a_sized_row_keeps_its_own_cell(self):
        f = food_kg_factors(older_and_sized())
        assert rows_of(f, u='Tas (Petit)')['kg_per_unit'].iloc[0] == pytest.approx(0.5)
        assert rows_of(f, u='Tas (Grand)')['kg_per_unit'].iloc[0] == pytest.approx(2.0)
        assert not rows_of(f, u='Tas (Grand)')['kg_size_pooled'].any()

    def test_a_bare_sibling_does_not_touch_a_sized_estimate(self):
        with_bare = older_and_sized(
            extra=units('T2', 'A', 'Tas', 9, 400.0, start=300))
        without = older_and_sized()
        keep = ['Tas (Petit)', 'Tas (Grand)', 'Kg']
        got = food_kg_factors(with_bare)
        want = food_kg_factors(without[~without.index.get_level_values('u').isin(['Tas'])])
        for u in keep:
            assert rows_of(got, u=u)['kg_per_unit'].unique() == pytest.approx(
                rows_of(want, u=u)['kg_per_unit'].unique())
            assert set(rows_of(got, u=u)['KgFactorSource']) \
                == set(rows_of(want, u=u)['KgFactorSource'])


class TestOtherLabelsAreLeftAlone:
    def test_a_stated_quantity_is_not_a_size(self):
        """`Sac (50 Kg)` is read, not inferred, and bare `Sac` does not pool it."""
        rows = kg('T1', 'A') + units('T1', 'A', 'Sac (50 Kg)', 6, 5000.0) \
            + units('T1', 'A', 'Sac', 6, 700.0, start=200) \
            + units('T1', 'A', 'Sac (50 Kg) (Moyen)', 2, 5000.0, start=300) \
            + units('T1', 'A', 'Paquet (250 g)', 2, 25.0, start=400)
        f = food_kg_factors(frame(rows))
        for u, stated in (('Sac (50 Kg)', 50.0), ('Sac (50 Kg) (Moyen)', 50.0),
                          ('Paquet (250 g)', 0.25)):
            got = rows_of(f, u=u)
            assert set(got['KgFactorSource']) == {'metric'}
            assert got['kg_per_unit'].iloc[0] == pytest.approx(stated)
        assert rows_of(f, u='Sac')['kg_per_unit'].iloc[0] == pytest.approx(7.0)
        assert f.attrs['kg_factor_size_pool']['rows_served'] == 0

    def test_ugandas_and_malawis_form_is_not_pooled(self):
        """`Heap (Small)` predates #1224; a bare `Heap` beside it is its own cell."""
        rows = kg('T1', 'A') + units('T1', 'A', 'Heap (Small)', 6, 50.0) \
            + units('T1', 'A', 'Heap (Large)', 6, 300.0, start=200) \
            + units('T1', 'A', 'Heap', 6, 100.0, start=300)
        df = frame(rows)
        assert _size_pooled_frame(df)[0] is None
        f = food_kg_factors(df)
        assert rows_of(f, u='Heap')['kg_per_unit'].iloc[0] == pytest.approx(1.0)
        assert not f['kg_size_pooled'].any()
        assert f.attrs['kg_factor_size_pool'] == {
            'rows_served': 0, 'served_by_item_rung': 0, 'served_by_unit_rung': 0, 'labels': []}

    def test_a_family_with_no_bare_row_makes_no_second_estimate(self):
        rows = kg('T1', 'A') + units('T1', 'A', 'Tas (Petit)', 6, 50.0)
        assert _size_pooled_frame(frame(rows))[0] is None

    def test_a_frame_with_u_as_a_column_is_left_as_it_was(self):
        df = older_and_sized().reset_index('u')
        assert _size_pooled_frame(df)[0] is None
        assert not food_kg_factors(df)['kg_size_pooled'].any()

    def test_a_base_with_a_residual_code_pools(self):
        rows = kg('T1', 'A') + units('T1', 'A', '662 (Moyen)', 6, 50.0) \
            + units('T1', 'A', '662', 2, 400.0, start=200)
        f = food_kg_factors(frame(rows))
        assert rows_of(f, u='662')['kg_per_unit'].iloc[0] == pytest.approx(0.5)
        assert rows_of(f, u='662')['kg_size_pooled'].all()


class TestTheGatesKeepTheirMeaning:
    def test_the_layers_still_partition(self):
        df = older_and_sized(extra=(
            units('T1', 'B', 'Tas', 3, 80.0, start=500)
            + [('T1', 'x', 'A', 'Unknown', 1.0, 10.0),
               ('T1', 'y', 'A', 'Sac (50 Kg)', 1.0, 5000.0)]))
        f = food_kg_factors(df)
        counts = f.attrs['kg_factor_sources']
        assert set(counts) == set(FOOD_KG_FACTOR_LAYERS)
        assert sum(counts.values()) == len(df)
        pool = f.attrs['kg_factor_size_pool']
        assert pool['rows_served'] == int(f['kg_size_pooled'].sum()) > 0
        assert pool['rows_served'] == pool['served_by_item_rung'] + pool['served_by_unit_rung']
        assert set(f.loc[f['kg_size_pooled'], 'KgFactorSource']) \
            <= {'item_unit', 'item_unit_tight', 'unit'}

    def test_a_size_never_reaches_the_baseline(self):
        """The (t, j) baseline reads kg-labelled rows only, and #1224 keeps those
        bare: with or without a suffix in any label the gate decides the same."""
        rows = kg('T1', 'A', 4) + [('T1', 'hx', 'A', 'Kg', 1.0, 12500.0)]   # refused
        rows += units('T1', 'A', 'Tas', 6, 80.0) + units('T1', 'A', 'Tas (Petit)', 6, 50.0, start=200)
        rows += kg('T1', 'B', 10) + units('T1', 'B', 'Tas', 6, 80.0, start=300) \
            + units('T1', 'B', 'Tas (Grand)', 6, 200.0, start=400)
        df = frame(rows)
        got, want = food_kg_factors(df), food_kg_factors(strip_sizes(df))
        gate = got.attrs['kg_factor_baseline_gate']
        assert gate['baseline_max_spread'] == FOOD_KG_BASELINE_MAX_SPREAD
        assert gate['rows_refused'] == gate['refused_to_unit'] + gate['refused_to_none']
        bare = got.index.get_level_values('u') == 'Tas'
        # the gate cost A's bare rows their item factor, and says so, as it does
        # with no size in any label
        a_bare = rows_of(got, u='Tas', j='A')
        assert a_bare['baseline_refused_spread'].all()
        assert set(a_bare['KgFactorSource']) <= {'unit', 'none'}
        np.testing.assert_array_equal(got.loc[bare, 'baseline_refused_spread'],
                                      want.loc[bare, 'baseline_refused_spread'])
        np.testing.assert_allclose(got.loc[bare, 'kg_per_unit'], want.loc[bare, 'kg_per_unit'])

    def test_a_refused_sized_cell_is_not_rescued_by_the_pool(self):
        """A sized row whose baseline the gate refuses stays refused."""
        rows = kg('T1', 'A', 4) + [('T1', 'hx', 'A', 'Kg', 1.0, 12500.0)]
        rows += units('T1', 'A', 'Tas (Petit)', 6, 50.0) + units('T1', 'A', 'Tas', 6, 80.0, start=200)
        rows += kg('T1', 'B', 10, start=300) + units('T1', 'B', 'Tas (Grand)', 6, 200.0, start=400)
        f = food_kg_factors(frame(rows))
        sized = rows_of(f, u='Tas (Petit)')
        assert sized['baseline_refused_spread'].all()
        assert not sized['kg_size_pooled'].any()

    def test_a_metric_label_never_enters_the_pool(self):
        """A `Kg (Petit)` is never minted; if one arrives it must not become a
        baseline row of the pooled estimate."""
        rows = units('T1', 'A', 'Kg (Petit)', 6, 5000.0) + units('T1', 'A', 'Tas', 6, 80.0, start=200) \
            + units('T1', 'A', 'Tas (Petit)', 6, 50.0, start=300)
        f = food_kg_factors(frame(rows))
        assert set(f['KgFactorSource']) == {'none'}   # no baseline exists to divide by

    @pytest.mark.parametrize('n,source', [(4, 'unit'), (5, 'item_unit')])
    def test_min_reports_counts_the_pool_per_wave(self, n, source):
        """Below the floor the wave contributes nothing, exactly as for any cell;
        the sizes count together because that is what the cell now is."""
        rows = []
        for k in range(10):
            rows += [('T2', f'h{k}', 'A', 'Kg', 1.0, 100.0),
                     ('T2', f'h{k}', 'B', 'Kg', 1.0, 100.0),
                     ('T2', f'h{k}', 'B', 'Tas (Grand)', 1.0, 200.0)]
        rows += [('T2', f'p{k}', 'A', 'Tas (Petit)', 1.0, 50.0) for k in range(n - 1)]
        rows += [('T2', 'p9', 'A', 'Tas (Grand)', 1.0, 50.0)]
        rows += [('T1', f'o{k}', 'A', 'Tas', 1.0, 80.0) for k in range(5)]
        f = food_kg_factors(frame(rows))
        got = rows_of(f, u='Tas', j='A')
        assert set(got['KgFactorSource']) == {source}

    def test_no_fabrication_a_row_nothing_serves_stays_none(self):
        """No price, no estimate; no kg-labelled row, no factor at any rung."""
        rows = units('T1', 'A', 'Tas', 6, 80.0) + units('T2', 'A', 'Tas (Petit)', 6, 50.0, start=200)
        f = food_kg_factors(frame(rows))
        assert set(f['KgFactorSource']) == {'none'}
        assert f['kg_per_unit'].isna().all()
        assert not f['kg_size_pooled'].any()
        no_price = frame(rows).drop(columns='Expenditure')
        assert set(food_kg_factors(no_price)['KgFactorSource']) == {'none'}

    def test_the_missing_unit_sentinel_does_not_read_the_pool(self):
        rows = kg('T1', 'A') + units('T1', 'A', 'Unknown', 6, 50.0) \
            + units('T1', 'A', 'Tas (Petit)', 6, 50.0, start=200) \
            + units('T1', 'A', 'Tas', 2, 50.0, start=300)
        f = food_kg_factors(frame(rows))
        assert set(rows_of(f, u='Unknown')['KgFactorSource']) == {'none'}

    def test_a_declared_no_kg_cell_stays_declared(self):
        """GH #1042: the pool does not hand a declared row an inferred factor."""
        df = older_and_sized()
        f = food_kg_factors(df, no_kg=[('A', 'Tas')])
        bare = rows_of(f, u='Tas')
        assert set(bare['KgFactorSource']) == {'none'}
        assert not bare['kg_size_pooled'].any()
        assert f.attrs['kg_factor_declared_none']['rows_refused'] == len(bare)

    @staticmethod
    def _variant_frame(bare, *, sized=('Tas (Petit)', 'Tas (Grand)')):
        """The pool's median (0.8) is neither the sized reports' (1.4) nor the
        bare reports' own (0.2), so the factor says which cell a row read."""
        return frame(kg('T1', 'A')
                     + units('T1', 'A', bare, 6, 20.0)
                     + units('T1', 'A', sized[0], 5, 200.0, start=200)
                     + units('T1', 'A', sized[1], 5, 80.0, start=300))

    @pytest.mark.parametrize('variant', ['tas', 'TAS', 'Tas ', ' Tas'])
    def test_a_case_or_padding_variant_reads_the_pool(self, variant):
        """The factor, not only the mark: the mark must never claim a pool the
        row did not read."""
        want = rows_of(food_kg_factors(self._variant_frame('Tas')), u='Tas')
        got = rows_of(food_kg_factors(self._variant_frame(variant)), u=variant)
        assert want['kg_per_unit'].iloc[0] == pytest.approx(0.8)
        assert len(got) == 6
        np.testing.assert_allclose(got['kg_per_unit'], want['kg_per_unit'])
        assert got['kg_size_pooled'].all()
        assert set(got['KgFactorSource']) == set(want['KgFactorSource']) == {'item_unit'}

    @pytest.mark.parametrize('variant', ['tas', 'Tas '])
    def test_a_variant_reads_the_u_pooled_rung_too(self, variant):
        """B has no kg-labelled row, so only the `unit` rung can serve it.  The
        pool's households give 0.5 and 2.0, so its median is 1.25."""
        rows = []
        for k in range(10):
            rows += [('T1', f'h{k}', 'A', 'Kg', 1.0, 100.0),
                     ('T1', f'h{k}', 'B', 'Tas (Petit)', 1.0, 50.0),
                     ('T1', f'x{k}', 'A', 'Kg', 1.0, 100.0),
                     ('T1', f'x{k}', 'B', variant, 1.0, 200.0)]
        got = rows_of(food_kg_factors(frame(rows)), u=variant)
        assert set(got['KgFactorSource']) == {'unit'}
        assert got['kg_per_unit'].iloc[0] == pytest.approx(1.25)
        assert got['kg_size_pooled'].all()

    def test_two_spellings_of_a_base_are_one_family(self):
        """No built country has this; it must still read one cell, not whichever
        spelling the join happened to keep."""
        same = rows_of(food_kg_factors(self._variant_frame('Tas')), u='Tas')
        two = rows_of(food_kg_factors(self._variant_frame(
            'Tas', sized=('Tas (Petit)', 'tas (Grand)'))), u='Tas')
        np.testing.assert_allclose(two['kg_per_unit'], same['kg_per_unit'])

    def test_the_gate_ledger_reads_the_pool(self):
        """Four bare reports could not clear `min_reports` alone, so with their
        own cell these rows would never have had a factor for the gate to cost.
        In the pool they do, so the gate is what cost it."""
        rows = kg('T1', 'A', 4) + [('T1', 'hx', 'A', 'Kg', 1.0, 12500.0)]     # refused
        rows += units('T1', 'A', 'Tas', 4, 80.0) \
            + units('T1', 'A', 'Tas (Petit)', 6, 50.0, start=200)
        rows += kg('T1', 'B', 10, start=300) \
            + units('T1', 'B', 'Tas (Grand)', 6, 200.0, start=400)
        df = frame(rows)
        f = food_kg_factors(df)
        bare = rows_of(f, u='Tas', j='A')
        assert bare['baseline_refused_spread'].all()
        assert bare['kg_item_unit'].isna().all()
        want = food_kg_factors(strip_sizes(df))
        is_bare = df.index.get_level_values('u') == 'Tas'
        np.testing.assert_array_equal(f.loc[is_bare, 'baseline_refused_spread'],
                                      want.loc[is_bare, 'baseline_refused_spread'])
        gate = f.attrs['kg_factor_baseline_gate']
        assert gate['rows_refused'] == gate['refused_to_unit'] + gate['refused_to_none']

    def test_tightness_reads_the_pool(self):
        """T1's baseline is the 3-report tight exception, T2's is strict.  The
        bare label's own cell rests on T1 alone, but its pool has a strict
        baseline, so it is not thinner evidence and must not say it is."""
        df = frame(kg('T1', 'A', 3) + units('T1', 'A', 'Tas', 6, 80.0)
                   + kg('T2', 'A', 10) + units('T2', 'A', 'Tas (Petit)', 6, 50.0))
        f = food_kg_factors(df)
        bare = rows_of(f, u='Tas')
        assert set(bare['KgFactorSource']) == {'item_unit'}
        assert bare['kg_per_unit'].iloc[0] == pytest.approx(0.65)   # median of 0.8, 0.5

    def test_a_pool_resting_only_on_a_tight_baseline_says_so(self):
        df = frame(kg('T1', 'A', 3) + units('T1', 'A', 'Tas', 3, 80.0)
                   + units('T1', 'A', 'Tas (Petit)', 3, 80.0, start=200))
        f = food_kg_factors(df)
        assert set(rows_of(f, u='Tas')['KgFactorSource']) == {'item_unit_tight'}
        assert f.attrs['kg_factor_size_pool']['served_by_item_rung'] == 3


class TestTheDerivedTablesCarryTheReport:
    def test_food_quantities_and_food_prices(self):
        df = older_and_sized()
        q = food_quantities_from_acquired(df, units='kgs')
        p = food_prices_from_acquired(df, units='kgvalue')
        assert q.attrs['kg_factor_size_pool'] == p.attrs['kg_factor_size_pool']
        assert q.attrs['kg_factor_size_pool']['rows_served'] == 6

    def test_the_older_waves_kilograms_are_the_pooled_ones(self):
        df = older_and_sized()
        q = food_quantities_from_acquired(df, units='kgs')
        older = q.xs('T1', level='t').xs('kg', level='u')['Quantity']
        assert older.sum() == pytest.approx(6 * 0.5)


# ---------------------------------------------------------------------------
# data-gated: the two cases the issue names
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def ladder():
    import lsms_library as ll
    out = {}

    def get(country):
        if country not in out:
            try:
                c = ll.Country(country)
                raw = c._aggregate_wave_data(None, 'food_acquired')
                out[country] = food_kg_factors(raw, unit_kg=c._unit_kg_factors(),
                                               no_kg=c._no_kg_factor_cells())
            except Exception as e:            # pragma: no cover - no microdata
                pytest.skip(f'{country} food_acquired not buildable here: {e}')
        return out[country]
    return get


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    @pytest.mark.parametrize('country,wave,item,unit,low,high', [
        # against the survey's own weight tables: 11 g and 41 g
        ('Burkina_Faso', '2014', 'Cube alimentaire', 'Unité', 0.015, 0.025),
        ('Mali', '2014-15', 'Piment', 'Tas', 0.05, 0.10),
    ])
    def test_the_named_cells_are_back_on_the_item_rung(self, ladder, country, wave, item, unit, low, high):
        got = rows_of(ladder(country), t=wave, j=item, u=unit)
        assert len(got)
        assert set(got['KgFactorSource']) <= {'item_unit', 'item_unit_tight'}
        assert got['kg_size_pooled'].all()
        assert low < got['kg_per_unit'].iloc[0] < high

    @pytest.mark.parametrize('country', ['Burkina_Faso', 'Mali', 'Togo', 'Niger'])
    def test_no_sized_row_is_marked_pooled(self, ladder, country):
        f = ladder(country)
        sized = np.array([_unit_size_base(u) is not None
                          for u in f.index.get_level_values('u')])
        assert sized.any()
        assert not f.loc[sized, 'kg_size_pooled'].any()
        assert f.loc[~sized, 'kg_size_pooled'].any()

    @pytest.mark.parametrize('country', ['Uganda', 'Malawi'])
    def test_a_survey_with_its_own_size_words_is_not_pooled(self, ladder, country):
        f = ladder(country)
        assert not f['kg_size_pooled'].any()
        assert f.attrs['kg_factor_size_pool']['rows_served'] == 0
