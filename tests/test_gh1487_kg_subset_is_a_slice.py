"""A ``waves=`` subset's kilograms are a slice of the all-waves result (GH #1487).

``food_kg_factors`` estimates from the frame it is handed, and a ``waves=``
request used to hand it only those waves, so a wave's kilograms depended on
which other waves were requested.  Ruled 2026-11-04: estimate from every wave
of the country and filter on the way out.

Three tiers.  Synthetic frames for the rule in the transforms; the real
dispatch and cache gates on controlled native frames (the ``test_gh1210``
fixture), for which frame is read and what is left on disk; and real data,
``requires_s3``, for the rule on a built country.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
import lsms_library.country as C
from lsms_library.local_tools import read_parquet_cache_waves, to_parquet
from lsms_library.paths import data_root
from lsms_library.transformations import (
    FOOD_KG_FACTOR_LAYERS, StatedVsInferredKgWarning, UnmatchedNoKgCellWarning,
    food_kg_factors, food_prices_from_acquired, food_quantities_from_acquired,
)

IDX = ['t', 'i', 'j', 'u']
KG_ROWS = ['kg_per_unit', 'KgFactorSource', 'kg_item_unit', 'kg_unit',
           'kg_item_n_waves', 'kg_item_wave_spread', 'baseline_refused_spread',
           'kg_size_pooled']


def frame(rows):
    """``(t, i, j, u)``-indexed frame from ``(t, i, j, u, Quantity, Expenditure)``."""
    idx = pd.MultiIndex.from_tuples([r[:4] for r in rows], names=IDX)
    return pd.DataFrame({'Quantity': [float(r[4]) for r in rows],
                         'Expenditure': [float(r[5]) for r in rows]}, index=idx)


def kg(t, item, n=10, price=100.0, start=0):
    return [(t, f'{t}h{k}', item, 'Kg', 1.0, price) for k in range(start, start + n)]


def tins(t, item, n, price, start=100, u='Tin'):
    return [(t, f'{t}h{k}', item, u, 1.0, price) for k in range(start, start + n)]


def two_waves():
    """T2 holds the item's kilogram baseline; T1 buys it only by the tin.

    Alone, T1 has no baseline, so its tins get no item factor; with T2 in the
    frame they read 2.0 kg per tin.  A third item keeps a served row in each
    wave that needs no pooling at all.
    """
    return frame(kg('T2', 'A') + tins('T2', 'A', 8, 200.0)
                 + tins('T1', 'A', 8, 240.0, u='Tin')
                 + kg('T1', 'B') + kg('T2', 'B'))


def of_wave(df, *waves):
    t = df.index.get_level_values('t').astype(str)
    return df[t.isin({str(w) for w in waves})]


class TestTheFactorIsASlice:
    def test_the_frame_is_one_where_the_subset_alone_differs(self):
        """Otherwise every test below would pass on the old code too."""
        df = two_waves()
        alone = food_kg_factors(of_wave(df, 'T1'))
        pooled = food_kg_factors(df, serve_waves=['T1'])
        tin = alone.index.get_level_values('u') == 'Tin'
        assert not (alone.loc[tin, 'KgFactorSource'] == pooled.loc[tin, 'KgFactorSource']).all()
        assert (pooled.loc[tin, 'KgFactorSource'] == 'item_unit').all()

    @pytest.mark.parametrize('served', [['T1'], ['T2'], ['T1', 'T2'], ('T1',), {'T2'}])
    def test_rows_equal_the_all_waves_rows(self, served):
        df = two_waves()
        everything = food_kg_factors(df)
        got = food_kg_factors(df, serve_waves=served)
        pd.testing.assert_frame_equal(
            got[KG_ROWS], of_wave(everything, *served)[KG_ROWS], check_exact=True)

    def test_no_serve_waves_is_untouched(self):
        df = two_waves()
        a = food_kg_factors(df)
        b = food_kg_factors(df, serve_waves=None)
        pd.testing.assert_frame_equal(a, b, check_exact=True)
        assert a.attrs['kg_factor_sources'] == b.attrs['kg_factor_sources']

    def test_a_frame_with_no_t_level_has_nothing_to_filter(self):
        df = two_waves().droplevel('t')
        assert len(food_kg_factors(df, serve_waves=['T1'])) == len(df)


class TestAttrsDescribeTheServedRows:
    @pytest.mark.parametrize('served', [['T1'], ['T2']])
    def test_the_partition_partitions_the_served_rows(self, served):
        df = two_waves()
        f = food_kg_factors(df, serve_waves=served)
        counts = f.attrs['kg_factor_sources']
        assert set(counts) == set(FOOD_KG_FACTOR_LAYERS)
        assert sum(counts.values()) == len(of_wave(df, *served)) == len(f)
        for layer, n in counts.items():
            assert (f['KgFactorSource'] == layer).sum() == n

    def test_the_counts_are_the_all_waves_rows_counted(self):
        """Each ledger, recomputed from the all-waves per-row columns."""
        df = two_waves()
        everything = food_kg_factors(df)
        served = of_wave(everything, 'T1')
        f = food_kg_factors(df, serve_waves=['T1'])
        spread = f.attrs['kg_factor_wave_spread']
        item = served['KgFactorSource'].isin(['item_unit', 'item_unit_tight'])
        assert spread['rows_served_by_item_rung'] == item.sum()
        assert spread['rows_single_wave'] == (item & (served['kg_item_n_waves'] == 1)).sum()
        gate = f.attrs['kg_factor_baseline_gate']
        assert gate['rows_refused'] == served['baseline_refused_spread'].sum()
        assert gate['rows_refused'] == gate['refused_to_unit'] + gate['refused_to_none']
        pool = f.attrs['kg_factor_size_pool']
        assert pool['rows_served'] == served['kg_size_pooled'].sum()

    def test_declared_none_counts_served_rows_and_judges_the_read_rows(self):
        df = two_waves()
        declared = [('A', 'Kg')]                   # only T2 holds such rows
        with warnings.catch_warnings():
            warnings.simplefilter('error', UnmatchedNoKgCellWarning)
            t1 = food_kg_factors(df, no_kg=declared, serve_waves=['T1'])
            t2 = food_kg_factors(df, no_kg=declared, serve_waves=['T2'])
        assert t1.attrs['kg_factor_declared_none']['rows_matched'] == 0
        assert t1.attrs['kg_factor_declared_none']['cells'] == {
            'A|Kg': {'rows_matched': 0, 'rows_refused': 0}}
        assert t2.attrs['kg_factor_declared_none']['rows_matched'] == 10

    def test_a_cell_no_wave_holds_still_warns(self):
        with pytest.warns(UnmatchedNoKgCellWarning, match='Eggs'):
            food_kg_factors(two_waves(), no_kg=[('Eggs', None)], serve_waves=['T1'])

    def test_a_contradiction_on_a_label_not_served_is_not_reported(self):
        """The stated sack lives only in T2, where it prices at 26 kg, not 50."""
        rows = []
        for k in range(30):
            rows.append(('T2', f'h{k}', 'A', 'kg', 1.0, 100.0))
            rows.append(('T2', f'h{k}', 'A', 'Sac moyen (50 kg)', 1.0, 2620.0))
        df = frame(rows + kg('T1', 'A'))
        with pytest.warns(StatedVsInferredKgWarning, match='sac moyen'):
            food_kg_factors(df, serve_waves=['T2'])
        with warnings.catch_warnings():
            warnings.simplefilter('error', StatedVsInferredKgWarning)
            food_kg_factors(df, serve_waves=['T1'])


class TestTheTransforms:
    @pytest.mark.parametrize('transform,units', [
        (food_quantities_from_acquired, 'kgs'),
        (food_quantities_from_acquired, 'units'),
        (food_prices_from_acquired, 'kgvalue'),
        (food_prices_from_acquired, 'unitvalue'),
    ])
    @pytest.mark.parametrize('served', [['T1'], ['T2'], ['T1', 'T2']])
    def test_served_is_a_slice(self, transform, units, served):
        df = two_waves()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            everything = transform(df, units=units)
            got = transform(df, units=units, serve_waves=served)
        pd.testing.assert_frame_equal(got, of_wave(everything, *served), check_exact=True)

    @pytest.mark.parametrize('units', ['kgprice', 'unitprice'])
    def test_served_is_a_slice_on_reported_prices(self, units):
        df = two_waves().assign(Price=lambda d: d['Expenditure'] / d['Quantity'])
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            everything = food_prices_from_acquired(df, units=units)
            got = food_prices_from_acquired(df, units=units, serve_waves=['T1'])
        pd.testing.assert_frame_equal(got, of_wave(everything, 'T1'), check_exact=True)

    def test_kg_mode_differs_from_estimating_on_the_slice(self):
        df = two_waves()
        got = food_quantities_from_acquired(df, serve_waves=['T1'])
        alone = food_quantities_from_acquired(of_wave(df, 'T1'))
        assert got.xs('kg', level='u')['Quantity'].sum() != alone.xs('kg', level='u')['Quantity'].sum()

    def test_attrs_ride_through(self):
        df = two_waves()
        for out in (food_quantities_from_acquired(df, serve_waves=['T1']),
                    food_prices_from_acquired(df, serve_waves=['T1'])):
            assert sum(out.attrs['kg_factor_sources'].values()) == len(of_wave(df, 'T1'))

    @pytest.mark.parametrize('transform', [food_quantities_from_acquired,
                                           food_prices_from_acquired])
    def test_a_wave_with_no_rows_is_an_empty_slice(self, transform):
        df = two_waves()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = transform(df, serve_waves=['absent'])
            everything = transform(df)
        assert len(got) == 0
        assert list(got.index.names) == list(everything.index.names)
        assert list(got.columns) == list(everything.columns)
        assert sum(got.attrs['kg_factor_sources'].values()) == 0


@pytest.fixture
def native_food(tmp_path, monkeypatch):
    """Real Malawi registry, dispatch and cache gates over controlled frames.

    Each wave yields kilograms and tins of one item, at a wave-specific tin
    price, so a tin's weight depends on which waves the estimator reads.  No
    wave but the first buys tins without a kilogram baseline: see *last*.
    """
    root = tmp_path / 'data'
    monkeypatch.setenv('LSMS_DATA_DIR', str(root))
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    data_root.cache_clear()
    c = C.Country('Malawi', preload_panel_ids=False)
    c._panel_ids_attempted = True
    c._updated_ids_cache = {}
    built = []
    order = {w: n for n, w in enumerate(c.waves)}

    def grab(wave, request, *, _with_origin=False):
        assert request == 'food_acquired'
        built.append(wave.year)
        y = wave.year
        rows = [(y, f'{y}h{k}', 'Rice', 'Tin', 'purchased', 1.0, 100.0 + 50 * order[y])
                for k in range(8)]
        rows += [(y, f'{y}k{k}', 'Rice', 'Kg', 'purchased', 1.0, 100.0) for k in range(10)]
        out = pd.DataFrame(rows, columns=['t', 'i', 'j', 'u', 's', 'Quantity', 'Expenditure'])
        out = out.set_index(['t', 'i', 'j', 'u', 's'])
        path = root / 'Malawi' / y / '_' / 'food_acquired.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        to_parquet(out, path, absolute_path=True)
        return (out, 'parquet') if _with_origin else out

    monkeypatch.setattr(C.Wave, 'grab_data', grab)
    monkeypatch.setattr(C.Country, '_join_v_from_sample', lambda self, df, *a, **k: df)
    try:
        yield c, root, built
    finally:
        data_root.cache_clear()


def spy_on_reads(monkeypatch):
    seen = []
    real = C.Country._aggregate_wave_data

    def spy(self, waves=None, method_name=None, currency=None):
        seen.append((None if waves is None else list(waves), method_name))
        return real(self, waves, method_name, currency=currency)

    monkeypatch.setattr(C.Country, '_aggregate_wave_data', spy)
    return seen


KG_CALLS = [('food_quantities', {}), ('food_quantities', {'units': 'kgs'}),
            ('food_prices', {}), ('food_prices', {'units': 'kgvalue'})]
NON_KG_CALLS = [('food_quantities', {'units': 'units'}),
                ('food_prices', {'units': 'unitvalue'}),
                ('food_expenditures', {})]


class TestTheDispatch:
    @pytest.mark.parametrize('table,kw', KG_CALLS)
    def test_a_kg_subset_reads_every_wave(self, native_food, monkeypatch, table, kw):
        c, _, _ = native_food
        seen = spy_on_reads(monkeypatch)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            getattr(c, table)(waves=[c.waves[1]], **kw)
        assert seen[0] == (None, 'food_acquired')

    @pytest.mark.parametrize('table,kw', NON_KG_CALLS)
    def test_a_mode_with_no_factor_stays_on_the_request(self, native_food, monkeypatch, table, kw):
        c, _, built = native_food
        seen = spy_on_reads(monkeypatch)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            getattr(c, table)(waves=[c.waves[1]], **kw)
        assert seen[0] == ([c.waves[1]], 'food_acquired')
        assert built == [c.waves[1]], 'built another wave it does not need'

    @pytest.mark.parametrize('table,kw', KG_CALLS)
    def test_every_wave_named_is_the_all_waves_line(self, native_food, monkeypatch, table, kw):
        c, _, _ = native_food
        seen = spy_on_reads(monkeypatch)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            getattr(c, table)(waves=list(reversed(c.waves)), **kw)
        assert seen[0] == (list(reversed(c.waves)), 'food_acquired')

    @pytest.mark.parametrize('table,kw', KG_CALLS + NON_KG_CALLS)
    def test_a_subset_is_a_slice_on_a_cold_and_a_warm_read(self, native_food, table, kw):
        c, _, _ = native_food
        w = c.waves[1]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cold = getattr(c, table)(waves=[w], **kw)
            everything = getattr(c, table)(**kw)
            warm = getattr(c, table)(waves=[w], **kw)
        expected = of_wave(everything, w)
        assert len(expected)
        pd.testing.assert_frame_equal(cold, expected, check_exact=True)
        pd.testing.assert_frame_equal(warm, expected, check_exact=True)

    def test_a_multi_wave_subset_is_a_slice(self, native_food):
        c, _, _ = native_food
        s = [c.waves[0], c.waves[-1]]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            everything = c.food_quantities()
            got = c.food_quantities(waves=s)
        pd.testing.assert_frame_equal(got, of_wave(everything, *s), check_exact=True)

    def test_the_cold_read_leaves_full_coverage(self, native_food):
        c, root, built = native_food
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities(waves=[c.waves[1]])
        assert sorted(read_parquet_cache_waves(root / 'Malawi/var/food_acquired.parquet')) == sorted(c.waves)
        assert sorted(built) == sorted(c.waves)

    def test_assume_cache_fresh_serves_the_same_slice(self, native_food):
        c, _, _ = native_food
        w = c.waves[1]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            everything = c.food_quantities()
            c.assume_cache_fresh = True
            got = c.food_quantities(waves=[w])
        pd.testing.assert_frame_equal(got, of_wave(everything, w), check_exact=True)

    def test_the_make_backend_serves_the_same_slice(self, native_food, monkeypatch):
        c, _, _ = native_food
        w = c.waves[1]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            everything = c.food_quantities()
            monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')
            got = c.food_quantities(waves=[w])
        pd.testing.assert_frame_equal(got, of_wave(everything, w), check_exact=True)

    def test_the_rungs_really_do_differ_here(self, native_food):
        """The fixture has a request-dependent tin, or this tier proves nothing."""
        c, _, _ = native_food
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            fa = c.food_acquired()
            w = c.waves[-1]
            alone = food_kg_factors(of_wave(fa, w))
            pooled = food_kg_factors(fa, serve_waves=[w])
        tin = alone.index.get_level_values('u') == 'Tin'
        assert not np.allclose(alone.loc[tin, 'kg_per_unit'], pooled.loc[tin, 'kg_per_unit'])


class TestLsmsNoCache:
    def test_a_subset_is_estimated_from_all_waves(self, native_food, monkeypatch):
        c, _, built = native_food
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            everything = c.food_quantities()
            built.clear()
            monkeypatch.setenv('LSMS_NO_CACHE', '1')
            got = c.food_quantities(waves=[c.waves[1]])
        assert sorted(built) == sorted(c.waves)
        pd.testing.assert_frame_equal(got, of_wave(everything, c.waves[1]), check_exact=True)

    def test_it_leaves_full_coverage(self, native_food, monkeypatch):
        """Re-pinned for GH #1455: this parquet was removed while such a build
        could be degraded yet graded fresh.  That build is now correct, and the
        next ordinary read serves it."""
        c, root, built = native_food
        w = c.waves[1]
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = c.food_quantities(waves=[w])
        path = root / 'Malawi/var/food_acquired.parquet'
        assert sorted(read_parquet_cache_waves(path)) == sorted(c.waves)
        monkeypatch.delenv('LSMS_NO_CACHE')
        built.clear()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            again = c.food_quantities(waves=[w])
        assert built == []
        pd.testing.assert_frame_equal(again, got, check_exact=True)

    def test_zero_is_not_the_lever(self, native_food, monkeypatch):
        """Re-pinned for GH #1455's ruling: only 1, true or yes turn the flag on."""
        c, root, _ = native_food
        monkeypatch.setenv('LSMS_NO_CACHE', '0')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities(waves=[c.waves[1]])
        path = root / 'Malawi/var/food_acquired.parquet'
        assert sorted(read_parquet_cache_waves(path)) == sorted(c.waves)

    def test_a_parquet_only_served_stays(self, native_food, monkeypatch):
        """``LSMS_NO_CACHE=0`` passes the read gate, so a fresh parquet is served
        and not rewritten."""
        c, root, _ = native_food
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_acquired()
            before = (root / 'Malawi/var/food_acquired.parquet').stat().st_mtime_ns
            monkeypatch.setenv('LSMS_NO_CACHE', '0')
            c.food_quantities(waves=[c.waves[1]])
        assert (root / 'Malawi/var/food_acquired.parquet').stat().st_mtime_ns == before

    def test_assume_cache_fresh_keeps_what_it_served(self, native_food, monkeypatch):
        c, root, _ = native_food
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_acquired()
            before = (root / 'Malawi/var/food_acquired.parquet').stat().st_mtime_ns
            monkeypatch.setenv('LSMS_NO_CACHE', '1')
            c.assume_cache_fresh = True
            c.food_quantities(waves=[c.waves[1]])
        assert (root / 'Malawi/var/food_acquired.parquet').stat().st_mtime_ns == before

    def test_an_all_waves_read_keeps_its_parquet(self, native_food, monkeypatch):
        c, root, _ = native_food
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities()
        assert (root / 'Malawi/var/food_acquired.parquet').exists()

    def test_a_mode_with_no_factor_writes_what_it_wrote(self, native_food, monkeypatch):
        c, root, _ = native_food
        monkeypatch.setenv('LSMS_NO_CACHE', '1')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities(waves=[c.waves[1]], units='units')
        assert read_parquet_cache_waves(root / 'Malawi/var/food_acquired.parquet') == [c.waves[1]]


class TestTheAuditsDescribeTheServedWaves:
    @pytest.mark.parametrize('audit', ['check_declared_columns', 'check_quantities',
                                       'check_index_levels'])
    def test_a_subset_audits_food_acquired_on_the_subset(self, native_food, monkeypatch, audit):
        c, _, _ = native_food
        seen = []
        real = getattr(C, audit)

        def spy(df, *a, **k):
            if k.get('table') == 'food_acquired':
                seen.append(set(df.index.get_level_values('t')))
            return real(df, *a, **k)

        monkeypatch.setattr(C, audit, spy)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities(waves=[c.waves[1]])
        assert seen == [{c.waves[1]}]
        seen.clear()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities()
        assert seen == [set(c.waves)]

    def test_the_scope_ends_with_the_read(self, native_food):
        c, _, _ = native_food
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            c.food_quantities(waves=[c.waves[1]])
        assert C._READ_AUDIT_WAVES.get() is None


def _slice_of(table, country, waves, **kw):
    c = ll.Country(country)
    everything = getattr(c, table)(**kw)
    t = everything.index.get_level_values('t').astype(str)
    return getattr(c, table)(waves=waves, **kw), everything[t.isin(set(waves))]


@pytest.mark.requires_s3
class TestOnRealData:
    """Mali: a small panel where 2014-15 borrows from the EHCVM waves (issue table)."""

    @pytest.mark.parametrize('table,kw', [
        ('food_quantities', {'units': 'kgs'}),
        ('food_prices', {'units': 'kgvalue'}),
        ('food_quantities', {'units': 'units'}),
        ('food_prices', {'units': 'unitvalue'}),
        ('food_expenditures', {}),
    ])
    @pytest.mark.parametrize('waves', [['2014-15'], ['2014-15', '2021-22']])
    def test_a_subset_is_a_slice(self, table, kw, waves):
        got, expected = _slice_of(table, 'Mali', waves, **kw)
        assert len(expected)
        pd.testing.assert_frame_equal(got, expected, check_exact=True)

    @pytest.mark.parametrize('kw', [{'labels': '2018-19'}, {'market': 'Region'}])
    def test_labels_and_market_keep_the_slice(self, kw):
        got, expected = _slice_of('food_quantities', 'Mali', ['2014-15'], units='kgs', **kw)
        pd.testing.assert_frame_equal(got, expected, check_exact=True)

    def test_the_partition_partitions_the_served_rows(self):
        c = ll.Country('Mali')
        n = len(c.food_acquired(waves=['2014-15']))
        got = c.food_quantities(waves=['2014-15'])
        assert sum(got.attrs['kg_factor_sources'].values()) == n

    def test_feature_serves_the_same_slice(self):
        got = ll.Feature('food_quantities')(['Mali'], waves=['2014-15'], units='kgs')
        one = ll.Country('Mali').food_quantities(units='kgs', waves=['2014-15'])
        assert np.isclose(got.xs('kg', level='u')['Quantity'].sum(),
                          one.xs('kg', level='u')['Quantity'].sum())

    def test_a_wave_with_no_food_table_is_an_empty_slice(self):
        c = ll.Country('Mali')
        got = c.food_quantities(waves=['2017-18'])
        assert len(got) == 0
        assert list(got.index.names) == list(c.food_quantities().index.names)
