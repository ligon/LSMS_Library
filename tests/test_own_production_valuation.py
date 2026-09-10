"""Own-production / in-kind valuation — GH #585.

``food_expenditures(basis='total', valuation=...)`` values the non-purchased
``food_acquired`` rows a country's source left without an ``Expenditure``, at
purchase-side unit prices, through the same-household rung and/or the
``median_price_valuation`` geographic ladder.  It is OPT-IN and never stored.

The first class is the one that matters most: ``valuation=None`` must be
byte-identical to the pre-#585 behaviour.  It is pinned against a verbatim
copy of the pre-change function body, not against a re-derivation.
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    ValuationLadderWarning,
    VALUATION_SOURCES,
    food_acquired_valued,
    food_expenditures_from_acquired,
)


# ---------------------------------------------------------------------------
# The pre-#585 body, copied VERBATIM from git 9e4c0867
# lsms_library/transformations.py::food_expenditures_from_acquired.  If a
# refactor changes what the default path returns, these tests go red.
# ---------------------------------------------------------------------------
def _legacy_food_expenditures_from_acquired(df, basis='purchased'):
    from lsms_library.transformations import _normalize_columns
    valid_basis = {'purchased', 'total'}
    if basis not in valid_basis:
        raise ValueError(
            f"food_expenditures basis= must be one of {sorted(valid_basis)}, "
            f"got {basis!r}"
        )

    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns:
        raise ValueError("food_acquired must have an 'Expenditure' column")

    idx_names = list(df.index.names)

    x = df[['Expenditure']].replace(0, np.nan).dropna()
    if basis == 'purchased' and 's' in idx_names:
        x = x[x.index.get_level_values('s').astype(str) == 'purchased']

    group_by = [n for n in ['t', 'i', 'j', 's'] if n in idx_names]
    x = x.groupby(group_by).sum()
    return x


def _fa(rows, names=('t', 'v', 'i', 'j', 'u', 's')):
    """Build a food_acquired frame from (index..., Quantity, Expenditure)."""
    n = len(names)
    idx = pd.MultiIndex.from_tuples([r[:n] for r in rows], names=list(names))
    return pd.DataFrame({'Quantity': [r[n] for r in rows],
                         'Expenditure': [r[n + 1] for r in rows]}, index=idx)


def _corpus():
    """Two clusters in one district, one wave.

    ``v1`` carries four maize purchases at unit prices 10, 10, 12, 12 and
    ``v2`` two at 20, 30; every household also produces maize.  Beans are
    purchased only in ``v2``.  Deliberately small so every ladder rung can be
    reached by choosing ``threshold``.
    """
    rows = [
        # t     v    i     j       u     s            Qty   Exp
        ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 2.0, 20.0),   # p=10
        ('20', 'v1', 'h2', 'maize', 'kg', 'purchased', 3.0, 30.0),   # p=10
        ('20', 'v1', 'h3', 'maize', 'kg', 'purchased', 5.0, 60.0),   # p=12
        ('20', 'v1', 'h4', 'maize', 'kg', 'purchased', 1.0, 12.0),   # p=12
        ('20', 'v2', 'h5', 'maize', 'kg', 'purchased', 1.0, 20.0),   # p=20
        ('20', 'v2', 'h6', 'maize', 'kg', 'purchased', 2.0, 60.0),   # p=30
        ('20', 'v1', 'h1', 'maize', 'kg', 'produced', 10.0, np.nan),
        ('20', 'v2', 'h5', 'maize', 'kg', 'produced', 10.0, np.nan),
        ('20', 'v2', 'h7', 'beans', 'kg', 'purchased', 4.0, 40.0),   # p=10
        ('20', 'v1', 'h8', 'beans', 'kg', 'produced', 3.0, np.nan),
    ]
    return _fa(rows)


def _geo():
    """Both clusters in district D1; one region R1."""
    idx = pd.MultiIndex.from_tuples([('20', 'v1'), ('20', 'v2')],
                                    names=['t', 'v'])
    return pd.DataFrame({'District': ['D1', 'D1'], 'Region': ['R1', 'R1']},
                        index=idx)


class TestDefaultPathUnchanged:
    """valuation=None must run the pre-#585 code path verbatim."""

    @pytest.mark.parametrize('basis', ['purchased', 'total'])
    def test_synthetic_frame_identical(self, basis):
        df = _corpus()
        got = food_expenditures_from_acquired(df, basis=basis)
        want = _legacy_food_expenditures_from_acquired(df, basis=basis)
        pd.testing.assert_frame_equal(got, want)

    def test_no_s_level_identical(self):
        df = _fa([('20', 'h1', 'j1', 4.0, 40.0),
                  ('20', 'h1', 'j2', 1.0, 0.0)],
                 names=('t', 'i', 'j'))
        for basis in ('purchased', 'total'):
            pd.testing.assert_frame_equal(
                food_expenditures_from_acquired(df, basis=basis),
                _legacy_food_expenditures_from_acquired(df, basis=basis))

    def test_no_valuation_attrs_on_the_default_path(self):
        out = food_expenditures_from_acquired(_corpus(), basis='total')
        assert not any(k.startswith('valuation') for k in out.attrs)

    @pytest.mark.slow
    @pytest.mark.parametrize('basis', ['purchased', 'total'])
    def test_warm_uganda_identical(self, basis):
        """The real thing: warm Uganda food_acquired, both bases."""
        ll = pytest.importorskip('lsms_library')
        try:
            fa = ll.Country('Uganda')._aggregate_wave_data(None, 'food_acquired')
        except Exception as exc:                      # pragma: no cover
            pytest.skip(f'Uganda food_acquired unavailable: {exc!r}')
        pd.testing.assert_frame_equal(
            food_expenditures_from_acquired(fa, basis=basis),
            _legacy_food_expenditures_from_acquired(fa, basis=basis))


class TestOwnPriceRung:
    def test_fills_only_same_household_same_t_j_u(self):
        out = food_acquired_valued(_corpus(), 'own_price')
        src = out['ValuationSource']
        # h1 and h5 each purchased maize/kg in the same wave -> valued.
        assert src.xs(('h1', 'maize', 'produced'),
                      level=('i', 'j', 's')).iloc[0] == 'own_price'
        assert src.xs(('h5', 'maize', 'produced'),
                      level=('i', 'j', 's')).iloc[0] == 'own_price'
        # h8 produced beans but never bought any -> no own price, and NOTHING
        # is borrowed from h7, who did.
        assert src.xs(('h8', 'beans', 'produced'),
                      level=('i', 'j', 's')).iloc[0] == 'none'
        # h1's own maize price is 20/2 = 10 -> 10 kg produced is worth 100.
        row = out.xs(('h1', 'maize', 'produced'), level=('i', 'j', 's'))
        assert row['valuation_price'].iloc[0] == pytest.approx(10.0)
        assert row['Expenditure'].iloc[0] == pytest.approx(100.0)

    def test_own_price_pools_a_households_repeat_purchases(self):
        """Two purchases of the same item -> ONE quantity-weighted price."""
        df = _fa([
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0),
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 3.0, 90.0),
            ('20', 'v1', 'h1', 'maize', 'kg', 'produced', 2.0, np.nan),
        ])
        out = food_acquired_valued(df, 'own_price')
        # (10 + 90) / (1 + 3) = 25, not mean(10, 30) = 20.
        assert out['valuation_price'].iloc[-1] == pytest.approx(25.0)

    def test_a_different_unit_is_a_different_price(self):
        df = _fa([
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 2.0, 20.0),
            ('20', 'v1', 'h1', 'maize', 'sack', 'produced', 1.0, np.nan),
        ])
        out = food_acquired_valued(df, 'own_price')
        assert out['ValuationSource'].iloc[-1] == 'none'


class TestMedianPriceRung:
    def test_hand_computed_v_rung(self):
        """v1's four maize prices are 10, 10, 12, 12 -> median 11."""
        out = food_acquired_valued(_corpus(), 'median_price', geo=_geo(),
                                   threshold=4)
        row = out.xs(('h1', 'maize', 'produced'), level=('i', 'j', 's'))
        assert row['ValuationSource'].iloc[0] == 'median_price'
        assert row['valuation_price'].iloc[0] == pytest.approx(11.0)
        assert row['Expenditure'].iloc[0] == pytest.approx(110.0)

    def test_hand_computed_district_rung_when_v_is_too_thin(self):
        """v2 has only 2 maize prices; at threshold=4 it borrows D1's median
        of all six (10, 10, 12, 12, 20, 30) = 12."""
        out = food_acquired_valued(_corpus(), 'median_price', geo=_geo(),
                                   threshold=4)
        row = out.xs(('h5', 'maize', 'produced'), level=('i', 'j', 's'))
        assert row['valuation_price'].iloc[0] == pytest.approx(12.0)

    def test_national_fallback_is_unconditional(self):
        """beans are purchased once, nationally: below every threshold, yet
        h8's produced beans are still valued -- at that one price, 10."""
        out = food_acquired_valued(_corpus(), 'median_price', geo=_geo(),
                                   threshold=99)
        row = out.xs(('h8', 'beans', 'produced'), level=('i', 'j', 's'))
        assert row['ValuationSource'].iloc[0] == 'median_price'
        assert row['valuation_price'].iloc[0] == pytest.approx(10.0)

    def test_the_price_pool_is_purchases_only(self):
        """A produced row the survey DID value must not feed the price pool.

        Uganda is the live instance -- its produced/in-kind rows carry the
        household's own valuation, which the price study measured at ~23%
        above a sale price.  Letting it in would make the ladder partly a
        median of the thing it is imputing.
        """
        df = _fa([
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0),
            # A survey-imputed produced value at an absurd unit price.
            ('20', 'v1', 'h2', 'maize', 'kg', 'produced', 1.0, 1000.0),
            ('20', 'v1', 'h3', 'maize', 'kg', 'produced', 1.0, np.nan),
        ])
        out = food_acquired_valued(df, 'median_price', threshold=1)
        assert out['valuation_price'].iloc[-1] == pytest.approx(10.0)

    def test_waves_do_not_pool(self):
        """``t`` is an item key: median_price_valuation has no wave axis, so
        without it the national rung would pool currency across a decade."""
        df = _fa([
            ('10', 'v1', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0),
            ('20', 'v1', 'h2', 'maize', 'kg', 'purchased', 1.0, 1000.0),
            ('10', 'v1', 'h3', 'maize', 'kg', 'produced', 1.0, np.nan),
            ('20', 'v1', 'h4', 'maize', 'kg', 'produced', 1.0, np.nan),
        ])
        out = food_acquired_valued(df, 'median_price', threshold=1)
        prices = out['valuation_price'].to_numpy()
        assert prices[2] == pytest.approx(10.0)
        assert prices[3] == pytest.approx(1000.0)

    def test_null_geo_never_forms_a_cell(self):
        """Null ``v`` must not collapse to one pseudo-cluster.

        Ten households with an unrecoverable cluster id would otherwise clear
        threshold=10 as the cluster ``'nan'``.  Each null gets its own token,
        so the cell count is 1 and the rows fall through to the national rung.
        """
        rows = []
        for k in range(10):
            rows.append(('20', None, f'x{k}', 'maize', 'kg', 'purchased',
                         1.0, 100.0))
        for k in range(10):
            rows.append(('20', 'v1', f'y{k}', 'maize', 'kg', 'purchased',
                         1.0, 5.0))
        rows.append(('20', None, 'z', 'maize', 'kg', 'produced', 1.0, np.nan))
        out = food_acquired_valued(_fa(rows), 'median_price', threshold=10)
        # National median of ten 100s and ten 5s = 52.5; the pseudo-cluster
        # median would have been 100.
        assert out['valuation_price'].iloc[-1] == pytest.approx(52.5)


class TestProvenance:
    def test_sources_partition_the_input(self):
        out = food_acquired_valued(_corpus(), ('own_price', 'median_price'),
                                   geo=_geo(), threshold=4)
        counts = out.attrs['valuation_sources']
        assert set(VALUATION_SOURCES) <= set(counts)
        assert sum(counts[s] for s in VALUATION_SOURCES) == len(out)
        assert set(out['ValuationSource']) <= set(VALUATION_SOURCES)

    def test_candidates_is_outside_the_partition(self):
        out = food_acquired_valued(_corpus(), 'own_price')
        c = out.attrs['valuation_sources']
        # 3 unvalued non-purchased rows; 2 got an own price, 1 did not.
        assert c['candidates'] == 3
        assert c['own_price'] == 2
        assert c['none'] == 1

    def test_ladder_recorded(self):
        out = food_acquired_valued(_corpus(), 'median_price', geo=_geo(),
                                   threshold=4)
        assert out.attrs['valuation_geo_levels'] == ['v', 'District', 'Region']
        assert out.attrs['valuation'] == ('median_price',)

    def test_counts_ride_on_the_aggregate(self):
        out = food_expenditures_from_acquired(
            _corpus(), basis='total', valuation=('own_price', 'median_price'),
            geo=_geo(), threshold=4)
        assert out.attrs['valuation_sources']['candidates'] == 3
        assert out.attrs['valuation'] == ('own_price', 'median_price')

    def test_item_grain_frame_is_indexed_like_the_input(self):
        df = _corpus()
        out = food_acquired_valued(df, 'own_price')
        pd.testing.assert_index_equal(out.index, df.index)


class TestReportedRowsUntouched:
    def test_a_recorded_produced_value_is_reported_not_revalued(self):
        """Serbia / GhanaSPS / Uganda record a produced value; it stands."""
        df = _fa([
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0),
            ('20', 'v1', 'h1', 'maize', 'kg', 'produced', 5.0, 7.0),
        ])
        out = food_acquired_valued(df, ('own_price', 'median_price'),
                                   threshold=1)
        assert out['ValuationSource'].iloc[-1] == 'reported'
        assert out['Expenditure'].iloc[-1] == pytest.approx(7.0)
        assert pd.isna(out['valuation_price'].iloc[-1])

    def test_a_zero_value_is_a_candidate_not_a_report(self):
        """The existing body treats 0 as missing (``replace(0, np.nan)``);
        the valuation must agree, or the two halves would disagree about what
        a zero-valued produced row is."""
        df = _fa([
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0),
            ('20', 'v1', 'h1', 'maize', 'kg', 'produced', 5.0, 0.0),
        ])
        out = food_acquired_valued(df, 'own_price')
        assert out['ValuationSource'].iloc[-1] == 'own_price'
        assert out['Expenditure'].iloc[-1] == pytest.approx(50.0)

    def test_a_purchase_with_no_outlay_is_never_a_candidate(self):
        df = _fa([
            ('20', 'v1', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0),
            ('20', 'v1', 'h2', 'maize', 'kg', 'purchased', 5.0, np.nan),
        ])
        out = food_acquired_valued(df, ('own_price', 'median_price'),
                                   threshold=1)
        assert out['ValuationSource'].iloc[-1] == 'none'
        assert out.attrs['valuation_sources']['candidates'] == 0


class TestApiSurface:
    def test_scalar_median_price_does_not_run_own_price(self):
        """A scalar names EXACTLY that rung -- no hidden first rung."""
        out = food_acquired_valued(_corpus(), 'median_price', geo=_geo(),
                                   threshold=4)
        assert set(out['ValuationSource']) == {'reported', 'median_price'}
        assert out.attrs['valuation_sources']['own_price'] == 0

    def test_order_is_the_order_given(self):
        """own_price first: h1 gets its OWN 10, not v1's median 11."""
        first = food_acquired_valued(_corpus(), ('own_price', 'median_price'),
                                     geo=_geo(), threshold=4)
        second = food_acquired_valued(_corpus(), ('median_price', 'own_price'),
                                      geo=_geo(), threshold=4)
        h1 = ('h1', 'maize', 'produced')
        assert first.xs(h1, level=('i', 'j', 's'))['valuation_price'].iloc[0] \
            == pytest.approx(10.0)
        assert second.xs(h1, level=('i', 'j', 's'))['valuation_price'].iloc[0] \
            == pytest.approx(11.0)

    def test_valuation_requires_basis_total(self):
        with pytest.raises(ValueError, match="requires basis='total'"):
            food_expenditures_from_acquired(_corpus(), basis='purchased',
                                            valuation='own_price')

    @pytest.mark.parametrize('bad', ['market_price', ('own_price', 'nope'),
                                     ('own_price', 'own_price'), ()])
    def test_bad_valuation_raises(self, bad):
        with pytest.raises(ValueError):
            food_expenditures_from_acquired(_corpus(), basis='total',
                                            valuation=bad)

    def test_no_s_level_raises(self):
        df = _fa([('20', 'h1', 'j1', 4.0, np.nan)], names=('t', 'i', 'j'))
        with pytest.raises(ValueError, match="no 's'"):
            food_acquired_valued(df, 'own_price')

    def test_geo_without_v_raises(self):
        df = _fa([('20', 'h1', 'maize', 'kg', 'purchased', 1.0, 10.0)],
                 names=('t', 'i', 'j', 'u', 's'))
        with pytest.raises(ValueError, match='carries no'):
            food_acquired_valued(df, 'median_price', geo=_geo())

    def test_geo_must_be_t_v_indexed(self):
        bad = _geo().reset_index('v').set_index('District', append=True)
        with pytest.raises(ValueError, match=r'indexed by \(t, v\)'):
            food_acquired_valued(_corpus(), 'median_price', geo=bad)

    def test_valuation_never_changes_the_output_grain(self):
        plain = food_expenditures_from_acquired(_corpus(), basis='total')
        valued = food_expenditures_from_acquired(
            _corpus(), basis='total', valuation='own_price')
        assert plain.index.names == valued.index.names
        # It only ever ADDS rows -- a candidate that gets a value.
        assert set(plain.index) <= set(valued.index)
        assert valued['Expenditure'].sum() >= plain['Expenditure'].sum()


class TestDegradedLadder:
    def test_no_geo_at_all_leaves_v_and_national(self):
        out = food_acquired_valued(_corpus(), 'median_price', threshold=4)
        assert out.attrs['valuation_geo_levels'] == ['v']

    def test_country_path_warns_when_a_rung_is_missing(self):
        """``Country._valuation_geo`` must NAME the rungs it could not build.

        Ethiopia is the motivating case: it degrades to a coarser ladder than
        EPAR's admin cascade and has to say so.
        """
        from lsms_library.country import Country

        class _Stub(Country):
            def __init__(self):
                self.name = 'Nowhere'

            def cluster_features(self, waves=None):
                idx = pd.MultiIndex.from_tuples([('20', 'v1')],
                                                names=['t', 'v'])
                return pd.DataFrame({'Region': ['R1']}, index=idx)

        with pytest.warns(ValuationLadderWarning, match=r"\['District'\]"):
            geo = _Stub()._valuation_geo()
        assert list(geo.columns) == ['Region']

    def test_a_rung_that_resolves_to_nothing_warns(self):
        """A geo column that is NULL for the cluster is not an available rung.

        Measured live: Uganda's ``cluster_features`` carries a null
        ``District`` for 27.8% of 2010-11 clusters and 33.0% of 2011-12's, so
        the ladder is shorter for those rows than
        ``attrs['valuation_geo_levels']`` advertises.  Silence there would be
        a lie about which market the price came from.
        """
        geo = _geo().copy()
        geo.loc[('20', 'v2'), 'District'] = None
        with pytest.warns(ValuationLadderWarning, match='resolved to nothing'):
            food_acquired_valued(_corpus(), 'median_price', geo=geo,
                                 threshold=4)

    def test_a_cluster_missing_from_the_geo_frame_warns(self):
        """The dtype/spelling half of the same failure."""
        geo = _geo().drop(index=('20', 'v2'))
        with pytest.warns(ValuationLadderWarning, match='resolved to nothing'):
            food_acquired_valued(_corpus(), 'median_price', geo=geo,
                                 threshold=4)

    def test_a_complete_join_is_silent(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error', ValuationLadderWarning)
            food_acquired_valued(_corpus(), 'median_price', geo=_geo(),
                                 threshold=4)

    def test_country_path_survives_no_cluster_features(self):
        from lsms_library.country import Country

        class _Stub(Country):
            def __init__(self):
                self.name = 'Nowhere'

            def cluster_features(self, waves=None):
                raise RuntimeError('no such table')

        with pytest.warns(ValuationLadderWarning, match='unavailable'):
            assert _Stub()._valuation_geo() is None


@pytest.mark.slow
class TestCountryApi:
    def test_uganda_default_unchanged_and_attrs_ride_through(self):
        ll = pytest.importorskip('lsms_library')
        try:
            c = ll.Country('Uganda')
            plain = c.food_expenditures(basis='total')
        except Exception as exc:                      # pragma: no cover
            pytest.skip(f'Uganda unavailable: {exc!r}')
        assert not any(k.startswith('valuation') for k in plain.attrs)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            valued = c.food_expenditures(
                basis='total', valuation=('own_price', 'median_price'))
        counts = valued.attrs['valuation_sources']
        assert sum(counts[s] for s in VALUATION_SOURCES) > 0
        # Uganda's source RECORDS a produced/in-kind value, so almost every
        # row is `reported` -- the finding, not a failure.
        assert counts['reported'] > 100 * counts['candidates']
        assert valued['Expenditure'].sum() >= plain['Expenditure'].sum()

    def test_attrs_survive_the_market_index_merge(self):
        """``_add_market_index`` is the disagreeing merge the explicit
        re-attach exists for."""
        ll = pytest.importorskip('lsms_library')
        try:
            c = ll.Country('Uganda')
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                out = c.food_expenditures(basis='total', valuation='own_price',
                                          market='Region')
        except Exception as exc:                      # pragma: no cover
            pytest.skip(f'Uganda unavailable: {exc!r}')
        assert 'm' in out.index.names
        assert 'valuation_sources' in out.attrs

    def test_attrs_survive_a_labels_reaggregation(self):
        ll = pytest.importorskip('lsms_library')
        c = ll.Country('Uganda')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = c.food_expenditures(basis='total', valuation='own_price',
                                      labels='Aggregate')
        assert 'valuation_sources' in out.attrs

    def test_valuation_rejected_on_another_table(self):
        ll = pytest.importorskip('lsms_library')
        c = ll.Country('Uganda')
        with pytest.raises(TypeError, match='valuation'):
            c.food_quantities(valuation='own_price')

    def test_valuation_requires_explicit_total_basis(self):
        ll = pytest.importorskip('lsms_library')
        c = ll.Country('Uganda')
        with pytest.raises(ValueError, match="basis='total'"):
            c.food_expenditures(valuation='own_price')
