"""GH #891 / #892 -- the L2-country cache across wave subsets.

#891: single-wave and all-wave requests share one cache path
(``var/{table}.parquet``) while ``Country._table_cache_hash`` folded the
REQUESTED wave set into the hash, so alternating requests each graded the
other's cache stale and rebuilt it.

#892: the ``assume_cache_fresh`` short-circuit returned that parquet whole,
ignoring ``waves`` entirely -- a one-wave request was served every wave.

The two are one mechanism: the wave set in the hash is what *prevented* #892 on
the ordinary path, so removing it without adding a coverage manifest and a
serve-path filter would have propagated #892 to every call.  These tests pin
both halves, and the invariant that the hash change moved no existing all-wave
hash.
"""
import json
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import (
    to_parquet,
    read_parquet_cache_waves,
    read_parquet_cache_hash,
)


COUNTRY = 'Albania'   # 6 waves, no script-path complications


def _country():
    try:
        return ll.Country(COUNTRY)
    except Exception:                                   # pragma: no cover
        pytest.skip(f"{COUNTRY} config unavailable")


class TestHashIsWaveSetIndependent:
    """The #891 fix proper: one path, one hash."""

    def test_subset_and_all_waves_agree(self):
        c = _country()
        for table in ('household_roster', 'sample'):
            h_all = c._table_cache_hash(table, c.waves)
            if h_all is None:
                continue
            assert h_all == c._table_cache_hash(table, [c.waves[-1]]), (
                f"{table}: a single-wave request computes a different hash than "
                "an all-wave one, so they invalidate each other (GH #891)")
            assert h_all == c._table_cache_hash(table, c.waves[:2])
            assert h_all == c._table_cache_hash(table, list(reversed(c.waves)))

    def test_all_waves_request_is_unchanged_by_the_fix(self):
        """The union folds ``set(waves) | set(self.waves)``.

        For an all-waves request that is the same SET as the pre-#891
        ``sorted(waves)``, which is what keeps the change from cold-rebuilding
        every warm all-wave parquet in the corpus.  Pinned as an explicit
        equality on the sorted wave lists the payload is built from, because a
        future edit that folded, say, the request ORDER would break it silently.
        """
        c = _country()
        assert sorted(set(c.waves) | set(c.waves)) == sorted(c.waves)

    def test_an_exotic_wave_is_not_silently_unhashed(self):
        """A wave outside ``self.waves`` still participates, rather than being
        dropped on the floor by a plain ``self.waves`` substitution."""
        c = _country()
        h_all = c._table_cache_hash('household_roster', c.waves)
        if h_all is None:
            pytest.skip("table not introspectable")
        assert c._table_cache_hash('household_roster', c.waves + ['1899']) != h_all


class TestUnionWaves:
    def test_ordered_by_declared_wave_order(self):
        c = _country()
        assert c._union_waves([c.waves[3]], [c.waves[0], c.waves[5]]) == [
            c.waves[0], c.waves[3], c.waves[5]]

    def test_is_idempotent_and_monotone(self):
        c = _country()
        once = c._union_waves([c.waves[0]], [c.waves[1]])
        assert c._union_waves(once, once) == once
        grown = c._union_waves(once, [c.waves[2]])
        assert set(once) < set(grown), "coverage must only ever grow"

    def test_unknown_waves_are_appended_deterministically(self):
        c = _country()
        assert c._union_waves(['zzz', 'aaa'], [c.waves[0]]) == [c.waves[0], 'aaa', 'zzz']


class TestRestrictToWaves:
    def _frame(self, waves):
        idx = pd.MultiIndex.from_tuples(
            [(w, f"hh{n}") for w in waves for n in range(3)], names=['t', 'i'])
        return pd.DataFrame({'x': range(len(idx))}, index=idx)

    def test_selects_only_the_requested_waves(self):
        c = _country()
        df = self._frame(['2002', '2005', '2012'])
        out = c._restrict_to_waves(df, ['2005'])
        assert sorted(set(out.index.get_level_values('t'))) == ['2005']
        assert len(out) == 3

    def test_is_a_row_selection_not_an_aggregation(self):
        """Core aggregates nothing (GH #323).  Every surviving row must be
        identical to its counterpart in the input -- no reducer, no collapse."""
        c = _country()
        df = self._frame(['2002', '2005'])
        out = c._restrict_to_waves(df, ['2005'])
        pd.testing.assert_frame_equal(out, df.xs('2005', level='t', drop_level=False))

    def test_preserves_attrs(self):
        """Downstream of ``id_walk``; losing ``attrs`` here would re-run it and
        produce duplicate index entries on transitive panel chains."""
        c = _country()
        df = self._frame(['2002', '2005'])
        df.attrs['id_converted'] = True
        assert c._restrict_to_waves(df, ['2005']).attrs.get('id_converted') is True

    def test_a_full_waves_request_passes_through_untouched(self):
        """Not merely "the filter matches everything" -- the ordinary
        ``Country.table()`` call must be byte-identical to pre-#891."""
        c = _country()
        df = self._frame(list(c.waves))
        assert c._restrict_to_waves(df, list(c.waves)) is df

    def test_a_null_wave_row_is_not_deleted_by_a_full_waves_request(self):
        """A null ``t`` stringifies to 'nan' and matches no requested wave, so a
        naive filter DELETES it on every serve -- a second instance of the
        ``groupby(dropna=True)`` row deletion this repo already tracks as open
        (grain_aggregation_policy.org Section 3b).  The pass-through prevents it."""
        c = _country()
        df = self._frame(list(c.waves))
        holed = pd.concat([df, pd.DataFrame(
            {'x': [999]},
            index=pd.MultiIndex.from_tuples([(None, 'hhX')], names=['t', 'i']))])
        out = c._restrict_to_waves(holed, list(c.waves))
        assert len(out) == len(holed), "a null-t row was dropped on the serve path"

    def test_passes_through_a_frame_with_no_t_level(self):
        c = _country()
        df = pd.DataFrame({'x': [1, 2]}, index=pd.Index(['a', 'b'], name='i'))
        pd.testing.assert_frame_equal(c._restrict_to_waves(df, ['2005']), df)


class TestCoverageManifest:
    def test_roundtrips_through_to_parquet(self, tmp_path):
        df = pd.DataFrame({'x': [1]}, index=pd.MultiIndex.from_tuples(
            [('2005', 'hh1')], names=['t', 'i']))
        fn = tmp_path / 'x.parquet'
        to_parquet(df, fn, absolute_path=True, cache_hash='H',
                   cache_waves=['2002', '2005'])
        assert read_parquet_cache_waves(fn) == ['2002', '2005']
        assert read_parquet_cache_hash(fn) == 'H', "must not disturb the hash key"

    def test_absent_on_a_pre_891_parquet(self, tmp_path):
        df = pd.DataFrame({'x': [1]}, index=pd.Index(['a'], name='i'))
        fn = tmp_path / 'legacy.parquet'
        to_parquet(df, fn, absolute_path=True)
        assert read_parquet_cache_waves(fn) is None


class TestMigrationOfUnstampedParquets:
    """A pre-#891 parquet carries no manifest, so its coverage is unknown.

    It must never be *guessed*: GhanaLSS ``household_roster`` was observed
    holding 1 of 7 waves with no manifest, so "assume full coverage" would
    serve a one-wave answer to an all-wave request.
    """

    def test_unknown_coverage_is_not_treated_as_full(self, tmp_path):
        c = _country()
        fn = tmp_path / 'nomanifest.parquet'
        to_parquet(pd.DataFrame({'x': [1]}, index=pd.Index(['a'], name='i')),
                   fn, absolute_path=True)
        assert c._cache_covers_waves(fn, c.waves, 'legacy') is None
        assert c._cache_covers_waves(fn, c.waves, 'unverifiable') is None

    def test_a_fresh_hash_corroborates_full_coverage(self, tmp_path):
        """With the hash now wave-independent, a stored hash that MATCHES the
        all-waves expectation can only have been stamped by an all-waves
        request under the old scheme -- a subset build stamped a subset hash,
        which cannot match.  That is what makes the migration safe."""
        c = _country()
        fn = tmp_path / 'fresh.parquet'
        to_parquet(pd.DataFrame({'x': [1]}, index=pd.Index(['a'], name='i')),
                   fn, absolute_path=True)
        assert c._cache_covers_waves(fn, c.waves, 'fresh') == list(c.waves)

    def test_an_explicit_manifest_always_wins(self, tmp_path):
        c = _country()
        fn = tmp_path / 'stamped.parquet'
        to_parquet(pd.DataFrame({'x': [1]}, index=pd.Index(['a'], name='i')),
                   fn, absolute_path=True, cache_waves=[c.waves[0]])
        for freshness in ('fresh', 'legacy', 'unverifiable'):
            assert c._cache_covers_waves(fn, c.waves, freshness) == [c.waves[0]]


@pytest.mark.requires_s3
class TestEndToEnd:
    """The regression scenario the issue asks for, in-process against one cache."""

    def test_alternating_requests_return_exactly_what_was_requested(self):
        c = ll.Country(COUNTRY)
        for waves in (c.waves, [c.waves[-1]], c.waves, c.waves[:2], [c.waves[-1]]):
            df = c.household_roster(waves=waves)
            got = sorted({str(t) for t in df.index.get_level_values('t')})
            assert set(got) <= {str(w) for w in waves}, (
                f"requested {waves}, got waves {got}")

    def test_assume_cache_fresh_honours_the_wave_argument(self):
        """GH #892 -- this returned all six Albania waves for a one-wave
        request, 90,731 rows, with no warning."""
        one = ll.Country(COUNTRY).waves[-1]
        df = ll.Country(COUNTRY, assume_cache_fresh=True).household_roster(waves=[one])
        got = sorted({str(t) for t in df.index.get_level_values('t')})
        assert got == [str(one)], f"requested [{one}], got {got}"

    def test_a_warmed_cache_covers_a_subset_request(self):
        """After an all-waves build the manifest must make a subset request a
        HIT -- the actual point of #891."""
        from lsms_library.paths import data_root
        c = ll.Country(COUNTRY)
        c.household_roster()
        pq = data_root(COUNTRY) / 'var' / 'household_roster.parquet'
        covered = read_parquet_cache_waves(pq)
        assert covered is not None, "an all-waves build must stamp a manifest"
        assert set(map(str, c.waves)) <= set(map(str, covered))
