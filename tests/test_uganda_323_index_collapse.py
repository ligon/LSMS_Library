"""GH #323 -- Uganda: the silent duplicate-index collapse.

``_normalize_dataframe_index`` collapses a non-unique DECLARED index with
``groupby().first()``.  Uganda was hit by this in two independent ways.

**A. people_last7days (2018-19, 2019-20) -- silently WRONG (class-1).**
From 2018-19 the questionnaire went LONG: ``GSEC15A`` carries two rows per
household discriminated by ``CEA01`` in {'Household members', 'Visitors'}, and
``CEA01A-D`` are the counts *for the selected category*.  The declared index is
``(i, t)`` and ``CEA01`` was left undeclared (the YAML literally commented it
out), so both rows collided and ``first()`` kept whichever came first in the
file -- a coin flip (1,636 of 3,242 households list Visitors first).  Result:
~49% of households carried VISITOR counts, and ~1,985 households reported ZERO
people while really having some.  Members-only is not a judgement call: the
three earlier long-format waves (2005-06 GSEC14 h14q1-4, 2009-10 GSEC15A
h15a1-4, 2013-14 GSEC15 T6FQ01a-d) all map Men/Women/Boys/Girls to the source's
"hh members" block and ignore the parallel "visitors" block.  Fixed with a
``where:`` row filter, which makes the index genuinely unique so the collapse
never fires.

**B. cluster_features -- an INTENDED reduction, run silently.**
Each wave extracts one row per household then declares ``final_index: [t, v]``,
deliberately reducing to cluster grain (GH #161).  That reduction is fine; doing
it with an undeclared ``first()`` is not.  Now declared via ``aggregation:`` and
enforced.  Two real defects rode on it: ``v`` was not a cluster key in 2018-19 /
2019-20 (parish NAMES collide across districts -- 'CENTRAL' appears in ten), and
2009-10's 565 out-of-frame households had no cluster at all.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll

# NOTE: `_reduce_unique` / `_declared_aggregation` / `_apply_where` are imported
# INSIDE the tests that exercise them, not at module scope.  A module-scope
# import would raise ImportError on a pre-fix checkout and abort COLLECTION of
# the whole file -- so the data assertions below (the ones that actually
# demonstrate the bug) would never run, and "the tests fail pre-fix" would be
# true only in the uninteresting sense that a symbol is missing.  Deferring the
# import lets the data tests collect and fail on their real assertions.

COUNT_COLS = ['Men', 'Women', 'Boys', 'Girls']
LONG_WAVES = ['2018-19', '2019-20']


@pytest.fixture(scope='module')
def uga():
    return ll.Country('Uganda')


@pytest.fixture(scope='module')
def built():
    """Build the three affected tables once, capturing warnings."""
    import os
    os.environ['LSMS_NO_CACHE'] = '1'
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        c = ll.Country('Uganda')
        out = {
            'people_last7days': c.people_last7days(),
            'cluster_features': c.cluster_features(),
            'sample': c.sample(),
        }
        out['warnings'] = [str(w.message) for w in caught]
    return out


# ---------------------------------------------------------------- A ---------
class TestPeopleLast7Days:
    """The member/visitor collapse: silently WRONG for ~49% of households."""

    def test_no_silent_collapse_warning(self, built):
        """The declared index must be genuinely unique -- not collapsed."""
        bad = [m for m in built['warnings']
               if 'duplicate tuple' in m and 'groupby().first()' in m]
        assert not bad, f"people_last7days/cluster_features still collapse: {bad}"

    @pytest.mark.parametrize('wave,expected', [('2018-19', 4.51), ('2019-20', 4.53)])
    def test_mean_household_size_is_members_not_visitors(self, built, wave, expected):
        """Pre-fix this was 2.81 / 2.77 -- the mean of a coin-flipped mix of
        member and visitor rows.  Members-only restores ~4.5 people/HH."""
        f = built['people_last7days'].reset_index()
        tot = f[f.t == wave][COUNT_COLS].sum(axis=1)
        assert tot.mean() == pytest.approx(expected, abs=0.15), (
            f"{wave}: mean people/HH is {tot.mean():.2f}; expected ~{expected} "
            f"(members).  ~2.8 means the visitor rows are still winning.")

    @pytest.mark.parametrize('wave', LONG_WAVES)
    def test_no_spurious_zero_person_households(self, built, wave):
        """~1,985 households reported ZERO people because the visitor row (all
        zeros) won the collapse.  A household with zero people is only legitimate
        if its own MEMBER row is all-zero -- which is true for just 3 + 4 of them.
        """
        f = built['people_last7days'].reset_index()
        s = f[f.t == wave]
        n_zero = int((s[COUNT_COLS].sum(axis=1) == 0).sum())
        assert n_zero <= 5, (
            f"{wave}: {n_zero} zero-person households.  Only 3 (2018-19) / 4 "
            f"(2019-20) are genuinely all-zero in the source; the rest are "
            f"visitor rows that won groupby().first().")

    def test_worked_example_returns_the_member_row(self, built):
        """hhid 9147bc2f... has source rows Visitors=(0,0,0,0) and
        Household members=(1,1,1,3).  Pre-fix the API returned (0,0,0,0) --
        a 6-person household reported as a 0-person household."""
        f = built['people_last7days'].reset_index()
        row = f[(f.i == '9147bc2f6725476eaa4bd9e36e701e80') & (f.t == '2018-19')]
        assert len(row) == 1, "worked-example household missing from the API"
        got = [int(row.iloc[0][c]) for c in COUNT_COLS]
        assert got == [1, 1, 1, 3], (
            f"expected the MEMBER row (1,1,1,3); got {got}.  [0,0,0,0] means the "
            f"Visitors row is still being returned.")


# ---------------------------------------------------------------- B ---------
class TestClusterFeatures:
    """The intended household->cluster reduction, now declared and enforced."""

    def test_aggregation_policy_is_declared(self, uga):
        """`aggregation:` used to be documentation-only -- nothing read it.
        It must now be both declared AND parsed."""
        from lsms_library.country import _declared_aggregation
        entry = uga._materialization_entry('cluster_features')
        policy = _declared_aggregation(entry)
        assert policy, "cluster_features declares no aggregation: policy"
        for col in ['Region', 'Rural', 'District']:
            assert policy.get(col) == 'unique', (
                f"{col} must reduce with `unique` (NA on disagreement), not a "
                f"silent arbitrary pick")
        for col in ['Latitude', 'Longitude']:
            assert policy.get(col) == 'median'

    def test_reduction_is_not_silent(self, built):
        """The reduction is intended, so no data-loss warning -- but the cells it
        could not determine must be reported, not buried."""
        undeclared = [m for m in built['warnings']
                      if 'duplicate tuple' in m and 'groupby().first()' in m]
        assert not undeclared, f"cluster_features still collapses silently: {undeclared}"
        ambiguity = [m for m in built['warnings'] if "'unique' aggregation" in m]
        assert ambiguity, (
            "the `unique` reducer NA'd cells (member rows disagree) but reported "
            "nothing -- silent <NA> is just silent-wrongness wearing a policy")

    def test_cluster_grain_no_duplicates(self, built):
        """Regression guard, NOT a bug demonstration: this passes pre-fix too.
        It exists so that re-keying `v` and re-routing the reduction through the
        declared policy cannot accidentally break the (t, v) grain (GH #161)."""
        cf = built['cluster_features']
        assert int(cf.index.duplicated().sum()) == 0
        assert 'i' not in cf.columns, "cluster_features leaks the household id"

    @pytest.mark.parametrize('wave', LONG_WAVES)
    def test_v_is_an_actual_cluster_key(self, uga, wave):
        """Parish NAMES are not unique in Uganda: 'CENTRAL' occurs in ten
        districts, so v='CENTRAL' fused ten different parishes into one cluster
        whose District/Region/GPS first() then picked arbitrarily.  A cluster
        must sit in exactly one district.

        This MUST be asserted on the PRE-collapse, household-grain frame
        (``Wave.grab_data``).  Asserting it on the returned table would be
        VACUOUS: after the (t, v) collapse each v has exactly one row BY
        CONSTRUCTION, so ``nunique() == 1`` always holds -- the check would pass
        on the broken code too (verified: it did).  The invariant only has teeth
        where the member households are still visible.
        """
        df = uga[wave].grab_data('cluster_features').reset_index()
        df = df[df['v'].notna() & df['District'].notna()]
        per_v = df.groupby(df['v'].astype(str))['District'].nunique()
        offenders = per_v[per_v > 1]
        assert offenders.empty, (
            f"{wave}: {len(offenders)} v-group(s) span >1 district among their "
            f"member households, so `v` is not a cluster key: "
            f"{list(offenders.index[:5])}")

    def test_every_sample_cluster_exists_in_cluster_features(self, built):
        """sample() is the source of truth for a household's cluster, and
        _join_v_from_sample hands that v to every other table.  If a v in
        sample has no cluster_features row, the join yields nothing.  Pre-fix
        339 (t, v) pairs were orphaned -- almost all of them 2009-10's
        out-of-frame households, whose synthetic @lat,lon label sample already
        built but cluster_features did not."""
        cf = built['cluster_features'].reset_index()
        sm = built['sample'].reset_index()
        have = set(map(tuple, cf[['t', 'v']].dropna().astype(str).values))
        want = set(map(tuple, sm[['t', 'v']].dropna().drop_duplicates().astype(str).values))
        orphans = want - have
        assert not orphans, (
            f"{len(orphans)} sample (t, v) pairs have no cluster_features row, "
            f"e.g. {sorted(orphans)[:3]}")


# --------------------------------------------------------- machinery --------
class TestReducer:
    """`unique`: agree -> the value; disagree -> <NA>.  Never a guess."""

    def test_agreement_returns_the_value(self):
        from lsms_library.country import _reduce_unique
        assert _reduce_unique(pd.Series(['KAMPALA', 'KAMPALA'])) == 'KAMPALA'

    def test_agreement_ignores_missing(self):
        from lsms_library.country import _reduce_unique
        assert _reduce_unique(pd.Series(['KAMPALA', None, 'KAMPALA'])) == 'KAMPALA'

    def test_disagreement_is_na_not_a_guess(self):
        """`first` would return 'KAMPALA'; `mode` would return 'WAKISO'.  Both
        assert knowledge we do not have."""
        from lsms_library.country import _reduce_unique
        assert pd.isna(_reduce_unique(pd.Series(['KAMPALA', 'WAKISO', 'WAKISO'])))

    def test_all_missing_is_na(self):
        from lsms_library.country import _reduce_unique
        assert pd.isna(_reduce_unique(pd.Series([None, None], dtype='object')))


class TestWhereFilter:
    """The `where:` row filter must fail LOUDLY -- a silently-empty table is the
    exact failure mode it exists to prevent."""

    def test_filters_source_rows(self):
        from lsms_library.local_tools import _apply_where
        df = pd.DataFrame({'cat': ['a', 'b', 'a'], 'x': [1, 2, 3]})
        out = _apply_where(df, "cat == 'a'", 'test')
        assert list(out.x) == [1, 3]

    def test_zero_matches_raises(self):
        from lsms_library.local_tools import _apply_where
        df = pd.DataFrame({'cat': ['a', 'b'], 'x': [1, 2]})
        with pytest.raises(ValueError, match='matched 0'):
            _apply_where(df, "cat == 'typo'", 'test')

    def test_unknown_column_raises(self):
        from lsms_library.local_tools import _apply_where
        df = pd.DataFrame({'cat': ['a'], 'x': [1]})
        with pytest.raises(ValueError, match='could not be evaluated'):
            _apply_where(df, "nosuchcol == 'a'", 'test')
