"""
GH #323 -- a non-unique DECLARED index must not be silently collapsed with
``groupby().first()``.

Tanzania is two different bugs wearing one costume, and they need two
different fixes:

* ``cluster_features`` (index ``(t, v)``) was fed the HOUSEHOLD grain.  The geo
  columns come from the household cover page (where the household was
  INTERVIEWED), while ``v`` is its ORIGINAL sampling EA, which the NPS panel
  carries forward unchanged when it tracks a mover or a split-off.  So the geo
  columns are NOT constant within a cluster, and ``.first()`` did not merely
  dedup -- it MISLABELED 210 cluster-cells (a KILIMANJARO cluster returned as
  MOROGORO).  Fixed by DECLARING ``aggregation: majority`` in data_scheme.yml.

* ``interview_date`` / ``sample`` hit the same source replication (upd4_hh_a.dta
  is keyed on the panel-tracking line ``UPHI``, not the household), but there the
  collapse is VALUE-PRESERVING -- so the fix is to make the dedup explicit and
  ASSERT the invariant, not to change any value.

Each test below FAILS on pristine ``development``.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll

pytestmark = pytest.mark.filterwarnings('ignore::RuntimeWarning')


def _machinery():
    """Import the GH #323 reducers lazily: on pristine `development` they do not
    exist, and a module-level ImportError would mask the end-to-end tests below
    (which must fail on their own merits, not on a collection error)."""
    from lsms_library.country import _declared_aggregation, _majority
    return _declared_aggregation, _majority


# --------------------------------------------------------------------------
# The reducer itself.  These are pure-unit and need no data.
# --------------------------------------------------------------------------
class TestMajorityReducer:
    def test_strict_majority_wins(self):
        _, _maj = _machinery()
        assert _maj(pd.Series(['DODOMA', 'DODOMA', 'ARUSHA'])) == 'DODOMA'

    def test_no_strict_majority_is_na_not_a_guess(self):
        _, _maj = _machinery()
        # 2-2 tie: .first() would pick one; we must refuse to guess.
        assert pd.isna(_maj(pd.Series(['DODOMA', 'DODOMA', 'ARUSHA', 'ARUSHA'])))

    def test_plurality_is_not_enough(self):
        _, _maj = _machinery()
        # 2 of 5 is the mode but NOT a majority -> <NA>.
        assert pd.isna(_maj(pd.Series(['A', 'A', 'B', 'C', 'D'])))

    def test_blank_strings_are_not_votes(self):
        _, _maj = _machinery()
        # Tanzania rounds 1-2 record District as '' for EVERY row.  A plain
        # mode() would elect '' and ship an empty string as a district *name*.
        assert pd.isna(_maj(pd.Series(['', '', ''])))
        assert _maj(pd.Series(['', '', 'KONDOA'])) == 'KONDOA'

    def test_all_null_is_na(self):
        _, _maj = _machinery()
        assert pd.isna(_maj(pd.Series([pd.NA, pd.NA], dtype='string')))


class TestDeclaredAggregationParsing:
    def test_legacy_level_keyed_entry_is_ignored(self):
        """Nine countries carry a documentation-only ``aggregation: {visit: first}``
        on interview_date ("nothing reads this yet").  Reading it as a COLUMN
        policy would silently hijack them, so a purely level-keyed entry must
        parse as *no policy*."""
        _dagg, _ = _machinery()
        entry = {'index': '(t, i, visit)', 'aggregation': {'visit': 'first'}}
        assert _dagg(entry) is None

    def test_scalar_policy(self):
        _dagg, _ = _machinery()
        assert _dagg(
            {'index': '(t, v)', 'aggregation': 'majority'}) == 'majority'

    def test_unknown_reducer_rejected(self):
        _dagg, _ = _machinery()
        with pytest.raises(ValueError):
            _dagg({'index': '(t, v)', 'aggregation': 'bogus'})


# --------------------------------------------------------------------------
# Tanzania, end to end.
# --------------------------------------------------------------------------
def _ballot():
    """The HOUSEHOLD-grain ballot the cluster reduction votes over.

    Taken from ``Wave.grab_data`` -- the pre-collapse grain -- rather than from a
    cached wave parquet, so the test can never quietly ``skip`` itself into
    passing just because the cache happens to be cold.
    """
    country = ll.Country('Tanzania')
    frames = []
    for wave in country.waves:          # round labels, not the 2008-15 folder name
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            d = country[wave].grab_data('cluster_features').reset_index()
        if 't' not in d.columns:
            d['t'] = wave
        frames.append(d)
    ballot = pd.concat(frames, ignore_index=True)
    # one household = one vote (the source replicates a household-round once per
    # descendant panel line; geo is constant within (i, t, v))
    hh = 'i' if 'i' in ballot.columns else 'j'
    return ballot.drop_duplicates(subset=[hh, 't', 'v'])


@pytest.fixture(scope='module')
def tza():
    return ll.Country('Tanzania')


@pytest.fixture(scope='module')
def cluster_features(tza):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return tza.cluster_features()


class TestTanzaniaClusterFeatures:
    def test_declares_a_majority_policy(self, tza):
        _dagg, _ = _machinery()
        entry = tza._materialization_entry('cluster_features')
        assert _dagg(entry) == 'majority', (
            'cluster_features must DECLARE its household->cluster reduction; '
            'prose in CONTENTS.org is not enforcement.'
        )

    def test_index_is_unique(self, cluster_features):
        assert not cluster_features.index.duplicated().any()

    def test_no_empty_string_district(self, cluster_features):
        """FAILS pre-fix: 818 clusters were served District='' -- an empty string
        masquerading as a district NAME (class-1, silently wrong).  Rounds 1-2 of
        the NPS record no district, so the honest answer is <NA> (class-2)."""
        district = cluster_features['District'].astype('string')
        empty = int((district.notna() & district.str.strip().eq('')).sum())
        assert empty == 0, f'{empty} clusters carry a fake empty-string District'

    def test_geography_matches_the_household_majority(self, cluster_features):
        """The load-bearing test.  FAILS pre-fix with 210 mislabeled cells.

        For every cluster whose households hold a STRICT majority view of their
        own geography, the API must report that majority -- not whichever row
        ``.first()`` happened to land on.
        """
        import re

        ballot = _ballot()

        norm = lambda x: re.sub(r'[^a-z0-9]', '', str(x).casefold())
        bad = []
        for (t, v), g in ballot.groupby(['t', 'v']):
            if (t, v) not in cluster_features.index:
                continue
            row = cluster_features.loc[(t, v)]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            for col in ['Region', 'District', 'Rural']:
                s = g[col].astype('string').str.strip()
                s = s[s.notna() & s.ne('')]
                if not len(s):
                    continue
                counts = s.value_counts()
                if counts.iloc[0] * 2 <= len(s):
                    continue                     # genuinely ambiguous -> <NA> is correct
                api = row[col]
                if pd.isna(api) or norm(api) != norm(counts.index[0]):
                    bad.append((t, v, col, api, counts.index[0]))
        assert not bad, (
            f'{len(bad)} cluster-cell(s) disagree with the majority of their own '
            f'households (first 5: {bad[:5]})'
        )


class TestTanzaniaInterviewDateAndSample:
    def test_interview_date_wave_parquet_has_no_duplicate_it(self, tza):
        """FAILS pre-fix: the 2008-15 wave parquet carried 29,250 rows for 16,540
        distinct (i, t) -- 12,710 duplicates the framework then absorbed silently.
        The dedup is value-preserving (upd4_hh_a replicates a household-round once
        per descendant panel line), so it belongs in the wave script, declared."""
        from pathlib import Path
        from lsms_library.local_tools import data_root

        with warnings.catch_warnings():          # force materialization
            warnings.simplefilter('ignore')
            tza.interview_date()

        p = Path(data_root()) / 'Tanzania' / '2008-15' / '_' / 'interview_date.parquet'
        assert p.exists(), f'{p} was not materialized by Country.interview_date()'
        df = pd.read_parquet(p).reset_index()
        dup = int(df.duplicated(subset=['i', 't']).sum())
        assert dup == 0, f'{dup} duplicate (i, t) rows still reach the framework'

    def test_ambiguous_cluster_assignment_is_na_not_a_coin_flip(self, tza):
        """FAILS pre-fix: 59 households hold two panel lines with DIFFERENT origin
        EAs, so their sampling cluster is genuinely ambiguous.  ``.first()`` picked
        one at random -- and _join_v_from_sample() propagates that v onto EVERY
        Tanzania household table.  <NA> (loudly missing) beats a coin flip."""
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            sample = tza.sample()
        assert sample['v'].isna().sum() == 59, (
            'the 59 households with conflicting origin EAs must carry v=<NA>, '
            'not an arbitrarily-chosen cluster'
        )
