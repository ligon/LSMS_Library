"""GH #323 -- Uganda: the silent duplicate-index collapse, config-level fixes.

Two independent sites, both fixed with configuration only (no core change; see
``slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org``, decision D1).

**A. ``people_last7days`` (2018-19, 2019-20) -- silently WRONG.**
From 2018-19 the questionnaire went LONG: ``GSEC15A`` carries two rows per
household discriminated by ``CEA01`` in {'Household members', 'Visitors'}, and
``CEA01A-D`` are the counts *for the selected category*.  The declared index is
``(i, t)`` and ``CEA01`` was left undeclared (the YAML literally commented it
out), so both rows collided and ``groupby().first()`` kept whichever the file
listed first -- a coin flip: 1,636 of 3,242 households (50.5%) in 2018-19 and
1,507 of 3,078 (49.0%) in 2019-20 list Visitors first, so they were served
VISITOR counts.  1,053 / 1,011 households came back with ZERO people.  Mean
people per household 2.81 / 2.77 instead of 4.51 / 4.53.

Members-only is not a judgement call: the three earlier waves that declare this
table (2005-06 ``h14q1-4``, 2009-10 ``h15a1-4``, 2013-14 ``T6FQ01a-d``) all map
``Men/Women/Boys/Girls`` onto the source's member block and ignore the parallel
visitor block -- confirmed against the Stata variable labels.

**B. ``cluster_features`` -- an INTENDED reduction, run silently.**
Every wave extracts one row per HOUSEHOLD from the GSEC1 cover page and declares
``(t, v)``, deliberately reducing to cluster grain (GH #161).  The reduction is
fine; doing it with an undeclared ``groupby().first()`` -- which takes the first
non-null value of each column INDEPENDENTLY, and so can return a composite row
that exists in no source record -- is not.  Three real defects rode on it:

* ``v`` was not a cluster key in 2018-19 / 2019-20 (parish NAMES collide across
  districts: ``CENTRAL`` appears in ten of them);
* 2009-10's 565 out-of-frame households carried a blank ``comm`` and were
  DELETED outright by ``groupby(dropna=True)``, so 339 ``(t, v)`` pairs that
  ``sample()`` knew about had no ``cluster_features`` row at all;
* the projection itself was never declared, so nothing checked it.

**A note on skipping.**  Nothing in this module converts a build failure into a
skip.  An earlier draft wrapped every fixture in ``except Exception:
pytest.skip(...)``, which would have made the whole end-to-end half of this file
VACUOUS the moment the Uganda build broke -- the exact silence it is here to
prevent.  The only sanctioned skip is ``tests/conftest.py``'s
``aws_creds_available()`` guard on the ``uga`` fixture (plus that file's narrow
hook, which converts a *missing-credentials* failure and nothing else), so the
data-free CI ``unit-tests`` job skips while a genuine regression is red.
"""
import importlib.util
import os
import warnings

import pandas as pd
import pytest

import lsms_library as ll

# NB `from conftest import ...` -- which tests/conftest.py's own docstring
# suggests -- resolves to the ROOT conftest.py, which does not define this.
# `tests/` is a package (it has an __init__.py), so import it by package path.
from tests.conftest import aws_creds_available

COUNT_COLS = ['Men', 'Women', 'Boys', 'Girls']
LONG_WAVES = ['2018-19', '2019-20']

#: GSEC1 cover-page columns for the two long-form waves: the source file, the
#: ``(district, parish)`` pair ``v`` is built from, and the survey's own
#: administrative triple that says whether two ``v``\ s are really one place.
GSEC1 = {
    '2018-19': {
        'file': 'Data/GSEC1.dta',
        'pair': ['distirct_name', 'parish_name'],   # sic: source misspelling
        'triple': ['county_name', 'subcounty_name', 'parish_name'],
    },
    '2019-20': {
        'file': 'Data/HH/gsec1.dta',
        'pair': ['district', 's1aq04a'],
        'triple': ['s1aq02a', 's1aq03a', 's1aq04a'],
    },
}


def _wave_mapping(wave):
    """The wave's ``_/mapping.py``, loaded as a module."""
    from lsms_library.paths import countries_root
    path = countries_root() / 'Uganda' / wave / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location(f'_uganda_{wave}_mapping', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def uga():
    """The country handle.

    Skips ONLY when S3 credentials are absent (the data-free CI job), never on
    a build error -- see the module docstring.  Attached to the fixture rather
    than to the classes so the two data-free tests in this file (the hook
    contract, and the missing-``_category`` guard) still run there.
    """
    if not aws_creds_available():
        pytest.skip("needs S3 credentials to build Uganda; the unit-tests job "
                    "is deliberately data-free (see tests/conftest.py)")
    return ll.Country('Uganda')


@pytest.fixture(scope='module')
def built(uga):
    """Build the three affected tables once, capturing the warnings.

    ``LSMS_NO_CACHE`` is deliberately NOT forced here: forcing it would make
    every run a cold build (minutes), and the assertions below are about
    returned VALUES, which the warm parquet reproduces faithfully.  The
    warning-shaped assertions are written to tolerate a warm read, where the
    grain report is replayed from the parquet stamp rather than re-derived.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = {
            'people_last7days': uga.people_last7days(),
            'cluster_features': uga.cluster_features(),
            'sample': uga.sample(),
        }
        out['warnings'] = [str(w.message) for w in caught]
    return out


# ---------------------------------------------------------------- A ---------
class TestPeopleLast7Days:
    """The member/visitor collapse: silently WRONG for ~half the households."""

    def test_declared_index_is_unique(self, built):
        """``(i, t)`` must be genuinely unique, so the collapse never fires."""
        df = built['people_last7days']
        assert int(df.index.duplicated().sum()) == 0

    def test_no_grain_collapse_report(self, uga):
        """The #323 audit must file nothing for this table."""
        from lsms_library.country import grain_reports
        uga.people_last7days()
        reports = [r for r in grain_reports('Uganda', 'people_last7days')
                   if r.get('destroyed') or r.get('nan_key_rows')]
        assert not reports, f"people_last7days still destroys rows: {reports}"

    @pytest.mark.parametrize('wave,expected', [('2018-19', 4.60), ('2019-20', 4.55)])
    def test_mean_household_size_is_members_not_visitors(self, built, wave, expected):
        """Pre-fix this was 2.81 / 2.77 -- the mean of a coin-flipped mix of
        member and visitor rows.  Members-only restores ~4.6 people/HH, in line
        with the 5.0-5.4 of the earlier waves."""
        f = built['people_last7days'].reset_index()
        tot = f[f.t == wave][COUNT_COLS].sum(axis=1)
        assert tot.mean() == pytest.approx(expected, abs=0.2), (
            f"{wave}: mean people/HH is {tot.mean():.2f}; expected ~{expected} "
            f"(members).  ~2.8 means the visitor rows are still winning.")

    @pytest.mark.parametrize('wave,cap', [('2018-19', 20), ('2019-20', 20)])
    def test_no_spurious_zero_person_households(self, built, wave, cap):
        """1,053 (2018-19) and 1,011 (2019-20) households reported ZERO people
        because the visitor row -- all zeros for two thirds of households -- won
        the collapse.  A zero-person household is only legitimate where its own
        MEMBER row is all-zero, which is true of 64 and 16 source rows; after
        the panel-id walk 3 and 4 survive into the API."""
        f = built['people_last7days'].reset_index()
        s = f[f.t == wave]
        n_zero = int((s[COUNT_COLS].sum(axis=1) == 0).sum())
        assert n_zero <= cap, (
            f"{wave}: {n_zero} zero-person households; expected a handful.  "
            f"~1,000 means visitor rows are still winning groupby().first().")

    @pytest.mark.parametrize('wave,n_hh', [('2018-19', 3242), ('2019-20', 3078)])
    def test_wave_extraction_keeps_one_row_per_household(self, uga, wave, n_hh):
        """At the wave level the extraction must already be one row per
        household -- the member row.  Asserted PRE-collapse, because after the
        ``(i, t)`` collapse uniqueness holds by construction and the check would
        be vacuous (it passed on the broken code)."""
        df = uga[wave].grab_data('people_last7days')
        assert len(df) == n_hh, (
            f"{wave}: {len(df)} rows for {n_hh} households -- the long-form "
            f"member/visitor rows are not being filtered")
        assert df.index.is_unique
        assert '_category' not in df.columns, (
            "the temporary _category myvar leaked into the returned table")

    def test_missing_category_myvar_is_caught_not_collapsed(self):
        """A future long-form wave that forgets the ``_category`` myvar must
        FAIL, not silently return visitor counts for half its households."""
        import importlib.util
        from lsms_library.paths import countries_root
        path = countries_root() / 'Uganda' / '_' / 'uganda.py'
        spec = importlib.util.spec_from_file_location('_uganda_hooks', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        df = pd.DataFrame({'Men': [1, 0]},
                          index=pd.MultiIndex.from_tuples(
                              [('2018-19', 'h1'), ('2018-19', 'h1')], names=['t', 'i']))
        with pytest.raises(ValueError, match='duplicate rows'):
            mod.people_last7days(df)


# ---------------------------------------------------------------- B ---------
class TestClusterFeatures:
    """The intended household->cluster projection, now declared and enforced."""

    def test_cluster_grain_no_duplicates(self, built):
        cf = built['cluster_features']
        assert list(cf.index.names) == ['t', 'v']
        assert int(cf.index.duplicated().sum()) == 0
        assert 'i' not in cf.columns, "cluster_features leaks the household id"

    def test_no_grain_collapse_report(self, uga):
        """The projection is now performed by the country hook with
        ``reduce_to_agreed``, so neither Site 1 nor Site 2 has anything left to
        destroy: 10,837 destroyed + 935 NaN-key rows -> 0."""
        from lsms_library.country import grain_reports
        uga.cluster_features()
        reports = [r for r in grain_reports('Uganda', 'cluster_features')
                   if r.get('destroyed') or r.get('nan_key_rows')]
        assert not reports, f"cluster_features still destroys rows: {reports}"

    @pytest.mark.parametrize('wave', LONG_WAVES)
    def test_v_is_an_actual_cluster_key(self, uga, wave):
        """Parish NAMES are not unique in Uganda: ``CENTRAL`` occurs in ten
        districts, so ``v='CENTRAL'`` fused ten different parishes into one
        "cluster" whose District/Region/GPS ``first()`` then picked arbitrarily.
        A cluster must sit in exactly one district.

        Asserted on the PRE-collapse household-grain frame.  On the returned
        table the check would be VACUOUS -- after the ``(t, v)`` reduction each
        ``v`` has exactly one row by construction, so it passes on the broken
        code too.
        """
        df = uga[wave].grab_data('cluster_features').reset_index()
        if 'i' not in df.columns and 'District' not in df.columns:  # pragma: no cover
            pytest.skip("wave extraction shape changed")
        df = df[df['v'].notna() & df['District'].notna()]
        per_v = df.groupby(df['v'].astype(str))['District'].nunique()
        offenders = per_v[per_v > 1]
        assert offenders.empty, (
            f"{wave}: {len(offenders)} v-group(s) span >1 district among their "
            f"member households, so `v` is not a cluster key: "
            f"{list(offenders.index[:5])}")

    @pytest.mark.parametrize('wave', LONG_WAVES)
    def test_v_does_not_over_split_one_real_parish(self, uga, wave):
        """The converse of ``test_v_is_an_actual_cluster_key`` -- and the one
        that actually has teeth.

        "Clusters spanning more than one district: 20 -> 0, 23 -> 0" was the
        only evidence originally offered for the DISTRICT/PARISH key, and it is
        **zero by construction** under a key that contains the district.  It
        cannot see OVER-SPLITTING: one real parish torn into two clusters
        because a household or two coded the wrong district.

        The survey's own ``(county, subcounty, parish)`` triple is an
        independent witness, so: no triple may map to more than one ``v``.
        2019-20 violated this three times -- ``NAMTUMBA``/``NAMUTUMBA`` (a
        misspelling), and the ``MUBENDE``->``KASSANDA`` (2019) and
        ``GULU``->``OMORO`` (2016) district carve-outs, where a straggler
        household still codes the parent -- putting four households in a 1-2
        household phantom cluster instead of the 10-11 household cluster they
        belong to.  ``mapping._V_ALIASES`` resolves them.  2018-19 is clean and
        always was, so for that wave this test pins an invariant rather than a
        fix.
        """
        from lsms_library.local_tools import get_dataframe
        spec = GSEC1[wave]
        df = get_dataframe(uga[wave].file_path / spec['file'])
        for c in set(spec['pair']) | set(spec['triple']):
            df[c] = df[c].astype(str).str.strip().str.upper()
        mapping = _wave_mapping(wave)
        df['_v'] = df[spec['pair']].apply(mapping.v, axis=1)
        df = df[df['_v'].notna()]
        grouped = df.groupby(spec['triple'])['_v'].unique()
        offenders = grouped[grouped.map(len) > 1]
        detail = {k: sorted(vs) for k, vs in offenders.head(5).items()}
        assert offenders.empty, (
            f"{wave}: {len(offenders)} (county, subcounty, parish) triple(s) "
            f"are split across more than one v, so the district component is "
            f"fragmenting real parishes: {detail}")

    def test_the_2019_20_alias_table_is_the_thing_being_tested(self):
        """A data-free companion to the audit above, so the fix is pinned even
        in the credential-free CI job: the three aliased ``(district, parish)``
        pairs must collapse onto their canonical partner's key."""
        mapping = _wave_mapping('2019-20')
        for coded, canonical, parish in [('NAMTUMBA', 'NAMUTUMBA', 'NAWANSAGWA'),
                                         ('MUBENDE', 'KASSANDA', 'KIJUNA'),
                                         ('GULU', 'OMORO', 'PALWO')]:
            got = mapping.v(pd.Series([coded, parish]))
            want = mapping.v(pd.Series([canonical, parish]))
            assert got == want == f'{canonical}/{parish}', (
                f"{coded}/{parish} -> {got!r}, but it is the same place as "
                f"{canonical}/{parish} -> {want!r}")
            assert mapping.District(pd.Series([coded, parish])) == canonical, (
                "District must be resolved through the same alias table, or "
                "reduce_to_agreed blanks it for the merged cluster")
        # ... and an unaliased pair must pass through untouched, so the table
        # is not quietly rewriting the rest of the wave.
        assert mapping.v(pd.Series(['MUBENDE', 'KIGANDA'])) == 'MUBENDE/KIGANDA'
        assert mapping.District(pd.Series(['GULU', 'LAROO'])) == 'GULU'

    @pytest.mark.parametrize('wave', LONG_WAVES)
    def test_v_is_the_district_parish_composite(self, built, wave):
        cf = built['cluster_features'].reset_index()
        vals = cf.loc[cf['t'] == wave, 'v'].dropna().astype(str)
        assert (vals.str.count('/') == 1).all(), (
            f"{wave}: v is not a DISTRICT/PARISH composite: "
            f"{sorted(vals[vals.str.count('/') != 1])[:5]}")

    def test_2009_10_out_of_frame_households_have_a_cluster(self, built):
        """2009-10 is a panel-refresh wave: ``comm`` is the 2005-06 EA, so the
        565 movers / split-offs fall outside that frame and carry a blank
        ``comm``.  ``sample()`` already gave them a synthetic ``@lat,lon``
        cluster; ``cluster_features()`` did not, so all 565 were deleted by
        ``groupby(dropna=True)``.  541 of them have usable coordinates."""
        cf = built['cluster_features'].reset_index()
        synth = cf[(cf['t'] == '2009-10') & cf['v'].astype(str).str.startswith('@')]
        assert len(synth) > 300, (
            f"only {len(synth)} synthetic @lat,lon clusters in 2009-10; the "
            f"out-of-frame households are being dropped again")

    def test_cluster_features_v_is_a_subset_of_sample_v(self, built):
        """``sample()`` is the source of truth for a household's cluster and
        ``_join_v_from_sample`` hands that ``v`` to every other table.  The two
        keys must be the SAME key -- built by the same formatter from the same
        columns -- or the join matches nothing."""
        cf = built['cluster_features'].reset_index()
        sm = built['sample'].reset_index()
        have = set(map(tuple, sm[['t', 'v']].dropna().drop_duplicates().astype(str).values))
        want = set(map(tuple, cf[['t', 'v']].dropna().astype(str).values))
        orphans = want - have
        assert not orphans, (
            f"{len(orphans)} cluster_features (t, v) pairs are unknown to "
            f"sample(), e.g. {sorted(orphans)[:3]}")

    def test_almost_every_sample_cluster_has_a_cluster_features_row(self, built):
        """The converse direction.  Pre-fix 339 ``(t, v)`` pairs were orphaned,
        almost all of them 2009-10's out-of-frame households.

        Four 2010-11 clusters legitimately remain absent: their households agree
        on NOTHING (Region, Rural, District and both coordinates all conflict),
        so every cell is blanked and the core's ``dropna(how='all')`` safety net
        removes the now-empty row.  That is the honest outcome -- the ``comm``
        code is not identifying a place there -- and it is a fabricated
        composite that used to fill the gap."""
        cf = built['cluster_features'].reset_index()
        sm = built['sample'].reset_index()
        have = set(map(tuple, cf[['t', 'v']].dropna().astype(str).values))
        want = set(map(tuple, sm[['t', 'v']].dropna().drop_duplicates().astype(str).values))
        orphans = want - have
        assert len(orphans) <= 5, (
            f"{len(orphans)} sample (t, v) pairs have no cluster_features row, "
            f"e.g. {sorted(orphans)[:5]}")


# --------------------------------------------------------- the hook ---------
class TestClusterGrainHook:
    """``uganda.cluster_features`` is lossless or loud -- never a guess."""

    @pytest.fixture(scope='class')
    def hook(self):
        import importlib.util
        from lsms_library.paths import countries_root
        path = countries_root() / 'Uganda' / '_' / 'uganda.py'
        spec = importlib.util.spec_from_file_location('_uganda_hooks', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.cluster_features

    @staticmethod
    def _frame(rows):
        df = pd.DataFrame(rows)
        return df.set_index(['t', 'v'])

    def test_agreeing_households_collapse_silently(self, hook):
        df = self._frame([
            {'t': 't1', 'v': 'c1', 'i': 'h1', 'Region': 'Central', 'Rural': 'Urban'},
            {'t': 't1', 'v': 'c1', 'i': 'h2', 'Region': 'Central', 'Rural': 'Urban'},
        ])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            out = hook(df)
        assert len(out) == 1
        assert out.iloc[0]['Region'] == 'Central'
        assert not [w for w in caught if 'grain conflict' in str(w.message)]

    def test_missing_is_absence_not_contradiction(self, hook):
        """A household that reports nothing where another reports a value does
        NOT destroy the observed value -- that completion is lossless."""
        df = self._frame([
            {'t': 't1', 'v': 'c1', 'i': 'h1', 'Region': None, 'Rural': 'Urban'},
            {'t': 't1', 'v': 'c1', 'i': 'h2', 'Region': 'Central', 'Rural': None},
        ])
        out = hook(df)
        assert out.iloc[0]['Region'] == 'Central'
        assert out.iloc[0]['Rural'] == 'Urban'

    def test_disagreement_is_na_and_loud(self, hook):
        """``first()`` would return 'Central'.  We return ``<NA>`` and say so."""
        df = self._frame([
            {'t': 't1', 'v': 'c1', 'i': 'h1', 'Region': 'Central', 'Rural': 'Urban'},
            {'t': 't1', 'v': 'c1', 'i': 'h2', 'Region': 'Western', 'Rural': 'Urban'},
        ])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            out = hook(df)
        assert pd.isna(out.iloc[0]['Region'])
        assert out.iloc[0]['Rural'] == 'Urban', "the agreeing column must survive"
        assert [w for w in caught if 'grain conflict' in str(w.message)], (
            "a blanked cell that nobody is told about is silent wrongness "
            "wearing a policy")

    def test_household_id_is_dropped_not_treated_as_a_conflict(self, hook):
        """``i`` is distinct by construction; leaving it in would make every
        multi-household cluster look like a conflict."""
        df = self._frame([
            {'t': 't1', 'v': 'c1', 'i': 'h1', 'Region': 'Central'},
            {'t': 't1', 'v': 'c1', 'i': 'h2', 'Region': 'Central'},
        ])
        out = hook(df)
        assert 'i' not in out.columns and 'i' not in (out.index.names or [])
        assert out.iloc[0]['Region'] == 'Central'

    def test_rows_without_a_cluster_id_are_dropped_loudly(self, hook):
        df = self._frame([
            {'t': 't1', 'v': 'c1', 'i': 'h1', 'Region': 'Central'},
            {'t': 't1', 'v': None, 'i': 'h2', 'Region': 'Western'},
        ])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            out = hook(df)
        assert len(out) == 1
        assert [w for w in caught if 'no cluster id' in str(w.message)]

    def test_missing_v_is_an_error_not_a_silent_reshape(self, hook):
        df = pd.DataFrame({'t': ['t1'], 'i': ['h1'], 'Region': ['Central']}).set_index(['t', 'i'])
        with pytest.raises(ValueError, match=r"\['v'\]"):
            hook(df)
