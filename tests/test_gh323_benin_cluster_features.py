"""GH #323 -- Benin cluster_features must reach the framework at CLUSTER grain.

Companion to ``tests/test_gh323_benin_togo.py``, which covers Benin's *other*
#323 site (``plot_inputs``, where the identifier was broken and 71 reported
rows were genuinely destroyed).  This file covers a different failure mode, and
the difference is the whole point:

  ``cluster_features`` declares ``(t, v)`` -- 670 grappes -- but ``df_main`` in
  ``2018-19/_/data_info.yml`` reads Region/Rural from the HOUSEHOLD-level cover
  page ``s00_me_ben2018.dta`` while declaring only ``v: grappe``.  Each
  grappe's attributes are broadcast across its households, so 8,012 rows with
  7,342 duplicate ``(t, v)`` tuples were handed to
  ``_normalize_dataframe_index`` to reduce.

NO VALUE WAS EVER LOST HERE, and these tests must not pretend otherwise.  Zero
of the 670 grappes carry more than one distinct Region or Rural, and the GPS
file is already grappe-level -- so the reduction was value-lossless and the
framework's #323 audit is correctly SILENT about this cell.  Fixing the
extraction recovers zero rows.

What it buys is that the 670 rows are right BY CONSTRUCTION rather than by
luck.  ``groupby().first()`` skips NA *per column*, so a grappe that ever
straddled two regions would not collapse to one of its households -- it would
collapse to a composite row assembled column-by-column from different
households, a cluster that exists nowhere in the survey.  The
``cluster_features(df)`` hook in ``2018-19/_/mapping.py`` projects to cluster
grain in the EXTRACTION and RAISES if any cluster straddles two values.

INSTRUMENT NOTE (inherited from the CotedIvoire / Benin-Togo tests -- do not
undo it).  Do NOT assert that the API's returned index is unique: the collapse
makes it unique BY CONSTRUCTION, so such a test passes with the bug fully
present.  These tests assert on the PRE-collapse wave frame, on the invariant
itself, and -- because prose is not enforcement -- on the hook actually raising
when the invariant is violated.
"""
import pandas as pd
import pytest

from lsms_library.country import Country

# 2018-19 EHCVM: 670 grappes, 8,012 households.
N_CLUSTERS = 670


@pytest.fixture(scope='module')
def benin():
    try:
        return Country('Benin')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Benin unavailable: {exc}')


@pytest.fixture(scope='module')
def wave_frame(benin):
    """The PRE-collapse wave-level frame -- the only tier that can show this."""
    try:
        return benin['2018-19'].grab_data('cluster_features')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Benin cluster_features could not be built: '
                    f'{type(exc).__name__}: {exc}')


@pytest.fixture(scope='module')
def hook(benin):
    fns = benin['2018-19'].formatting_functions
    fn = fns.get('cluster_features')
    if fn is None:                                             # pragma: no cover
        pytest.fail('Benin 2018-19 mapping.py declares no cluster_features hook; '
                    'the household-level cover page will be broadcast onto the '
                    'cluster grain again (GH #323).')
    return fn


def test_wave_frame_is_at_cluster_grain(wave_frame):
    """The extraction must emit one row per cluster, not one per household."""
    assert len(wave_frame) == N_CLUSTERS, (
        f'Benin/cluster_features emits {len(wave_frame)} wave-level rows for '
        f'{N_CLUSTERS} clusters.  The household-level cover page is being '
        f'broadcast onto the (t, v) grain and left for the framework to reduce '
        f'with groupby().first() (GH #323).  Project to cluster grain in the '
        f'extraction instead.'
    )


def test_wave_frame_has_no_duplicate_cluster_tuples(wave_frame):
    """No manufactured duplicates for the framework collapse to eat."""
    n_dup = int(wave_frame.index.duplicated().sum())
    assert n_dup == 0, (
        f'Benin/cluster_features hands the framework {n_dup} duplicate (t, v) '
        f'tuple(s).  The fix for a manufactured duplicate is to stop '
        f'manufacturing it, not to choose a survivor (GH #323).'
    )


def test_every_cluster_carries_one_value_per_attribute(wave_frame):
    """The invariant the hook enforces -- asserted here on real data too.

    "Region/Rural are invariant within a cluster" is exactly the kind of claim
    that was licensed by a comment elsewhere in the library and turned out to
    be false.  Check it rather than believe it.
    """
    spread = wave_frame.groupby(level=list(wave_frame.index.names),
                                observed=True).nunique(dropna=False)
    straddling = spread[(spread > 1).any(axis=1)]
    assert straddling.empty, (
        f'{len(straddling)} Benin cluster(s) carry more than one distinct value '
        f'for a cluster attribute:\n{straddling.head(10)}\n'
        f'Reducing to cluster grain would have to GUESS.'
    )


def test_api_row_count_is_the_cluster_count(benin):
    """Regression guard: the API result is unchanged by the grain fix.

    This fix recovers no rows -- it was value-lossless before and is
    value-lossless now.  A change here means something else moved.
    """
    df = benin.cluster_features()
    assert len(df) == N_CLUSTERS, (
        f'Benin/cluster_features API returned {len(df)} rows, expected '
        f'{N_CLUSTERS} (one per grappe).'
    )


def test_hook_raises_when_a_cluster_straddles_two_values(hook):
    """Prose is not enforcement.  Prove the guard actually fires.

    Without this, the hook could silently degrade into
    ``drop_duplicates()``-with-extra-steps -- picking a winner exactly as
    ``first()`` did -- and every other test here would still pass.
    """
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '1'), ('2018-19', '1')], names=['t', 'v'])
    straddling = pd.DataFrame(
        {'Region': ['alibori', 'atlantique'],
         'Rural': ['Rural', 'Rural'],
         'Latitude': [11.29, 11.29],
         'Longitude': [2.43, 2.43]},
        index=idx)
    with pytest.raises(ValueError, match='more than one distinct value'):
        hook(straddling)


def test_hook_is_a_no_op_on_a_frame_already_at_cluster_grain(hook):
    """A well-formed frame must pass through untouched."""
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '1'), ('2018-19', '2')], names=['t', 'v'])
    clean = pd.DataFrame(
        {'Region': ['alibori', 'atlantique'],
         'Rural': ['Rural', 'Urban'],
         'Latitude': [11.29, 6.52],
         'Longitude': [2.43, 2.36]},
        index=idx)
    out = hook(clean)
    pd.testing.assert_frame_equal(out, clean)


def test_hook_tolerates_a_cluster_whose_attribute_is_uniformly_missing(hook):
    """All-NaN within a cluster is ONE distinct value, not two.

    ``nunique(dropna=False)`` counts NaN as a value; a cluster where every
    household is missing Region must not be reported as straddling.
    """
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '1'), ('2018-19', '1')], names=['t', 'v'])
    frame = pd.DataFrame(
        {'Region': [pd.NA, pd.NA],
         'Rural': ['Rural', 'Rural'],
         'Latitude': [11.29, 11.29],
         'Longitude': [2.43, 2.43]},
        index=idx)
    out = hook(frame)
    assert len(out) == 1


def test_hook_rejects_a_cluster_that_is_missing_a_value_in_one_household(hook):
    """One household has Region, another does not -- that is a straddle.

    This is the case that makes ``first()`` FABRICATE: it skips NA per column,
    so it would take Region from one household and Rural from another and hand
    back a cluster that exists in no survey record.  The guard must refuse.
    """
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '1'), ('2018-19', '1')], names=['t', 'v'])
    frame = pd.DataFrame(
        {'Region': [pd.NA, 'alibori'],
         'Rural': ['Urban', pd.NA],
         'Latitude': [11.29, 11.29],
         'Longitude': [2.43, 2.43]},
        index=idx)
    with pytest.raises(ValueError, match='more than one distinct value'):
        hook(frame)
