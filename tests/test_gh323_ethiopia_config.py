"""GH #323 -- Ethiopia's config must not feed the framework a broken identifier.

Two independent defects, both CONFIG, both true positives that the framework
was silently absorbing.  This file pins them so they cannot come back.

--------------------------------------------------------------------------
1. `cluster_features` (all five waves) -- SITE 4, the `dfs:` merge
--------------------------------------------------------------------------

Ethiopia is the only country in the corpus whose `dfs:` block appears in
`cluster_features` for *every* wave, and it used to merge two HOUSEHOLD-grain
sub-frames (the survey cover page and the geovariables file) on the CLUSTER key
`v`.  That is a many-to-many join, i.e. a cartesian product within each EA, and
`_normalize_dataframe_index`'s `groupby().first()` then tidied the evidence
away so the table reported clean.  Measured against PR #627's core:

    2013-14   5,262 x 5,287 households over 433 EAs -> 65,508 rows, 60,221 phantom
    2015-16   4,954 x 4,954 households over 432 EAs -> 57,786 rows, 52,832 phantom

and in the other three waves the geo sub-frame did not load at all -- a
`KeyError` the GH #515 fallback swallowed, so Latitude/Longitude were 100%
ABSENT with only a warning.  PR #627 turns that swallow into a hard error, so
these three cells RAISED.  Fixed in PR #628 by merging on the household id with
`i` in `final_index`; this file is the regression net.

THE KEY MUST BE THE WAVE-NATIVE HOUSEHOLD ID.  For W2/W3 that is
`household_id2`, NOT `household_id`: `household_id` is the W1 baseline id and
is BLANK for households new to the panel, so re-keying to it would trade an EA
cartesian for a NULL-KEY cartesian (`pd.merge` matches null keys).

--------------------------------------------------------------------------
2. `individual_education` (2013-14, 2015-16) -- SITE 1, a broken identifier
--------------------------------------------------------------------------

Both waves indexed education on the W1-BASELINE ids while `household_roster`
and `shocks` for the same waves correctly used the wave-native `_id2` pair:

  W2  5,248 of sect2_hh_w2.dta's 23,785 rows -- the urban REFRESHMENT sample,
      which has no W1 antecedent -- carry a BLANK household_id AND a blank
      individual_id.  The framework audit reported it exactly: "DESTROYED 5,247
      of 23,785 rows ... Additionally 5,248 row(s) carry NaN in a declared index
      level and are DELETED OUTRIGHT".

  W3  SPLIT-OFF households inherit their parent's baseline id (W3 households
      010501020100105031 and 010501088800105031 both carry baseline
      01050100105031), so 16 rows in 8 pairs collided and `first()` MERGED TWO
      REAL, DISTINCT PEOPLE -- one recorded '10th Grade Complete', the other
      '9th Grade Complete'.

It also broke the join to `household_roster`: 0% of W2/W3 (i, pid) pairs
matched a roster row, against 100% in W1/W4/W5.

INSTRUMENT NOTE (same one the Benin/Togo tests carry -- do not undo it).  Do
NOT assert on the API's post-collapse index uniqueness: `_normalize_dataframe_
index` makes that true BY CONSTRUCTION and such a test passes with the bug
fully present.  Assert at the WAVE level, which is upstream of the collapse.

The wave level is also the only DETERMINISTIC place to assert row counts here.
Country-level counts for this table are order-dependent: `id_walk` is applied
only once `panel_ids` has resolved, and whether it has depends on cache state
and on what else the process built first (`Country('Ethiopia').individual_
education()` alone returns 63,139 rows; the same call after touching
`c.panel_ids` returns 62,939).  That instability is PRE-EXISTING, is unrelated
to these fixes -- it moves the untouched waves by the same rows in both the old
and the new config -- and is recorded in `.coder/ledger/323-ethiopia-config.md`.
"""
import warnings

import pytest
import yaml

from lsms_library.country import Country
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

ETH_WAVES = ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22']

# The wave-native household id, per wave.  W2/W3 re-identified the panel and
# renamed the key; W1/W4/W5 use the plain spelling.
NATIVE_HHID = {'2011-12': 'household_id',
               '2013-14': 'household_id2',
               '2015-16': 'household_id2',
               '2018-19': 'household_id',
               '2021-22': 'household_id'}

NATIVE_PID = {'2013-14': 'individual_id2',
              '2015-16': 'individual_id2'}

# Raw row counts of the section-2 education files.  Every row is a distinct
# person and every one must reach the wave frame on its own index tuple.
EDU_ROWS = {'2013-14': 23785, '2015-16': 23393}


def _wave_yaml(wave):
    path = countries_root() / 'Ethiopia' / wave / '_' / 'data_info.yml'
    if not path.exists():                                      # pragma: no cover
        pytest.skip(f'Ethiopia/{wave}/_/data_info.yml absent')
    with open(path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope='module')
def ethiopia():
    try:
        return Country('Ethiopia')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Ethiopia unavailable: {exc}')


# ---------------------------------------------------------------- config only

@pytest.mark.parametrize('wave', ETH_WAVES)
def test_cluster_features_merges_on_the_household_id_not_the_cluster_id(wave):
    """The `dfs:` merge key must be `i`, and `i` must reach `final_index`.

    Merging on `v` is the cartesian (GH #323 site 4).  `i` must also survive
    into `final_index`, because `Wave.cluster_features`' GH #161 projection to
    the (t, v) cluster grain only fires when `i` is an index LEVEL -- drop it
    earlier and the frame stays household-grain on a non-unique (t, v).
    """
    block = _wave_yaml(wave)['cluster_features']
    assert block.get('merge_on') == ['i'], (
        f"Ethiopia/{wave}/cluster_features merges on "
        f"{block.get('merge_on')!r}. Both sub-frames are HOUSEHOLD-grain, so "
        f"merging on the cluster key `v` is a many-to-many CARTESIAN PRODUCT "
        f"(GH #323 site 4). Merge on `i`."
    )
    assert 'i' in (block.get('final_index') or []), (
        f"Ethiopia/{wave}/cluster_features drops `i` before `final_index`. "
        f"Wave.cluster_features' collapse to the (t, v) grain only fires when "
        f"`i` is an index level; without it the frame stays household-grain."
    )


@pytest.mark.parametrize('wave', ETH_WAVES)
def test_cluster_features_geo_subframe_is_keyed_on_the_native_household_id(wave):
    """The geo sub-frame keys on the wave-native household id, never `v`.

    `household_id` is the W1 baseline id and is BLANK for households new to the
    panel, so in W2/W3 it is non-unique on the empty value: re-keying to it
    would trade the EA cartesian for a NULL-KEY one, since `pd.merge` matches
    null keys like any other value.
    """
    block = _wave_yaml(wave)['cluster_features']
    geo = block['df_geo']['idxvars']
    assert 'v' not in geo, (
        f"Ethiopia/{wave}/cluster_features df_geo still declares `v`; it is a "
        f"household-grain file and must be keyed on the household id."
    )
    assert geo.get('i') == NATIVE_HHID[wave], (
        f"Ethiopia/{wave}/cluster_features df_geo keys on {geo.get('i')!r}; "
        f"the wave-native household id is {NATIVE_HHID[wave]!r}."
    )
    assert block['df_main']['idxvars'].get('i') == NATIVE_HHID[wave], (
        f"Ethiopia/{wave}/cluster_features df_main must key on the same "
        f"household id as df_geo ({NATIVE_HHID[wave]!r})."
    )


@pytest.mark.parametrize('wave', sorted(NATIVE_PID))
def test_individual_education_uses_the_wave_native_ids(wave):
    """W2/W3 education must use household_id2 / individual_id2.

    The W1-baseline pair is blank for the W2 refreshment sample and is shared
    by W3 split-offs with their parent household -- either way the declared
    (t, i, pid) index stops identifying a person.
    """
    idx = _wave_yaml(wave)['individual_education']['idxvars']
    assert idx.get('i') == NATIVE_HHID[wave], (
        f"Ethiopia/{wave}/individual_education keys `i` on {idx.get('i')!r}; "
        f"the wave-native household id is {NATIVE_HHID[wave]!r}. The baseline "
        f"`household_id` is blank / shared and destroys rows (GH #323)."
    )
    assert idx.get('pid') == NATIVE_PID[wave], (
        f"Ethiopia/{wave}/individual_education keys `pid` on "
        f"{idx.get('pid')!r}; the wave-native person id is "
        f"{NATIVE_PID[wave]!r} -- the same one household_roster uses, without "
        f"which education cannot be joined to the roster at all."
    )


def test_no_dead_aggregation_key_in_ethiopia_config():
    """`aggregation:` is dead config and contradicts D1 -- core never reduces.

    An earlier Ethiopia branch declared reducers here.  Nothing reads them
    (`SkunkWorks/grain_aggregation_policy.org` §3a), so they would read as a
    fix while changing nothing.
    """
    path = countries_root() / 'Ethiopia' / '_' / 'data_scheme.yml'
    # `data_scheme.yml` uses the repo's `!make` tag, so it needs SchemeLoader.
    scheme = load_yaml(path)['Data Scheme']
    offenders = [k for k, v in scheme.items()
                 if isinstance(v, dict) and 'aggregation' in v]
    assert not offenders, (
        f"Ethiopia/_/data_scheme.yml declares `aggregation:` for {offenders}. "
        f"The core does NOT aggregate (GH #323 decision D1); the key is dead "
        f"config that only puts a signature on the corpse."
    )


# ---------------------------------------------------------------- data-backed

@pytest.mark.parametrize('wave', sorted(EDU_ROWS))
def test_individual_education_wave_frame_keeps_every_person(ethiopia, wave):
    """Every reported person reaches the wave frame on their own index tuple.

    Asserted at the WAVE level, upstream of `_normalize_dataframe_index` --
    post-collapse uniqueness holds by construction and proves nothing.
    """
    try:
        df = ethiopia[wave].grab_data('individual_education')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Ethiopia/{wave} individual_education unbuildable: '
                    f'{type(exc).__name__}: {exc}')
    assert len(df) == EDU_ROWS[wave], (
        f'Ethiopia/{wave} individual_education wave frame has {len(df)} rows, '
        f'expected {EDU_ROWS[wave]} (one per person in the section-2 file).'
    )
    dupes = int(df.index.duplicated().sum())
    assert dupes == 0, (
        f'Ethiopia/{wave} individual_education lands {dupes} row(s) on an '
        f'already-used (t, i, pid) tuple. The framework will collapse them '
        f'with groupby().first() and those people will silently disappear '
        f'(GH #323). The identifier is wrong -- use the wave-native '
        f'{NATIVE_HHID[wave]} / {NATIVE_PID[wave]}, not the W1 baseline.'
    )


def test_individual_education_recovers_the_two_conflated_students(ethiopia):
    """The W3 split-off pair that `first()` used to merge into one person.

    Baseline individual 0105010010503101 appears in BOTH halves of a split
    household, with genuinely different attainment.  Under the baseline ids the
    two rows shared one index tuple and one was destroyed.
    """
    try:
        df = ethiopia['2015-16'].grab_data('individual_education')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Ethiopia/2015-16 unbuildable: {type(exc).__name__}: {exc}')
    flat = df.reset_index()
    pids = {'01050102010010503101', '01050108880010503101'}
    got = flat[flat['pid'].isin(pids)]
    assert set(got['pid']) == pids, (
        f'expected both halves of the split household ({pids}); '
        f'got {set(got["pid"])}. One of two REAL, DISTINCT people is being '
        f'destroyed by the collapse (GH #323).'
    )
    attainment = set(got['Educational Attainment'].dropna())
    assert len(attainment) == 2, (
        f'the two students should record DIFFERENT attainment; got '
        f'{attainment}. If they are identical the rows have been merged.'
    )


@pytest.mark.parametrize('wave', ETH_WAVES)
def test_cluster_features_merge_manufactures_no_phantom_rows(ethiopia, wave):
    """The `dfs:` merge must emit no cartesian warning (GH #323 site 4).

    This is the assertion PR #627's guard exists to make possible.  On cores
    predating that guard the warning cannot be raised at all, so the test
    degrades to "the wave builds and the merge did not explode", checked
    against the cover page's own household count.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = ethiopia[wave].grab_data('cluster_features')
        except Exception as exc:                               # pragma: no cover
            pytest.fail(
                f'Ethiopia/{wave} cluster_features raised '
                f'{type(exc).__name__}: {exc}. Under PR #627 a dropped sub-df '
                f'that costs a REQUIRED declared column is fatal -- the geo '
                f'file most likely spells its coordinate columns differently '
                f'in this wave.')
    cartesian = [str(w.message) for w in caught
                 if 'CARTESIAN' in str(w.message)]
    assert not cartesian, (
        f'Ethiopia/{wave} cluster_features: {cartesian[0][:400]}'
    )
    # A cartesian would multiply the frame far past household grain; the cover
    # page has ~3.9k-6.8k households in every wave.
    assert len(df) < 10000, (
        f'Ethiopia/{wave} cluster_features wave frame has {len(df)} rows -- '
        f'far more than the wave has households. The `dfs:` merge has '
        f'MANUFACTURED rows (GH #323 site 4).'
    )
