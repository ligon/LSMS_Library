"""GH #323: China's declared indexes must be unique AT THE SOURCE.

The #323 bug is ``_normalize_dataframe_index`` silently collapsing a non-unique
DECLARED index with ``groupby().first()``.  China contributed 2979 collapsed
rows across three tables:

  1995-97 / cluster_features       2972 of 3002   (EXTRACTION_BUG)
  1995-97 / household_roster          4 of 3002   (GENUINE_DUPLICATES)
  1995-97 / individual_education      3 of 2843   (GENUINE_DUPLICATES)

``cluster_features`` is a VILLAGE-level table (declared index ``(t, v)``) that
was built from the PERSON-level household roster with ``i: hid`` in its
idxvars, so all 3002 people emitted a cluster row for 30 villages.  Note that
this one never even warned: ``Wave.cluster_features`` collapses with an
un-warned ``groupby().first()`` whenever ``i`` is in the index (GH #161), so
the #323 warning was never reached.

These tests assert the fix at the tier where the truth lives -- the WAVE-level
frame handed to ``_normalize_dataframe_index`` -- not the country-level output,
which is written POST-collapse and would show a clean (t, v) index either way.
That distinction is the whole trap in #323: a scan of the collapsed tier
reports zero duplicates precisely because the collapse already happened.

All tests here FAIL on pristine ``development``.

Tests skip if China's data isn't available (no DVC / no .dta on disk).
"""
from __future__ import annotations

import importlib.util
import os
import warnings

import pandas as pd
import pytest

WAVE = '1995-97'


def _china_module():
    """Import China/_/china.py directly (it is config, not an importable package)."""
    from lsms_library.paths import countries_root
    path = countries_root() / 'China' / '_' / 'china.py'
    spec = importlib.util.spec_from_file_location('china_cfg', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _roster(rows):
    """(pid, Sex, Relationship, Age) tuples -> a wave-shaped roster frame."""
    df = pd.DataFrame(
        [{'t': WAVE, 'i': i, 'pid': pid, 'v': i[:3],
          'Sex': sex, 'Relationship': rel, 'Age': age}
         for i, pid, sex, rel, age in rows])
    return df.set_index(['t', 'i', 'pid', 'v'])

# The canonical index each table declares in China/_/data_scheme.yml.
DECLARED = {
    'cluster_features': ['t', 'v'],
    'household_roster': ['t', 'i', 'pid'],
    'individual_education': ['t', 'i', 'pid'],
    'sample': ['i', 't'],
    'plot_features': ['t', 'i', 'plot_id'],
}


def _china_or_skip():
    try:
        import lsms_library as ll
        c = ll.Country('China')
        c['1995-97'].grab_data('sample')
    except Exception as exc:                      # pragma: no cover - env dependent
        pytest.skip(f'China data unavailable: {exc}')
    return c


@pytest.fixture(scope='module')
def china():
    # Force a cold build: a warm L2 cache is written POST-collapse and would
    # hide exactly the defect under test.
    old = os.environ.get('LSMS_NO_CACHE')
    os.environ['LSMS_NO_CACHE'] = '1'
    try:
        yield _china_or_skip()
    finally:
        if old is None:
            os.environ.pop('LSMS_NO_CACHE', None)
        else:
            os.environ['LSMS_NO_CACHE'] = old


@pytest.mark.parametrize('table', sorted(DECLARED))
def test_wave_source_index_is_unique(china, table):
    """The frame handed to _normalize_dataframe_index has a unique declared index.

    Pre-fix this fails for cluster_features (2972 dups), household_roster (4)
    and individual_education (3).
    """
    src = china[WAVE].grab_data(table)
    flat = src.reset_index()
    key = [k for k in DECLARED[table] if k in flat.columns]
    assert key, f'{table}: none of the declared index levels {DECLARED[table]} present'
    dup = int(flat.duplicated(key, keep='first').sum())
    assert dup == 0, (
        f'China {WAVE} {table}: declared index {key} has {dup} duplicate row(s) at '
        f'the SOURCE tier ({len(flat)} rows).  These would be silently collapsed by '
        f'groupby().first() -- GH #323.'
    )


def test_cluster_features_is_village_grain_not_person_grain(china):
    """cluster_features must be extracted at village grain, not once per person.

    30 villages, 787 households, 3002 people.  Pre-fix the wave frame had 3002
    rows (one per PERSON); it must now have exactly one row per village.
    """
    src = china[WAVE].grab_data('cluster_features')
    assert len(src) == 30, (
        f'cluster_features should be one row per village (30); got {len(src)}. '
        f'3002 => the person-level roster is being used as the source (GH #323); '
        f'787 => the household file is being emitted un-reduced.'
    )
    assert 'i' not in src.index.names, (
        "cluster_features must not carry a household 'i' level: it is a (t, v) "
        "table, and an 'i' in the index triggers Wave.cluster_features' "
        "un-warned groupby().first() collapse (GH #161/#323)."
    )


def test_cluster_features_idxvars_declare_no_household_i(china):
    """Config-level guard: `i` must not reappear in cluster_features idxvars."""
    info = china[WAVE].resources['cluster_features']
    idxvars = info.get('idxvars') or {}
    assert 'i' not in idxvars, (
        "China cluster_features idxvars must not declare 'i' -- it makes every "
        'household (or person) emit a village row, which is then silently '
        'collapsed away.  GH #323.'
    )


def test_region_is_single_valued_within_village(china):
    """The invariant that made the old collapse *accidentally* lossless.

    cluster_features' only column, Region, is a deterministic function of the
    village.  The old code relied on that without ever checking it.  Now it is
    enforced -- and here it is pinned.
    """
    cf = china.cluster_features().reset_index()
    per_village = cf.groupby('v')['Region'].nunique()
    bad = per_village[per_village > 1]
    assert bad.empty, f'villages with >1 Region: {bad.to_dict()}'
    assert cf['Region'].value_counts().to_dict() == {'7': 15, '8': 15}, (
        'expected 15 villages in province 7 (Hebei) and 15 in province 8 '
        f"(Liaoning); got {cf['Region'].value_counts().to_dict()}"
    )


def test_no_323_collapse_warning_on_cold_build(china):
    """No table may reach the groupby().first() collapse.

    The #323 warning fires ONLY on a cold build -- in warm operation the loss
    is already baked into the cache, so this must be asserted cold.
    """
    offenders = {}
    for table in sorted(DECLARED):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            getattr(china, table)()
            hits = [str(w.message) for w in caught if 'GH #323' in str(w.message)]
        if hits:
            offenders[table] = hits
    assert not offenders, f'#323 collapse warning(s) still fired: {offenders}'


def test_household_30132_duplicate_record_resolved(china):
    """hid 30132's roster is recorded TWICE, byte-identically, in S01A2.DTA.

    The same whole-household duplicate RECORD also appears in S02.DTA
    (education) and S05B.DTA (plots -- GH #513).  Exact-duplicate removal is
    lossless: the household has 3 real members.
    """
    roster = china.household_roster().reset_index()
    hh = roster[roster['i'].astype(str) == '30132']
    assert len(hh) == 3, f'hid 30132 should have 3 members, got {len(hh)}'
    assert sorted(hh['pid'].astype(str)) == ['1', '2', '3']

    educ = china.individual_education().reset_index()
    ed = educ[educ['i'].astype(str) == '30132']
    assert len(ed) == 3, f'hid 30132 should have 3 education rows, got {len(ed)}'


def test_household_10108_miskeyed_daughter_in_law_resolved(china):
    """hid 10108 files its daughter-in-law both as pid 4 and (mis-keyed) as pid 3.

    The mis-keyed row is byte-identical to the pid-4 row on every non-pid
    column; S02.DTA independently lists exactly four members (pid 1-4).  The
    household therefore has 4 people and pid 3 is the SON.

    This is the row that looked like the dangerous "two distinct real people
    share a key" case and is not.  Pinned because ``groupby().first()`` is
    column-wise first-non-null and could have spliced the son and the
    daughter-in-law into a single fabricated person.
    """
    roster = china.household_roster().reset_index()
    hh = roster[roster['i'].astype(str) == '10108'].set_index('pid')
    assert len(hh) == 4, f'hid 10108 should have 4 members, got {len(hh)}'
    assert sorted(hh.index.astype(str)) == ['1', '2', '3', '4']

    # pid 3 is the son -- male -- NOT the daughter-in-law.
    assert hh.loc['3', 'Sex'] == 'M', (
        "hid 10108 pid 3 must be the son (M).  Getting 'F' means the mis-keyed "
        'daughter-in-law row won the collapse.'
    )
    assert hh.loc['4', 'Sex'] == 'F'
    assert int(hh.loc['3', 'Age']) == 25 and int(hh.loc['4', 'Age']) == 25


# --------------------------------------------------------------------------
# The guards must have TEETH.  A guard that cannot fire is not a guard, and
# "the collapse happened to be correct here" is not a reason to ship an
# unenforced invariant.  These drive the hooks directly with synthetic frames.
# --------------------------------------------------------------------------

def test_guard_fires_when_region_varies_within_a_village():
    """cluster_features: two Regions for one village must RAISE, not pick one.

    This is the invariant the old code silently relied on and never checked.
    """
    china_mod = _china_module()
    df = pd.DataFrame(
        {'Region': ['7', '8']},                     # same village, two provinces
        index=pd.MultiIndex.from_tuples(
            [(WAVE, '101'), (WAVE, '101')], names=['t', 'v']),
    )
    with pytest.raises(ValueError, match=r'not unique'):
        china_mod.cluster_features(df)


def test_guard_fires_on_ambiguous_person_collision():
    """household_roster: two genuinely DIFFERENT people sharing (i, pid) must RAISE.

    This is the dangerous INDEX_INCOMPLETE shape.  It must never be absorbed by
    groupby().first() -- which, being column-wise first-non-null, could splice
    the two into a person who does not exist.  Loudly missing beats quietly wrong.
    """
    china_mod = _china_module()
    df = _roster([
        ('10101', '1', 'M', 'Head', 40),
        ('10101', '2', 'F', 'Spouse', 38),
        ('10101', '3', 'M', 'Child', 12),
        ('10101', '3', 'F', 'Child', 9),   # different person, same key, NOT a copy
    ])
    with pytest.raises(ValueError, match=r'not unique'):
        china_mod.household_roster(df)


def test_guard_fires_on_undocumented_miskeyed_row():
    """household_roster: a mis-keyed row we have NOT verified must RAISE.

    The erratum is scoped to hid 10108.  A same-shaped defect in another
    household means the raw source changed and must be re-verified by a human,
    not silently swallowed by a rule that generalised itself.
    """
    china_mod = _china_module()
    df = _roster([
        ('20202', '1', 'M', 'Head', 50),
        ('20202', '2', 'F', 'Spouse', 48),
        ('20202', '2', 'M', 'Child', 20),   # collides with the spouse...
        ('20202', '3', 'M', 'Child', 20),   # ...and duplicates pid 3 exactly
    ])
    with pytest.raises(ValueError, match=r'undocumented mis-keyed'):
        china_mod.household_roster(df)


def test_identical_twins_are_never_deleted():
    """The mis-key rule must NOT delete identical twins.

    Two siblings with the same sex, age and relationship are byte-identical on
    every non-pid column.  A naive "drop within-household duplicate persons on
    all non-pid columns" rule would silently erase one of them.  Ours requires a
    real (i, pid) collision, and twins carry distinct pids -- so they survive.
    """
    china_mod = _china_module()
    df = _roster([
        ('30303', '1', 'M', 'Head', 40),
        ('30303', '2', 'F', 'Spouse', 38),
        ('30303', '3', 'F', 'Child', 7),    # twin
        ('30303', '4', 'F', 'Child', 7),    # twin -- identical but a real person
    ])
    out = china_mod.household_roster(df)
    assert len(out) == 4, 'identical twins must both survive'
    assert sorted(out.reset_index()['pid']) == ['1', '2', '3', '4']


def test_exact_duplicate_rows_are_dropped_losslessly():
    """The hid-30132 shape: a whole household filed twice, byte-identically."""
    china_mod = _china_module()
    df = _roster([
        ('40404', '1', 'M', 'Head', 60),
        ('40404', '1', 'M', 'Head', 60),    # byte-identical duplicate
        ('40404', '2', 'F', 'Spouse', 58),
        ('40404', '2', 'F', 'Spouse', 58),  # byte-identical duplicate
    ])
    out = china_mod.household_roster(df)
    assert len(out) == 2
    assert sorted(out.reset_index()['pid']) == ['1', '2']
