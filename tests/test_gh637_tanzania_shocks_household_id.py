"""GH #637 -- Tanzania 2008-15 ``shocks`` must be keyed on the HOUSEHOLD.

``lsms_library/countries/Tanzania/2008-15/_/shocks.py`` used to build ``i`` from
``UPHI``, the NPS panel-tracking LINE index ("1".."14985"), rather than from
``r_hhid``, the household id that ``sample``, ``household_roster`` and every
other Tanzania table use.  Two consequences, both silent:

1. **No 2008-15 shocks row could be joined to its own household.**  The two id
   namespaces shared *zero* values.  The 2019-20 / 2020-21 waves (which key on
   ``sdd_hhid`` / ``y5_hhid``) overlapped the roster by 100%, so the defect was
   invisible unless you looked per wave.
2. **The panel-line replication survived as data.**  The upd4 household modules
   record a household-round once per DESCENDANT line, so one household's shock
   report arrived 1..11 times.  Keyed on UPHI those copies each became their
   own "household": 74,341 wave-level rows standing for 39,724 household-shock
   facts, inflating any count of who suffered a shock by ~1.9x.

These tests assert the *identifier* and the *row counts*, never post-collapse
index uniqueness -- uniqueness held perfectly with the bug present (that was
precisely how the bug hid), so it cannot discriminate.
"""
import pytest

import lsms_library as ll

# NB `from conftest import ...` resolves to the ROOT conftest.py (rootdir is on
# sys.path and `tests/` is a package), so the shared skip marker has to be
# imported by its package path.
from tests.conftest import requires_s3

# The four NPS rounds carried by the 2008-15 multi-round folder.
UPD_WAVES = ['2008-09', '2010-11', '2012-13', '2014-15']

# Cold-build row counts of Country('Tanzania').shocks() per wave.
EXPECTED_ROWS = {
    '2008-09': 7391,
    '2010-11': 7455,
    '2012-13': 8247,
    '2014-15': 6722,
    # untouched by the fix -- these waves already keyed on the household id
    '2019-20': 864,
    '2020-21': 5067,
}

# A household whose 2008-09 shock report was recorded on three panel lines
# (UPHI 1, 2, 3) -- 9 identical source rows for 3 shocks.
EXAMPLE_HH = '01010140020171'
EXAMPLE_SHOCKS_2008 = {
    'CROP DISEASE OR CROP PESTS',
    'DEATH OF OTHER FAMILY MEMBER',
    'LARGE RISE IN PRICE OF FOOD',
}


@pytest.fixture(scope='module')
def tanzania():
    return ll.Country('Tanzania')


@pytest.fixture(scope='module')
def shocks(tanzania):
    return tanzania.shocks()


@pytest.fixture(scope='module')
def sample(tanzania):
    return tanzania.sample()


def _ids(df, t):
    """The distinct household ids a table carries for wave *t*."""
    lv = df.index.get_level_values
    return set(lv('i')[lv('t').astype(str) == t].astype(str))


@requires_s3
@pytest.mark.parametrize('t', UPD_WAVES)
def test_shocks_i_is_a_household_known_to_sample(shocks, sample, t):
    """Every 2008-15 shocks household must be a household ``sample()`` knows.

    With ``i = UPHI`` the overlap was 0 in all four rounds.
    """
    got, known = _ids(shocks, t), _ids(sample, t)
    assert got, f'Tanzania shocks carries no rows for {t}'
    orphans = got - known
    assert not orphans, (
        f'Tanzania shocks {t}: {len(orphans)} of {len(got)} household ids are '
        f'unknown to sample() -- e.g. {sorted(orphans)[:5]}.  `i` is not a '
        f'household id (GH #637: it used to be the UPHI panel-line index).')


@requires_s3
@pytest.mark.parametrize('t', UPD_WAVES)
def test_shocks_i_is_not_a_bare_line_index(shocks, t):
    """The UPHI line index rendered as "1", "2", ... -- r_hhid never does.

    r_hhid is 14 characters in round 1, 16 in round 2 and ``NNNN-NNN`` in
    rounds 3-4, so a short all-digit id is a positive signature of the bug
    rather than merely an absence of the fix.
    """
    bad = {i for i in _ids(shocks, t) if i.isdigit() and len(i) < 8}
    assert not bad, (
        f'Tanzania shocks {t}: {len(bad)} household ids look like UPHI line '
        f'indices, e.g. {sorted(bad)[:5]} (GH #637).')


@requires_s3
def test_shocks_per_wave_row_counts(shocks):
    """Pins the de-replication, and pins that it touched only 2008-15."""
    got = {str(t): int(n) for t, n
           in shocks.groupby(level='t', observed=True).size().items()}
    assert got == EXPECTED_ROWS, (
        f'Tanzania shocks row counts changed: {got} != {EXPECTED_ROWS}.  A '
        f'2008-15 count near double the expected one means the UPHI panel-line '
        f'replication is being carried as data again (GH #637).')


@requires_s3
def test_a_replicated_household_reports_each_shock_once(shocks):
    """Household 01010140020171, 2008-09: 3 shocks on 3 panel lines.

    Keyed on UPHI this household did not appear at all -- its rows lived under
    ``i`` = "1", "2" and "3".  Keyed on the household it must appear exactly
    once per shock, not once per line.
    """
    lv = shocks.index.get_level_values
    mask = ((lv('i').astype(str) == EXAMPLE_HH)
            & (lv('t').astype(str) == '2008-09'))
    rows = shocks[mask]
    assert len(rows) == len(EXAMPLE_SHOCKS_2008), (
        f'household {EXAMPLE_HH} has {len(rows)} shock rows in 2008-09, '
        f'expected {len(EXAMPLE_SHOCKS_2008)} (one per shock, not one per '
        f'panel line) -- GH #637')
    assert set(rows.index.get_level_values('Shock').astype(str)) == \
        EXAMPLE_SHOCKS_2008


@requires_s3
def test_household_ids_are_shared_across_shocks_and_roster(tanzania, shocks):
    """The point of the identifier: shocks must be joinable to the roster.

    Checked per wave, because the 2019-20 / 2020-21 waves always overlapped and
    would mask a 2008-15 overlap of zero in an aggregate test.
    """
    roster = tanzania.household_roster()
    for t in UPD_WAVES:
        s, r = _ids(shocks, t), _ids(roster, t)
        assert s and r, f'no rows for {t}'
        assert s <= r, (
            f'Tanzania {t}: {len(s - r)} of {len(s)} shocks households are '
            f'absent from household_roster (GH #637)')
