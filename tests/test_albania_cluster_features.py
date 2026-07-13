"""Albania cluster_features -- GH #323 regression tests.

The bug: cluster_features is CLUSTER grain (index ``(t, v)``), but every Albania
wave extracted it from a HOUSEHOLD cover page (2003: the PERSON roster) while
declaring an extra household ``idxvars`` entry.  The framework then dropped that
level and silently collapsed the leftover duplicate ``(t, v)`` tuples with
``.first()`` -- discarding 3,149 / 8,229 / 1,347 / 3,360 source rows across
2002 / 2003 / 2004 / 2005.

For 2004 this was not merely redundant: ``m0_q01`` is the household's ORIGINAL
2002 PSU while ``m0_distr`` is its CURRENT district, so ``.first()`` handed a
*mover household's* district to the whole cluster, and two administrative
sentinel codes (995 / 999) were promoted into fabricated "clusters".

These tests assert on the rows where the answer was genuinely AMBIGUOUS -- the
conflicted PSUs, the ties, the single-household movers and the sentinels -- NOT
on the ~430 clean PSUs where any reducer trivially agrees.  A test that reports
"450/450 agree" would be measuring nothing.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll


@pytest.fixture(scope='module')
def cf():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ll.Country('Albania').cluster_features()


@pytest.fixture(scope='module')
def cf04(cf):
    f = cf.reset_index()
    return f[f['t'] == '2004'].set_index('v')['Region']


# --- the grain itself ------------------------------------------------------

def test_cluster_features_is_cluster_grain(cf):
    """One row per (t, v).  A duplicate here means the household level leaked
    back in and something is collapsing it silently again."""
    assert cf.index.names == ['t', 'v']
    dup = cf.index.duplicated().sum()
    assert dup == 0, f"{dup} duplicate (t, v) tuples"


@pytest.mark.parametrize('wave,n', [('2002', 450), ('2003', 449),
                                    ('2004', 448), ('2005', 480)])
def test_cluster_counts(cf, wave, n):
    """2004 is 448, not 450: the two sentinel 'PSUs' are not clusters.
    2003 is 449, not 450: 711 source rows carry a null PSU and cannot key a
    cluster row."""
    got = int((cf.reset_index()['t'] == wave).sum())
    assert got == n, f"{wave}: expected {n} clusters, got {got}"


# --- 2004: the rows where the answer was genuinely ambiguous ---------------

def test_2004_sentinel_psus_are_not_clusters(cf04):
    """995 (split-off/new HH) and 999 (moved/untraceable HH) are administrative
    codes.  None of their 83 households keys into 2002's (psu, hh).  Before the
    fix both existed as cluster rows -- and both were labelled 'BERAT', because
    .first() grabbed whichever household came first."""
    for sentinel in ('995', '999'):
        assert sentinel not in cf04.index, (
            f"sentinel {sentinel} is being emitted as a real cluster")


@pytest.mark.parametrize('psu', ['43', '44', '47', '52', '53'])
def test_2004_tied_psus_resolve_to_the_2002_district(cf04, psu):
    """Each of these five PSUs holds exactly two households: one reporting KUKES
    and one reporting TIRANE -- a 1-vs-1 tie.  Neither .first() nor a majority
    vote can resolve it (first() only 'wins' here by source row order).  The
    2002 anchor does: all five are 2002 district code 17 = KUKES."""
    assert cf04.get(psu) == 'KUKES'


@pytest.mark.parametrize('psu,expected,was', [
    ('16',  'SHKODER',     'DURRES'),    # 4 SHKODER vs 1 mover to DURRES
    ('259', 'MALLAKASTER', 'FIER'),      # 3 MALLAKASTER vs 1 mover to FIER
    ('280', 'SKRAPAR',     'TIRANE'),    # 5 SKRAPAR vs 1 mover to TIRANE
    ('297', 'KORCE',       'KOLONJE'),   # 6 KORCE vs 1 mover to KOLONJE
    ('344', 'TEPELENE',    'SARANDE'),   # 3 TEPELENE / 2 SARANDE / 2 TIRANE
])
def test_2004_conflicted_psus_take_the_psu_district_not_a_movers(
        cf04, psu, expected, was):
    """A household that moved keeps its original PSU code but acquires a new
    district.  .first() therefore relabelled these clusters with a mover's
    district."""
    got = cf04.get(psu)
    assert got == expected, f"PSU {psu}: expected {expected}, got {got} (was {was})"


def test_2004_single_household_mover_psu(cf04):
    """PSU 223 holds exactly ONE household, and that household moved to ELBASAN.
    A lone mover makes its PSU look perfectly 'unanimous', so NO within-2004
    consistency check can catch this -- only the 2002 anchor can.  49 of the 448
    real PSUs hold a single household, so this is a class, not a curiosity."""
    assert cf04.get('223') == 'GRAMSH'


def test_2004_no_mojibake_district_names(cf04):
    """Korce appears in the source as both 'KORCE' and 'KOR\\x80E' (a mis-decoded
    C-cedilla) -- two spellings of one district."""
    bad = [v for v in cf04.dropna().unique() if '\x80' in str(v)]
    assert not bad, f"mojibake district names survive: {bad}"


# --- sample(): the sentinels must not masquerade as cluster ids ------------

def test_sample_2004_does_not_invent_sentinel_clusters():
    """sample() used to emit v = 995 / 999 for 78 households, citing two
    administrative codes as genuine sampling clusters.  The households are kept
    (they are real, weighted households); only their cluster id is <NA>, because
    we do not know which cluster they belong to."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        s = ll.Country('Albania').sample().reset_index()
    s04 = s[s['t'] == '2004']
    leaked = s04['v'].isin(['995', '999']).sum()
    assert leaked == 0, f"{leaked} households still carry a sentinel cluster id"
    assert s04['v'].notna().sum() > 1600, "the real cluster ids went missing too"


# --- the reducer refuses to guess -----------------------------------------

def test_cluster_reduce_emits_NA_rather_than_guessing():
    """The whole point of GH #323: a cluster carrying two different values for a
    cluster attribute must NOT be silently assigned one of them.  Silently
    MISSING (class-2) is strictly safer than silently WRONG (class-1)."""
    import importlib.util
    from lsms_library.paths import countries_root
    spec = importlib.util.spec_from_file_location(
        'albania_mod', countries_root() / 'Albania' / '_' / 'albania.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    df = pd.DataFrame({
        'v': ['1', '1', '2', '2'],
        'Region': ['A', 'B', 'C', 'C'],   # cluster 1 is inconsistent
    })
    with pytest.warns(RuntimeWarning, match='GH #323'):
        out = mod.cluster_reduce(df, columns=['Region'], wave='test')
    assert pd.isna(out.loc['1', 'Region']), "inconsistent cluster was GUESSED"
    assert out.loc['2', 'Region'] == 'C', "consistent cluster was lost"
