"""Regression tests for GH #323 in Niger: the silently-collapsed household key.

The ECVMA-II (2014-15) household identifier is the TRIPLE
``(GRAPPE, MENAGE, EXTENSION)``.  Niger declared only ``(GRAPPE, MENAGE)``, so
59 (grappe, menage) pairs -- each hosting TWO DISTINCT HOUSEHOLDS -- shared one
index key, and ``_normalize_dataframe_index`` silently collapsed them with
``groupby().first()``, ERASING one real household per collision.

These tests are deliberately anchored on the AMBIGUOUS cases (the 59 collided
pairs, the 4 conflicting clusters, the seed slots that actually collide), NOT on
the ~3,558 households where the answer was never in doubt.  A test that passes
only on the rows that were already trivially right proves nothing.

Every test here FAILS on pristine ``development``.

Skips if Niger data isn't available (no DVC / no ``.dta`` on disk).
"""
from __future__ import annotations

import json
import warnings

import pandas as pd
import pytest


def _niger_or_skip():
    try:
        import lsms_library as ll
        c = ll.Country('Niger')
        c.sample()
        return c
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Niger data unavailable: {exc}")


@pytest.fixture(scope='module')
def niger():
    return _niger_or_skip()


@pytest.fixture(scope='module')
def wave14(niger):
    """The 2014-15 Wave object (pre-id_walk ids, so `i` is niger.i's output)."""
    return niger['2014-15']


# ---------------------------------------------------------------------------
# FIX A -- EXTENSION restored to the household key
# ---------------------------------------------------------------------------

def test_sample_2014_15_keeps_all_3617_households(niger):
    """59 households were being erased outright.  3558 -> 3617."""
    s = niger.sample()
    n = int((s.index.get_level_values('t') == '2014-15').sum())
    assert n == 3617, (
        f"Niger sample() 2014-15 has {n} households; the ECVMA-II cover page "
        f"has 3617.  3558 means EXTENSION was dropped from the household key "
        f"and 59 real households were collapsed away (GH #323)."
    )


@pytest.mark.parametrize('table,expected', [
    ('sample', 3617),
    ('housing', 3617),
])
def test_household_level_tables_have_every_household(niger, table, expected):
    df = getattr(niger, table)()
    n = int((df.index.get_level_values('t') == '2014-15').sum())
    assert n == expected, f"{table} 2014-15: {n} rows, expected {expected}"


def test_no_niger_table_silently_collapses_its_declared_index(niger):
    """The #323 invariant: a declared index must be a KEY of the data.

    Any duplicate here means the framework's groupby().first() is discarding
    rows.  (``assets`` is excluded: its (t, i, j) index is legitimately
    non-unique -- the asset roster is item-level, several items per asset TYPE
    -- and the framework RETURNS those rows rather than dropping them, so it is
    not silent loss.  That is a separate, pre-existing schema question.)
    """
    offenders = {}
    for table in ['sample', 'cluster_features', 'household_roster',
                  'individual_education', 'housing', 'shocks', 'food_security',
                  'crop_production', 'plot_inputs', 'livestock', 'plot_labor',
                  'people_last7days']:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df = getattr(niger, table)()
        n = int(df.index.duplicated().sum())
        if n:
            offenders[table] = n
    assert not offenders, (
        f"declared index is not a key -- groupby().first() is silently "
        f"discarding rows: {offenders} (GH #323)"
    )


def test_the_59_collided_pairs_are_two_distinct_households(wave14):
    """Each of the 59 (grappe, menage) pairs must yield TWO household ids."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        s = wave14.grab_data('sample').reset_index()
    assert len(s) == 3617
    assert s['i'].is_unique, "household id is not unique within 2014-15"
    # niger.i() = grappe + '0' + zeropad2(menage) + extension, so stripping the
    # trailing extension digit recovers the (grappe, menage) pair.
    gm = s['i'].str[:-1]
    pairs_with_two = int((gm.value_counts() == 2).sum())
    assert pairs_with_two == 59, (
        f"{pairs_with_two} (grappe, menage) pairs carry two households; the "
        f"ECVMA-II cover has exactly 59.  0 means EXTENSION is gone again."
    )


def test_split_off_household_2_1_survives_with_its_own_head(wave14):
    """GRAPPE=2 MENAGE=1: first() kept the EXT=0 roster and ERASED the EXT=2
    household -- a 29-year-old male head and his 22-year-old wife -- by
    overwriting them at pids 1 and 2."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        r = wave14.grab_data('household_roster').reset_index()
    # '20010' = (g=2, m=1, ext=0); '20012' = (g=2, m=1, ext=2)
    parent = r[r['i'] == '20010']
    split = r[r['i'] == '20012']
    assert len(parent) == 15, f"EXT=0 roster has {len(parent)} members, expected 15"
    assert len(split) == 2, (
        f"EXT=2 household has {len(split)} members, expected 2.  0 means the "
        f"split-off household was erased by the collapse (GH #323)."
    )
    # NB: wave-level rows are pre-_finalize_result, so Sex is the raw French label.
    head = split[split['pid'] == '1'].iloc[0]
    assert head['Sex'] == 'Masculin' and int(head['Age']) == 29, (
        f"the EXT=2 head is {head['Sex']}/{head['Age']}; expected Masculin/29.  "
        f"Féminin/56 means pid 1 was overwritten by the EXT=0 household's head."
    )
    spouse = split[split['pid'] == '2'].iloc[0]
    assert spouse['Sex'] == 'Féminin' and int(spouse['Age']) == 22
    # the two rosters must be genuinely disjoint households, not one chimera
    assert set(parent['pid']) & set(split['pid']) == {'1', '2'}
    assert int(parent[parent['pid'] == '1'].iloc[0]['Age']) == 56


def test_chimera_rosters_do_not_exist(wave14):
    """6 of the 59 produced CHIMERA households -- one roster mixing members
    drawn from two different real households.  No household may contain more
    members than its own real roster."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        r = wave14.grab_data('household_roster').reset_index()
    # Every (i, pid) must be unique: a chimera shows up as a repeated pid.
    dup = r.duplicated(['i', 'pid']).sum()
    assert dup == 0, f"{dup} duplicated (household, pid) -- chimera rosters (GH #323)"


def test_harvest_of_both_households_at_grappe60_menage7_survives(niger):
    """Plot 1_1 Mil was reported by BOTH households at (60, 7): the EXT=0
    household 20 Botte and the EXT=2 household 43.  first() kept 20 and DROPPED
    the 43 -- the harvest was truncated, not summed.

    NB `df['plot']`, never `df.plot` -- `.plot` is pandas' plotting accessor, so
    `df.plot == '1_1'` is a scalar False and silently matches nothing.
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = niger.crop_production().reset_index()
    # '6007'   = (g=60, m=7, ext=0), rewritten by id_walk to its 2011-12 hid
    # '600072' = (g=60, m=7, ext=2), a split-off: keeps its own id, unlinked
    both = cp[(cp['t'] == '2014-15') & (cp['i'].astype(str).isin(['6007', '600072']))]
    mil11 = both[(both['plot'] == '1_1') & (both['crop'] == 'Mil')]
    qty = set(mil11['Quantity'].dropna().astype(float))
    assert {20.0, 43.0} <= qty, (
        f"plot 1_1 Mil quantities for (grappe 60, menage 7) are {sorted(qty)}; "
        f"expected both 20.0 (EXT=0) and 43.0 (EXT=2).  A missing 43 means the "
        f"split-off household's harvest was truncated away (GH #323)."
    )
    # the split-off's whole harvest must be present, not just the one plot
    split = both[both['i'].astype(str) == '600072']
    assert set(split['Quantity'].dropna().astype(float)) >= {43.0, 70.0, 66.0}


# ---------------------------------------------------------------------------
# FIX A co-change -- the panel rewrite must stay INJECTIVE
# ---------------------------------------------------------------------------

def test_panel_rewrite_is_injective(niger):
    """updated_ids is an identity-REWRITE map: two households mapping to one
    canonical id would be MERGED by id_walk -- reintroducing #323 one layer up.
    Split-off households must therefore NOT inherit their parent's link."""
    updated = niger.updated_ids
    for wave, mapping in updated.items():
        canonical = list(mapping.values())
        dupes = {c for c in canonical if canonical.count(c) > 1}
        assert not dupes, (
            f"{wave}: canonical id(s) {sorted(dupes)[:5]} are claimed by more "
            f"than one household -- id_walk would MERGE them (GH #323)"
        )


# ---------------------------------------------------------------------------
# FIX B -- cluster_features is de-duplicated by MAJORITY, not by row order
# ---------------------------------------------------------------------------

def test_cluster_features_is_one_row_per_cluster(niger):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cf = niger.cluster_features()
    assert int(cf.index.duplicated().sum()) == 0
    counts = cf.reset_index().groupby('t', observed=True).size().to_dict()
    assert counts == {'2011-12': 270, '2014-15': 270,
                      '2018-19': 504, '2021-22': 555}, counts


def test_typo_clusters_get_the_majority_not_the_first_row(niger):
    """4 clusters in 2014-15 disagree on Region/District (enumerator typos).
    groupby().first() picks by ROW ORDER and can hand a whole cluster the typo;
    we take the within-cluster MODE."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cf = niger.cluster_features().reset_index()
    cf = cf[cf['t'] == '2014-15'].set_index('v')
    # NB District is passed through categorical_mapping at API time, so v=201's
    # majority raw label 'Niamey1' surfaces as 'Niamey'.  What matters is that
    # each cluster takes its MAJORITY, never the single typo'd row.
    expected = {          # v -> (majority District, the typo it must NOT take)
        '201': ('Niamey', 'Mirriah'),
        '7':   ('Arlit', 'Tchirozérine'),
        '84':  ('Aguié', 'Madarounfa'),
        '86':  ('Tessaoua', 'Abalak'),
    }
    for v, (majority, typo) in expected.items():
        if v not in cf.index:
            pytest.skip(f'cluster {v} absent')
        got = cf.loc[v, 'District']
        assert got != typo, (
            f"cluster {v} took the typo'd District {typo!r} -- groupby().first() "
            f"picks by ROW ORDER, not by majority (GH #323)"
        )
        assert got == majority, f"cluster {v}: District={got!r}, expected {majority!r}"


# ---------------------------------------------------------------------------
# FIX C -- lossy harmonized labels must not manufacture key collisions
# ---------------------------------------------------------------------------

def test_seed_slots_are_summed_not_discarded(niger):
    """The 7 'Semences' questionnaire slots all harmonize to input='Seed', so a
    household planting one crop's seed from several slots lands several rows on
    ONE index key.  first() kept one and dropped the rest.  They are ADDITIVE.

    Checked at the WAVE level, where `i` is niger.i()'s output, so the seed sum
    is isolated from the panel id rewrite.
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        pi = niger['2014-15'].grab_data('plot_inputs').reset_index()
    assert int(pi.duplicated(['i', 'input', 'crop', 'u']).sum()) == 0, (
        "plot_inputs 2014-15 still has duplicate index keys -- the seed slots "
        "are colliding and first() is discarding rows (GH #323)"
    )
    # grappe=38 menage=16 ext=0 reported ONE crop's seed in THREE slots: 5+20+25.
    row = pi[(pi['i'].astype(str) == '380160')
             & (pi['input'] == 'Seed') & (pi['u'] == 'Tiya')]
    if row.empty:
        pytest.skip('household 38/16 seed row not present')
    total = float(row['Quantity'].sum())
    assert total == pytest.approx(50.0), (
        f"seed quantity for 38/16 is {total}; expected 50 (5 + 20 + 25 across "
        f"three 'Semences' slots).  Getting 5, 20 or 25 means first() kept one "
        f"slot and discarded the others (GH #323)."
    )


def test_residual_seed_crop_bucket_is_injective(niger):
    """The 4 EHCVM catch-all seed slots used to share ONE 'Autre crop' label, so
    genuinely different residual seed types -- other-cereal seed vs tuber
    cuttings -- collided on the `crop` INDEX level and one was dropped.

    Scope: the EHCVM waves.  'Autre crop' remains a legitimate label in the
    ECVMA waves, where it comes from harmonize_food's own crop column, not from
    the seed-label fallback.
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        pi = niger.plot_inputs().reset_index()
    ehcvm = pi[pi['t'].isin(['2018-19', '2021-22'])]
    crops = set(ehcvm['crop'].dropna().astype(str).unique())
    assert 'Autre crop' not in crops, (
        "the EHCVM residual seed slots have collapsed back onto a single "
        "'Autre crop' bucket -- distinct residual seed types will collide on "
        "the crop index level again (GH #323)"
    )
    assert any(c.startswith('Autre crop (') for c in crops), (
        f"expected injective residual labels like 'Autre crop (céréales)'; "
        f"got {sorted(c for c in crops if 'Autre' in c)}"
    )
    # and the EHCVM waves must no longer lose rows to that collision
    for t, n in [('2018-19', 13401), ('2021-22', 12436)]:
        got = int((pi['t'] == t).sum())
        assert got == n, f"plot_inputs {t}: {got} rows, expected {n}"
