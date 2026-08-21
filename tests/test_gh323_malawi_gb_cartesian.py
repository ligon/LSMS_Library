"""GH #323 site 4 / GH #627 — Malawi's and Guinea-Bissau's `dfs:` cartesians.

PR #627's 40-country census found 8 many-to-many `dfs:` merges fabricating
4,907,774 phantom rows.  Mali's 4.3M went in PR #641; these three are the rest
of the ones nobody owned:

    Malawi 2010-11        12,271 x 12,271 -> 196,083 rows  (183,812 phantom)
    Malawi 2019-20        14,612 x 11,434 -> 185,842 rows  (171,230 phantom)
    Guinea-Bissau 2018-19  5,351 x    450 ->   5,410 rows  (     59 phantom)

They are TWO DIFFERENT SHAPES and take two different cures.

**Malawi — a household-grain frame keyed on the cluster.**  Both sub-frames are
one row per household, merged on `v`, so `pd.merge` paired every household of
an EA with every other.  The geo file carries `case_id`, so the honest key was
there all along: `idxvars: {i: case_id}` + `merge_on: [i]`.

**Guinea-Bissau — a duplicated source record.**  Not the Malawi shape.  The GPS
file is genuinely at grappe grain; it just ships five grappes TWICE, as
verbatim duplicate rows.  There is no household id to re-key on and no
single-file cure (the cover page carries no GPS at all), so this one takes the
guard's other named remedy — reduce the sub-frame to the merge-key grain BEFORE
the merge — via the `df_geo` hook in Guinea-Bissau/2018-19/_/mapping.py.

WHERE THE TESTS HAVE TO LOOK.  Not at the country-level API frame.  The
`(t, v)` collapse makes it 768 / 819 / 450 rows with or without the bug, and
its values are bit-for-bit identical before and after (verified cold over the
whole 3,235-row Malawi frame and the 450-row Guinea-Bissau frame:
`DataFrame.equals` True, index equal, dtypes equal).  A country-level assertion
therefore passes with the bug fully present — so these tests count rows in the
WAVE-level frame, upstream of the collapse, and assert the source invariants
that license the re-key rather than merely asserting them in a comment.
"""
import importlib.util as _iu
from pathlib import Path as _Path

import pandas as pd
import pytest
import yaml

# `requires_s3` lives in tests/conftest.py, and importing it is fiddlier than it
# looks: the repo ROOT also has a conftest.py, so a bare `from conftest import`
# picks up the wrong one, while `from tests.conftest import` resolves as
# `tests.tests.conftest` when pytest imports this module as part of the `tests`
# package (which is what CI does -- it failed there and passed locally).  Load
# the sibling file by PATH, which is the same in every import mode.
_conftest = _iu.module_from_spec(
    _iu.spec_from_file_location(
        'lsms_tests_conftest', _Path(__file__).with_name('conftest.py')))
_conftest.__loader__.exec_module(_conftest)
requires_s3 = _conftest.requires_s3

from lsms_library.country import Country
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.paths import countries_root

pytestmark = requires_s3


# Wave-level `cluster_features` row counts: the PRE-collapse frames, where a
# cartesian is visible and nowhere else.  Each equals the number of households
# the wave's primary sub-frame actually has.
EXPECTED_WAVE_ROWS = {
    ('Malawi', '2010-11'): 12271,        # was 196,083
    ('Malawi', '2019-20'): 14612,        # was 185,842 (11,434 IHS5 + 3,178 IHPS)
    ('Guinea-Bissau', '2018-19'): 5351,  # was 5,410
}

# Cluster counts the collapsed table returns — unchanged by these fixes, and
# pinned so a "fix" that silently drops clusters cannot pass.
EXPECTED_CLUSTERS = {
    ('Malawi', '2010-11'): 768,
    ('Malawi', '2019-20'): 819,
    ('Guinea-Bissau', '2018-19'): 450,
}


@pytest.fixture(scope='module')
def countries():
    out = {}
    for name in ('Malawi', 'Guinea-Bissau'):
        try:
            out[name] = Country(name)
        except Exception as exc:                                # pragma: no cover
            pytest.skip(f'{name} unavailable: {exc}')
    return out


def _wave_frame(countries, country, wave):
    """The wave frame AS THE MERGE LEFT IT.

    ``grab_data``, not ``Wave.cluster_features``: the latter runs the site-2
    projection onto ``(t, v)`` first, which returns 768 / 819 rows for Malawi
    with or without the cartesian and would make this whole module vacuous.
    """
    c = countries[country]
    if wave not in c.waves:                                     # pragma: no cover
        pytest.skip(f'{wave} is not a {country} wave')
    try:
        return c[wave].grab_data('cluster_features')
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'{country}/{wave} cluster_features could not be built: '
                    f'{type(exc).__name__}: {exc}')


def _wave_config(country, wave):
    path = countries_root() / country / wave / '_' / 'data_info.yml'
    if not path.exists():                                       # pragma: no cover
        pytest.skip(f'{country}/{wave} data_info.yml missing')
    with open(path) as f:
        return yaml.safe_load(f)


def _source(country, wave, rel):
    try:
        return get_dataframe(str(countries_root() / country / wave / 'Data' / rel))
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'{country}/{wave} {rel} unavailable: '
                    f'{type(exc).__name__}: {exc}')


# ---------------------------------------------------------------------------
# 1.  The assertion that fails with the bug present.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('cell', sorted(EXPECTED_WAVE_ROWS))
def test_wave_cluster_features_is_not_a_cartesian(countries, cell):
    """The wave frame must not be finer than the grain of its own sources."""
    country, wave = cell
    df = _wave_frame(countries, country, wave)
    expected = EXPECTED_WAVE_ROWS[cell]
    assert len(df) == expected, (
        f'{country}/{wave} cluster_features returned {len(df)} wave rows, '
        f'expected {expected}.  A count far above {expected} means the `dfs:` '
        f'merge is many-to-many again — both sub-frames repeat the merge key, '
        f'so pandas emits a cartesian product within each key group.  Fix the '
        f'MERGE (GH #323 D1); do not reduce afterwards.'
    )


@pytest.mark.parametrize('cell', sorted(EXPECTED_CLUSTERS))
def test_cluster_grain_is_unchanged(countries, cell):
    """The re-key is value-preserving: same clusters, same count."""
    country, wave = cell
    df = _wave_frame(countries, country, wave)
    v = df.reset_index()['v'].dropna().astype(str)
    assert v.nunique() == EXPECTED_CLUSTERS[cell], (
        f'{country}/{wave} should cover {EXPECTED_CLUSTERS[cell]} clusters, '
        f'got {v.nunique()}'
    )


# ---------------------------------------------------------------------------
# 2.  Malawi: the config, and the source invariants that license it.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('wave', ['2010-11', '2019-20'])
def test_malawi_geo_subframe_is_keyed_on_the_household(wave):
    """`df_geo` must join on `i`, not on the cluster `v`.

    Structural, so it fails on the CONFIG rather than waiting for a build.  The
    inverse of #639's test, which pinned these two cells as *known cartesian*
    while #627 still needed them as evidence; #627's census is now recorded and
    the cells are cured.
    """
    cfg = _wave_config('Malawi', wave)['cluster_features']
    geo = cfg['df_geo']
    assert 'i' in geo['idxvars'], (
        f'Malawi/{wave} df_geo must declare `i` — keying the household-grain '
        f'geo file on the cluster `v` is what made this merge a cartesian'
    )
    assert 'v' not in geo['idxvars'], (
        f'Malawi/{wave} df_geo must NOT also declare `v`: df_main owns it, and '
        f'declaring it on both sides of an `i` merge yields a v_x/v_y collision'
    )
    assert cfg['merge_on'] == ['i'], (
        f'Malawi/{wave} cluster_features must merge on [i], got {cfg["merge_on"]}'
    )


MALAWI_SOURCES = {
    '2010-11': ('Full_Sample/Household/hh_mod_a_filt.dta',
                'Full_Sample/Geovariables/HH_level/householdgeovariables.dta',
                'lat_modified', 'lon_modified', 12271),
    '2019-20': ('Cross_Sectional/hh_mod_a_filt.dta',
                'Cross_Sectional/householdgeovariables_ihs5.dta',
                'ea_lat_mod', 'ea_lon_mod', 11434),
}


@pytest.mark.parametrize('wave', sorted(MALAWI_SOURCES))
def test_malawi_main_and_geo_are_exactly_one_to_one_on_case_id(wave):
    """What makes `merge_on: [i]` an exact join rather than a lossy one.

    If a future release ships a geo extract that is not 1:1 with the cover
    page, the outer merge starts emitting orphan rows on a null `v` and this
    says so — instead of the orphans quietly collapsing into one phantom
    null-keyed cluster.
    """
    main_rel, geo_rel, _, _, n = MALAWI_SOURCES[wave]
    main = _source('Malawi', wave, main_rel)
    geo = _source('Malawi', wave, geo_rel)
    m = main['case_id'].apply(format_id).astype(str)
    g = geo['case_id'].apply(format_id).astype(str)
    assert len(m) == m.nunique() == len(g) == g.nunique() == n, (
        f'Malawi/{wave}: expected {n} unique case_id on both sides, got '
        f'main {len(m)}/{m.nunique()}, geo {len(g)}/{g.nunique()}'
    )
    assert set(m) == set(g), (
        f'Malawi/{wave}: {len(set(m) - set(g))} households have no geo row and '
        f'{len(set(g) - set(m))} geo rows have no household'
    )


@pytest.mark.parametrize('wave', sorted(MALAWI_SOURCES))
def test_malawi_published_gps_is_the_cluster_fix_not_a_household_fix(wave):
    """Why re-keying on `i` returns exactly the coordinates `v` was returning.

    The published lat/lon is the EA's DISPLACED fix stamped on every household
    of the EA, so `first()`-ing over the EA (what the `v` merge did) and taking
    the household's own row (what the `i` merge does) agree — for every EA.
    That equality is the value-preservation argument; assert it, don't narrate
    it.

    **Do not widen this to 2016-17.**  `Malawi/_/CONTENTS.org` ("2016-17 had no
    GPS, and the reason was a merge key") records that IHS4's displacement is
    "nearly but not exactly EA-constant" — 7 of its 779 EAs carry more than one
    `lat_modified`.  That wave is exempt by measurement, not by oversight, and
    it is not one of the cells this PR re-keys.
    """
    _, geo_rel, lat, lon, _ = MALAWI_SOURCES[wave]
    geo = _source('Malawi', wave, geo_rel)
    g = pd.DataFrame({
        'v': geo['ea_id'].apply(format_id).astype(str),
        'lat': pd.to_numeric(geo[lat], errors='coerce'),
        'lon': pd.to_numeric(geo[lon], errors='coerce'),
    })
    spread = g.groupby('v')[['lat', 'lon']].nunique(dropna=True)
    bad = spread[(spread['lat'] > 1) | (spread['lon'] > 1)]
    assert len(bad) == 0, (
        f'Malawi/{wave}: {len(bad)} EAs carry more than one distinct '
        f'coordinate, so the household\'s own fix and its EA\'s fix are no '
        f'longer the same value and the re-key is NOT value-preserving:\n'
        f'{bad.head()}'
    )


def test_malawi_2019_20_panel_half_has_no_geovariables():
    """The NaN coordinates on the IHPS half are absent data, not a broken join.

    Pinned because it is the one thing that could be mistaken for damage done
    by the re-key: 102 panel EAs carry no coordinates before or after, because
    IHS5 publishes no geovariables for the panel.  If a future release DOES
    ship them, this fails and someone wires them up.
    """
    panel = _source('Malawi', '2019-20', 'Panel/hh_mod_a_filt_19.dta')
    geo = _source('Malawi', '2019-20',
                  'Cross_Sectional/householdgeovariables_ihs5.dta')
    pn_ea = set(panel['ea_id'].apply(format_id).astype(str))
    geo_ea = set(geo['ea_id'].apply(format_id).astype(str))
    assert pn_ea and not (pn_ea & geo_ea), (
        f'{len(pn_ea & geo_ea)} IHPS panel EAs now appear in the IHS5 geo '
        f'file; the panel half\'s coordinates are no longer structurally '
        f'absent and cluster_features should carry them'
    )


# ---------------------------------------------------------------------------
# 3.  Guinea-Bissau: a duplicated record, not a household-grain frame.
# ---------------------------------------------------------------------------

def test_guinea_bissau_gps_duplicates_are_verbatim():
    """What licenses `drop_duplicates()` in the `df_geo` hook.

    `grappe_gps_gnb2018.dta` ships 450 rows for 445 grappes.  The de-dup is
    lossless ONLY because the five extra rows are identical in every column,
    GPS timestamp included — there is no choice being made between two
    disagreeing fixes.  If a future export ships two grappe rows that DIFFER,
    both survive `drop_duplicates`, the merge is cartesian again, and #627's
    guard fires — which is the behaviour we want, and this test states the
    premise it rests on.
    """
    gps = _source('Guinea-Bissau', '2018-19', 'grappe_gps_gnb2018.dta')
    v = gps['grappe'].apply(format_id).astype(str)
    assert len(gps) == 450 and v.nunique() == 445, (
        f'expected 450 rows / 445 grappes, got {len(gps)} / {v.nunique()}'
    )
    g = gps.copy()
    g['_v'] = v.values
    for key, sub in g[g['_v'].isin(set(v[v.duplicated(keep=False)]))].groupby('_v'):
        assert len(sub.drop(columns='_v').drop_duplicates()) == 1, (
            f'grappe {key} has {len(sub)} GPS rows that are NOT identical; '
            f'de-duplicating them would be a choice between disagreeing fixes, '
            f'which drop_duplicates() deliberately refuses to make'
        )


def test_guinea_bissau_cover_page_has_no_gps_of_its_own():
    """Why Guinea-Bissau cannot take Mali's single-file cure.

    Mali 2021-22's `dfs:` block was deleted outright because its cover page
    carried the geography itself.  Guinea-Bissau's does not, so the merge is
    genuinely needed and only its grain could be fixed.
    """
    cov = _source('Guinea-Bissau', '2018-19', 's00_me_gnb2018.dta')
    gpsish = [c for c in cov.columns
              if any(k in c.lower() for k in ('gps', 'lat', 'lon', 'coord'))]
    assert not gpsish, (
        f'the cover page now carries {gpsish}; the `dfs:` block may be '
        f'collapsible to a single-file extraction, as Mali 2021-22 was'
    )


def test_guinea_bissau_df_geo_hook_is_wired():
    """The hook is dispatched BY NAME off the sub-frame key — pin that.

    `Wave.grab_data` resolves a sub-frame's `df_edit` via
    `column_mapping(<sub-frame name>, ...)`, so the function must be called
    exactly `df_geo` and the sub-frame must be keyed exactly `df_geo`.  Neither
    file says so on its own; a rename of either silently drops the de-dup and
    the cartesian returns.
    """
    cfg = _wave_config('Guinea-Bissau', '2018-19')['cluster_features']
    assert 'df_geo' in cfg['dfs'], (
        "the sub-frame must stay keyed `df_geo` — the hook is found by this name"
    )
    c = Country('Guinea-Bissau')
    hook = c['2018-19'].formatting_functions.get('df_geo')
    assert callable(hook), (
        'Guinea-Bissau/2018-19/_/mapping.py must define `df_geo(df)`; without '
        'it the 5 duplicate GPS records make the merge cartesian again'
    )
