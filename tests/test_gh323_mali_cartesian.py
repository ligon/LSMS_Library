"""GH #323 site 4 / GH #627 -- Mali's `cluster_features` merge must not MANUFACTURE rows.

Mali 2021-22 was the single worst `dfs:` cartesian in the corpus: 88% of the
4,907,774 phantom rows PR #627's 40-country census found came from this one
cell.  The block joined

    df_main  ehcvm_conso_mli2021.dta   393,480 rows  (household x food item)
    df_geo   s00_me_mli2021.dta          6,143 rows  (household cover page)

on the CLUSTER key `v`.  Both sub-frames repeat `v`, so the merge was
many-to-many: every consumption line-item paired with every household of its
grappe.  393,480 x 6,143 -> 4,718,148 rows, 4,324,668 of them phantoms.
`_normalize_dataframe_index` then collapsed the wreck to 513 rows with
groupby().first(), which is why the table looked clean the whole time.

WHERE THE TEST HAS TO LOOK.  Do NOT assert on the country-level API frame: the
collapse makes it 513 rows with or without the bug, and its VALUES are
bit-for-bit identical before and after this fix (verified over all 513 grappes
x 4 columns).  A country-level test therefore passes with the bug fully
present.  The phantoms exist only in the WAVE-level frame, upstream of the
collapse -- so that is what these tests count.

THE FIX IS TO THE MERGE, NOT A REDUCER (#323 decision D1).  s00_me_mli2021.dta
carries the geography itself -- s00q01 == `region`, s00q04 == `milieu` -- so the
merge was never needed and the block is now a single-file extraction.  The
equivalence that licenses that is asserted below rather than merely asserted in
a comment: if a future edit repoints Region/Rural at a column that does NOT
agree with the consumption file, the test says so.
"""
import pandas as pd
import pytest

from lsms_library.country import Country

# Wave-level `cluster_features` row counts.  These are the PRE-collapse frames,
# so a cartesian shows up here and nowhere else.
#
# 2021-22 pre-fix: 4,718,148.  The other three waves merge a household- or
# consumption-grain primary against a GEO file that is one row per grappe, so
# they are m:1 joins and were never cartesian -- they are pinned here as a
# regression net, not because they were ever broken.
EXPECTED_WAVE_ROWS = {
    '2014-15': 3804,     # eaci2014_agregatconso  x  eaci_geovariables_2014 (989 grappes)
    '2017-18': 8390,     # eaci17_s00p1           x  eaci_geovariables_2017 (953 grappes)
    '2018-19': 366639,   # ehcvm_conso_mli2018    x  grappe_gps_mli2018     (549 grappes)
    '2021-22': 6143,     # single file: s00_me_mli2021 (household cover page)
}

# True cluster counts, straight off the source files.
EXPECTED_GRAPPES = {'2014-15': 989, '2017-18': 953, '2018-19': 551, '2021-22': 513}


@pytest.fixture(scope='module')
def mali():
    try:
        return Country('Mali')
    except Exception as exc:                                     # pragma: no cover
        pytest.skip(f'Mali unavailable: {exc}')


def _wave_cluster_features(c, wave):
    try:
        return c[wave].cluster_features()
    except Exception as exc:                                     # pragma: no cover
        pytest.skip(f'Mali/{wave} cluster_features could not be built: '
                    f'{type(exc).__name__}: {exc}')


@pytest.mark.parametrize('wave', sorted(EXPECTED_WAVE_ROWS))
def test_wave_cluster_features_is_not_a_cartesian(mali, wave):
    """The wave frame must not exceed the grain of its own sources.

    This is the assertion that fails with the bug present: pre-fix Mali 2021-22
    returned 4,718,148 rows against 6,143 households in 513 grappes.
    """
    if wave not in mali.waves:                                   # pragma: no cover
        pytest.skip(f'{wave} not a Mali wave')
    df = _wave_cluster_features(mali, wave)
    expected = EXPECTED_WAVE_ROWS[wave]
    assert len(df) == expected, (
        f'Mali/{wave} cluster_features returned {len(df)} wave-level rows, '
        f'expected {expected}.  A blow-up here means the `dfs:` merge in '
        f'{wave}/_/data_info.yml has gone many-to-many again -- both '
        f'sub-frames repeating the merge key `v` produces a CARTESIAN '
        f'PRODUCT, not a join (GH #323 site 4).  Fix the merge; do NOT add a '
        f'reducer downstream.'
    )


def test_2021_22_cluster_features_is_a_single_file_extraction(mali):
    """No `dfs:` block -- the cover page supplies all four columns itself."""
    info = mali['2021-22'].resources.get('cluster_features')
    assert info is not None, 'Mali/2021-22 declares no cluster_features'
    assert 'dfs' not in info, (
        'Mali/2021-22 cluster_features has grown a `dfs:` merge block again. '
        's00_me_mli2021.dta carries Region (s00q01), Rural (s00q04) AND the '
        'GPS fix, so no merge is needed; merging it against the '
        '(household x food item) consumption file on the cluster key `v` is '
        'the 4.3M-phantom-row cartesian of GH #323 site 4.'
    )
    assert info.get('file') == 's00_me_mli2021.dta', info.get('file')


@pytest.mark.parametrize('wave', sorted(EXPECTED_GRAPPES))
def test_country_cluster_features_is_one_row_per_grappe(mali, wave):
    """After the collapse: exactly one row per surveyed cluster, no more."""
    cf = mali.cluster_features()
    if wave not in cf.index.get_level_values('t'):               # pragma: no cover
        pytest.skip(f'{wave} absent from cluster_features')
    sub = cf.xs(wave, level='t')
    assert len(sub) == EXPECTED_GRAPPES[wave], (
        f'Mali/{wave} cluster_features has {len(sub)} rows for '
        f'{EXPECTED_GRAPPES[wave]} grappes')
    assert sub.index.is_unique


def test_cover_page_geography_agrees_with_the_consumption_file():
    """s00q01 == `region` and s00q04 == `milieu`, per grappe, 513/513.

    This equivalence is what makes dropping the merge value-preserving.  It is
    checked against the data rather than asserted in a YAML comment, because an
    unevidenced "these are the same column" claim is exactly the kind of thing
    that rots silently.
    """
    from lsms_library.local_tools import get_dataframe
    s00 = get_dataframe('Mali/2021-22/Data/s00_me_mli2021.dta')
    conso = get_dataframe('Mali/2021-22/Data/ehcvm_conso_mli2021.dta')
    for df in (s00, conso):
        df['grappe'] = df['grappe'].astype(str)

    cover = (s00.groupby('grappe')[['s00q01', 's00q04']]
                .agg(lambda s: sorted(set(s.astype(str)))))
    cons = (conso.groupby('grappe')[['region', 'milieu']]
                 .agg(lambda s: sorted(set(s.astype(str)))))

    assert (cover['s00q01'].map(len) == 1).all(), 'Region not constant within grappe'
    assert (cover['s00q04'].map(len) == 1).all(), 'Rural not constant within grappe'

    joined = cover.join(cons, how='outer')
    assert len(joined) == 513, f'{len(joined)} grappes, expected 513'
    assert joined.notna().all().all(), 'grappe sets differ between the two files'

    bad_region = joined[joined['s00q01'] != joined['region']]
    bad_milieu = joined[joined['s00q04'] != joined['milieu']]
    assert bad_region.empty, (
        f'{len(bad_region)} grappe(s) disagree on Region between the cover page '
        f'(s00q01) and the consumption file (region):\n{bad_region.head()}')
    assert bad_milieu.empty, (
        f'{len(bad_milieu)} grappe(s) disagree on Rural between the cover page '
        f'(s00q04) and the consumption file (milieu):\n{bad_milieu.head()}')


def test_2021_22_gps_is_a_cluster_fix_not_a_household_fix():
    """One distinct (lat, lon) per grappe -- so `.first()` cannot pick wrongly.

    The published EHCVM GPS is the cluster's displaced fix stamped on every
    household of the cluster (CLAUDE.md, GH #161).  Verified here for Mali
    2021-22: 513/513 grappes carry exactly one distinct coordinate pair.
    """
    from lsms_library.local_tools import get_dataframe
    s00 = get_dataframe('Mali/2021-22/Data/s00_me_mli2021.dta')
    s00['grappe'] = s00['grappe'].astype(str)
    n = s00.groupby('grappe')[['GPS__Latitude', 'GPS__Longitude']].nunique()
    assert (n['GPS__Latitude'] == 1).all() and (n['GPS__Longitude'] == 1).all(), (
        'Mali 2021-22 GPS varies WITHIN a grappe; the (t, v) collapse would '
        'then be picking one household\'s coordinates for the whole cluster.')
