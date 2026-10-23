"""Nigeria cluster_features: the post-planting cover is the LEFT frame of the
household-geovariables merge (GH #1261).

Each Nigeria wave builds ``cluster_features`` from a ``dfs:`` block: ``df_key``
and ``df_main`` from the post-planting cover (``secta_plantingw*``), plus
``df_geo`` from the household geovariables file, merged on ``i``.  The geo file
of 2012-13 carries 39 households the PP cover lacks (all 39 are
post-harvest-only households), and that of 2015-16 one household neither cover
has.  Under the default ``merge_how: outer`` they arrived with a null ``v`` and
the Site 2 cluster-grain collapse deleted them (grain report
``nan_key_rows`` 39 / 1, every round of the wave).  ``merge_how: left`` stops
admitting them; served ``cluster_features`` is unchanged (measured cold,
development vs branch: ``assert_frame_equal`` exact, dtypes included).

2010-11 and 2018-19 have no geo-only household (0 / 0), so ``left`` would be a
no-op there and is not declared; 2023-24 has no ``df_geo`` (GH #978).

Tiers: config-only (no microdata), and ``requires_s3`` checks on the built
wave frames and the served table.  Evidence:
``slurm_logs/isa_backlog_2026-10-21/1261-nigeria-geo-merge/``.
"""
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = 'Nigeria'
NG = countries_root() / COUNTRY

# Wave folder -> its rounds, the PP cover the block reads, and the number of
# households on that cover.
AFFECTED = {
    '2012-13': (('2012Q3', '2013Q1'),
                'Post Planting Wave 2/Household/secta_plantingw2.dta', 4859),
    '2015-16': (('2015Q3', '2016Q1'), 'secta_plantingw3.dta', 4696),
}


def _block(folder):
    with open(NG / folder / '_' / 'data_info.yml') as f:
        return yaml.safe_load(f)['cluster_features']


# --- config ------------------------------------------------------------------

@pytest.mark.parametrize('folder', sorted(AFFECTED))
def test_geo_merge_is_left(folder):
    """The waves whose geo file carries households the cover lacks declare
    ``merge_how: left``."""
    assert _block(folder).get('merge_how') == 'left'


@pytest.mark.parametrize('folder', sorted(AFFECTED))
def test_the_cover_is_the_left_frame(folder):
    """``left`` keeps the FIRST sub-df's rows (``Wave.grab_data`` merges
    ``dfs:`` in order, the running result on the left), so the cover must come
    first and the geo file last.  ``df_key`` and ``df_main`` read the same
    cover, so the first merge drops nothing under ``left`` either."""
    b = _block(folder)
    assert b['dfs'] == ['df_key', 'df_main', 'df_geo']
    cover = AFFECTED[folder][1]
    assert b['df_key']['file'] == b['df_main']['file'] == cover
    assert 'geovar' in b['df_geo']['file'].lower()
    assert b['merge_on'] == ['i']


# --- built frames (need the microdata) -----------------------------------------

@pytest.fixture(scope='module')
def country():
    ll = pytest.importorskip('lsms_library')
    return ll.Country(COUNTRY)


def _cover_households(folder):
    from lsms_library.local_tools import format_id, get_dataframe
    cover = get_dataframe(str(NG / folder / 'Data' / AFFECTED[folder][1]),
                          convert_categoricals=False)
    return {format_id(h) for h in cover['hhid'].dropna()}


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize('folder,t', [(f, t) for f, (ts, _, _) in AFFECTED.items()
                                      for t in ts])
def test_no_geo_only_household_enters_the_merge(country, folder, t):
    """The household-grain frame the collapse receives is exactly the PP
    cover: no null ``v``, no household the cover lacks.  On development
    2012-13 carried 39 extra null-``v`` rows and 2015-16 one."""
    df = country[t].grab_data('cluster_features')
    assert int(df.index.get_level_values('v').isna().sum()) == 0
    hh = set(df.index.get_level_values('i'))
    cover = _cover_households(folder)
    assert len(cover) == AFFECTED[folder][2]
    assert hh - cover == set()
    assert len(df) == len(cover)


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize('t', [t for ts, _, _ in AFFECTED.values() for t in ts])
def test_site2_collapse_deletes_no_null_key_row(country, t):
    """The Site 2 grain report for the round no longer counts NaN-key
    deletions (development: ``nan_key_rows`` 39 for 2012-13, 1 for 2015-16).

    No report at all also passes: the collapse files one only when it loses
    something, and today it does for an unrelated reason (households of one
    cluster disagree on coordinates)."""
    from lsms_library.country import grain_reports
    before = len(grain_reports(country=COUNTRY, table='cluster_features'))
    country[t].cluster_features()
    reps = [r for r in grain_reports(country=COUNTRY, table='cluster_features')[before:]
            if r.get('site') == 'Wave.cluster_features' and r.get('wave') == t]
    assert all(r.get('nan_key_rows', 0) == 0 for r in reps), reps


# Served cluster_features per round -- identical on development and on this
# branch.  (rows, moved-singleton clusters, clusters with a coordinate,
# sum Latitude, sum Longitude.)
SERVED = {
    '2012Q3': (647, 152, 645, 5188.8614, 4465.7640),
    '2013Q1': (647, 152, 645, 5188.8614, 4465.7640),
    '2015Q3': (792, 306, 784, 6140.8846, 5285.7961),
    '2016Q1': (792, 306, 784, 6140.8846, 5285.7961),
}


@pytest.fixture(scope='module')
def served(country):
    try:
        df = country.cluster_features()
    except Exception as exc:                      # noqa: BLE001 - data absent
        pytest.skip(f'Nigeria cluster_features unavailable: {exc}')
    if df is None or df.empty:
        pytest.skip('Nigeria cluster_features empty (no microdata)')
    return df


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize('t', sorted(SERVED))
def test_served_cluster_features_unchanged(served, t):
    g = served.xs(t, level='t')
    v = g.index.get_level_values('v')
    assert int(v.isna().sum()) == 0
    rows, moved, with_gps, lat, lon = SERVED[t]
    assert len(g) == rows
    assert int(v.astype(str).str.startswith('moved-').sum()) == moved
    assert int(g['Latitude'].notna().sum()) == with_gps
    assert float(g['Latitude'].sum()) == pytest.approx(lat, abs=1e-3)
    assert float(g['Longitude'].sum()) == pytest.approx(lon, abs=1e-3)
    assert {str(x) for x in served.dtypes} == {'string', 'Float64'}
