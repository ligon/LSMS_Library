"""GhanaLSS -- two config defects found under GH #323.

Both are properties of the COUNTRY's wiring, independent of how core collapses a
non-unique index:

1. 1987-88 / 1988-89 `cluster_features` was wired to Y01A.DAT's `REGION`, which
   is the person's REGION OF BIRTH, not the cluster's region.  A column that is
   not cluster-invariant must not be projected onto the cluster at all.  Both
   waves are now deliberately NOT wired.

2. 2016-17 `food_security` keyed on the [clust, nh] pair, which is NaN for the
   last 110 rows of g7sec9c.dta.  Rekeyed to the complete `hid`.

These tests DO discriminate: run against the pre-fix (`origin/development`)
config tree via ``LSMS_COUNTRIES_ROOT``, all three fail with their intended
messages; against this tree all three pass.  Verified 2026-07-21.
"""
import pytest
import yaml

from lsms_library.paths import countries_root


class _SchemeLoader(yaml.SafeLoader):
    """data_scheme.yml carries ``!make`` tags; ignore them for these tests."""


_SchemeLoader.add_multi_constructor('!', lambda loader, suffix, node: None)


def _data_info(country, wave):
    path = countries_root() / country / wave / '_' / 'data_info.yml'
    return yaml.load(path.read_text(), Loader=_SchemeLoader)


@pytest.mark.parametrize('wave', ['1987-88', '1988-89'])
def test_early_waves_do_not_map_birthplace_as_cluster_region(wave):
    """Y01A.DAT's REGION is a person's BIRTHPLACE -- never a cluster's region.

    mapping.py's ``Region()`` and ``Birthplace()`` are the same function body over
    the same ``region_dict``, and the wave's own code list runs to 11=Nigeria,
    12=Ivory Coast, 13=Togo, 14=Burkina Faso -- values a Ghanaian enumeration
    area cannot take.  (Nor is that "the survey followed movers": code 11 is
    spread over 87 of 1988-89's 170 clusters and no cluster is more than 17.5%
    foreign-coded.)  REGION varies WITHIN a household in ~1/3 of households -- up
    to 6 distinct values in one household -- and within a cluster in 174 of 176
    (1987-88) / 167 of 170 (1988-89).

    NOT because no region source exists.  1988-89's HEALTH.DAT and DRUG.DAT do
    carry a facility-level ``REGION`` keyed to the clusters served (``CL1..CL5``,
    ``CL + 2000 == CLUST``), covering 168 of 170 clusters; it is unvalidated and
    unwired, a `todo` rather than a closure.  See the waves' ``data_info.yml``.
    These waves must not declare ``cluster_features`` off the ROSTER column:
    silently MISSING beats silently WRONG.
    """
    info = _data_info('GhanaLSS', wave)
    cf = info.get('cluster_features')
    if cf is None:
        return  # correct: not wired
    myvars = cf.get('myvars', {}) or {}
    assert myvars.get('Region') != 'REGION', (
        f'GhanaLSS {wave}: cluster_features.Region is wired to Y01A.DAT REGION, '
        'the person-level region of BIRTH.  Collapsed to one row per cluster '
        'this fabricates the cluster region -- GH #323.'
    )
    # 1987-88 only.  `Age: AGEY` never produced a cluster "Age" column: it fed
    # mapping.py's table-level ``cluster_features(df)`` df_edit hook, whose
    # ``Age<12`` filter makes the cluster's Region the modal BIRTHPLACE of its
    # under-12s -- the same guess, reached a different way.  Re-adding AGEY here
    # re-arms that hook, so keep it out.
    assert 'Age' not in myvars, (
        f'GhanaLSS {wave}: `Age` is a PERSON attribute, and here it re-arms '
        "mapping.py's cluster_features() hook, which infers a cluster's region "
        'from the modal birthplace of its under-12s -- GH #323.'
    )


def test_food_security_keys_on_complete_hid():
    """2016-17 food_security must key on `hid`, not the incomplete [clust, nh].

    g7sec9c.dta has 14,009 rows.  The last 110 have clust=NaN AND nh=NaN (and all
    eight FIES items NaN) but a perfectly valid `hid`.  Keyed on [clust, nh] all
    110 distinct households collapse onto a single (t, NaN) tuple and are dropped
    outright by groupby's dropna -- 110 households vanishing into one phantom.

    `hid` reconstructs the compound key exactly: on all 13,899 well-keyed rows
    hid == f"{clust}/{nh:02d}" with 100% fidelity, so the rekey moves no existing
    id and additionally recovers the 110 as the distinct households they are.
    """
    info = _data_info('GhanaLSS', '2016-17')
    idx = info['food_security']['idxvars']
    assert idx.get('i') == 'hid', (
        'food_security must key on the complete `hid` column; keying on '
        '[clust, nh] silently merges 110 NaN-keyed households into one phantom '
        '(GH #323).'
    )
