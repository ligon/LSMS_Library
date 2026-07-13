"""GhanaLSS -- two config defects found under GH #323.

Both are properties of the COUNTRY's wiring, independent of how core collapses a
non-unique index:

1. 1987-88 / 1988-89 `cluster_features` was wired to Y01A.DAT's `REGION`, which
   is the person's REGION OF BIRTH, not the cluster's region -- and to `AGEY`,
   a person's age.  A column that is not cluster-invariant must not be projected
   onto the cluster at all.  Both waves are now deliberately NOT wired.

2. 2016-17 `food_security` keyed on the [clust, nh] pair, which is NaN for the
   last 110 rows of g7sec9c.dta.  Rekeyed to the complete `hid`.
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
    area cannot take.  REGION varies WITHIN a household in ~1/3 of households (up
    to 6 distinct values in one household), and within a cluster in 174 of 176.

    No cluster-invariant region variable exists in either wave's 93 source files,
    so these waves must not declare cluster_features at all.  Silently MISSING
    beats silently WRONG.
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
    assert 'Age' not in myvars, (
        f'GhanaLSS {wave}: `Age` is a PERSON attribute; as a cluster feature it '
        "becomes the age of the cluster's first-listed person."
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
