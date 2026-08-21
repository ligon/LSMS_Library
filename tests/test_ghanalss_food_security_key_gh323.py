"""GhanaLSS 2016-17 `food_security` keyed on an incomplete pair (GH #323).

`food_security` keyed on the [clust, nh] pair, which is NaN for the last 110
rows of g7sec9c.dta.  Rekeyed to the complete `hid`.

WITHDRAWN (2026-08-21): this module previously also asserted that 1987-88 /
1988-89 must NOT declare `cluster_features`, on the grounds that Y01A.DAT's
`REGION` is the person's region of BIRTH.  That half is superseded, and its
assertions are now FALSE against `development`.

PR #673 gave both waves a `cluster_features` df_edit hook that infers the
cluster's region as the modal birth region of members under 12, instead of
letting `groupby().first()` elect an arbitrary person.  Built cold that yields
176 and 170 clusters, `Region` 0% null, ZERO GrainCollapseWarnings, and only
the ten real Ghanaian regions -- a foreign birth code cannot win a mode.  The
deletion this module used to demand would now remove a working, documented
approximation.

That the value is an approximation is real, and is recorded as "Trap 3" in
`GhanaLSS/_/CONTENTS.org`.  Replacing it with a MEASUREMENT -- 1988-89's
HEALTH.DAT / DRUG.DAT facility `REGION`, bridged to 1987-88 via CLYR1YR2.DAT --
is the genuinely valuable part of the withdrawn work and is tracked separately.
Corroborating that route: the facility region matches the cluster's modal birth
region in 160 of 166 clusters.

This test DOES discriminate: against the pre-fix config tree via
``LSMS_COUNTRIES_ROOT`` it fails with its intended message.
"""
import yaml

from lsms_library.paths import countries_root


class _SchemeLoader(yaml.SafeLoader):
    """data_scheme.yml carries ``!make`` tags; ignore them for these tests."""


_SchemeLoader.add_multi_constructor('!', lambda loader, suffix, node: None)


def _data_info(country, wave):
    path = countries_root() / country / wave / '_' / 'data_info.yml'
    return yaml.load(path.read_text(), Loader=_SchemeLoader)


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
