"""
Regression tests for GH #323 -- ``_normalize_dataframe_index`` silently
collapsing a non-unique DECLARED index with ``groupby().first()``.

The collapse has two failure modes, and GhanaLSS exhibits BOTH from the same
line of code:

* **Benign but silent.**  ``cluster_features`` sources are HOUSEHOLD-grain files
  whose projected columns (region, urban/rural) are genuinely cluster-invariant,
  so collapsing 934,584 rows to 1,000 clusters is a lossless de-duplication --
  the right answer, arrived at silently.
* **Silently WRONG.**  GhanaLSS 1987-88 / 1988-89 wired ``Region`` to Y01A.DAT's
  ``REGION`` column, which is the person's REGION OF BIRTH.  Collapsed with
  ``.first()``, a cluster inherits the birth-region of its first-listed person --
  labelling 9 of 170 (1988-89) and 7 of 176 (1987-88) Ghanaian enumeration areas
  as being located in **Nigeria**, and disagreeing with even the cluster's modal
  birthplace in 53 / 49 clusters respectively.

The fix makes the collapse DECLARED and CHECKED rather than silent: a
``data_scheme.yml`` table may declare ``aggregation: {column: invariant}``,
which asserts the column is constant within every declared-index group and
RAISES if it is not.  The same mechanism that blesses the benign case is the one
that catches the wrong case.

NOTE on why the guard is tested against *synthetic* birthplace values below: in
the live 1988-89 tree ``Region`` is currently all-NA (an unrelated defect --
``get_categorical_mapping`` returns an empty dict for that wave), so the guard
passes there *trivially*.  Testing only that would be validating where
validation is free.  The case that matters is the one where that masking bug is
repaired and the real birth-region values reach the collapse -- so the tests
below feed the guard exactly that shape.
"""
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library.country import _normalize_dataframe_index
from lsms_library.paths import countries_root


class _SchemeLoader(yaml.SafeLoader):
    """data_scheme.yml carries ``!make`` tags; ignore them for these tests."""


_SchemeLoader.add_multi_constructor('!', lambda loader, suffix, node: None)


def _scheme(country):
    path = countries_root() / country / '_' / 'data_scheme.yml'
    return yaml.load(path.read_text(), Loader=_SchemeLoader)


def _data_info(country, wave):
    path = countries_root() / country / wave / '_' / 'data_info.yml'
    return yaml.load(path.read_text(), Loader=_SchemeLoader)


# --------------------------------------------------------------------------
# The framework guard (the CLASS-level fix)
# --------------------------------------------------------------------------

_SCHEMA = {
    'index': '(t, v)',
    'Region': 'str',
    'aggregation': {'Region': 'invariant'},
}


def test_invariant_reducer_raises_when_column_is_not_constant():
    """A column declared `invariant` that VARIES within its index group must raise.

    This is the 1988-89 shape: one row per PERSON, `Region` holding the person's
    birthplace, collapsed to one row per cluster.  Pre-fix this silently returned
    the first person's birthplace as the cluster's region.
    """
    df = pd.DataFrame({
        't': ['1988-89'] * 5,
        'v': ['2001', '2001', '2001', '2002', '2002'],
        # cluster 2001 holds three people born in three different regions
        'Region': ['Ashanti', 'Nigeria', 'Volta', 'Central', 'Central'],
    }).set_index(['t', 'v'])

    with pytest.raises(ValueError, match='invariant|INVARIANT'):
        _normalize_dataframe_index(df, _SCHEMA, '1988-89', 'cluster_features')


def test_invariant_reducer_collapses_silently_when_column_is_constant():
    """A genuinely cluster-invariant column de-duplicates without warning.

    This is the 1991-92..2016-17 shape: a household-grain source projecting a
    cluster-level attribute.  The collapse is correct, and now DECLARED, so it
    must not emit the GH #323 warning.
    """
    df = pd.DataFrame({
        't': ['2016-17'] * 5,
        'v': ['7001', '7001', '7001', '7002', '7002'],
        'Region': ['Ashanti', 'Ashanti', 'Ashanti', 'Volta', 'Volta'],
    }).set_index(['t', 'v'])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = _normalize_dataframe_index(df, _SCHEMA, '2016-17', 'cluster_features')

    assert len(out) == 2, 'should de-duplicate to one row per cluster'
    assert out.loc[('2016-17', '7001'), 'Region'] == 'Ashanti'
    assert not [w for w in caught if 'GH #323' in str(w.message)], \
        'a DECLARED + verified de-duplication must not warn'


def test_undeclared_collapse_still_warns():
    """Without an `aggregation:` policy the collapse must still be surfaced.

    Guards against the fix accidentally silencing the warning for every table.
    """
    df = pd.DataFrame({
        't': ['x'] * 3,
        'v': ['1', '1', '2'],
        'Region': ['a', 'b', 'c'],
    }).set_index(['t', 'v'])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        _normalize_dataframe_index(df, {'index': '(t, v)'}, 'x', 'cluster_features')

    assert [w for w in caught if 'GH #323' in str(w.message)], \
        'an UNDECLARED collapse must still warn'


# --------------------------------------------------------------------------
# The GhanaLSS instances
# --------------------------------------------------------------------------

def test_ghanalss_cluster_features_declares_its_aggregation():
    """The de-duplication must be DECLARED, not left silent."""
    entry = _scheme('GhanaLSS')['Data Scheme']['cluster_features']
    policy = entry.get('aggregation')
    assert policy, 'cluster_features collapses a household-grain source; declare it'
    assert policy.get('Region') == 'invariant'
    assert policy.get('Rural') == 'invariant'


@pytest.mark.parametrize('wave', ['1987-88', '1988-89'])
def test_ghanalss_early_waves_do_not_map_birthplace_as_cluster_region(wave):
    """Y01A.DAT's REGION is a person's BIRTHPLACE -- never a cluster's region.

    mapping.py's ``Region()`` and ``Birthplace()`` are the same function body over
    the same ``region_dict``, and the wave's own code list runs to 11=Nigeria,
    12=Ivory Coast, 13=Togo, 14=Burkina Faso -- values a Ghanaian enumeration
    area cannot take.  These waves must therefore not declare cluster_features
    at all (no cluster-invariant region variable exists in their 93 source files).
    """
    info = _data_info('GhanaLSS', wave)
    cf = info.get('cluster_features')
    if cf is None:
        return  # correct: not wired
    myvars = cf.get('myvars', {}) or {}
    assert myvars.get('Region') != 'REGION', (
        f'GhanaLSS {wave}: cluster_features.Region is wired to Y01A.DAT REGION, '
        'the person-level region of BIRTH (varies within a single household in '
        '~1/3 of households, up to 6 distinct values).  Collapsed to one row per '
        'cluster this fabricates the cluster region -- GH #323.'
    )
    assert 'Age' not in myvars, (
        f'GhanaLSS {wave}: `Age` is a PERSON attribute; as a cluster feature it '
        "becomes the age of the cluster's first-listed person."
    )


def test_ghanalss_food_security_keys_on_complete_hid():
    """2016-17 food_security must key on `hid`, not the incomplete [clust, nh].

    The last 110 rows of g7sec9c.dta have clust=NaN AND nh=NaN (with all eight
    FIES items NaN) but a valid `hid`.  Keyed on [clust, nh] those 110 distinct
    households collapse onto a single NaN tuple and are dropped outright.
    """
    info = _data_info('GhanaLSS', '2016-17')
    idx = info['food_security']['idxvars']
    assert idx.get('i') == 'hid', (
        'food_security must key on the complete `hid` column; keying on '
        '[clust, nh] silently merges 110 NaN-keyed households into one phantom '
        '(GH #323).'
    )
