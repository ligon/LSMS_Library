"""GH #637 — the three `groupby().first()` sites in `Malawi/_/malawi.py`.

`.first()` defaults to `skipna=True`, so a group of several rows collapses to a
COMPOSITE assembled from the first non-null value of each column independently —
a row that appears nowhere in the survey.  That is usually CORRECT (NULL is
absence, not contradiction).  It is wrong only when the duplicate rows describe
DIFFERENT REAL ENTITIES, which is a broken identifier and is fixed at the
identifier, never with `skipna=False`.

The audit (patch `DataFrameGroupBy.first` in-process, drive the wave scripts
with `runpy` — all three tables are `materialize: make`, so an in-process patch
cannot see them through the subprocess) found **zero groups of more than one row
on a non-null key at any of the three sites, in any wave**.  No composite is
ever built, because there is never more than one row to build one from.

These tests pin the invariants that verdict rests on, so a future source release
or `format_id` change that breaks a key is caught rather than silently
`.first()`-ed away.  Note what the audit means for the two known traps:

* *"they're only exact duplicates"* is not reassurance (PR #646 found broken
  Uganda keys where most groups classified 'exact' because only the payload
  agreed).  Not applicable here — there are no duplicate groups AT ALL, which
  is a strictly stronger statement than "the duplicates agreed".
* *invariance by missingness* — a key can look clean by being 100% NULL.  Not
  applicable either: the household id is non-null in every row of all three
  tables, and the one key level that IS ever null (`plot_id`) is measured
  below rather than assumed away.
* **a broken key can produce ZERO duplicates** — the Tanzania inversion, which
  turns the duplicate-count instrument inside out.  `Tanzania/2008-15`'s
  `shocks` built `i` from the panel-LINE index instead of the household id;
  the two namespaces shared *no* values, so every replicated line became its
  own distinct "household" and `groupby().first()` reported 0 duplicate
  groups — indistinguishable from a perfectly sound key.  **"0 duplicates" is
  therefore not sufficient**, and the check that catches it is a PER-WAVE
  namespace overlap against the roster (`test_i_lives_in_the_same_namespace_
  as_the_roster` below).  Malawi passes it at 100% in every wave of all three
  tables — and not vacuously: the id strings match literally.

  Worth running rather than waving away, because Malawi 2019-20 has the
  surface shape of a namespace split: its `data_info.yml` runs `case_id`
  through `cs_i` (`'cs-19-' + format_id`) for `sample` / `household_roster` /
  `cluster_features`, while all three wave scripts here pass `i_prefix=''` /
  bare `format_id`.  That asymmetry is a DELIBERATE, ALREADY-RECORDED decision
  — `Malawi/_/CONTENTS.org`, "plot_features (GH #167)": *"2016-17
  cross-sectional id is `cs-17-`-prefixed in `sample()` ...; the wave script
  applies the same prefix so XS plots are not ~100% orphaned.  All other halves
  emit the raw wave hhid and rely on framework id_walk / `panel_ids`
  chaining."*  Measured, the `cs-19-` prefix survives to the API on NEITHER
  side, so both are in the raw `case_id` namespace and `i` joins exactly.  The
  test below keeps it that way.
"""
import importlib.util as _iu
from pathlib import Path as _Path

import pandas as pd
import pytest

# See tests/test_gh323_malawi_gb_cartesian.py for why this is loaded by path
# rather than imported: the root conftest.py shadows a bare `from conftest`,
# and `from tests.conftest` resolves as `tests.tests.conftest` under CI's
# package import mode.
_conftest = _iu.module_from_spec(
    _iu.spec_from_file_location(
        'lsms_tests_conftest', _Path(__file__).with_name('conftest.py')))
_conftest.__loader__.exec_module(_conftest)
requires_s3 = _conftest.requires_s3

from lsms_library.country import Country
from lsms_library.local_tools import get_dataframe, format_id
from lsms_library.paths import countries_root

pytestmark = requires_s3


# Module C rows, and how many of them carry NO plot id.  Those are empty roster
# stubs, not plots: 0 of all 468 has a non-null ag_c04a / ag_c04b / ag_c04c.
# `groupby(dropna=True)` was deleting them silently; the drop is now explicit.
MODULE_C = {
    ('2010-11', 'Full_Sample/Agriculture/ag_mod_c.dta', 'case_id', ('ag_c00',), ''):
        (19265, 275),
    ('2013-14', 'AG_MOD_C_13.dta', 'y2_hhid', ('ag_c00',), ''):
        (6489, 158),
    ('2016-17', 'Cross_Sectional/ag_mod_c.dta', 'case_id', ('gardenid', 'plotid'), 'cs-17-'):
        (15751, 27),
    ('2016-17', 'Panel/ag_mod_c_16.dta', 'y3_hhid', ('gardenid', 'plotid'), ''):
        (4042, 6),
    ('2019-20', 'Cross_Sectional/ag_mod_c.dta', 'case_id', ('gardenid', 'plotid'), 'cs-17-'):
        (17693, 0),
    ('2019-20', 'Panel/ag_mod_c_19.dta', 'y4_hhid', ('gardenid', 'plotid'), ''):
        (5570, 2),
}

AREA_COLS = ('ag_c04a', 'ag_c04b', 'ag_c04c')

# Module H is one row per household — the premise under BOTH Module-H sites.
# (2016-17 and 2019-20 build the cross-sectional half only; the IHPS panel
# halves are absent from both tables.  A coverage gap, not a key defect.)
MODULE_H = {
    '2010-11': ('Full_Sample/Household/hh_mod_h.dta', 'case_id', 12271),
    '2013-14': ('HH_MOD_H_13.dta', 'y2_hhid', 4000),
    '2016-17': ('Cross_Sectional/hh_mod_h.dta', 'case_id', 12447),
    '2019-20': ('Cross_Sectional/HH_MOD_H.dta', 'case_id', 11434),
}


@pytest.fixture(scope='module')
def malawi():
    try:
        return Country('Malawi')
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'Malawi unavailable: {exc}')


def _source(wave, rel, **kw):
    try:
        return get_dataframe(
            str(countries_root() / 'Malawi' / wave / 'Data' / rel), **kw)
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'Malawi/{wave} {rel} unavailable: '
                    f'{type(exc).__name__}: {exc}')


def _table(malawi, name):
    try:
        df = getattr(malawi, name)()
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'Malawi {name} unavailable: {type(exc).__name__}: {exc}')
    if df is None or df.empty:                                  # pragma: no cover
        pytest.skip(f'Malawi {name} empty')
    return df


def _plotkey(df, cols):
    if len(cols) == 1:
        return df[cols[0]].apply(format_id).astype('string')
    return (df[cols[0]].apply(format_id).astype('string') + '_'
            + df[cols[1]].apply(format_id).astype('string'))


# ---------------------------------------------------------------------------
# Site 1 — plot_features_for_wave, groupby(['t','i','plot_id']).first()
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('spec', sorted(MODULE_C))
def test_plot_key_identifies_the_plot(spec):
    """No two Module C rows share an (i, plot_id).

    This is the whole verdict for this site: with no duplicate group there is
    nothing for `.first()` to collapse and no composite for it to assemble.  It
    also disposes of the collision question — two distinct plots given the same
    id, or two distinct household ids merged by `format_id`, would BOTH appear
    here as a duplicate group.
    """
    wave, rel, idcol, keycols, prefix = spec
    df = _source(wave, rel, convert_categoricals=False)
    k = pd.DataFrame({
        'i': prefix + df[idcol].apply(format_id).astype('string'),
        'plot_id': _plotkey(df, keycols),
    })
    k = k[k['plot_id'].notna()]
    dup = k.groupby(['i', 'plot_id']).size()
    dup = dup[dup > 1]
    assert len(dup) == 0, (
        f'Malawi/{wave} {rel}: {len(dup)} (i, plot_id) groups hold more than '
        f'one row, so `.first()` is now assembling a skipna COMPOSITE.  Check '
        f'whether those rows are the same plot or DIFFERENT plots colliding on '
        f'one id — if different, the identifier is broken and must be fixed '
        f'there (GH #323 D1), not with skipna=False.\n{dup.head()}'
    )


@pytest.mark.parametrize('spec', sorted(MODULE_C))
def test_rows_with_no_plot_id_carry_no_plot_data(spec):
    """The 468 rows dropped on a null key are empty stubs, not lost plots.

    `groupby(dropna=True)` deleted these silently; the drop is now an explicit
    `dropna(subset=['plot_id'])`.  What licenses either is that they measure
    NOTHING — no farmer-estimated area, no unit, no GPS area.  If a future
    release ships an un-keyed row that DOES carry an area, this fails and the
    row stops being safe to drop.
    """
    wave, rel, idcol, keycols, prefix = spec
    expected_rows, expected_null = MODULE_C[spec]
    df = _source(wave, rel, convert_categoricals=False)
    assert len(df) == expected_rows, (
        f'Malawi/{wave} {rel}: expected {expected_rows} rows, got {len(df)}'
    )
    null = _plotkey(df, keycols).isna()
    assert int(null.sum()) == expected_null, (
        f'Malawi/{wave} {rel}: expected {expected_null} rows with no plot id, '
        f'got {int(null.sum())}'
    )
    areas = [c for c in AREA_COLS if c in df.columns]
    if expected_null and areas:
        carrying = int(df.loc[null, areas].notna().any(axis=1).sum())
        assert carrying == 0, (
            f'Malawi/{wave} {rel}: {carrying} of the {expected_null} rows with '
            f'no plot id now carry an area measurement.  They are no longer '
            f'empty roster stubs, and dropping them loses real data.'
        )


def test_plot_features_index_is_unique_and_fully_keyed(malawi):
    df = _table(malawi, 'plot_features')
    assert df.index.is_unique, 'plot_features (t, i, plot_id) is not unique'
    plot_id = df.reset_index()['plot_id']
    assert plot_id.notna().all(), (
        f'{int(plot_id.isna().sum())} plot_features rows have a NULL plot_id; '
        f'they would be deleted unannounced by the groupby that follows'
    )


# ---------------------------------------------------------------------------
# Sites 2 and 3 — the two Module H tables.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('wave', sorted(MODULE_H))
def test_module_h_is_one_row_per_household(wave):
    """The premise under both Module-H `.first()` sites.

    Also pins that `format_id` merges no two distinct ids: if it did, the
    unique count would fall below the raw one and `(t, i)` would stop
    identifying a household.
    """
    rel, idcol, expected = MODULE_H[wave]
    df = _source(wave, rel, convert_categoricals=False)
    raw = df[idcol].astype(str)
    fid = df[idcol].apply(format_id).astype(str)
    assert len(df) == raw.nunique() == expected, (
        f'Malawi/{wave} Module H: expected {expected} rows, one per household; '
        f'got {len(df)} rows / {raw.nunique()} distinct {idcol}'
    )
    assert fid.nunique() == raw.nunique(), (
        f'Malawi/{wave}: format_id merges '
        f'{raw.nunique() - fid.nunique()} distinct {idcol} values, so `i` no '
        f'longer distinguishes households'
    )


# ---------------------------------------------------------------------------
# Trap 3 — the Tanzania inversion.  A key from the WRONG NAMESPACE produces
# ZERO duplicates, so every assertion above can pass on a thoroughly broken
# key.  This is the check that distinguishes them.
# ---------------------------------------------------------------------------

# Households per wave that answered each module.  Below the roster's count
# wherever the module was not administered to the whole sample — 2016-17 and
# 2019-20 build the cross-sectional half only.
EXPECTED_HOUSEHOLDS = {
    'plot_features': {'2010-11': 10126, '2013-14': 3061,
                      '2016-17': 11536, '2019-20': 11135},
    'food_coping': {'2010-11': 12271, '2013-14': 3999,
                    '2016-17': 12447, '2019-20': 11432},
    'months_food_inadequate': {'2010-11': 12271, '2013-14': 4000,
                               '2016-17': 12447, '2019-20': 11434},
}


@pytest.mark.parametrize('table', sorted(EXPECTED_HOUSEHOLDS))
def test_i_lives_in_the_same_namespace_as_the_roster(malawi, table):
    """PER WAVE, never in aggregate.

    Aggregate is the trap inside the trap: in Tanzania's case two clean waves
    carried an aggregate check over four whose overlap was 0.  So this asserts
    per wave, and asserts the household COUNT too — a wrong-namespace key
    inflates the row count without changing the number of underlying facts, and
    that inflation is invisible to any duplicate-based instrument.
    """
    roster = _table(malawi, 'household_roster').reset_index()
    ref = {str(t): set(g['i'].astype(str))
           for t, g in roster.groupby(roster['t'].astype(str))}

    flat = _table(malawi, table).reset_index()
    seen = set()
    for t, g in flat.groupby(flat['t'].astype(str)):
        seen.add(t)
        ids = set(g['i'].astype(str))
        known = ref.get(t, set())
        missing = ids - known
        assert not missing, (
            f'Malawi {table} t={t}: {len(missing)} of {len(ids)} household ids '
            f'do not appear in household_roster for the SAME wave — `i` is '
            f'built from a different namespace than the roster\'s, which no '
            f'duplicate count can detect.  Examples: {sorted(missing)[:5]}'
        )
        expected = EXPECTED_HOUSEHOLDS[table].get(t)
        if expected is not None:
            assert len(ids) == expected, (
                f'Malawi {table} t={t}: {len(ids)} distinct households, '
                f'expected {expected}'
            )
    assert seen == set(EXPECTED_HOUSEHOLDS[table]), (
        f'Malawi {table}: waves changed — expected '
        f'{sorted(EXPECTED_HOUSEHOLDS[table])}, got {sorted(seen)}'
    )


@pytest.mark.parametrize('table,levels', [
    ('food_coping', ['t', 'i', 'Strategy']),
    ('months_food_inadequate', ['t', 'i']),
])
def test_module_h_tables_are_unique_on_their_declared_index(malawi, table, levels):
    """`.first()` collapses nothing at either site — assert the consequence."""
    df = _table(malawi, table)
    flat = df.reset_index()
    have = [c for c in levels if c in flat.columns]
    assert have == levels, (
        f'{table} lost an index level: expected {levels}, have {have}'
    )
    dup = flat.groupby(levels, dropna=False).size()
    dup = dup[dup > 1]
    assert len(dup) == 0, (
        f'Malawi {table}: {len(dup)} {tuple(levels)} groups hold more than one '
        f'row.  `.first()` is now assembling a skipna composite; find out '
        f'whether those rows are one household or two before touching the '
        f'reducer.\n{dup.head()}'
    )
    assert flat['i'].notna().all(), (
        f'Malawi {table}: {int(flat["i"].isna().sum())} rows have a NULL '
        f'household id, which groupby(dropna=True) deletes outright'
    )
