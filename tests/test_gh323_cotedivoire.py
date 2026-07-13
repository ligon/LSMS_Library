"""GH #323 -- CotedIvoire must not silently collapse a non-unique declared index.

`_normalize_dataframe_index` reduces a non-unique DECLARED index with
groupby().first(), silently discarding the dropped rows.  CotedIvoire tripped it
in three distinct ways; all three are pinned here.

  A cluster_features (all 5 waves) -- EXTRACTION_BUG.  The YAML reads a
    HOUSEHOLD-grain file (WEIGHT{85..88}.DAT / the EHCVM cover page
    s00_me_CIV2018.dta -- the very files `sample` reads per household) and
    declares only `v`, emitting 17,896 redundant household rows into a table
    whose grain is one row per cluster.  first() landed on the right value only
    by luck; grappe 648 (11 Rural / 1 Urbain) is where the luck ran out.

  B plot_inputs (2018-19) -- INDEX_INCOMPLETE.  A non-injective
    harmonize_seed_crop merged four distinct reported seed items into one
    'Autre crop' bucket, colliding two real line-items on one index tuple and
    silently discarding 9 rows of reported quantity.

  C household_roster / individual_education (1988-89) -- GENUINE_DUPLICATES in
    the raw source (CLUST 122).  Resolved deliberately and LOUDLY, never by a
    silent first().

INSTRUMENT NOTE (this cost a debugging cycle -- do not undo it).  Do NOT assert
that the API's returned index is unique: `_normalize_dataframe_index` collapses
it, so API-level uniqueness holds BY CONSTRUCTION and such a test passes even
with the bug fully present.  The bug is that ROWS DISAPPEAR.  So these tests
assert on the pre-collapse WAVE extraction, on exact row counts, on the specific
rows that were being eaten, and on the warnings -- never on post-collapse
uniqueness.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import Country

WAVES = ['1985-86', '1986-87', '1987-88', '1988-89', '2018-19']

# Row counts of the reported source, i.e. what must survive to the API.
PLOT_INPUTS_ROWS = 11259          # was 11250: 9 rows eaten by the seed collapse
ROSTER_1988_ROWS = 10563          # raw SEC01A.DAT rows (CLUST 122 defects incl.)


@pytest.fixture(scope='module')
def civ():
    try:
        return Country('CotedIvoire')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'CotedIvoire unavailable: {exc}')


def _build(c, table):
    try:
        return getattr(c, table)()
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'{table} could not be built: {type(exc).__name__}: {exc}')


# --------------------------------------------------------------------------- A
@pytest.mark.parametrize('wave', WAVES)
def test_cluster_features_extraction_is_cluster_grain(civ, wave, monkeypatch):
    """The EXTRACTION must emit one row per cluster, not one per household.

    Checked at the wave level, BEFORE the framework's duplicate-collapse can
    launder the mistake.  Pre-fix this returns 1588/1600/1600/1600/12992
    household rows for 100/100/100/100/1084 clusters.
    """
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    try:
        df = civ[wave].grab_data('cluster_features')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'{wave} cluster_features unavailable: {exc}')
    dup = int(df.index.duplicated().sum())
    assert dup == 0, (
        f'CotedIvoire/{wave}/cluster_features: the extraction emitted {len(df)} '
        f'rows for only {df.index.nunique()} clusters ({dup} duplicate (t, v) '
        f'tuples). It is reading a HOUSEHOLD-grain source and letting the '
        f'framework collapse the surplus with groupby().first() (GH #323).'
    )


def test_cluster_features_columns_are_cluster_constant(civ):
    """Every cluster_features column is a cluster-CONSTANT attribute.

    This is what makes (t, v) the right grain -- and what makes the surplus rows
    redundant repeats rather than lost information.
    """
    df = _build(civ, 'cluster_features')
    flat = df.reset_index()
    for col in df.columns:
        nun = flat.groupby(['t', 'v'], observed=True)[col].nunique(dropna=True)
        assert (nun <= 1).all(), (
            f'cluster_features column {col!r} is not constant within a cluster'
        )


def test_grappe_648_conflict_is_reported_not_silently_first_ed(civ, monkeypatch):
    """The one cell where validation was actually NEEDED (GH #323).

    grappe 648 (2018-19) has 11 households coded Rural and 1 coded Urbain --
    distinct raw labels, not a case variant.  groupby().first() returned Rural
    only because the Urbain household sorts LAST: a row-order accident, not a
    decision.  The extraction must now SAY it saw a conflict, and resolve it by
    strict majority.
    """
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = civ['2018-19'].grab_data('cluster_features')
        msgs = [str(w.message) for w in caught]

    assert any('648' in m and 'GH #323' in m for m in msgs), (
        'the grappe-648 Rural/Urbain conflict must be surfaced, not resolved by '
        f'row order. warnings seen: {[m[:90] for m in msgs][:4]}'
    )
    row = df.reset_index()
    row = row[row['v'] == '648']
    assert len(row) == 1
    assert row.iloc[0]['Rural'] == 'Rural', (
        "grappe 648 must resolve to the strict-majority value 'Rural' "
        '(11 Rural vs 1 Urbain).'
    )


# --------------------------------------------------------------------------- B
def test_plot_inputs_keeps_every_reported_row(civ):
    """All 11,259 reported input line-items must survive (9 were being eaten)."""
    df = _build(civ, 'plot_inputs')
    assert len(df) == PLOT_INPUTS_ROWS, (
        f'plot_inputs has {len(df)} rows, expected {PLOT_INPUTS_ROWS}. '
        f'{PLOT_INPUTS_ROWS - len(df)} reported line-item(s) are being silently '
        f'discarded by the framework collapse (GH #323).'
    )


def test_plot_inputs_recovers_the_discarded_market_seed(civ):
    """The exact row the catch-all bucket used to eat.

    Raw s16b, grappe 744 / menage 12 reports BOTH
        "Autres semences"             400 kg
        "Semences d'autres céréales"  350 kg   <- silently discarded pre-fix
    Both mapped to crop 'Autre crop', colliding on (i, Seed, Autre crop, Kg);
    first() kept the 400 and dropped the 350.  Distinct reported items must keep
    distinct crop keys.

    NB: assert the exact (crop, Quantity) PAIR.  Merely checking that 350.0
    appears somewhere in this household's seed rows passes even with the bug --
    the household separately reports 350 kg of MAIZE seed.  (That false pass is
    how this test was caught being useless.)
    """
    df = _build(civ, 'plot_inputs')
    flat = df.reset_index()
    seeds = flat[(flat['t'] == '2018-19') & (flat['i'] == '744012')
                 & (flat['input'] == 'Seed')]
    if seeds.empty:                                            # pragma: no cover
        pytest.skip('household 744012 not present')
    pairs = {(r['crop'], float(r['Quantity']))
             for _, r in seeds.iterrows() if pd.notna(r['Quantity'])}
    assert ('Autre crop', 400.0) in pairs, f'lost the 400 kg row; got {pairs}'
    assert ('Autre céréale', 350.0) in pairs, (
        f'the 350 kg of market-purchased "Semences d\'autres céréales" is still '
        f'being discarded -- harmonize_seed_crop is merging it into the same '
        f'"Autre crop" bucket as "Autres semences" (GH #323). got {pairs}'
    )


def test_harmonize_seed_crop_separates_the_two_labels_in_use(civ):
    """harmonize_seed_crop must be injective over the labels actually present.

    `crop` is an index level and harmonize_input maps EVERY seed label onto the
    single input 'Seed', so two seed labels sharing a Preferred Label collide on
    one index tuple.  The two labels that actually occur in CotedIvoire's s16b
    ("Autres semences", "Semences d'autres céréales") must not share a bucket.
    """
    m = civ.categorical_mapping.get('harmonize_seed_crop')
    if m is None:                                              # pragma: no cover
        pytest.skip('harmonize_seed_crop absent')
    m = m.set_index('Original Label')['Preferred Label']
    a, b = 'Autres semences', "Semences d'autres céréales"
    if a not in m.index or b not in m.index:                   # pragma: no cover
        pytest.skip('labels absent from mapping')
    assert m[a] != m[b], (
        f'"{a}" and "{b}" both map to {m[a]!r}. They are DIFFERENT reported seed '
        f'items; sharing a crop key collides them on the plot_inputs index and '
        f'the framework silently drops one (GH #323).'
    )


# --------------------------------------------------------------------------- C
def test_1988_89_person_collisions_are_loud_not_silent(monkeypatch):
    """The raw 1988-89 source repeats (CLUST, NH, PID) in CLUST 122.

    Whatever we do with them, it must not be a SILENT first().

    LSMS_NO_CACHE is forced: the resolution happens in the wave EXTRACTION, so a
    warm L2 read replays a frame in which the collisions are already resolved and
    no warning can fire.  (That is the same cache-hiding mechanism that kept #323
    invisible in the first place -- a test that skips it silently passes.)
    """
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        Country('CotedIvoire')['1988-89'].grab_data('household_roster')
        msgs = [str(w.message) for w in caught]
    assert any('GH #323' in m and '122' in m for m in msgs), (
        'the 1988-89 CLUST-122 person-key collisions must be surfaced loudly; '
        f'no such warning was emitted. warnings seen: {[m[:80] for m in msgs][:4]}'
    )


def test_1988_89_both_children_under_the_shared_pid_survive(civ):
    """CLUST 122 / NH 21 reports TWO 10-year-old children under one PID.

    (SEX=F, AGEY=10, REL=3) and (SEX=M, AGEY=10, REL=3) -- the sex disagrees, so
    the two rows cannot be one person's consistent record.  groupby().first()
    kept the girl and silently deleted the boy.  Both reported records must
    survive (re-keyed); no measurement is invented, only the key.
    """
    df = _build(civ, 'household_roster')
    flat = df.reset_index()
    hh = flat[(flat['t'] == '1988-89') & (flat['i'] == '122021')]
    if hh.empty:                                               # pragma: no cover
        pytest.skip('household 122021 not present')
    ten = hh[hh['Age'] == 10]
    sexes = sorted(str(s) for s in ten['Sex'].dropna().unique())
    assert sexes == ['F', 'M'], (
        f'household 122021 should retain BOTH 10-year-olds (one F, one M); '
        f'found sexes {sexes} among {len(ten)} ten-year-old(s). One reported '
        f'child is being silently deleted by the pid collapse (GH #323).'
    )
    assert len(hh) == 13, (
        f'household 122021 has {len(hh)} members; the source reports 13 person '
        f'records (pids 1..12 with 10 used twice).'
    )


def test_1988_89_roster_keeps_every_reported_person_record(civ):
    """The 1988-89 roster must not shed person records to a silent collapse.

    Full accounting from the raw SEC01A.DAT (every term measured, none assumed):

        10,563  reported person records
           - 1  true double-entry, deliberately collapsed
                (CLUST 122 / NH 19 / PID 2: same F, REL 10, age 46 vs 45)
        -------
        10,562  = the L2-wave parquet
           - 1  BLANK roster line dropped by _finalize_result
                (CLUST 137 / NH 6 / PID 12: Sex <NA>, Age '.', Relationship <NA>
                 -- an empty record carrying no person; PRE-EXISTING behaviour,
                 unrelated to #323, and identical before and after this fix)
        -------
        10,561  = the API

    Pre-fix the API returned 10,560: the framework collapsed BOTH duplicate keys,
    deleting the boy of NH 21 as well.  The +1 is that boy.
    """
    df = _build(civ, 'household_roster')
    n88 = int((df.reset_index()['t'] == '1988-89').sum())
    expected = ROSTER_1988_ROWS - 1 - 1
    assert n88 == expected, (
        f'1988-89 roster has {n88} rows; expected {expected} '
        f'({ROSTER_1988_ROWS} reported records, less the ONE true double-entry, '
        f'less the ONE blank record). (GH #323)'
    )
