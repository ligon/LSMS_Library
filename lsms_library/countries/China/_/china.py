# Formatting functions for China (CHNS).
#
# China is otherwise fully YAML-driven; this module exists for the
# auto-wired (by table name) df_edit hooks below (`cluster_features`,
# `household_roster`, `individual_education`, `plot_features`).
#
# GH #323 -- the three hooks `cluster_features`, `household_roster` and
# `individual_education` exist to make China's declared indexes unique AT
# THE SOURCE, so that no downstream `groupby().first()` collapse is ever
# reached.  Two facts about that collapse motivate the belt-and-braces
# assertions below:
#
#   * It is SILENT (or, in `_normalize_dataframe_index`, merely a warning
#     that fires only on a cold build -- so in warm operation the loss is
#     already baked into the cache and never announced again).
#   * `groupby().first()` is COLUMN-WISE first-non-null, not first-row.
#     Given two rows for one person-key it can synthesise a row that
#     belongs to NEITHER of them.  A dedup we perform ourselves, and can
#     prove lossless, is strictly safer than one we inherit.
#
# So each hook resolves its duplicates explicitly and then ASSERTS the
# declared index is unique.  Prose in a CONTENTS.org is not enforcement;
# a raise is.  If the raw source ever changes shape, these fail loudly
# (class-2, silently MISSING) rather than quietly wrong (class-1).

import pandas as pd


# --- documented raw-source errata (China CLSS 1995-97) ---------------------
# S01A2.DTA files household 10108's daughter-in-law TWICE: once correctly as
# pid 4, and once mis-keyed as pid 3 (colliding with the son, who is the real
# pid 3).  The mis-keyed row is byte-identical to the pid-4 row on all 14
# non-pid columns.  S02.DTA independently lists exactly four members (pid
# 1,2,3,4) for this household, corroborating that the household has 4 people
# and that the spurious row is the pid-3 copy of pid 4.
#
# Scoped to this verified case ON PURPOSE.  The general rule "drop a
# within-household duplicate person on all non-pid columns" would silently
# delete IDENTICAL TWINS, so we additionally require an actual (i, pid)
# collision -- twins carry distinct pids and therefore never match -- and we
# assert the affected keys are exactly the ones documented here.
_MISKEYED_ROSTER_ROWS = {('10108', '3')}

_PERSON_KEY = ('t', 'i', 'pid')


def _assert_unique(flat, key, table):
    """Raise (loudly) if *key* is not unique -- never collapse quietly."""
    dup = flat.duplicated(list(key), keep=False)
    if dup.any():
        offenders = (flat.loc[dup, list(key)]
                     .drop_duplicates().to_dict('records'))
        raise ValueError(
            f"China 1995-97 {table}: declared index {list(key)} is not unique "
            f"after explicit de-duplication -- {int(dup.sum())} row(s) over "
            f"{len(offenders)} key(s), e.g. {offenders[:5]}.  Refusing to fall "
            f"through to groupby().first(), which would silently drop rows (and "
            f"can column-wise splice two different people into one).  Resolve "
            f"the new collision in China/_/china.py.  GH #323."
        )


def _dedup_person_table(df, table, allow_miskeyed=False):
    """Make a person-level (t, i, pid) table unique at the source (GH #323).

    (a) Drop byte-identical duplicate rows.  Household 30132's entire roster
        is recorded TWICE, byte-for-byte, in both S01A2.DTA (roster) and
        S02.DTA (education) -- the same whole-household duplicate RECORD that
        also produces its duplicate plot rows in S05B (see plot_features /
        GH #513).  Dropping exact duplicates is lossless by construction.

    (b) Only when *allow_miskeyed*: drop the documented mis-keyed pid rows
        (see _MISKEYED_ROSTER_ROWS).  Requires BOTH an (i, pid) collision and
        byte-identity, on every non-pid field, with a row filed under a
        different pid in the same household.

    (c) Assert the declared index is now unique.
    """
    index_names = list(df.index.names)
    flat = df.reset_index()
    key = [c for c in _PERSON_KEY if c in flat.columns]

    flat = flat.drop_duplicates()                                        # (a)

    if allow_miskeyed:                                                   # (b)
        collides = flat.duplicated(key, keep=False)
        if collides.any():
            # Every field except the (suspect) pid, i.e. the person's actual
            # attributes plus their household.  A mis-keyed row duplicates
            # another row exactly here while disagreeing only on pid.
            attrs = [c for c in flat.columns if c != 'pid']
            miskeyed = collides & flat.duplicated(attrs, keep=False)
            found = {(str(i), str(p)) for i, p
                     in zip(flat.loc[miskeyed, 'i'], flat.loc[miskeyed, 'pid'])}
            unexpected = found - _MISKEYED_ROSTER_ROWS
            if unexpected:
                raise ValueError(
                    f"China 1995-97 {table}: undocumented mis-keyed roster "
                    f"row(s) {sorted(unexpected)} -- an (i, pid) collision whose "
                    f"row is byte-identical to another pid in the same household. "
                    f"The raw source has changed.  Verify against S02.DTA and "
                    f"extend _MISKEYED_ROSTER_ROWS deliberately; do NOT widen the "
                    f"rule (it would delete identical twins).  GH #323."
                )
            flat = flat[~miskeyed]

    _assert_unique(flat, key, table)                                     # (c)
    return flat.set_index(index_names)


def cluster_features(df):
    """One row per VILLAGE -- China's cluster table (GH #323).

    The wave YAML previously built this village-level table (declared index
    ``(t, v)``) with ``idxvars: {i: hid, v: [hid]}``, i.e. one row per
    PERSON of the household roster: 3002 rows for 30 villages, a 100x
    redundant extraction whose surplus was then collapsed away -- by
    ``Wave.cluster_features``' un-warned ``groupby().first()`` (GH #161)
    when ``i`` is in the index, and by ``_normalize_dataframe_index``
    otherwise.  It was lossless only BY ACCIDENT: the single column,
    Region, happens to be a deterministic function of the index.  That is
    one un-enforced invariant away from being silently wrong.

    The table is now extracted at household grain (TOTEXP.DTA -- the same
    787-row file ``sample`` reads, so the village universe here is exactly
    the village universe of the sample) with NO ``i`` in idxvars, and
    reduced here to one row per village.  ``drop_duplicates`` over
    (t, v, Region) is lossless iff Region is single-valued within a
    village; if it ever is not, the village emits two rows and the
    uniqueness assert RAISES instead of silently keeping one.  That is the
    invariant, enforced.

    Note on provenance: the community questionnaire (NPT0101.DTA) is a
    genuine one-row-per-village file and would be the principled source --
    but its village codes CANNOT be linked to the household ``v``.  See
    the wave data_info.yml for the (dispositive) reason.
    """
    index_names = [c for c in df.index.names if c in ('t', 'v')]
    flat = df.reset_index()[index_names + list(df.columns)]
    out = flat.drop_duplicates()
    _assert_unique(out, index_names, 'cluster_features')
    return out.set_index(index_names)


def household_roster(df):
    """De-duplicate the (t, i, pid) roster explicitly (GH #323).

    S01A2.DTA carries 4 duplicate person-keys: household 30132's roster is
    present twice byte-identically (3 rows), and household 10108's
    daughter-in-law is filed both as pid 4 and, mis-keyed, as pid 3 (1 row).
    Both are resolved here, provably, rather than left to a downstream
    ``groupby().first()``.
    """
    return _dedup_person_table(df, 'household_roster', allow_miskeyed=True)


def plot_features(df):
    """Resolve (i, plot_id) collisions in China CHNS 1995-97 land roster (GH #513).

    S05B.DTA carries a few duplicate (hid, s05bpn) plot rows for two
    households (hid 30132, 30137; documented in the wave data_info.yml as
    a raw-source quirk, previously left as-is).  Most are exact-duplicate
    rows (benign redundancy); a couple differ in recorded plot Area for the
    same plot_id.  Under the downstream ``groupby().first()`` the divergent
    ones silently drop a row.

    Drop the exact-duplicate rows (lossless), then cumcount-suffix any
    residual genuinely-divergent (i, plot_id) collisions -- Albania
    precedent (albania.py:287-295) -- so each survives rather than being
    dropped.  No canonical-index change (suffixes are just ``_2`` on the
    few real collisions).
    """
    flat = df.reset_index().drop_duplicates()
    key = [c for c in ('i', 'plot_id') if c in flat.columns]
    if key and flat.duplicated(key, keep=False).any():
        if 'Area' in flat.columns:
            flat = flat.sort_values('Area', ascending=False, na_position='last')
        n = flat.groupby(key, dropna=False).cumcount()
        extra = n > 0
        flat.loc[extra, 'plot_id'] = (
            flat.loc[extra, 'plot_id'].astype('string')
            + '_' + (n[extra] + 1).astype('string'))
    idx = [c for c in ('t', 'i', 'plot_id') if c in flat.columns]
    return flat.set_index(idx)


def _years_to_education_level(y):
    """Bin years-of-schooling -> canonical Educational Attainment label (#495).

    CLSS 1995-97 has NO categorical attainment variable; education exists only
    as continuous years (S02.DTA s0201).  Cutoffs follow China's 6+3+3+4 system
    (primary 6 / junior-secondary 3 / senior-secondary 3 / bachelor 4) mapped
    onto canonical_education_labels.org; complete/incomplete is inferred from
    cycle length (the standard approach when only years are recorded).  99 is
    the survey's missing sentinel -> NaN.
    """
    if pd.isna(y):
        return pd.NA
    try:
        y = int(round(float(y)))
    except (TypeError, ValueError):
        return pd.NA
    if y == 99 or y < 0:
        return pd.NA                       # missing sentinel
    if y == 0:
        return 'None'
    if y <= 5:
        return 'Primary incomplete'
    if y == 6:
        return 'Primary complete'
    if y <= 8:
        return 'Lower secondary'
    if y == 9:
        return 'Lower secondary complete'
    if y <= 11:
        return 'Upper secondary'
    if y == 12:
        return 'Upper secondary complete'
    if y <= 15:
        return 'Tertiary certificate/diploma'   # da-zhuan / non-degree college
    if y == 16:
        return 'Bachelor'                        # 12 + 4-yr degree
    return 'Postgraduate'                        # >=17 (none observed this wave)


def individual_education(df):
    """df_edit hook (auto-wired by table name): derive canonical Educational
    Attainment from S02 years-of-schooling for China CLSS 1995-97 (#495).

    The wave data_info.yml extracts the raw years (s0201) into the
    ``Educational Attainment`` column; here we bin it to the canonical ordinal
    vocabulary.  Index (t, i, pid) is preserved.  Replaces the prior wiring to
    S01B s01b10 (the *mother's occupation* code) -- a silent data-correctness bug.

    Also de-duplicates the (t, i, pid) index (GH #323): S02.DTA repeats
    household 30132's three education rows byte-for-byte (the same
    whole-household duplicate RECORD seen in S01A2 and S05B).  Exact-duplicate
    removal is lossless and resolves all 3.  Unlike the roster, NO mis-keyed-pid
    rule is applied here: S02 carries a single value column, so "identical on all
    non-pid fields" is far too weak a signature to justify deleting a row.  Any
    residual collision therefore RAISES -- loudly missing beats quietly wrong.
    """
    out = _dedup_person_table(df, 'individual_education', allow_miskeyed=False)
    out['Educational Attainment'] = (
        out['Educational Attainment'].map(_years_to_education_level).astype('string'))
    return out
