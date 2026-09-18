"""Ghana AHIES (Annual Household Income and Expenditure Survey, GSS, 2022-2024).

A quarterly household panel: 10,800 households in 600 EAs, interviewed every
quarter from 2022Q1 to 2024Q4.  GSS ships one person x quarter file and one
household x quarter (Sections 5-7) file per calendar YEAR, so the wave FOLDERS
are years while the wave LABELS -- the values of ``t`` -- are quarters, mapped
through ``wave_folder_map`` exactly as Nigeria's post-planting / post-harvest
rounds and Tanzania's ``2008-15`` folder are.  ``t`` must be the quarter, not
the year: ``hh_weight`` is a per-quarter weight and ``sample`` carries one
``weight`` per ``(i, t)`` (EL, 2026-09-17; see ``_/CONTENTS.org`` and
``.coder/ledger/ghana-ahies.md``).

Each wave script reads its folder's file once and emits all four quarters with
``t`` in the index; ``Wave.grab_data`` filters to its own ``t``.

Helpers shared by the wave scripts live HERE (not in a wave-level module):
``Wave.data_scheme`` treats every ``.py`` stem in a wave's ``_/`` as a table,
so a ``mapping.py`` there would become a phantom table.  This module must stay
import-safe (no data reads at import): the framework imports it for ``waves``.
"""
from pathlib import Path

import pandas as pd

waves = [f"{y}Q{q}" for y in (2022, 2023, 2024) for q in (1, 2, 3, 4)]

wave_folder_map = {w: w[:4] for w in waves}

_HERE = Path(__file__).resolve().parent


# Household id: (cluster, HholdID) is the stable panel key; the shipped `hhid`
# is quarter-scoped (qtr + cluster + household) and must NOT be used as `i`.
def household_id(cluster, hholdid) -> str:
    return f"{int(cluster)}-{int(hholdid)}"


# The person-file relationship code that is NOT a relationship.  Code 17 is a
# CAPI-added code (the paper questionnaire's list stops at 16 = Househelp)
# marking a panel line whose person has left the household.  Such rows are
# not members in that quarter and are dropped by the person-level scripts
# with a count (52 rows on `s1aq2x` in 2024; see CONTENTS.org).
NOT_MEMBER_LABEL = "Not member anymore"


def person_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Return the canonical ``(t, i, pid)`` key columns for a person-file frame.

    ``t`` is the shipped ``quarter`` string (``'2024Q1'``); ``i`` is
    ``household_id(cluster, HholdID)``; ``pid`` is ``new_pid`` as a string.
    The 2024 person file has 0 nulls and 0 duplicates on this key.
    """
    keys = pd.DataFrame({
        't': df['quarter'].astype(str),
        'i': [household_id(c, h) for c, h in zip(df['cluster'], df['HholdID'])],
        'pid': df['new_pid'].astype(int).astype(str),
    }, index=df.index)
    return keys


def household_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Return the canonical ``(t, i)`` key columns for a household-grain frame
    (``cluster``, ``HholdID``, ``quarter`` columns; ``HholdID`` non-null)."""
    return pd.DataFrame({
        't': df['quarter'].astype(str),
        'i': [household_id(c, h) for c, h in zip(df['cluster'], df['HholdID'])],
    }, index=df.index)


# Section 2: the levels whose grade completed (`s2aq4`) distinguishes an
# incomplete from a completed cycle.  Everything else (pre-primary, tertiary,
# vocational, certificates) is mapped from the level label alone.
GRADED_LEVELS = ('Primary', 'JSS/JHS', 'Middle', 'SSS/SHS', 'Secondary')


def education_label(level, grade) -> object:
    """Compose the raw "<level> <grade>" label that ``harmonize_education``
    (``_/categorical_mapping.org``) maps onto the canonical vocabulary.

    ``level`` is the decoded ``s2aq3`` label; ``grade`` is ``s2aq4`` (highest
    grade COMPLETED at that level, 0 = none yet -- field manual, Section 2
    Q4).  An unlabelled numeric residue in ``s2aq3`` (the value 0.0, 401 rows
    in 2024) and a missing level are returned as NA and become 'Unknown'.
    """
    if pd.isna(level) or not isinstance(level, str):
        return pd.NA
    level = level.strip()
    if level in GRADED_LEVELS and pd.notna(grade):
        return f"{level} {int(grade)}"
    return level


def harmonize_education_labels(series: pd.Series) -> pd.Series:
    """Map composed education labels onto the canonical ordinal vocabulary via
    the ``harmonize_education`` table in ``_/categorical_mapping.org`` (the
    Tanzania pattern: harmonised in the wave script so the per-wave parquet
    already carries canonical labels).  Unmapped, non-null labels fold to
    'Unknown'; nulls stay null (the row is then dropped by the framework's
    ``dropna(how='all')``, i.e. the member was not asked -- under 3)."""
    from lsms_library.local_tools import all_dfs_from_orgfile
    table = all_dfs_from_orgfile(str(_HERE / 'categorical_mapping.org'))['harmonize_education']
    rdict = (table.assign(**{'Original Label': table['Original Label'].str.strip()})
             .set_index('Original Label')['Preferred Label'].to_dict())
    raw = series.where(series.notna(), None)
    mapped = raw.map(lambda x: rdict.get(str(x).strip(), 'Unknown') if x is not None else pd.NA)
    return mapped


# ---------------------------------------------------------------------------
# Wave-script plumbing: which year a script belongs to, and which file it reads
# ---------------------------------------------------------------------------

def wave_year(script_file) -> str:
    """The wave FOLDER's name (the calendar year) for a wave script.

    ``Path(<script>).resolve().parents[1]`` is the wave directory, because a
    wave script always lives in ``<year>/_/``.
    """
    return Path(script_file).resolve().parents[1].name


def person_file(script_file) -> str:
    """Relative path to the year's person x quarter ``.dta``, by GLOB.

    The file names are not uniform across the three folders -- 2022 and 2023
    ship ``<year> AHIES Q1-Q4_Rev_20250827.dta`` and 2024 ships
    ``2024 AHIES Q1-Q4_20250827.dta`` -- so the name is discovered rather than
    hardcoded.  The household (Sections 5-7) file in the same directory is
    excluded by its ``SEC`` marker, and ``.dvc`` sidecars are stripped: the
    in-tree copies are refused by the reader (GH #831) and only the NAME is
    taken from here, the bytes coming from the L1 blob cache via
    ``get_dataframe``.
    """
    data = Path(script_file).resolve().parents[1] / 'Data'
    names = sorted({p.name[:-4] if p.name.endswith('.dvc') else p.name
                    for p in data.glob('*.dta*')
                    if 'SEC' not in p.name})
    assert len(names) == 1, f'expected exactly one person file in {data}, found {names}'
    return f'../Data/{names[0]}'


def wave_quarters(year) -> set:
    """The four ``t`` labels a year folder must emit."""
    return {f'{year}Q{q}' for q in (1, 2, 3, 4)}


def assert_quarters(df: pd.DataFrame, year, where: str) -> None:
    """The shipped ``quarter`` column must spell exactly this folder's four
    quarters.  ``Wave.grab_data`` filters the folder parquet on ``t ==
    self.year``, so a respelt label would serve ZERO rows silently."""
    got = set(df['quarter'].astype(str).unique())
    assert got == wave_quarters(year), f'{where} {year}: quarter labels {sorted(got)}'


# ---------------------------------------------------------------------------
# Relationship to head: the source column is chosen PER QUARTER
# ---------------------------------------------------------------------------
#
# GSS ships two relationship-to-head codes, the raw `s1aq2` and the corrected
# `s1aq2x`, and WHICH ONE IS USABLE VARIES BY QUARTER.  The rule below is the
# one measured per year and recorded in `_/CONTENTS.org`:
#
#   2022Q1-Q4   s1aq2   -- `s1aq2x` is one of 319 columns that are 100% NULL
#                          in the 2022 person file; the raw code is clean
#                          there (0 nulls, 0 double-headed).
#   2023Q1      s1aq2   -- `s1aq2x` is 83.5% unfilled in that quarter.
#   2023Q2-Q4   s1aq2x  -- complete, one head per household-quarter.
#   2024Q1-Q4   s1aq2x  -- the corrected code throughout.
#
# A per-QUARTER choice of source column, never a row-wise coalesce: the 2023Q1
# fill is partial WITHIN 329 households and a coalesce manufactures 4
# double-headed households there (measured 2026-09-17).
#
# Every person-level table must use the same rule, because `NOT_MEMBER_LABEL`
# (code 17) is read off this column: reading `s1aq2x` in 2022 would make the
# departed-member filter a silent no-op rather than a measured zero.  NOTE
# `household_roster.py` and `individual_education.py` in `2022/_`, `2023/_`
# and `2024/_` still carry their own copies of this rule, written before it
# was hoisted; they agree with the table below, and should adopt it when next
# touched.
RELATIONSHIP_SOURCE = {
    **{f'2022Q{q}': 's1aq2' for q in (1, 2, 3, 4)},
    '2023Q1': 's1aq2', '2023Q2': 's1aq2x', '2023Q3': 's1aq2x', '2023Q4': 's1aq2x',
    **{f'2024Q{q}': 's1aq2x' for q in (1, 2, 3, 4)},
}


def relationship(df: pd.DataFrame, year, report: str = '') -> pd.Series:
    """Relationship-to-head label per row, from each quarter's own source
    column (``RELATIONSHIP_SOURCE``).  Non-string codes -- nulls and the
    unlabelled numeric ``0.0`` residue -- become NA.

    With ``report`` set, the label sets of BOTH candidate columns are printed
    per quarter, so the choice above stays measured rather than inherited: a
    re-release that fills ``s1aq2x`` in 2022, or that respells
    ``'Not member anymore'``, shows up in the build log.
    """
    q = df['quarter'].astype(str)
    out = pd.Series(pd.NA, index=df.index, dtype=object)
    for t in sorted(wave_quarters(year)):
        col = RELATIONSHIP_SOURCE[t]
        m = (q == t).values
        out.loc[m] = df.loc[m, col].astype(object).map(
            lambda x: x.strip() if isinstance(x, str) else pd.NA).values
    if report:
        for col in ('s1aq2', 's1aq2x'):
            lab = df[col].astype(object).map(lambda x: x.strip() if isinstance(x, str) else pd.NA)
            by_q = {t: g.value_counts(dropna=False).to_dict()
                    for t, g in lab.groupby(q, observed=True)}
            print(f'{report} {year}: {col} labels by quarter: {by_q}')
        print(f'{report} {year}: relationship source by quarter: '
              f'{ {t: RELATIONSHIP_SOURCE[t] for t in sorted(wave_quarters(year))} }')
    return out


def departed_mask(rel: pd.Series) -> pd.Series:
    """Rows whose relationship code is 17 ``'Not member anymore'`` -- a panel
    line whose person has LEFT the household.  Those rows carry sex and age
    but are not members in that quarter, and the instrument has no
    residence-duration question to carry the fact, so the person-level tables
    drop them with a count."""
    return (rel == NOT_MEMBER_LABEL).fillna(False).astype(bool)


def age_years_months(df: pd.DataFrame, report: str = '', year='') -> tuple:
    """``(completed years, months beyond years)`` from ``s1aq4y`` / ``s1aq4m``.

    ``s1aq4m`` is asked of under-fives and is 0-11, but the column also
    carries the questionnaire's ``98. Do not know`` code in some years (11
    rows in 2022, 0 in 2024), so values outside 0-11 are treated as absent
    and counted rather than asserted away.
    """
    years = pd.to_numeric(df['s1aq4y'], errors='coerce')
    months = pd.to_numeric(df['s1aq4m'], errors='coerce')
    bad = months.notna() & ~months.between(0, 11)
    if report:
        print(f'{report} {year}: s1aq4m filled on {int(months.notna().sum())} rows, '
              f'{int(bad.sum())} outside 0-11 (treated as absent)')
    return years, months.where(~bad)


# ---------------------------------------------------------------------------
# Shared table builders
# ---------------------------------------------------------------------------
#
# The three person-file tables below are built HERE, year-parameterised, and
# called by a thin `<year>/_/<table>.py` in each wave folder.  The builders take
# an already-read DataFrame: this module must stay import-safe (the framework
# imports it for `waves`), and the wave script owns the read and the write.
#
# The docstrings state the RULE.  Per-year MEASUREMENTS -- how many rows each
# rule touches in each year -- live in `_/CONTENTS.org`; every builder prints
# its own, so the build log is the evidence for what CONTENTS.org records.


def _label(s: pd.Series) -> pd.Series:
    """Decoded value label as a stripped string, NA where the source is NA."""
    out = s.astype(object).where(s.notna(), pd.NA)
    return out.map(lambda x: x.strip() if isinstance(x, str) else x)


CLUSTER_KEYS = ['cluster', 'quarter']
CLUSTER_COLS = {'region': 'Region', 'regdist': 'District', 'urbrur': 'Rural'}


def build_cluster_features(df: pd.DataFrame, year) -> pd.DataFrame:
    """Cluster features (Region, District, Rural) at the ``(t, v)`` grain.

    Built DIRECTLY at the cluster grain from the person file's design
    variables ``cluster`` / ``region`` / ``regdist`` / ``urbrur``: the three
    attributes are ASSERTED constant within every ``(cluster, quarter)`` group
    and the person rows are then de-duplicated to one per cluster-quarter.
    That is a checked de-dup, not an aggregation, and it keeps ``i`` out of the
    frame entirely, so the framework's household-to-cluster projection
    (``Wave.cluster_features``, GH #323 Site 2) has nothing to collapse and
    nothing to audit.  The assert is the grain guarantee: if a year ever
    disagrees, that is a finding to report, never something to average.

    Keys: ``v`` = ``cluster`` (the EA number as a string, exactly as ``sample``
    spells it); ``t`` = the shipped ``quarter``.  ``District`` is the
    ``regdist`` value label -- the 2021 PHC district / municipal /
    metropolitan-area name, whose raw labels carry leading blanks.  The number
    of clusters per quarter is MEASURED and reported per year, not asserted at
    a constant (the design draws 600 EAs; see CONTENTS.org for what each year
    actually carries).  No GPS of any kind is shipped in any AHIES file.
    """
    assert_quarters(df, year, 'cluster_features')
    assert df[CLUSTER_KEYS].notna().all().all(), 'null cluster / quarter in the person file'
    raw = df[CLUSTER_KEYS + list(CLUSTER_COLS)].copy()
    for c in CLUSTER_COLS:
        raw[c] = _label(raw[c])

    g = raw.groupby(CLUSTER_KEYS, observed=True)[list(CLUSTER_COLS)].nunique(dropna=False)
    bad = g[(g > 1).any(axis=1)]
    assert bad.empty, f'cluster attributes vary within a cluster-quarter:\n{bad.head()}'

    cf = raw.drop_duplicates(subset=CLUSTER_KEYS).rename(columns=CLUSTER_COLS)
    cf['t'] = cf['quarter'].astype(str)
    cf['v'] = cf['cluster'].astype(int).astype(str)
    cf = cf[['t', 'v'] + list(CLUSTER_COLS.values())].set_index(['t', 'v']).sort_index()

    assert cf.index.is_unique
    assert cf[list(CLUSTER_COLS.values())].notna().all().all(), 'null cluster attribute'

    # Measured, not inherited: clusters per quarter, and whether a cluster's
    # attributes are also constant ACROSS the four quarters of this year.
    per_t = cf.groupby(level='t').size().to_dict()
    across = cf.groupby(level='v')[list(CLUSTER_COLS.values())].nunique(dropna=False)
    moved = across[(across > 1).any(axis=1)]
    print(f'cluster_features {year}: clusters per quarter {per_t}; '
          f'{cf.index.get_level_values("v").nunique()} distinct v; '
          f'{len(moved)} clusters whose attributes change across quarters'
          + (f' {moved.head().to_dict()}' if len(moved) else ''))
    print(f'cluster_features {year}: {cf["District"].nunique()} districts, '
          f'{cf["Region"].nunique()} regions, Rural {cf["Rural"].value_counts().to_dict()}')
    return cf


ANTHRO_MEASURES = ['Weight', 'Height']


def _decode_measure(raw: pd.Series, name: str, year, quarter: pd.Series) -> pd.Series:
    """Numeric measure with every NEGATIVE value decoded to NA, counted per
    distinct value so the decision is auditable in the build log.

    THE RULE IS: a negative value is not a measurement and is decoded to NA;
    every other value is served exactly as recorded and its count reported.
    The rule is uniform; WHAT IT TOUCHES IS NOT.  The paper questionnaire says
    "NOT MEASURED CODE 999" for both Q3 and Q4, and each year's CAPI file
    spells "not measured" differently -- a negative sentinel in some quarters,
    plain missingness in others, and a POSITIVE sentinel (0, a near-zero
    float, 99) in others still.  Those non-negative spellings are deliberately
    NOT decoded, because separating them from a real measurement needs a
    threshold and a threshold is screening, which this library does not do
    inside a build; they are counted per quarter below and written up in
    `_/CONTENTS.org` (WAITING @ligon).  Trust no year's story about another.
    """
    x = pd.to_numeric(raw, errors='coerce')
    neg = x < 0
    counts = x[neg].round(3).value_counts().sort_index().to_dict() if neg.any() else {}
    print(f'anthropometry {year}: {name}: {int(neg.sum())} negative values -> NA {counts}; '
          f'{int(x.notna().sum())} of {len(x)} rows non-null before decoding '
          f'({100 * x.notna().mean():.1f}%)')
    probes = {'null': x.isna(), 'negative': neg, 'zero': x == 0,
              '0<x<1': (x > 0) & (x < 1), '==99': x == 99, '>220': x > 220}
    for label, m in probes.items():
        if m.any():
            print(f'anthropometry {year}: {name} {label}: {int(m.sum())} rows '
                  f'{m.groupby(quarter).sum().to_dict()}')
    return x.where(~neg)


def build_anthropometry(df: pd.DataFrame, year) -> pd.DataFrame:
    """Reported body measures at ``(t, i, pid)`` -- the GAP 5 shape of Uganda /
    Malawi / Tanzania / Nigeria: REPORTED measures only, no z-scores, no BMI,
    nothing derived, one row per MEASURED individual.

    Section 3B Q3-Q5, asked of EVERY household member every quarter (the
    questionnaire's "RESPONDENTS: ALL HOUSEHOLD MEMBERS").

      Weight      s3bq3, kilograms.
      Height      s3bq4, centimetres -- standing height or lying length, which
                  s3bq5 records; the precedents fold the two into one column
                  and serve no mode, and so does this table.
      Age_months  12 * s1aq4y + s1aq4m (the Malawi ``cage`` definition);
                  s1aq4m is asked of under-fives, so for members 5+ this is
                  exactly twelve times the completed years.

    NEGATIVE values are decoded to NA and counted (decoding, not screening).
    Every other value is served as recorded and never clipped -- INCLUDING the
    positive not-measured spellings that some quarters use (a zero weight, a
    near-zero height, 99): they are counted per quarter by `_decode_measure`
    and recorded in CONTENTS.org, not silently repaired.  Rows with neither
    measure after decoding are dropped with a count, as are the departed
    'Not member anymore' lines.

    GSS's own derived ``BMI`` column is reported on here as a possible
    cross-check: where it is populated it can be compared with the served
    measures, and where it is empty it cannot.
    """
    assert_quarters(df, year, 'anthropometry')
    rel = relationship(df, year, report='anthropometry')
    departed = departed_mask(rel)

    years, months = age_years_months(df, report='anthropometry', year=year)
    age_months = 12.0 * years + months.fillna(0.0)

    if 'BMI' in df.columns:
        bmi = pd.to_numeric(df['BMI'], errors='coerce')
        print(f'anthropometry {year}: GSS BMI column non-null on {int(bmi.notna().sum())} '
              f'of {len(bmi)} rows'
              + (f' (min {bmi.min()}, median {bmi.median()}, max {bmi.max()})'
                 if bmi.notna().any() else ' -- no cross-check possible'))

    out = person_keys(df)
    quarter = df['quarter'].astype(str)
    out['Weight'] = _decode_measure(df['s3bq3'], 'Weight', year, quarter)
    out['Height'] = _decode_measure(df['s3bq4'], 'Height', year, quarter)
    out['Age_months'] = age_months.astype(float)

    n_departed = int((departed & out[ANTHRO_MEASURES].notna().any(axis=1)).sum())
    out = out.loc[~departed]
    unmeasured = out[ANTHRO_MEASURES].isna().all(axis=1)
    print(f'anthropometry {year}: dropping {int(departed.sum())} "{NOT_MEMBER_LABEL}" rows '
          f'({n_departed} of them measured) and {int(unmeasured.sum())} rows with neither '
          f'Weight nor Height ({out.loc[unmeasured].groupby("t").size().to_dict()})')
    out = out.loc[~unmeasured]

    for c in ANTHRO_MEASURES + ['Age_months']:
        out[c] = out[c].astype('Float64')
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    assert out.index.is_unique, 'duplicate (t, i, pid) in anthropometry'

    # Report, never clip: the range of what is served, and the in-domain
    # values worth a second look.
    for c in ANTHRO_MEASURES:
        q = out[c].quantile([0.01, 0.99])
        print(f'anthropometry {year}: {c} n={int(out[c].notna().sum())} min={out[c].min()} '
              f'p1={q.iloc[0]:.1f} p99={q.iloc[1]:.1f} max={out[c].max()} '
              f'| 5 commonest {out[c].value_counts().head().to_dict()}')
    print(f'anthropometry {year}: heights above 220 cm: {int((out["Height"] > 220).sum())}; '
          f'weights above 150 kg: {int((out["Weight"] > 150).sum())}')
    return out


WORKING_AGE = 5          # Section 4A eligibility: members aged 5 years or older
DAYS = 'abcdefg'
ACTIVITIES = {           # column -> (yes/no question, per-day hours stem)
    'wage_work': ('s4aq1', 's4aq3'),
    'farm_work': ('s4aq7', 's4aq9'),
    'SOB_work': ('s4aq11', 's4aq13'),
}
HOURS = {'wage_work': 'wage_hrs', 'farm_work': 'farm_hrs', 'SOB_work': 'SB_hrs'}
# The order in which the CAPI asks the eight activity questions (Q1 wage, Q4
# domestic, Q7 farm, Q11 non-farm enterprise, Q15 family help, Q19 fishing /
# gathering, Q22 apprentice, Q25 voluntary).  Each is asked only if every
# earlier one was No -- see build_people_last7days.  Only the FIRST FIVE are
# plain Yes/No questions: `s4aq19` carries the production-destination label set
# ('Only for sale/barter', ...) beside 'No', `s4aq22` has three Yes variants
# (received pay / had to pay / neither) and `s4aq25` spells its Yes 'Yes,'.
# The cascade identity is therefore measured over the yes/no gates and the
# other three are reported as raw label counts, with nothing assumed about
# which of their labels mean "did this activity".
CASCADE = ['s4aq1', 's4aq4', 's4aq7', 's4aq11', 's4aq15']
CASCADE_TAIL = ['s4aq19', 's4aq22', 's4aq25']


def _yes_no(s: pd.Series) -> pd.Series:
    """'Yes' -> True, 'No' -> False, anything else (unasked, or skipped by the
    first-yes cascade) -> NA, as a nullable boolean."""
    lab = _label(s)
    out = pd.Series(pd.NA, index=s.index, dtype='boolean')
    out = out.mask(lab == 'Yes', True)
    out = out.mask(lab == 'No', False)
    unexpected = lab.dropna().loc[~lab.dropna().isin(['Yes', 'No'])]
    assert unexpected.empty, f'unexpected labels: {unexpected.unique()[:5]}'
    return out


def _hours(df: pd.DataFrame, stem: str, year) -> pd.Series:
    """Sum of the seven per-day hour entries; NA when none is reported."""
    days = df[[f'{stem}{d}' for d in DAYS]].apply(pd.to_numeric, errors='coerce')
    over = int((days > 24).sum().sum())
    if over:
        print(f'people_last7days {year}: {stem}a-g: {over} daily entries above 24 h '
              f'(max {days.max().max()}), served as recorded')
    return days.sum(axis=1, min_count=1).astype('Float64')


def _report_cascade(df: pd.DataFrame, year) -> None:
    """Re-measure the first-yes cascade rather than inherit it.

    If the module is a cascade, then for consecutive questions A then B,
    ``null(B) == null(A) | yes(A)`` up to the rows answered anyway.  Print the
    residual for each consecutive pair, per year: a year whose residual is
    large is NOT a cascade and the NA-not-False reading would be wrong there.
    """
    ans = {q: _yes_no(df[q]) for q in CASCADE if q in df.columns}
    for a, b in zip(CASCADE, CASCADE[1:]):
        if a not in ans or b not in ans:
            continue
        expect_null = ans[a].isna() | ans[a].fillna(False)
        extra = int((~expect_null & ans[b].isna()).sum())       # skipped for another reason
        answered_anyway = int((expect_null & ans[b].notna()).sum())
        print(f'people_last7days {year}: cascade {a} -> {b}: '
              f'null({b})={int(ans[b].isna().sum())}, expected {int(expect_null.sum())}; '
              f'{answered_anyway} answered after a prior Yes, {extra} null for another reason')
    print(f'people_last7days {year}: Yes counts over the yes/no cascade gates '
          f'{ {q: int(v.fillna(False).sum()) for q, v in ans.items()} }')
    for q in CASCADE_TAIL:
        if q in df.columns:
            print(f'people_last7days {year}: {q} (unserved activity, not a plain yes/no) '
                  f'labels {_label(df[q]).value_counts().to_dict()}')


def build_people_last7days(df: pd.DataFrame, year) -> pd.DataFrame:
    """Individual 7-day labour participation at ``(t, i, pid)`` -- the
    Guinea-Bissau / Niger ``people_last7days`` shape.

    Section 4A ("current economic activity status and characteristics of main
    job"), asked of ALL HOUSEHOLD MEMBERS AGED 5 YEARS OR OLDER (questionnaire
    header; field manual s9.4), so ``working_age`` is ``Age >= 5`` -- the
    module's OWN eligibility threshold and the corpus convention (EHCVM 6,
    Malawi 5), not the ILO 15, which is the threshold of Parts B, D, E and F.

      wage_work  s4aq1   worked >= 1 h in the past 7 days for a wage / salary /
                         commission / in-kind pay for a NON-member.
      farm_work  s4aq7   worked >= 1 h on a farm owned or rented by a member.
      SOB_work   s4aq11  ran / managed a household non-farm enterprise.
      *_hrs      the sum of that activity's seven per-day hour entries
                 (s4aq3a-g / s4aq9a-g / s4aq13a-g).  NOTE s4aq2 / s4aq8 /
                 s4aq12 are DAYS (1-7) despite the .dta label on s4aq2 saying
                 "hours": the manual's Q2 is "for how many days".
      Industry   s4aq41a1, the ISIC Rev.4 SECTION label of the main job,
                 served as recorded -- it is also asked of anyone temporarily
                 absent from a job, so rows carry an industry with none of the
                 three served activities Yes.

    NA IS NOT FALSE.  The CAPI asks the eight activity questions in order (Q1
    wage, Q4 domestic, Q7 farm, Q11 non-farm enterprise, Q15 family help, Q19
    fishing / gathering, Q22 apprentice, Q25 voluntary), and an unasked
    question is served AS NA (a nullable bool), never as False: a "share doing
    farm work" must be computed on the answered rows only.  That much holds in
    every year and is the rule.

    WHY a question goes unasked does NOT hold in every year, which is why
    ``_report_cascade`` re-measures it for the year being built instead of
    inheriting it.  In 2022 and 2024 the module is a strict FIRST-YES CASCADE
    -- once one activity is Yes the rest are skipped, so ``farm_work`` is NA
    for a wage worker -- but in 2023 thousands of rows answer a later question
    after an earlier Yes (measured: 7,444 on Q4, 16,344 on Q11), so that
    reading is FALSE there.  The per-year residuals are printed by every build
    and recorded in CONTENTS.org; the five unserved forms and GSS's own
    derived dummies are documented there too.
    """
    assert_quarters(df, year, 'people_last7days')
    rel = relationship(df, year, report='people_last7days')
    departed = departed_mask(rel)
    _report_cascade(df, year)

    out = person_keys(df)
    for col, (q, stem) in ACTIVITIES.items():
        out[col] = _yes_no(df[q])
        out[HOURS[col]] = _hours(df, stem, year)
        yes = out[col].fillna(False)
        # A Yes with no hours recorded is a per-year fact, not a build error:
        # it is 0 rows in 2024 but not in every year, and `*_hrs` is declared
        # optional.  Counted and served as NA rather than asserted away.
        nohrs = int((yes & out[HOURS[col]].isna()).sum())
        if nohrs:
            print(f'people_last7days {year}: {col} Yes with no {HOURS[col]} recorded on '
                  f'{nohrs} rows, served as NA')
    out['Industry'] = _label(df['s4aq41a1']).astype('string')
    age = pd.to_numeric(df['s1aq4y'], errors='coerce')
    out['working_age'] = (age >= WORKING_AGE).where(age.notna(), pd.NA).astype('boolean')

    n_departed = int((departed & out['wage_work'].notna()).sum())
    print(f'people_last7days {year}: dropping {int(departed.sum())} "{NOT_MEMBER_LABEL}" rows '
          f'({n_departed} with a Section 4A answer)')
    out = out.loc[~departed]
    out = out[['t', 'i', 'pid', 'farm_work', 'SOB_work', 'wage_work',
               'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']]
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    assert out.index.is_unique, 'duplicate (t, i, pid) in people_last7days'

    print(f'people_last7days {year}: answered / yes / NA per column',
          {c: (int(out[c].notna().sum()), int(out[c].fillna(False).sum()), int(out[c].isna().sum()))
           for c in ACTIVITIES})
    absent = out['Industry'].notna() & ~out[list(ACTIVITIES)].fillna(False).any(axis=1)
    print(f'people_last7days {year}: Industry present on {int(out["Industry"].notna().sum())} rows, '
          f'{int(absent.sum())} of them with no served 7-day activity')
    return out
