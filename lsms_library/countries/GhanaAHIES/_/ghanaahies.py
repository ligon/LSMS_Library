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
