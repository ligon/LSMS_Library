"""Wave-level formatting helpers for Uganda 2018-19.

Reconstructs ``Age`` from h2q9a/b/c via the shared ``age_handler``
glue in ``../../_/_age_helpers.py``.  Month is an English string
('October', "Don't know", ...); the helper's _MONTH_NAME_TO_INT
table converts it to 1..12 and treats DK tokens as missing.
GH #177.

Also overrides the country-level scalar ``v`` formatter with a composite
DISTRICT/PARISH cluster key -- see ``v`` below (GH #323).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import format_id

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_uganda_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

INTERVIEW_YEAR = 2018


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)


def v(x):
    """Composite cluster key ``DISTRICT/PARISH`` (GH #323).

    Overrides the country-level scalar ``v`` (``uganda.v`` = ``format_id``).
    This wave has no usable cluster CODE -- ``parish_code`` carries only 19
    distinct values (the YAML has flagged it BROKEN for a while), so ``v`` was
    set to the free-text ``parish_name``.  Parish names are NOT unique in
    Uganda: 20 of them recur across districts, and ``CENTRAL`` alone appears in
    TEN (HOIMA, KAGADI, KALUNGU, KIRYANDONGO, KYOTERA, MITOOMA, ...).  Keying on
    the bare name fused genuinely different parishes into one "cluster", whose
    Region/District/GPS ``groupby().first()`` then picked arbitrarily.

    Qualifying by district makes ``v`` an actual cluster key: multi-district
    groups drop 20 -> 0 and the p90 within-cluster GPS spread falls 24.0 -> 14.9
    km.  Both ``sample`` and ``cluster_features`` declare ``v`` as the same
    ``[distirct_name, parish_name]`` pair (note the source's misspelling of
    "district"), so the two stay in lock-step -- ``_join_v_from_sample`` hands
    every other table the same key.

    Deliberately NOT keyed any finer.  Adding ``subcounty_name`` splits 10 more
    groups, but every one of those splits is a SPELLING artefact of a single
    subcounty, not a real second place: NYENGA / "NYENGA DIVISION" (centroids
    0.0 km apart), LUBAGA / RUBAGA DIVISION (5.3 km), "KAGADI  TOWN COUNCIL" /
    "KAGADI TOWN COUNCIL" (a doubled space).  Keying on it would fragment real
    parishes on data-entry noise -- a new bug traded for the old one.

    Returns ``None`` when either component is blank, so an incompletely-located
    household gets no cluster rather than a fabricated one (class-2 missing over
    class-1 wrong).  In this wave neither component is ever blank.
    """
    parts = x.tolist() if isinstance(x, pd.Series) else [x]
    cleaned = [format_id(p) for p in parts]
    if any(p is None or not str(p).strip() for p in cleaned):
        return None
    return '/'.join(str(p).strip().upper() for p in cleaned)
