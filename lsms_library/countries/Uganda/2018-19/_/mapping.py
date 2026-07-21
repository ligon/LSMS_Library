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


def v(value):
    """Composite cluster key ``DISTRICT/PARISH`` (GH #323).

    Overrides the country-level scalar ``uganda.v`` (= ``format_id``).  This
    wave has no usable cluster CODE -- ``parish_code`` carries only 19 distinct
    values, which the YAML has flagged BROKEN for a while -- so ``v`` was set to
    the free-text ``parish_name``.

    **Parish names are not unique in Uganda.**  20 of this wave's 751 names
    recur across districts and ``CENTRAL`` alone appears in TEN of them, so the
    bare name fused genuinely different parishes into one "cluster" whose
    Region / District / GPS were then resolved by an undeclared
    ``groupby().first()`` -- i.e. arbitrarily.  Qualifying the name by its
    district makes ``v`` an actual key: 751 -> 781 clusters, and clusters
    spanning more than one district go 20 -> 0.

    Both ``sample`` and ``cluster_features`` declare ``v`` as the same
    ``[distirct_name, parish_name]`` pair (note the source's misspelling of
    "district"), so the two cannot drift -- ``_join_v_from_sample`` propagates
    one key to every other Uganda table.

    Deliberately NOT keyed any finer.  Adding ``subcounty_name`` splits 10 more
    groups, but every one of those splits is a SPELLING artefact of a single
    subcounty, not a second place: ``NYENGA`` / ``NYENGA DIVISION`` (centroids
    0.0 km apart), ``LUBAGA`` / ``RUBAGA DIVISION`` (5.3 km),
    ``'KAGADI  TOWN COUNCIL'`` / ``'KAGADI TOWN COUNCIL'`` (a doubled space).
    Keying on it would fragment real parishes on data-entry noise -- a new bug
    traded for the old one.

    Returns ``None`` when either component is blank, so an incompletely-located
    household gets NO cluster rather than a fabricated one (class-2 missing
    beats class-1 wrong).  In this wave neither component is ever blank.
    """
    parts = list(value) if isinstance(value, pd.Series) else [value]
    cleaned = [format_id(p) for p in parts]
    if any(p is None or not str(p).strip() for p in cleaned):
        return None
    return '/'.join(str(p).strip().upper() for p in cleaned)
