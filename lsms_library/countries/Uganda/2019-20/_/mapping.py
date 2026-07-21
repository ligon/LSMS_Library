"""Wave-level formatting helpers for Uganda 2019-20.

Reconstructs ``Age`` from h2q9a/b/c via the shared ``age_handler``
glue in ``../../_/_age_helpers.py``.  Month is an English string
in this wave; see 2018-19 for the same pattern.  GH #177.

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

INTERVIEW_YEAR = 2019


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)


def v(value):
    """Composite cluster key ``DISTRICT/PARISH`` (GH #323).

    Identical in intent to the 2018-19 formatter of the same name -- see that
    file for the full evidence.  This wave carries the same defect: the parish
    CODE is broken (the YAML has said so for a while), so ``v`` fell back to the
    free-text parish name ``s1aq04a``, and 23 of this wave's 794 parish names
    recur across districts (``CENTRAL`` in ten), fusing distinct parishes into a
    single "cluster" that ``groupby().first()`` then resolved arbitrarily.

    ``district`` + ``s1aq04a`` makes ``v`` a real key: 794 -> 828 clusters, and
    clusters spanning more than one district go 23 -> 0.  ``sample`` and
    ``cluster_features`` declare the same pair, so they stay in lock-step and
    ``_join_v_from_sample`` propagates one key to every other table.

    Returns ``None`` when either component is blank, so an incompletely-located
    household gets no cluster rather than a fabricated one.
    """
    parts = list(value) if isinstance(value, pd.Series) else [value]
    cleaned = [format_id(p) for p in parts]
    if any(p is None or not str(p).strip() for p in cleaned):
        return None
    return '/'.join(str(p).strip().upper() for p in cleaned)
