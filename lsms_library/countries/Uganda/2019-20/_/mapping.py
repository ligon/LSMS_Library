"""Wave-level formatting helpers for Uganda 2019-20.

Reconstructs ``Age`` from h2q9a/b/c via the shared ``age_handler``
glue in ``../../_/_age_helpers.py``.  Month is an English string
in this wave; see 2018-19 for the same pattern.  GH #177.

Also overrides the country-level scalar ``v`` formatter with a composite
DISTRICT/PARISH cluster key, and the country-level ``District`` formatter with
the row-wise version that reads the same alias table -- see ``_V_ALIASES``,
``v`` and ``District`` below (GH #323).
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


#: ``(district as coded, parish) -> canonical district`` (GH #323, and the
#: adversarial review of PR #634).
#:
#: The DISTRICT/PARISH composite fixes parish-name COLLISIONS, and the metric
#: originally offered as evidence for it -- "clusters spanning more than one
#: district: 23 -> 0" -- is ZERO BY CONSTRUCTION under a key that contains the
#: district.  It therefore cannot detect the opposite error, OVER-SPLITTING one
#: real parish into two clusters because its households disagree about which
#: district they are in.  The audit that can is: group GSEC1 by its own
#: ``(county, subcounty, parish)`` triple and count distinct composite keys.
#: Run on this wave it finds three (2018-19: none):
#:
#:   county    subcounty  parish      district as coded          households
#:   BUSIKI    NAMUTUMBA  NAWANSAGWA  NAMTUMBA / NAMUTUMBA           1 / 10
#:   KASSANDA  KITUMBI    KIJUNA      MUBENDE  / KASSANDA            1 /  9
#:   OMORO     BOBI       PALWO       GULU     / OMORO               2 /  8
#:
#: In each case both groups also agree on ``region`` (and, but for one blank,
#: on ``urban``), so this is one place recorded two ways, not two places:
#:
#: * ``NAMTUMBA`` is a plain misspelling of ``NAMUTUMBA``; it occurs ONCE in
#:   the wave's 3,098 households.
#: * Kassanda District was carved out of Mubende in 2019 and Omoro out of Gulu
#:   in 2016; one and two households respectively still code the PARENT
#:   district while their county/subcounty/parish are the new district's.
#:
#: Four households would otherwise be put in a 1-2 household phantom cluster
#: instead of the 10-11 household cluster they belong to -- exactly the failure
#: mode ("a new bug traded for the old one") this file cites when it declines
#: to key on ``subcounty_name``.
#:
#: Keyed on the ``(district, parish)`` PAIR, not on the district alone: Mubende
#: and Gulu are still real, populated districts in this wave (29 and 37
#: households), so a bare ``MUBENDE -> KASSANDA`` alias would be wrong.
#:
#: Two further district misspellings exist in this wave -- ``PALISA`` for
#: ``PALLISA`` (parish ``KOBIUN``) and ``RUKINGIRI`` for ``RUKUNGIRI`` (parish
#: ``NYABITETE``), one household each.  They are deliberately NOT aliased: the
#: correctly-spelled district contains no household in either parish, so they
#: split nothing.  They are cosmetic label warts, not grain defects, and
#: ``tests/test_uganda_323_grain.py`` asserts the invariant that would catch
#: them if that ever changed.
_V_ALIASES = {
    ('NAMTUMBA', 'NAWANSAGWA'): 'NAMUTUMBA',
    ('MUBENDE', 'KIJUNA'): 'KASSANDA',
    ('GULU', 'PALWO'): 'OMORO',
}


def _district_parish(value):
    """``(district, parish)`` from a row, alias-resolved; ``None`` if blank.

    Both components are cleaned with ``format_id`` and upper-cased *before*
    the alias lookup, so the table above is keyed on canonical text.
    """
    parts = list(value) if isinstance(value, pd.Series) else [value]
    cleaned = [format_id(p) for p in parts]
    if any(p is None or not str(p).strip() for p in cleaned):
        return None
    district, parish = (str(p).strip().upper() for p in cleaned)
    return _V_ALIASES.get((district, parish), district), parish


def v(value):
    """Composite cluster key ``DISTRICT/PARISH`` (GH #323).

    Identical in intent to the 2018-19 formatter of the same name -- see that
    file for the full evidence.  This wave carries the same defect: the parish
    CODE is broken (the YAML has said so for a while), so ``v`` fell back to the
    free-text parish name ``s1aq04a``, and 23 of this wave's 793 parish names
    recur across districts *as coded* (``CENTRAL`` in ten), fusing distinct
    parishes into a single "cluster" that ``groupby().first()`` then resolved
    arbitrarily.

    ``district`` + ``s1aq04a``, with the three ``_V_ALIASES`` entries applied,
    makes ``v`` a real key: 793 parish names -> 824 clusters; clusters whose
    households span more than one district as coded go 23 -> 0 (a metric that
    holds by construction, so it is not by itself evidence -- see
    ``_V_ALIASES``); and ``(county, subcounty, parish)`` triples split across
    more than one ``v`` go 3 -> 0.  ``sample`` and ``cluster_features`` declare
    the same pair and reach this same function, so they stay in lock-step and
    ``_join_v_from_sample`` propagates one key to every other table.

    Returns ``None`` when either component is blank, so an incompletely-located
    household gets no cluster rather than a fabricated one.  Exactly one
    household in this wave has a blank ``district``.
    """
    pair = _district_parish(value)
    return None if pair is None else '/'.join(pair)


def District(value):
    """The ``District`` attribute, resolved through the same ``_V_ALIASES``.

    Overrides the country-level scalar ``uganda.District`` (= ``format_id``),
    and is declared in this wave's ``cluster_features`` as the row-valued
    ``[district, s1aq04a]`` so it can see the parish.

    Without this, the three aliased clusters would carry households whose raw
    ``district`` strings disagree, and ``uganda.cluster_features`` ->
    ``reduce_to_agreed`` would (correctly, given what it was told) blank
    ``District`` for all three.  Aliasing ``v`` without aliasing ``District``
    would trade three over-split clusters for three ``<NA>`` districts whose
    value we have just finished arguing that we know.
    """
    pair = _district_parish(value)
    return None if pair is None else pair[0]
