"""Relations *between* World Bank catalog entries (GH #600).

:mod:`lsms_library.provenance` answers "what does this directory of ours hold?".
This module answers the question no directory of ours can answer on its own:
**how do catalog entries relate to each other?**

Two relations, both invisible to id-matching and both a source of confident
false claims in :func:`lsms_library.data_access.discover_waves` before they were
recorded:

``same_study``
    The World Bank catalogued one survey **twice**, in two repositories, under
    two different ids, with nothing in the metadata linking them.  South
    Africa's 1993 survey is ``lsms`` id 297 (``ZAF_1993_IHS``) *and*
    ``datafirst`` id 902 (``ZAF_1993_PSLSD``) — identical file lists, one
    survey.  We hold it; a census that surfaced 902 would call it missing.

``derived_from``
    An entry **built out of** other entries: a harmonized "uniform panel"
    re-release, an anonymized subset.  Nigeria's 5835 (``NGA_2010-2019_NUPD``)
    is the four GHS-Panel waves we already hold, harmonized.  It is not new
    fieldwork and not an acquisition target.

The data lives in ``lsms_library/catalog_relations.yml`` — config, not code,
and every entry carries its evidence.  The **completeness rule** is what keeps
``derived_from`` honest: a derived entry counts as non-missing only when *every*
constituent is held.  Drop a constituent wave and the entry goes back to being
reported missing, without anyone having to remember to update this file.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from importlib.resources import files

import yaml

logger = logging.getLogger(__name__)

_RELATIONS_FILE = "catalog_relations.yml"


@lru_cache(maxsize=1)
def _load() -> dict:
    """Read ``catalog_relations.yml``.  A missing/broken file is not fatal."""
    try:
        path = files("lsms_library") / _RELATIONS_FILE
        with path.open(encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning("Could not read %s: %s.  Catalog relations "
                       "(same-study aliases, derived re-releases) will not be "
                       "applied.", _RELATIONS_FILE, exc)
        return {}


@lru_cache(maxsize=1)
def same_study_aliases() -> dict[str, str]:
    """Map every duplicate catalog id -> the canonical id we record.

    The canonical id maps to itself, so ``aliases.get(cid, cid)`` is always safe
    and idempotent.
    """
    out: dict[str, str] = {}
    for group in _load().get("same_study") or []:
        canonical = str(group.get("canonical") or "").strip()
        if not canonical:
            continue
        out[canonical] = canonical
        for cid in group.get("ids") or []:
            out[str(cid).strip()] = canonical
    return out


@lru_cache(maxsize=1)
def derived_from() -> dict[str, list[str]]:
    """Map a derived catalog id -> the complete list of ids it is built from.

    "Complete" is load-bearing: :func:`~lsms_library.data_access.discover_waves`
    treats a derived entry as non-missing only when **all** of these are held.
    """
    out: dict[str, list[str]] = {}
    for cid, spec in (_load().get("derived_from") or {}).items():
        constituents = [str(c).strip() for c in (spec or {}).get(
            "constituents", []) if str(c).strip()]
        if constituents:
            out[str(cid).strip()] = constituents
    return out


def evidence_for(catalog_id: str) -> str | None:
    """Return the recorded evidence for a relation involving *catalog_id*."""
    cid = str(catalog_id).strip()
    spec = (_load().get("derived_from") or {}).get(cid)
    if spec and spec.get("evidence"):
        return str(spec["evidence"]).strip()
    for group in _load().get("same_study") or []:
        ids = {str(i).strip() for i in group.get("ids") or []}
        ids.add(str(group.get("canonical") or "").strip())
        if cid in ids and group.get("evidence"):
            return str(group["evidence"]).strip()
    return None
