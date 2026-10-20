"""How a survey's sample was DRAWN -- recorded per ``(country, wave)``.

GH #793.  The library records what a sample **represents**
(``population.yml``, :mod:`lsms_library.population`) but had no record of how it
was **selected**: whether a design is two-stage, what the PSU is, how it is
stratified, whether a stratum was oversampled, whether the wave is a panel.
That lived in prose -- only 13 of 38 countries have a ``* Sampling Design``
section in ``CONTENTS.org``, organised differently in each -- so "which
countries are two-stage stratified cluster designs?" had no answer short of
reading them all.

This module is the reader for ``countries/{C}/_/sampling.yml``.  It mirrors
:mod:`lsms_library.population` deliberately and closely: the same immutable
``dict``-subclass record, the same ``lru_cache``d loader that never raises for
a missing file, the same ``df.attrs`` attach from ``Country._finalize_result``,
the same ``merge_attrs`` re-attached after ``Feature()``'s concat through
``feature._ATTRS_CARRIERS``.  Read ``population.py`` first; what follows names
only where this record differs.

The vocabulary is FROZEN from the Uganda exemplar, as-is
--------------------------------------------------------
@ligon, 2026-10-18 (GH #793): "Freeze the Uganda exemplar vocabulary as-is,
then build the ``sampling.yml`` loader mirroring ``population.py``."  So every
controlled vocabulary below is exactly the set of values
``Uganda/_/sampling.yml`` uses (plus the ``source_type`` / ``confidence``
legends its header documents), and every field name a record may carry --
at the top level and inside ``stratification``, ``selection``, ``panel`` and
each ``oversamples`` entry -- is exactly the set that file uses.  A value or a
field outside them is **refused**, not passed through: widening the
vocabulary is a decision, and it is made by editing a frozenset here with a
reason, not by a curator typing a new word into a ``.yml``.

What is controlled and what is not: ``design_tag``, ``source_type``,
``confidence``, ``documentation_status`` and ``oversamples[].status`` are
controlled vocabularies -- the three the issue named for freezing, plus the
provenance pair every editorial reading must travel with.  Free-text values
(``frame``, ``stratification.basis``, ``selection.psu``, ``panel.role``, ...)
are not controlled; their field NAMES are.

The tag is an editorial reading, so it never travels alone
-----------------------------------------------------------
Exactly as ``universe_tag``: ``design_tag`` is stored, returned and reported
only together with ``source_type`` (where the statement came from) and
``confidence`` (how strongly the source states a DESIGN).
:meth:`SamplingRecord.from_config` refuses a design record missing any of the
three.

``derived-from-data`` is NOT documentation
------------------------------------------
``source_type`` has a fourth value population does not:
``derived-from-data`` -- inferred from ``sample()`` rather than read in a
document.  The exemplar's header is explicit that this is "NOT a claim about
the design".  Two mechanisms keep it from being read as one:

* :attr:`SamplingRecord.documented` is ``True`` only for
  ``local-documentation`` / ``wb-catalog`` -- the one predicate a consumer
  needs, so nobody has to remember which source types count;
* a ``derived-from-data`` (or ``not-found``) record may not carry a
  ``design_statement`` (which is a verbatim document quote) or a
  ``documentation_status`` (which is an observation about a document):
  :meth:`SamplingRecord.from_config` refuses one that does.

Derived QUANTITIES do not belong here at all -- PSU counts, strata counts and
weight dispersion live in ``.coder/sampling/fingerprint.csv``
(``scripts/sampling_fingerprint.py``).  Generating a ``sampling.yml`` from that
screen would launder a screen into a record.

Placement costs no cache
------------------------
``sampling.yml`` is a sibling of ``data_scheme.yml``, never a block in it, for
the reason ``population.yml`` documents: ``.yml`` is not in
``_BUILD_INPUT_SUFFIXES``, so neither ``Wave._input_hash`` nor
``Country._table_cache_hash`` sees the file, while ``data_scheme.yml`` is
hashed whole for every table.  The attach point is ``_finalize_result``, which
is in ``_build_registry._EXCLUDED_CALLABLES``.
"""

from __future__ import annotations

import contextvars
import functools
import json
import os
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from .paths import countries_root
from .yaml_utils import load_yaml

__all__ = [
    "ATTRS_KEY",
    "ATTRS_RESOLUTION_KEY",
    "DESIGN_TAGS",
    "SOURCE_TYPES",
    "DOCUMENTED_SOURCE_TYPES",
    "CONFIDENCE_LEVELS",
    "DOCUMENTATION_STATUSES",
    "OVERSAMPLE_STATUSES",
    "UNRECORDED_TAG",
    "HOUSEHOLD_CONTINUITY",
    "CLUSTER_ID_CONTINUITY",
    "INCOMPARABLE_CLUSTER_IDS",
    "ClusterIdentityWarning",
    "ClusterIdentityError",
    "SamplingRecord",
    "sampling_records",
    "attach",
    "records_for_frame",
    "merge_attrs",
    "records_from_attrs",
    "outermost_call",
    "audit_cluster_identity",
    "check_cluster_identity",
    "sampling_reports",
]

#: ``df.attrs`` key holding ``{country: {wave: record}}``.
ATTRS_KEY = "sampling"
#: ``df.attrs`` key holding ``{country: how-the-waves-were-resolved}``.
ATTRS_RESOLUTION_KEY = "sampling_resolution"

# ---------------------------------------------------------------------------
# The frozen vocabulary (Uganda exemplar, GH #793 ruling 2026-10-18)
# ---------------------------------------------------------------------------

#: Every ``design_tag`` the exemplar uses.  An editorial reading -- no document
#: uses either string -- so it never travels without ``source_type`` and
#: ``confidence``.
DESIGN_TAGS: frozenset[str] = frozenset({
    "two-stage-stratified-cluster",                 # Uganda 2005-06 .. 2011-12
    "two-stage-stratified-cluster-rotating-panel",  # Uganda 2013-14 onward
})

SOURCE_TYPES: frozenset[str] = frozenset({
    "local-documentation",   # a document the repo holds
    "wb-catalog",            # WB catalog / DDI metadata only
    "derived-from-data",     # inferred from sample(); NOT a claim about the design
    "not-found",             # looked, found nothing
})

#: The source types that ARE documentation.  :attr:`SamplingRecord.documented`
#: reads this, so ``derived-from-data`` can never be mistaken for a document.
DOCUMENTED_SOURCE_TYPES: frozenset[str] = frozenset({
    "local-documentation", "wb-catalog",
})

#: How strongly the source states a DESIGN (the exemplar header's legend):
#: ``high`` the document describes the selection procedure; ``medium`` a design
#: mentioned in passing or partially stated; ``low`` inference, or text that
#: should not be read as design.
CONFIDENCE_LEVELS: frozenset[str] = frozenset({"high", "medium", "low"})

#: An observation about the DOCUMENT, not an inference about the design.
#: ``carried-forward`` marks boilerplate repeated from an earlier wave (Uganda
#: 2013-14 onward repeat the 2009-10 "3,123 households over 322 EAs").
DOCUMENTATION_STATUSES: frozenset[str] = frozenset({
    "wave-specific", "carried-forward", "inherited-from-panel-design",
})

#: ``oversamples[].status``.
OVERSAMPLE_STATUSES: frozenset[str] = frozenset({
    "attested", "attested-not-identifiable",
})

#: Sentinel for a wave nobody has curated.  Not a value config may carry --
#: the same distinction ``population.UNRECORDED_TAG`` draws between "nobody
#: has looked" and "looked and found nothing" (``not-found``).
UNRECORDED_TAG = "unrecorded"

# Field NAMES, per block -- exactly the exemplar's.  ``_status`` is the
# exemplar's "absence is written, not omitted" note: a ``null`` value plus a
# string saying what was searched.
_STRATIFICATION_FIELDS: frozenset[str] = frozenset({
    "variable", "basis", "explicit_labels", "implicit",
    "n_stated_in_document", "_status",
})
_SELECTION_FIELDS: frozenset[str] = frozenset({"psu", "ssu", "pps", "_status"})
_OVERSAMPLE_FIELDS: frozenset[str] = frozenset({
    "what", "status", "treatment", "_status",
})
#: ``role`` / ``rotation`` are the exemplar's (GH #793); the rest are the
#: per-boundary continuity declaration of GH #942 (DESIGN.org §3.3, accepted
#: as drafted 2026-10-18): two orthogonal fields plus their own provenance.
_PANEL_FIELDS: frozenset[str] = frozenset({
    "role", "rotation",
    "household_continuity", "cluster_id_continuity", "cluster_id_note",
    "source_type", "confidence", "source_file", "locator",
})

# ---------------------------------------------------------------------------
# GH #942 -- is `v` the same cluster id across a wave boundary?
# ---------------------------------------------------------------------------
#
# The `panel:` block of wave W describes W's relation to the wave that
# PRECEDES it in `Country.waves` -- the unit of declaration is the BOUNDARY,
# not the country (DESIGN Q1: a single per-country tag is wrong for all five
# countries where the question can be answered).  Two fields, because they are
# orthogonal facts (Q2): refresh-by-design is a fact about HOUSEHOLDS,
# renumbering is a fact about CLUSTER IDS, and Tanzania has both at different
# boundaries.

#: Is the household sample of W continuous with the preceding wave's?
HOUSEHOLD_CONTINUITY: frozenset[str] = frozenset({
    "panel",            # the same households, re-interviewed
    "rotating-panel",   # part retained, part replaced
    "refresh",          # no household carried over BY DESIGN (Tanzania 2020-21)
    "independent",      # a separate draw (Ethiopia 2018-19 starts Panel II)
    "unknown",
})

#: Does `v` denote the same cluster across the boundary?
CLUSTER_ID_CONTINUITY: frozenset[str] = frozenset({
    "stable",        # the same EA keeps its id
    "wave-scoped",   # renumbered; a crosswalk is meaningful
    "redefined",     # `v` denotes a different KIND of unit (EA -> parish)
    "mixed",         # some of the boundary's `v` are synthetic, not clusters
    "unobservable",  # no verified linkage; cannot be determined
})

#: The values on which a cross-wave `v` is NOT comparable -- what the
#: Country-level warning fires on.  Deliberately NOT `unobservable`: GhanaLSS
#: 1991-92 onward are genuinely different EAs with no linkage, so grouping on
#: `v` across them is CORRECT, and warning there would teach the reader to
#: ignore the warning (DESIGN §4.2).
INCOMPARABLE_CLUSTER_IDS: frozenset[str] = frozenset({
    "wave-scoped", "redefined", "mixed",
})


# ---------------------------------------------------------------------------
# Immutability, all the way down
# ---------------------------------------------------------------------------
#
# A PopulationRecord holds only strings, so making the top-level mapping
# immutable is enough.  A SamplingRecord holds nested mappings
# (`stratification`, `selection`, `panel`) and lists (`stage_units`,
# `explicit_labels`, `oversamples`), and `sampling_records` is lru_cached: a
# caller who did `rec['stratification']['variable'] = ...` would poison every
# later reader in the process.  So nested values are frozen too -- as `dict` /
# `list` SUBCLASSES, not `MappingProxyType` / `tuple`, for the two measured
# reasons the PopulationRecord docstring gives: `json.dumps` (and therefore
# `df.to_parquet()`) accepts them, and they compare EQUAL to the plain
# dict/list a parquet round-trip hands back (a tuple would not: `(1,) != [1]`),
# which is what pandas propagates `attrs` on.

def _immutable(self, *args: Any, **kwargs: Any):
    raise TypeError(
        f"{type(self).__name__} is immutable: sampling records come from an "
        "lru_cached loader and ride on df.attrs, so a mutation would be "
        "visible to every other reader. Build a new value, or copy it "
        "(dict(...) / list(...))."
    )


class _FrozenDict(dict):
    """A nested mapping inside a :class:`SamplingRecord`."""

    __slots__ = ()
    __setitem__ = __delitem__ = _immutable
    update = setdefault = pop = popitem = clear = __ior__ = _immutable

    def __hash__(self) -> int:
        return hash(json.dumps(self, sort_keys=True, default=str))

    def __reduce__(self):
        return (self.__class__, (dict(self),))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class _FrozenList(list):
    """A nested list inside a :class:`SamplingRecord`."""

    __slots__ = ()
    __setitem__ = __delitem__ = __iadd__ = __imul__ = _immutable
    append = extend = insert = remove = pop = clear = sort = reverse = _immutable

    def __hash__(self) -> int:
        return hash(json.dumps(self, sort_keys=True, default=str))

    def __reduce__(self):
        return (self.__class__, (list(self),))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _FrozenDict({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return _FrozenList(_freeze(v) for v in value)
    return value


class SamplingRecord(dict):
    """How one ``(country, wave)`` cell's sample was drawn.

    A ``dict`` subclass, for every reason :class:`population.PopulationRecord`
    gives (``json.dumps`` accepts it so ``df.to_parquet()`` keeps working;
    equality is by value, which is what pandas propagates ``attrs`` on; both
    ``rec['field']`` and ``rec.field`` work, on the object in ``attrs`` too).
    Two differences, both forced by content:

    * nested values are frozen too (see ``_freeze``), and
    * ``__hash__`` hashes a canonical JSON form -- ``hash(tuple(sorted(
      self.items())))``, population's spelling, raises ``TypeError`` on the
      lists and mappings this record carries.

    Absent optional fields are dropped from the mapping (an explicit ``null``
    in config is the same fact as an absent key, and dropping it keeps
    ``attrs`` small); reading one as an attribute returns ``None``.  Nested
    blocks keep their ``null`` values verbatim -- that is where the exemplar
    writes "absence is written, not omitted" (``selection.psu: null`` beside a
    ``_status`` saying what was searched).
    """

    #: Every top-level field a record may carry -- exactly the exemplar's.
    _FIELDS: tuple[str, ...] = (
        "country", "wave",
        # the design triple: travels together, see from_config
        "design_tag", "source_type", "confidence",
        "survey", "stages", "stages_note", "stage_units", "psu", "frame",
        "stratification", "selection", "take_per_psu", "allocation",
        "panel", "oversamples",
        "source_file", "locator", "design_statement",
        "documentation_status", "notes", "record_source",
    )
    #: The design triple.  ``design_tag`` alone is an editorial reading
    #: presented as a fact; see the module docstring.
    _DESIGN_TRIPLE: tuple[str, ...] = ("design_tag", "source_type", "confidence")
    #: Fields that are claims about a DOCUMENT, and so may not appear on a
    #: record whose source is not one.
    _DOCUMENT_ONLY: tuple[str, ...] = ("design_statement", "documentation_status")

    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        data = dict(*args, **kwargs)
        unknown = sorted(set(data) - set(self._FIELDS))
        if unknown:
            raise TypeError(
                f"SamplingRecord got unexpected field(s) {unknown}; "
                f"known fields are {list(self._FIELDS)}"
            )
        missing = [f for f in ("country", "wave") if data.get(f) is None]
        if missing:
            raise TypeError(
                f"SamplingRecord is missing required field(s) {missing}"
            )
        # ``dict.__init__`` is C-level and does not route through the
        # overridden ``__setitem__``, so this is the one legal write.
        dict.__init__(self, {k: _freeze(data[k]) for k in self._FIELDS
                             if data.get(k) is not None})

    # -- attribute access --------------------------------------------------
    def __getattr__(self, name: str) -> Any:
        if name in self._FIELDS:
            return self.get(name)
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    @property
    def documented(self) -> bool:
        """``True`` only when the design statement came from a document.

        ``derived-from-data`` (inferred from ``sample()``), ``not-found`` and
        the library's ``unrecorded`` placeholder are all ``False`` -- so a
        consumer asking "does a document say this?" cannot get the wrong
        answer by forgetting which source types count.
        """
        return self.get("source_type") in DOCUMENTED_SOURCE_TYPES

    @property
    def cluster_id_continuity(self) -> str | None:
        """``panel.cluster_id_continuity`` -- whether ``v`` denotes the same
        cluster as in the preceding wave (GH #942) -- or ``None``."""
        return (self.get("panel") or {}).get("cluster_id_continuity")

    @property
    def household_continuity(self) -> str | None:
        """``panel.household_continuity`` (GH #942), or ``None``."""
        return (self.get("panel") or {}).get("household_continuity")

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={self[k]!r}" for k in self._FIELDS if k in self)
        return f"SamplingRecord({inner})"

    # -- immutability ------------------------------------------------------
    __setitem__ = _immutable
    __delitem__ = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable
    clear = _immutable
    __ior__ = _immutable

    def __hash__(self) -> int:                     # dict sets this to None
        return hash(json.dumps(self, sort_keys=True, default=str))

    def __reduce__(self):
        """Pickle via ``__init__``, not by replaying ``__setitem__`` (which the
        immutability guard rejects).  See PopulationRecord."""
        return (self.__class__, (dict(self),))

    # pandas deepcopies ``df.attrs`` on EVERY operation that propagates them.
    # An immutable value is its own deep copy, so hand back ``self`` rather
    # than rebuilding a nested record per propagation.
    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    # ---------------------------------------------------------------------
    @classmethod
    def from_config(cls, country: str, wave: str,
                    block: Mapping[str, Any]) -> "SamplingRecord":
        """Build from one ``sampling.yml`` entry, validating everything.

        Raises ``ValueError`` on: a field outside the frozen set (at any
        nesting level); a value outside a controlled vocabulary; a design
        record missing any of ``design_tag`` / ``source_type`` /
        ``confidence``; a non-documentary record carrying a document quote or
        a documentation status; or a value ``json`` cannot serialise (which
        would turn a user's ``df.to_parquet()`` into a ``TypeError``).
        """
        where = f"{country}/{wave}"
        if not isinstance(block, Mapping):
            raise ValueError(f"{where}: a sampling record must be a mapping, "
                             f"got {type(block).__name__}")
        allowed = set(cls._FIELDS) - {"country", "wave"}
        unknown = sorted(set(block) - allowed)
        if unknown:
            raise ValueError(
                f"{where}: unknown sampling field(s) {unknown}. The vocabulary "
                f"is frozen (GH #793); known fields are {sorted(allowed)}"
            )
        if not block:
            raise ValueError(f"{where}: empty sampling record")

        # A PANEL-ONLY entry -- the `panel:` block and nothing else -- is a
        # record too (GH #942 ruling, option A: "land the panel: block now"),
        # so the design triple is required iff any DESIGN field is present.
        # The panel block then carries its own provenance; see _check_panel.
        is_design = bool(set(block) - {"panel"})
        _check_panel(where, block.get("panel"), panel_only=not is_design)
        if not is_design:
            return cls._finish(country, wave, block)

        missing = [k for k in cls._DESIGN_TRIPLE if not block.get(k)]
        if missing:
            raise ValueError(
                f"{where}: sampling record is missing {missing}. design_tag, "
                "source_type and confidence must travel together -- a tag "
                "without its provenance and confidence is an editorial reading "
                "presented as a fact."
            )
        _check_vocab(where, "design_tag", block["design_tag"], DESIGN_TAGS)
        _check_vocab(where, "source_type", block["source_type"], SOURCE_TYPES)
        _check_vocab(where, "confidence", block["confidence"], CONFIDENCE_LEVELS)
        if block.get("documentation_status") is not None:
            _check_vocab(where, "documentation_status",
                         block["documentation_status"], DOCUMENTATION_STATUSES)
        if str(block["source_type"]) not in DOCUMENTED_SOURCE_TYPES:
            claimed = [k for k in cls._DOCUMENT_ONLY if block.get(k) is not None]
            if claimed:
                raise ValueError(
                    f"{where}: source_type={block['source_type']!r} is not "
                    f"documentation, so the record may not carry {claimed} -- "
                    "those are claims about a document. A derived-from-data "
                    "reading must never be readable as documentation."
                )

        _check_nested(where, "stratification", block.get("stratification"),
                      _STRATIFICATION_FIELDS)
        _check_nested(where, "selection", block.get("selection"),
                      _SELECTION_FIELDS)
        oversamples = block.get("oversamples")
        if oversamples is not None:
            if not isinstance(oversamples, list):
                raise ValueError(f"{where}: oversamples must be a list")
            for i, entry in enumerate(oversamples):
                _check_nested(where, f"oversamples[{i}]", entry,
                              _OVERSAMPLE_FIELDS)
                if entry.get("status") is not None:
                    _check_vocab(where, f"oversamples[{i}].status",
                                 entry["status"], OVERSAMPLE_STATUSES)
        return cls._finish(country, wave, block)

    @classmethod
    def _finish(cls, country: str, wave: str,
                block: Mapping[str, Any]) -> "SamplingRecord":
        where = f"{country}/{wave}"
        try:
            json.dumps(dict(block), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{where}: sampling record is not JSON-serialisable ({exc}); "
                "it rides on df.attrs, which df.to_parquet() writes as JSON. "
                "Quote the value in sampling.yml."
            ) from exc
        return cls(country=country, wave=wave, **dict(block))

    @classmethod
    def unrecorded(cls, country: str, wave: str) -> "SamplingRecord":
        """A placeholder for a wave with no entry in config.

        Built by the library, never read from config: ``UNRECORDED_TAG`` is
        rejected by :meth:`from_config`.
        """
        return cls(
            country=country, wave=wave,
            design_tag=UNRECORDED_TAG, source_type="not-swept",
            confidence="none",
            notes="No sampling record for this wave. Nobody has curated it "
                  "(or its `t` label is not a wave label). This is an absence "
                  "of information, not a finding.",
        )

    def to_dict(self) -> dict[str, Any]:
        """Plain-``dict`` copy (nested values stay frozen subclasses)."""
        return dict(self)


def _check_vocab(where: str, field: str, value: Any,
                 allowed: frozenset[str]) -> None:
    if str(value) not in allowed:
        raise ValueError(
            f"{where}: {field}={value!r} is not in the controlled vocabulary "
            f"{sorted(allowed)} (frozen from the Uganda exemplar, GH #793; "
            "widening it is a decision, made in lsms_library/sampling.py)"
        )


def _check_nested(where: str, name: str, block: Any,
                  allowed: frozenset[str]) -> None:
    if block is None:
        return
    if not isinstance(block, Mapping):
        raise ValueError(f"{where}: {name} must be a mapping, "
                         f"got {type(block).__name__}")
    unknown = sorted(set(block) - allowed)
    if unknown:
        raise ValueError(
            f"{where}: unknown field(s) {unknown} in {name}. The vocabulary is "
            f"frozen (GH #793); known fields are {sorted(allowed)}"
        )


def _check_panel(where: str, panel: Any, *, panel_only: bool) -> None:
    """Validate a ``panel:`` block (GH #793 fields + the GH #942 declaration).

    * Field names are frozen, as everywhere.
    * ``household_continuity`` and ``cluster_id_continuity`` come TOGETHER
      (the two orthogonal facts of one boundary; ``unknown`` /
      ``unobservable`` exist so neither has to be omitted).
    * A continuity declaration carries its OWN ``source_type`` and
      ``confidence`` -- the record's design triple describes the design
      statement, not this reading -- and a ``locator`` unless the source is
      ``not-found`` (DESIGN §3.3: "source_type / confidence / locator,
      refused if absent").  A panel-only entry needs its own pair even
      without continuity, since nothing else vouches for it.
    * Every ``cluster_id_continuity`` but ``stable`` needs a
      ``cluster_id_note`` -- "``unobservable`` is a recorded verdict with a
      reason, not an exemption" (DESIGN §4), and the note is what the warning
      quotes.
    """
    if panel is None:
        if panel_only:
            raise ValueError(f"{where}: empty sampling record")
        return
    _check_nested(where, "panel", panel, _PANEL_FIELDS)
    hh = panel.get("household_continuity")
    cl = panel.get("cluster_id_continuity")
    if (hh is None) != (cl is None):
        raise ValueError(
            f"{where}: panel declares only one of household_continuity / "
            "cluster_id_continuity. They are the two orthogonal facts of one "
            "wave boundary (GH #942): declare both, using 'unknown' / "
            "'unobservable' where a fact cannot be determined.")
    if hh is not None:
        _check_vocab(where, "panel.household_continuity", hh,
                     HOUSEHOLD_CONTINUITY)
        _check_vocab(where, "panel.cluster_id_continuity", cl,
                     CLUSTER_ID_CONTINUITY)
    if hh is not None or panel_only:
        missing = [k for k in ("source_type", "confidence") if not panel.get(k)]
        if missing:
            raise ValueError(
                f"{where}: panel block is missing {missing}. A continuity "
                "reading travels with its own provenance and confidence -- "
                "the design triple describes the design statement, not this.")
    for field, allowed in (("source_type", SOURCE_TYPES),
                           ("confidence", CONFIDENCE_LEVELS)):
        if panel.get(field) is not None:
            _check_vocab(where, f"panel.{field}", panel[field], allowed)
    if hh is not None and panel.get("source_type") != "not-found" \
            and not panel.get("locator"):
        raise ValueError(
            f"{where}: a continuity declaration needs a panel.locator (where "
            "the statement or the measurement can be found).")
    if cl is not None and cl != "stable" and not panel.get("cluster_id_note"):
        raise ValueError(
            f"{where}: cluster_id_continuity={cl!r} needs a cluster_id_note "
            "saying why -- it is what the Country-level warning quotes.")


# ---------------------------------------------------------------------------
# config loading
# ---------------------------------------------------------------------------

def _sampling_path(country: str) -> Path:
    """Resolved via ``countries_root()`` so ``LSMS_COUNTRIES_ROOT`` is honoured
    (GH #436)."""
    return Path(countries_root()) / country / "_" / "sampling.yml"


@lru_cache(maxsize=None)
def sampling_records(country: str) -> dict[str, SamplingRecord]:
    """``{wave: SamplingRecord}`` for *country*, or ``{}`` when uncurated.

    Cached; call ``sampling_records.cache_clear()`` after changing
    ``LSMS_COUNTRIES_ROOT`` or editing config.

    Never raises for a missing or unreadable file -- this runs on every read,
    and a metadata annotation must not be able to break a data call.  A
    *malformed* record does raise, because that is a config bug someone must
    fix (``tests/test_sampling.py`` loads every shipped file).
    """
    path = _sampling_path(country)
    if not path.exists():
        return {}
    try:
        data = load_yaml(path) or {}
    except Exception as exc:                       # unreadable / invalid YAML
        warnings.warn(f"could not read {path}: {type(exc).__name__}: {exc}")
        return {}
    blocks = data.get("Sampling") or {}
    return {str(wave): SamplingRecord.from_config(country, str(wave), block)
            for wave, block in blocks.items()}


# ---------------------------------------------------------------------------
# attaching to frames -- population's shape, plus `known_waves` (see
# records_for_frame for why the two differ)
# ---------------------------------------------------------------------------

def _wave_labels(df) -> list[str] | None:
    """The distinct ``t`` values of *df*, or ``None`` when it has no ``t``.

    Population's reader, plus one fast path: on a ``MultiIndex`` the labels
    are read off the level's CODES rather than by materialising the level --
    measured on GhanaLSS ``food_acquired`` (5.3M rows), 0.9 s per call the
    slow way, which this runs on every read of every table of a curated
    country.
    """
    try:
        names = list(df.index.names)
    except AttributeError:
        return None
    if isinstance(df.index, pd.MultiIndex) and "t" in names:
        n = names.index("t")
        codes = np.asarray(df.index.codes[n])
        used = np.unique(codes[codes >= 0])
        return sorted({str(df.index.levels[n][c]) for c in used})
    values = None
    if "t" in names:
        try:
            values = df.index.get_level_values("t")
        except (KeyError, ValueError):
            values = None
    if values is None and hasattr(df, "columns") and "t" in getattr(df, "columns", []):
        values = df["t"]
    if values is None:
        return None
    try:
        return sorted({str(v) for v in values.unique() if v is not None})
    except Exception:
        return None


def records_for_frame(df, country: str, *,
                      known_waves: Iterable[str] | None = None,
                      ) -> tuple[dict[str, SamplingRecord], str]:
    """``({wave: record}, resolution)`` for the waves *df* actually contains.

    ``exact`` with an ``unrecorded`` placeholder for any ``t`` the country did
    not curate, ``all-waves (no t axis)`` for a frame with no ``t``, and ``all-waves
    (no t value matched a wave label)`` when nothing matched -- an over-broad
    answer that announces itself beats a silent empty one.

    *known_waves* (keyword-only; ``Country._finalize_result`` passes
    ``self.waves``) is what "a wave label" means, and it is where this differs
    from :func:`population.records_for_frame`.  Population curates every wave,
    so a ``t`` that matches no record genuinely is not a wave label there.
    Here most countries leave their FIRST wave uncurated by design (it has no
    preceding boundary for ``panel:`` to describe), so judging "matched" by the
    records alone sent a one-wave frame of that first wave down the fallback --
    attaching every OTHER wave's record and calling a real wave label
    unmatched (red team r1, GH #793: ``Country('GhanaLSS').sample(waves=
    ['1987-88'])`` carried 1988-89 .. 2016-17).  With *known_waves*, the
    fallback fires only when no ``t`` is a known wave; otherwise the answer is
    ``exact``, with ``unrecorded`` placeholders.  ``None`` keeps the
    records-only reading.
    """
    recs = sampling_records(country)
    labels = _wave_labels(df)
    if labels is None:
        return dict(recs), "all-waves (no t axis)"
    matched = {t: recs[t] for t in labels if t in recs}
    if known_waves is None:
        is_wave_label = bool(matched)
    else:
        known = {str(w) for w in known_waves}
        is_wave_label = any(t in known for t in labels)
    if not is_wave_label and recs:
        return dict(recs), "all-waves (no t value matched a wave label)"
    for t in labels:
        if t not in matched:
            matched[t] = SamplingRecord.unrecorded(country, t)
    return matched, "exact"


def attach(df, country: str, *,
           known_waves: Iterable[str] | None = None) -> None:
    """Attach *country*'s sampling record to ``df.attrs``, in place.

    ``df.attrs['sampling'] == {country: {wave: SamplingRecord}}`` and
    ``df.attrs['sampling_resolution'] == {country: 'exact'}`` -- identical from
    ``Country(...)`` and ``Feature(...)``.  A country with no ``sampling.yml``
    gets nothing attached: absence is silence, exactly like population's
    ``{}``.  Pass *known_waves* (the country's ``Country.waves``) so a frame of
    an uncurated wave resolves ``exact``; see :func:`records_for_frame`.

    Never raises.  This runs inside ``Country._finalize_result``, on every
    read of every table.
    """
    try:
        if not sampling_records(country):
            return
        recs, how = records_for_frame(df, country, known_waves=known_waves)
        if not recs:
            return
        existing = dict(df.attrs.get(ATTRS_KEY) or {})
        existing[country] = {w: r for w, r in sorted(recs.items())}
        df.attrs[ATTRS_KEY] = existing
        res = dict(df.attrs.get(ATTRS_RESOLUTION_KEY) or {})
        res[country] = how
        df.attrs[ATTRS_RESOLUTION_KEY] = res
    except Exception:                              # never break a data call
        pass


def merge_attrs(target, sources: Iterable[Mapping[str, Any]]) -> None:
    """Merge sampling ``attrs`` from per-country frames onto *target*.

    ``Feature()``'s cross-country ``concat`` drops ``attrs`` whenever its
    inputs disagree, and per-country records disagree by design -- see
    :func:`population.merge_attrs` and ``feature._ATTRS_CARRIERS``, which calls
    this.  A ``merge_attrs`` with no caller is how ``recall`` and
    ``derivations`` silently died until GH #873; this one is registered.
    """
    recs: dict[str, Any] = dict(target.attrs.get(ATTRS_KEY) or {})
    res: dict[str, Any] = dict(target.attrs.get(ATTRS_RESOLUTION_KEY) or {})
    for src in sources:
        if not src:
            continue
        for country, waves in (src.get(ATTRS_KEY) or {}).items():
            recs.setdefault(country, {}).update(waves)
        res.update(src.get(ATTRS_RESOLUTION_KEY) or {})
    if recs:
        target.attrs[ATTRS_KEY] = recs
    if res:
        target.attrs[ATTRS_RESOLUTION_KEY] = res


def records_from_attrs(df) -> list[SamplingRecord]:
    """Typed records from a frame's ``attrs`` -- records, or the plain dicts a
    ``to_parquet`` / ``read_parquet`` round trip hands back."""
    out: list[SamplingRecord] = []
    known = set(SamplingRecord._FIELDS)
    for country, waves in (df.attrs.get(ATTRS_KEY) or {}).items():
        for wave, block in waves.items():
            kwargs = {k: v for k, v in dict(block).items() if k in known}
            kwargs["country"] = country
            kwargs["wave"] = wave
            out.append(SamplingRecord(**kwargs))
    return out


# ---------------------------------------------------------------------------
# GH #942: the Country-level warning
# ---------------------------------------------------------------------------
#
# WHY AT `Country`, NOT AT `Feature`.  `PopulationHeterogeneityWarning` fires at
# `Feature()` because its hazard -- pooling different universes -- needs two
# countries.  This hazard needs two WAVES of ONE country: #942's own
# reproduction is `Country('GhanaLSS').household_characteristics(waves=[
# '1987-88', '1988-89'])`.  Copying population's placement would put the
# warning exactly where the bug is not (DESIGN §4.1; ruling 2026-10-18).
#
# WARN, NEVER FENCE, NEVER REWRITE.  Not one row, index value or column of any
# returned frame changes under any setting; the opt-in strict lever raises
# instead of warning.  No default `droplevel('v')` (a default that silently
# drops data -- what @ligon declined in #603), no crosswalk (out of scope by
# the same ruling), and NO ALLOWLIST: `unobservable` is a recorded verdict
# with a reason, not an exemption.
#
# WHICH BOUNDARIES COUNT.  A frame holding waves S is compared across every
# boundary from min(S) to max(S) in `Country.waves` order -- not only between
# adjacent members of S.  `waves=['1987-88', '1991-92']` crosses GLSS2's
# renumbering even though 1988-89 is not in the frame, and a per-boundary
# record cannot vouch for a pair of waves it does not describe.  The message
# says when a flagged boundary's endpoints are not both in the frame.
#
# ONCE PER CALL.  `_finalize_result` re-enters itself: the v-join
# (`_join_v_from_sample`) fetches `sample()` for ALL waves, and
# `_market_lookup` / `_valuation_geo` / `_location_lookup` fetch other tables.
# Those internal frames are not what the caller asked for, so a warning from
# them would name boundaries outside the request.  `outermost_call` wraps every
# generated table method and tracks the nesting depth in a ContextVar; only a
# `_finalize_result` running at depth 1 (or outside any table method) reports,
# and at most once per `(country, flagged boundaries)` per outermost call --
# which also dedupes the food-derived path (`food_acquired`, then the derived
# table, both finalised inside one `food_expenditures()` call).

_CLUSTER_STRICT_ENV = "LSMS_CLUSTER_STRICT"

#: First sentence of a note, capped -- the full record is on df.attrs.
_NOTE_CHARS = 240


class ClusterIdentityWarning(UserWarning):
    """A frame carries ``v`` across a wave boundary where ``v`` is not the
    same cluster id (GH #942).

    Its own class so callers, tests and CI can target it:
    ``warnings.simplefilter("error", ClusterIdentityWarning)``.  Advisory:
    nothing was dropped or rewritten.
    """


class ClusterIdentityError(Exception):
    """Raised instead of :class:`ClusterIdentityWarning` under
    ``LSMS_CLUSTER_STRICT=1``.

    Deliberately NOT a ``RuntimeError`` / ``ValueError`` / ``KeyError``: the
    derived-table paths in ``Country.__getattr__`` catch exactly those and
    fall back to a legacy build, which would turn a strict refusal into a
    silent second attempt.  Its own lever, not ``LSMS_GRAIN_STRICT`` /
    ``LSMS_READ_STRICT`` / ``LSMS_QUANTITY_STRICT``: a destroyed row, an empty
    column, an impossible value and an incomparable key are four concerns and
    ratchet separately.
    """


def _cluster_strict() -> bool:
    return os.environ.get(_CLUSTER_STRICT_ENV, "").lower() in {"1", "true", "yes"}


# (depth, keys already reported in this outermost call) or None outside any
# generated table method.
_CALL_STATE: contextvars.ContextVar = contextvars.ContextVar(
    "lsms_sampling_call_state", default=None)


def outermost_call(fn):
    """Wrap a generated ``Country`` table method so nested table fetches made
    while serving it are recognised as internal (see the note above)."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        state = _CALL_STATE.get()
        token = _CALL_STATE.set(
            (1, set()) if state is None else (state[0] + 1, state[1]))
        try:
            return fn(*args, **kwargs)
        finally:
            _CALL_STATE.reset(token)
    return wrapper


def _frame_waves(df) -> set[str] | None:
    """Distinct ``t`` values as a set (see :func:`_wave_labels`)."""
    labels = _wave_labels(df)
    return None if labels is None else set(labels)


def _first_sentence(text: Any) -> str:
    if not text:
        return ""
    flat = " ".join(str(text).split())
    cut = flat.find(". ")
    flat = flat if cut < 0 else flat[:cut + 1]
    return flat if len(flat) <= _NOTE_CHARS else flat[:_NOTE_CHARS - 3] + "..."


def audit_cluster_identity(df: Any, *, country: str, table: str,
                           waves: list[str]) -> dict[str, Any] | None:
    """The boundaries a frame's ``v`` crosses where ``v`` is not comparable.

    ``None`` -- the overwhelmingly common case -- when the country has no
    ``sampling.yml``, the frame carries no ``v`` (index level or column), it
    holds fewer than two waves of ``waves`` (the country's ``Country.waves``,
    in order), or no boundary between its first and last wave is declared
    ``wave-scoped`` / ``redefined`` / ``mixed``.
    """
    recs = sampling_records(country)
    if not recs or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    if "v" not in list(df.index.names) and "v" not in df.columns:
        return None
    present = _frame_waves(df)
    if not present or len(present) < 2:
        return None
    order = [str(w) for w in waves]
    pos = sorted(i for i, w in enumerate(order) if w in present)
    if len(pos) < 2:
        return None
    flagged = []
    for j in range(pos[0] + 1, pos[-1] + 1):
        rec = recs.get(order[j])
        if rec is None or rec.cluster_id_continuity not in INCOMPARABLE_CLUSTER_IDS:
            continue
        panel = rec.get("panel") or {}
        flagged.append({
            "from": order[j - 1],
            "to": order[j],
            "cluster_id_continuity": rec.cluster_id_continuity,
            "household_continuity": rec.household_continuity,
            "source_type": panel.get("source_type"),
            "confidence": panel.get("confidence"),
            "locator": panel.get("locator"),
            "note": panel.get("cluster_id_note"),
            "in_frame": order[j - 1] in present and order[j] in present,
        })
    if not flagged:
        return None
    return {
        "country": country,
        "table": table,
        "waves": [order[i] for i in pos],
        "boundaries": flagged,
    }


def _format_cluster_report(report: dict[str, Any]) -> str:
    c = report["country"]
    lines = [
        f"{c}/{report['table']}: `v` is not a comparable cluster id across "
        f"{len(report['boundaries'])} wave boundary(ies) in this frame "
        f"(waves {report['waves'][0]} .. {report['waves'][-1]}):"]
    for b in report["boundaries"]:
        src = b["source_type"]
        how = (f"{src}, confidence {b['confidence']}" if src in
               DOCUMENTED_SOURCE_TYPES else
               f"{src}: MEASURED, not documented; confidence {b['confidence']}")
        where = "" if b["in_frame"] else (
            "; this boundary lies BETWEEN the waves you asked for")
        lines.append(
            f"  {b['from']} -> {b['to']}: cluster_id_continuity="
            f"{b['cluster_id_continuity']} (household_continuity="
            f"{b['household_continuity']}; {how}{where}) -- "
            f"{_first_sentence(b['note'])}")
    lines.append(
        "Rows are returned unchanged -- nothing was dropped or rewritten. "
        "Group on `v` within a wave, or `.droplevel('v')` before a cross-wave "
        "reshape, groupby or join; do not cluster standard errors on `v` "
        "across these waves. Declared in "
        f"{c}/_/sampling.yml (Country({c!r}).sampling[wave]['panel']). "
        f"Set {_CLUSTER_STRICT_ENV}=1 to make this fatal.")
    return "\n".join(lines)


# Reports filed during this process, keyed by (country, table).  A ledger, not
# `attrs`, for the reason GH #323 gives: attrs do not survive every operation.
_CLUSTER_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def check_cluster_identity(df: Any, *, country: str, table: str,
                           waves: list[str]) -> Any:
    """Warn (or, strict, raise) when *df*'s ``v`` crosses an incomparable
    boundary.  Returns *df* unchanged -- always, by contract.

    Called from ``Country._finalize_result``, which is in
    ``_build_registry._EXCLUDED_CALLABLES``; this function and
    :func:`audit_cluster_identity` are excluded too, so a re-worded warning
    never moves a build fingerprint.
    """
    state = _CALL_STATE.get()
    if state is not None and state[0] > 1:
        return df                     # an internal fetch, not the caller's frame
    try:
        report = audit_cluster_identity(df, country=country, table=table,
                                        waves=waves)
    except Exception as exc:                                   # noqa: BLE001
        warnings.warn(
            f"{country}/{table}: the cluster-identity audit could not run "
            f"({type(exc).__name__}: {exc}). The table itself is unaffected, "
            "but `v` is NOT known to be comparable across its waves.",
            ClusterIdentityWarning, stacklevel=2)
        return df
    if report is None:
        return df
    key = (country, tuple((b["from"], b["to"]) for b in report["boundaries"]))
    if state is not None:
        if key in state[1]:
            return df
        state[1].add(key)
    filed = _CLUSTER_LEDGER.setdefault((country, table), [])
    if report not in filed:
        filed.append(report)
    msg = _format_cluster_report(report)
    if _cluster_strict():
        raise ClusterIdentityError(msg)
    warnings.warn(msg, ClusterIdentityWarning, stacklevel=2)
    return df


def sampling_reports(country: str | None = None,
                     table: str | None = None) -> list[dict]:
    """Cluster-identity reports filed during this process (GH #942).

    The twin of ``null_read_reports()`` / ``quantity_reports()``: for tests,
    ``bench/feature_audit``, and anyone who silenced the warning.
    """
    out: list[dict[str, Any]] = []
    for (c, t), reports in _CLUSTER_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


def _clear_sampling_reports() -> None:
    """For tests only -- the ledger is process-wide."""
    _CLUSTER_LEDGER.clear()
