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

import json
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping

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
    "SamplingRecord",
    "sampling_records",
    "attach",
    "records_for_frame",
    "merge_attrs",
    "records_from_attrs",
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
_PANEL_FIELDS: frozenset[str] = frozenset({"role", "rotation"})


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
        _check_nested(where, "panel", block.get("panel"), _PANEL_FIELDS)
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
# attaching to frames -- population's shape, verbatim
# ---------------------------------------------------------------------------

def _wave_labels(df) -> list[str] | None:
    """The distinct ``t`` values of *df*, or ``None`` when it has no ``t``."""
    try:
        names = list(df.index.names)
    except AttributeError:
        return None
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


def records_for_frame(df, country: str) -> tuple[dict[str, SamplingRecord], str]:
    """``({wave: record}, resolution)`` for the waves *df* actually contains.

    Same contract as :func:`population.records_for_frame`: ``exact`` with an
    ``unrecorded`` placeholder for any ``t`` the country did not curate,
    ``all-waves (no t axis)`` for a frame with no ``t``, and ``all-waves (no t
    value matched a wave label)`` when nothing matched -- an over-broad answer
    that announces itself beats a silent empty one.
    """
    recs = sampling_records(country)
    labels = _wave_labels(df)
    if labels is None:
        return dict(recs), "all-waves (no t axis)"
    matched = {t: recs[t] for t in labels if t in recs}
    if not matched and recs:
        return dict(recs), "all-waves (no t value matched a wave label)"
    for t in labels:
        if t not in matched:
            matched[t] = SamplingRecord.unrecorded(country, t)
    return matched, "exact"


def attach(df, country: str) -> None:
    """Attach *country*'s sampling record to ``df.attrs``, in place.

    ``df.attrs['sampling'] == {country: {wave: SamplingRecord}}`` and
    ``df.attrs['sampling_resolution'] == {country: 'exact'}`` -- identical from
    ``Country(...)`` and ``Feature(...)``.  A country with no ``sampling.yml``
    gets nothing attached: absence is silence, exactly like population's
    ``{}``.

    Never raises.  This runs inside ``Country._finalize_result``, on every
    read of every table.
    """
    try:
        if not sampling_records(country):
            return
        recs, how = records_for_frame(df, country)
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
