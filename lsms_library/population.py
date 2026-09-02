"""What a survey's sample REPRESENTS -- recorded per ``(country, wave)``.

Two "nationally representative" surveys can represent different populations,
and the library had no vocabulary for saying so.  ``Liberia/2018-19`` is the
National Household Forest Survey -- 32 clusters, 98.8% rural, target population
"forest-proximate communities" -- and it flowed into every cross-country
``Feature()`` call beside 33 general-purpose surveys.  ``Liberia/_/CONTENTS.org``
said so, correctly, in plain English, for months.  It changed nothing:
**prose in a CONTENTS.org is not enforcement.**

Ethiopia is the load-bearing case, because it is a core ISA panel rather than
an oddity.  ESS W1 (2011-12) covered rural areas and small towns -- 503 urban
households.  By 2018-19 the same "wave" of the same panel has 3,655.  A
household fixed-effects model across W1->W5 is pooling a universe that moved
underneath it, and every one of those waves grades ``sane``.  So the universe is
a property of the ``(country, wave)`` cell -- not of the country, and not of a
table.

What this module does, and the one thing it deliberately does NOT do
--------------------------------------------------------------------
1. **Config.**  Reads ``countries/{C}/_/population.yml`` -- the promotion of
   ``slurm_logs/POPULATION_STATEMENTS_2026-07-21.org``, a sweep of all 111 wave
   directories -- into typed, validated :class:`PopulationRecord` objects.
2. **Surface.**  :func:`attach` puts the record on ``df.attrs['population']`` so
   the universe travels with the data.
3. **Warn.**  :func:`pool_report` describes a cross-country pool that mixes
   materially different universes, naming them.

It does **not fence**.  #603 proposed excluding ``specialized`` frames from
``Feature()`` by default; @ligon declined it, and the reason is the one this
whole line of work exists to serve: *a default that silently drops data is the
same disease as a default that silently pools it, with the sign flipped.*  A
warning the user can read beats a filter the user cannot see.  Nothing in this
module removes a row from any result.

The tag is an editorial reading, so it never travels alone
-----------------------------------------------------------
The source document is emphatic, and the emphasis is load-bearing:

    THE TAG IS AN EDITORIAL READING.  IT IS NOT A QUOTE, AND NO DOCUMENT USES IT.

Promoting an editorial reading into config is exactly how a judgement gets
laundered into a fact.  What prevents that is that ``universe_tag`` is stored,
returned and reported **only** alongside ``source_type`` (where the statement
was found) and ``confidence`` (how strongly it is a universe statement at all).
Per the document's own legend, ``confidence: medium`` means "this is a coverage
or representativeness claim, not a universe declaration" and ``confidence: low``
means "this is sample-design text and should not be laundered into a universe".
:class:`PopulationRecord` refuses to exist without all three -- the same
invariant, and for the same reason, as ``capability.audit()``'s refusal to let a
``catalog-only`` record close a coverage cell.

Where the verbatim quotations matter most is ``exclusions``.  Quoting the source
document again: the exclusions "are the point of the exercise: they are what
makes two 'nationally representative' surveys represent different populations."
"""

from __future__ import annotations

import dataclasses
import warnings
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping

from .paths import countries_root
from .yaml_utils import load_yaml

__all__ = [
    "ATTRS_KEY",
    "ATTRS_RESOLUTION_KEY",
    "UNIVERSE_TAGS",
    "SOURCE_TYPES",
    "CONFIDENCE_LEVELS",
    "UNRECORDED_TAG",
    "PopulationRecord",
    "PopulationHeterogeneityWarning",
    "population_records",
    "comparability_class",
    "pool_report",
    "attach",
    "records_for_frame",
    "merge_attrs",
]

#: ``df.attrs`` key holding ``{country: {wave: record-dict}}``.
ATTRS_KEY = "population"
#: ``df.attrs`` key holding ``{country: how-the-waves-were-resolved}``.
ATTRS_RESOLUTION_KEY = "population_resolution"

#: The controlled vocabulary, verbatim from section 2 of the source document.
#: Ten values; the counts across the 111 swept waves are given for orientation
#: and are asserted by ``scripts/promote_population_records.py``.
UNIVERSE_TAGS: frozenset[str] = frozenset({
    "national-all-households",   # 37 -- the documentation NAMES the population
    "national-claimed",          # 28 -- only a coverage/representativeness claim
    "region-excluded",           # 18 -- national minus named regions/districts
    "subnational-area",          # 11 -- a named sub-national area from the outset
    "not-stated",                #  7 -- no population statement exists
    "panel-inherited",           #  5 -- defined only by reference to an earlier wave
    "mixed-national+panel",      #  2 -- one wave dir, two declared universes
    "rural-and-small-town",      #  1 -- Ethiopia ESS1
    "specialized",               #  1 -- Liberia NHFS (forest-proximate EAs)
    "agricultural-households",   #  1 -- Nigeria 2012-13 (per the WB catalog field)
})

SOURCE_TYPES: frozenset[str] = frozenset({
    "local-documentation",   # the statement is in a file this repo ships
    "wb-catalog",            # it exists only in catalog metadata / a catalog PDF
    "not-found",             # no statement of any kind was located
})

CONFIDENCE_LEVELS: frozenset[str] = frozenset({"high", "medium", "low"})

#: Sentinel for a wave the sweep never covered.  **Not** a value any config may
#: carry, and deliberately distinct from ``not-stated``: ``not-stated`` means
#: "we looked and the documentation says nothing", ``unrecorded`` means "nobody
#: has looked".  Collapsing the two would be the coverage matrix's original
#: ``absent`` mistake -- conflating "asked and answered no" with "never asked".
UNRECORDED_TAG = "unrecorded"


class PopulationHeterogeneityWarning(UserWarning):
    """A ``Feature()`` call pooled materially different population universes.

    Advisory only.  Nothing was excluded from the result -- see the module
    docstring on why this library warns instead of fencing.
    """


@dataclass(frozen=True)
class PopulationRecord:
    """What one ``(country, wave)`` cell's documentation says it represents.

    The three fields that must travel together are the first three, and the
    class refuses to be built without them (see :meth:`from_config`).  Every
    ``*_statement`` / ``exclusions`` / ``notes`` string is a verbatim
    transcription of survey documentation, sic spellings included.
    """

    country: str
    wave: str
    universe_tag: str
    source_type: str
    confidence: str
    survey: str | None = None
    language: str | None = None
    source_file: str | None = None
    locator: str | None = None
    population_statement: str | None = None
    exclusions: str | None = None
    translation: str | None = None
    notes: str | None = None
    #: The label the source document used, when it differs from the API wave
    #: label -- Nigeria's post-planting/post-harvest rounds (``2010Q3``,
    #: ``2011Q1``) both derive from the doc's ``2010-11`` row.
    documented_as: str | None = None
    record_source: str | None = None

    @classmethod
    def from_config(cls, country: str, wave: str,
                    block: Mapping[str, Any]) -> "PopulationRecord":
        """Build from one ``population.yml`` entry, validating the triple.

        Raises ``ValueError`` on a record that names a tag without saying where
        it came from or how strongly it is believed -- the laundering this
        module exists to prevent.
        """
        missing = [k for k in ("universe_tag", "source_type", "confidence")
                   if not block.get(k)]
        if missing:
            raise ValueError(
                f"{country}/{wave}: population record is missing {missing}. "
                "universe_tag, source_type and confidence must travel together "
                "-- a tag without its provenance and confidence is an editorial "
                "reading presented as a fact."
            )
        for field, allowed in (("universe_tag", UNIVERSE_TAGS),
                               ("source_type", SOURCE_TYPES),
                               ("confidence", CONFIDENCE_LEVELS)):
            value = str(block[field])
            if value not in allowed:
                raise ValueError(
                    f"{country}/{wave}: {field}={value!r} is not in the "
                    f"controlled vocabulary {sorted(allowed)}"
                )
        known = {f.name for f in dataclasses.fields(cls)}
        kwargs = {k: v for k, v in block.items() if k in known}
        kwargs.pop("country", None)
        kwargs.pop("wave", None)
        return cls(country=country, wave=wave, **kwargs)

    @classmethod
    def unrecorded(cls, country: str, wave: str) -> "PopulationRecord":
        """A placeholder for a wave with no entry in config.

        Built by the library, never read from config: ``UNRECORDED_TAG`` is
        rejected by :meth:`from_config`.
        """
        return cls(
            country=country, wave=wave,
            universe_tag=UNRECORDED_TAG, source_type="not-swept",
            confidence="none",
            notes="No population record for this wave. It was not covered by "
                  "the 2026-07-21 sweep (or its `t` label is not a wave "
                  "label). This is an absence of information, not a finding.",
        )

    def to_dict(self) -> dict[str, Any]:
        """Plain-dict form -- what goes into ``df.attrs``.

        ``attrs`` values are stored as plain types throughout this library so
        that an unknown downstream consumer can serialize a frame's metadata
        without needing to import us.
        """
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}


# ---------------------------------------------------------------------------
# config loading
# ---------------------------------------------------------------------------

def _population_path(country: str) -> Path:
    """Resolved via ``countries_root()`` so ``LSMS_COUNTRIES_ROOT`` is honoured
    (GH #436); never via a hardcoded package-relative path."""
    return Path(countries_root()) / country / "_" / "population.yml"


@lru_cache(maxsize=None)
def population_records(country: str) -> dict[str, PopulationRecord]:
    """``{wave: PopulationRecord}`` for *country*, or ``{}`` when unrecorded.

    Only *active* records are returned.  A record marked ``status: inert`` is a
    row the sweep wrote for a wave the library cannot currently build (an
    unconfigured country, or a duplicate staging directory); it is kept in
    config so the evidence is not lost, and withheld here so it can never be
    mistaken for a live wave.

    Cached; call ``population_records.cache_clear()`` after changing
    ``LSMS_COUNTRIES_ROOT`` or editing config, exactly like ``countries_root``.

    Never raises for a missing or unreadable file: this runs on every read, and
    a metadata annotation must not be able to break a data call.  A *malformed*
    record does raise, because that is a config bug someone must fix.
    """
    path = _population_path(country)
    if not path.exists():
        return {}
    try:
        data = load_yaml(path) or {}
    except Exception as exc:                       # unreadable / invalid YAML
        warnings.warn(f"could not read {path}: {type(exc).__name__}: {exc}")
        return {}
    blocks = data.get("Population") or {}
    out: dict[str, PopulationRecord] = {}
    for wave, block in blocks.items():
        if not isinstance(block, Mapping):
            continue
        if block.get("status") == "inert":
            continue
        out[str(wave)] = PopulationRecord.from_config(country, str(wave), block)
    return out


# ---------------------------------------------------------------------------
# comparability
# ---------------------------------------------------------------------------

# @ligon, 2026-08-22, on the pooling warning:
#
#     "I think we can actually treat 'national-all-households' and
#      'national-claimed' as both 'national'"
#
# The difference between those two tags is **epistemic, not substantive**:
# `national-all-households` means a document NAMES the population, and
# `national-claimed` means only a coverage or representativeness claim exists.
# Both are attempts to measure the same target population, so pooling them is
# not a comparability error.  It is a *provenance* difference -- and provenance
# is precisely what `source_type` and `confidence` already record, per record,
# in `attrs`.
#
# THE COLLAPSE IS A WARN-TIME JUDGEMENT AND NOTHING ELSE.  It must not be
# pushed back into the data model.  The two tags stay distinct in config, in
# `df.attrs`, and in every report: 28 waves are `national-claimed` -- including
# all eight Uganda waves and all four Cote d'Ivoire CILSS waves -- and how well
# documented a universe is remains a real fact about those surveys.  If you are
# reading this because you want to "simplify" the vocabulary to nine tags, this
# paragraph is the reason not to.
_COMPARABILITY_COLLAPSE: dict[str, str] = {
    "national-all-households": "national",
    "national-claimed": "national",
}

#: Human-readable gloss per comparability class, used in the warning text.
_CLASS_GLOSS: dict[str, str] = {
    "national": "national (documented or claimed)",
    "region-excluded": "national minus named regions/districts",
    "subnational-area": "a named sub-national area",
    "rural-and-small-town": "rural areas and small towns only",
    "panel-inherited": "defined only by an earlier wave's sample",
    "mixed-national+panel": "a cross-section and a panel with two universes",
    "specialized": "a purpose-built non-general population",
    "agricultural-households": "agricultural households",
    "not-stated": "UNKNOWN -- swept, and no document states a population",
    UNRECORDED_TAG: "UNKNOWN -- no population record for this wave",
}


def comparability_class(tag: str) -> str:
    """The class *tag* belongs to for the purpose of the pooling warning.

    Identity for every tag except the two national ones, which collapse into
    ``'national'``.  See the long comment above ``_COMPARABILITY_COLLAPSE`` for
    why, and for why that collapse lives here and nowhere else.
    """
    return _COMPARABILITY_COLLAPSE.get(tag, tag)


def _fmt_waves(items: list[tuple[str, str]], limit: int = 6) -> str:
    shown = [f"{c} {w}" for c, w in items[:limit]]
    if len(items) > limit:
        shown.append(f"+{len(items) - limit} more")
    return ", ".join(shown)


#: Classes that record an ABSENCE of information rather than a population.
#: They are reported as UNKNOWN, never counted as "different" -- see
#: :func:`pool_report`.
_UNKNOWN_CLASSES = frozenset({"not-stated", UNRECORDED_TAG})


def pool_report(records: Iterable[PopulationRecord], table_name: str) -> str | None:
    """Describe a pool of universes, or ``None`` when there is nothing to say.

    The report fires when the pool holds **two or more distinct comparability
    classes** (:func:`comparability_class`) -- or one documented class together
    with at least one wave whose universe is *unknown*.  Those two findings are
    different and the message keeps them apart, because only the first is a
    claim that the universes differ.

    Two judgement calls the ``national`` collapse does not settle, both decided
    here and both stated in the message rather than buried.  The firing rates
    quoted are measured over ``.coder/coverage/latest.csv`` (every
    ``(country, feature, wave)`` cell that actually builds).

    * ``region-excluded`` is its **own** class -- it warns against ``national``.
      "National minus Tigray" is not national: Ethiopia 2021-22's own weighting
      sentence puts Tigray outside the *represented* population, and Malawi's
      Likoma exclusion (IHS2/IHS3) then inclusion (IHS4 onward) is, in the
      source document's words, "a real break in the universe ... that a naive
      panel would silently absorb".  That break is precisely the case #603 was
      raised about, and folding it into ``national`` would be the laundering the
      tag legend forbids.  It is also cheap where it would be noise and dear
      where it matters: on all-country calls it changes nothing (they already
      warn via ``subnational-area`` / ``specialized`` -- 31 of 38 features
      either way), while it is the *only* difference in 11.3% of two-country
      pools and in 17 single-country panels, among them the EHCVM group, where
      Mali's exclusion of Kidal is a conflict exclusion and not a rounding
      error.
    * ``not-stated`` (7 waves) and the ``unrecorded`` sentinel **do** trigger,
      but as UNKNOWN, never as *different*: they are an absence of information
      and the message must not claim otherwise.  Treating an undocumented
      universe as equivalent to a documented national one would be silence
      masquerading as knowledge -- which ``provenance.py`` already refuses
      (``local_status='unknown'`` with ``local=False``) and which the coverage
      matrix already refuses (``unsure`` keeps a cell in the queue rather than
      closing it).  Cost, measured: 79 of 489 single-country calls and 13.0% of
      two-country pools fire on this alone; Tanzania is most of it, because
      2020-21 is undocumented while the five other NPS rounds declare a
      national universe.  That is a fact a Tanzania panel user should be told
      once, and Python's default warning filter shows it once per session.
    """
    records = list(records)
    if not records:
        return None

    by_class: dict[str, list[tuple[str, str]]] = {}
    for rec in records:
        by_class.setdefault(comparability_class(rec.universe_tag), []).append(
            (rec.country, rec.wave))
    for items in by_class.values():
        items.sort()

    known = {k: v for k, v in by_class.items() if k not in _UNKNOWN_CLASSES}
    unknown = {k: v for k, v in by_class.items() if k in _UNKNOWN_CLASSES}
    n_unknown = sum(len(v) for v in unknown.values())
    if len(known) < 2 and not (known and unknown):
        return None

    tail = ("Nothing was dropped -- every country you asked for is in the "
            "result; this is a comparability warning, not a filter.")
    if len(known) >= 2:
        head = (f"{table_name}: this result pools {len(known)} materially "
                f"different population universes")
        if n_unknown:
            head += (f", plus {n_unknown} wave(s) whose universe is not "
                     f"documented at all")
        head += f". {tail}"
    else:
        head = (f"{table_name}: {n_unknown} wave(s) in this result have no "
                f"documented population statement, so their comparability with "
                f"the rest cannot be verified -- this is an absence of "
                f"information, not a finding that they differ. {tail}")
    lines = [head]

    for cls in sorted(by_class, key=lambda k: (k in _UNKNOWN_CLASSES,
                                               -len(by_class[k]), k)):
        items = by_class[cls]
        n_countries = len({c for c, _ in items})
        gloss = _CLASS_GLOSS.get(cls, cls)
        lines.append(
            f"  {cls} [{gloss}] -- {len(items)} wave(s), {n_countries} "
            f"country(ies): {_fmt_waves(items)}"
        )

    if "national" in by_class:
        lines.append(
            "  ('national-all-households' and 'national-claimed' are pooled as "
            "one 'national' class here: the difference between them is whether "
            "a document NAMES the population, which is a provenance fact "
            "carried by source_type/confidence, not a different target "
            "population. The two tags stay distinct in df.attrs.)"
        )

    soft = sorted({(r.country, r.wave) for r in records if r.confidence == "low"})
    if soft:
        lines.append(
            "  confidence=low in this pool (the source is sample-design text "
            "and should not be read as a universe): " + _fmt_waves(soft)
        )
    mid = sorted({(r.country, r.wave) for r in records if r.confidence == "medium"})
    if mid:
        lines.append(
            "  confidence=medium (a coverage or representativeness claim, not a "
            "universe declaration): " + _fmt_waves(mid)
        )

    lines.append(
        "  The full per-wave record, with the verbatim population statements "
        "and exclusions, is on df.attrs['population']."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# attaching to frames
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


def records_for_frame(df, country: str) -> tuple[dict[str, PopulationRecord], str]:
    """``({wave: record}, resolution)`` for the waves *df* actually contains.

    ``resolution`` is one of:

    ``exact``
        every ``t`` value matched a wave label (with an ``unrecorded``
        placeholder for any that did not).
    ``all-waves (no t axis)``
        the frame has no ``t`` -- a country-level table -- so every wave the
        country declares is reported.
    ``all-waves (no t value matched a wave label)``
        the frame has ``t`` values but none is a wave label.  The country's full
        record is reported rather than nothing, and this string says so, because
        an over-broad answer that announces itself beats a silent empty one.
    """
    recs = population_records(country)
    labels = _wave_labels(df)
    if labels is None:
        return dict(recs), "all-waves (no t axis)"
    matched = {t: recs[t] for t in labels if t in recs}
    if not matched and recs:
        return dict(recs), "all-waves (no t value matched a wave label)"
    for t in labels:
        if t not in matched:
            matched[t] = PopulationRecord.unrecorded(country, t)
    return matched, "exact"


def attach(df, country: str) -> None:
    """Attach *country*'s population record to ``df.attrs``, in place.

    Shape -- identical from ``Country(...)`` and from ``Feature(...)`` so a
    consumer writes one piece of code::

        df.attrs['population'] == {country: {wave: {...record...}}}
        df.attrs['population_resolution'] == {country: 'exact'}

    Never raises.  This runs inside ``Country._finalize_result``, on every read
    of every table; a metadata annotation that can break a data call is worse
    than no annotation.
    """
    try:
        recs, how = records_for_frame(df, country)
        if not recs:
            return
        existing = dict(df.attrs.get(ATTRS_KEY) or {})
        existing[country] = {w: r.to_dict() for w, r in sorted(recs.items())}
        df.attrs[ATTRS_KEY] = existing
        res = dict(df.attrs.get(ATTRS_RESOLUTION_KEY) or {})
        res[country] = how
        df.attrs[ATTRS_RESOLUTION_KEY] = res
    except Exception:                              # never break a data call
        pass


def merge_attrs(target, sources: Iterable[Mapping[str, Any]]) -> None:
    """Merge population ``attrs`` from per-country frames onto *target*.

    The rule, measured on the pinned pandas 3.0.2: **``attrs`` survive an
    operation only when every input agrees; any disagreement -- including one
    side having none -- yields ``{}``.**  Single-input operations
    (``set_index``, ``rename``, ``dropna``, ``groupby().first()``) are therefore
    safe, and ``merge`` with the *same* ``attrs`` on both sides preserves them.

    Cross-country assembly is a ``concat`` over frames whose population records
    differ **by design** -- one per country -- so it lands in the ``{}`` case
    every time.  That is why ``Feature`` captures each country frame's ``attrs``
    before assembly and re-attaches through here afterwards: not belt-and-braces,
    but the only thing keeping the record alive.  Same family of hazard as the
    ``id_converted`` bug in ``CLAUDE.md``; see the measured table there.
    """
    pop: dict[str, Any] = dict(target.attrs.get(ATTRS_KEY) or {})
    res: dict[str, Any] = dict(target.attrs.get(ATTRS_RESOLUTION_KEY) or {})
    for src in sources:
        if not src:
            continue
        for country, waves in (src.get(ATTRS_KEY) or {}).items():
            pop.setdefault(country, {}).update(waves)
        res.update(src.get(ATTRS_RESOLUTION_KEY) or {})
    if pop:
        target.attrs[ATTRS_KEY] = pop
    if res:
        target.attrs[ATTRS_RESOLUTION_KEY] = res


def records_from_attrs(df) -> list[PopulationRecord]:
    """Rebuild typed records from a frame's ``attrs`` (the inverse of
    :func:`attach`).  Used by the pooling check and by callers who want the
    dataclass rather than the dict."""
    out: list[PopulationRecord] = []
    for country, waves in (df.attrs.get(ATTRS_KEY) or {}).items():
        for wave, block in waves.items():
            known = {f.name for f in dataclasses.fields(PopulationRecord)}
            kwargs = {k: v for k, v in block.items() if k in known}
            kwargs["country"] = country
            kwargs["wave"] = wave
            out.append(PopulationRecord(**kwargs))
    return out
