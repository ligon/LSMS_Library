"""Coping strategies in ``shocks``: reported positions and the unordered set (GH #1562).

Two rulings define the contract (``isa-decisions.org``):

* 2026-11-12, "Coping-strategy contract": ``HowCoped0..`` are the household's
  REPORTED order, filled only where the instrument records one; every shocks
  country also gets an UNORDERED set of chosen strategies.
* 2026-11-13, "Coping-strategy implementation": the set is ``HowCopedSet``,
  the distinct labels, sorted, joined with ``derivations.MULTI_SEP`` (``+``);
  a slot filler or missing code is served NA in its position and is never a
  member; a "did nothing" answer is a member only when it is the only answer
  (alone, the set is ``{did nothing}``, distinct from NA = no answer), and
  beside a real strategy it is dropped from the set while the positions keep
  what was recorded.  The set is written at BUILD time where positions cannot
  carry it (EHCVM 2018-19, Nigeria 2018-19) and derived at READ time from the
  positions everywhere else.

This module is the READ-time half.  :func:`normalise_coping` runs in
``Country._finalize_result`` on every ``shocks`` read, warm cache included,
and applies the two label rules to EVERY set, whether the build wrote it or
it is derived here -- so a rule lives in one place and a build-written set
(Mali 2018-19's code 26 beside other options, positions NA) cannot escape it.
It is read-path code: ``_finalize_result`` and every callable here are in
``_build_registry._EXCLUDED_CALLABLES``, so an edit moves no cache hash, and
the parquet keeps what the build wrote (as it keeps raw ``Relationship``
strings for kinship).  Build code must never call into this module, or build
bytes would depend on code no fingerprint sees; the build-side joiner is
``ehcvm.coping_set_from_grid``.

The label classes are DATA, per country: ``countries/<C>/_/coping.yml``, a
sibling of ``population.yml`` (a ``.yml`` there is no build input).  Labels
are matched EXACTLY on the served string, per wave where an entry says so;
every entry carries its evidence.  A country with no file gets the set and no
class rules.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import yaml

from .derivations import MULTI_SEP
from .paths import countries_root

__all__ = [
    "SET_COLUMN", "REGISTRY_FILE", "CLASSES", "CopingLabel", "CopingLabelError",
    "position_columns", "label_classes", "classes_for_wave", "alone_first_for_wave",
    "coping_set", "normalise_coping", "ALONE_FIRST",
]

#: The unordered set column.
SET_COLUMN = "HowCopedSet"
#: The per-country registry file, under ``countries/<C>/_/``.
REGISTRY_FILE = "coping.yml"
#: The label classes a registry may declare.  The third is SLOT-AWARE (ruling
#: 2026-11-13, "Coping follow-ups", "Did nothing when alone"): the label is a
#: "did nothing" answer when it is the FIRST position and nothing else is
#: recorded (every later position empty, a filler, or the same label), and is
#: then served as ``served_as`` -- one of the country's ``did_nothing`` labels --
#: in that position and in the set; anywhere else it falls through to the other
#: classes (Uganda's code 40 "Nothing else" is also a ``filler``).
CLASSES = ("filler", "did_nothing", "did_nothing_when_alone_first")
ALONE_FIRST = "did_nothing_when_alone_first"

_POSITION = re.compile(r"^HowCoped(\d+)$")
_ENTRY_FIELDS = frozenset({"label", "waves", "evidence", "served_as"})


class CopingLabelError(Exception):
    """A coping label the set cannot represent, or a malformed ``coping.yml``.

    Not a ``ValueError``/``KeyError``/``RuntimeError``: a refusal must not read
    as a failed build anywhere a handler catches those (CLAUDE.md, "A Failure
    Must Not Read as Absence").
    """


@dataclass(frozen=True)
class CopingLabel:
    """One registry entry: an exact served label, its class and its scope."""

    label: str
    klass: str
    waves: frozenset | None  # None: every wave
    evidence: str
    served_as: str | None = None  # ALONE_FIRST only: the did_nothing label served

    def applies_to(self, wave: Any) -> bool:
        return self.waves is None or (wave is not None and str(wave) in self.waves)


def position_columns(columns: Iterable[Any]) -> list[str]:
    """The ``HowCoped<n>`` columns present, in ``n`` order (any ``n``, not just 0..2)."""
    found = []
    for col in columns:
        m = _POSITION.match(str(col))
        if m:
            found.append((int(m.group(1)), col))
    return [col for _, col in sorted(found)]


def _parse_registry(data: Any, path: Path) -> tuple[CopingLabel, ...]:
    if data is None:
        return ()
    if not isinstance(data, dict):
        raise CopingLabelError(f"{path}: the top level must be a mapping of "
                               f"{' / '.join(CLASSES)} to lists")
    unknown = set(data) - set(CLASSES)
    if unknown:
        raise CopingLabelError(f"{path}: unknown key(s) {sorted(unknown)}; "
                               f"only {list(CLASSES)} are allowed")
    out = []
    for klass in CLASSES:
        entries = data.get(klass) or []
        if not isinstance(entries, list):
            raise CopingLabelError(f"{path}: {klass} must be a list of entries")
        for entry in entries:
            if not isinstance(entry, dict):
                raise CopingLabelError(f"{path}: {klass} entry {entry!r} is not a mapping")
            extra = set(entry) - _ENTRY_FIELDS
            if extra:
                raise CopingLabelError(f"{path}: {klass} entry {entry!r} has unknown "
                                       f"field(s) {sorted(extra)}")
            label = entry.get("label")
            if not isinstance(label, str) or not label:
                raise CopingLabelError(f"{path}: {klass} entry {entry!r} needs a "
                                       f"non-empty string label")
            evidence = entry.get("evidence")
            if not isinstance(evidence, str) or not evidence.strip():
                raise CopingLabelError(f"{path}: {klass} entry {label!r} has no evidence; "
                                       f"a label class is a claim and must say how it "
                                       f"was established")
            if MULTI_SEP in label:
                raise CopingLabelError(f"{path}: {klass} label {label!r} contains "
                                       f"{MULTI_SEP!r}, the set separator")
            waves = entry.get("waves")
            if waves is not None:
                if (not isinstance(waves, list) or not waves
                        or not all(isinstance(w, str) and w for w in waves)):
                    raise CopingLabelError(f"{path}: {klass} entry {label!r}: waves must "
                                           f"be a non-empty list of wave strings")
                waves = frozenset(waves)
            served_as = entry.get("served_as")
            if klass == ALONE_FIRST:
                if not isinstance(served_as, str) or not served_as or MULTI_SEP in served_as:
                    raise CopingLabelError(f"{path}: {klass} entry {label!r} needs a "
                                           f"served_as label (without {MULTI_SEP!r})")
            elif served_as is not None:
                raise CopingLabelError(f"{path}: served_as is only for {ALONE_FIRST} "
                                       f"entries, not {klass} {label!r}")
            out.append(CopingLabel(label, klass, waves, evidence.strip(), served_as))
    # An ALONE_FIRST entry serves one of the country's did_nothing labels, and
    # that label must apply wherever the entry does, or the "did nothing" rule
    # would not reach what it serves.
    for rec in out:
        if rec.klass != ALONE_FIRST:
            continue
        covering = [d for d in out if d.klass == "did_nothing" and d.label == rec.served_as
                    and (d.waves is None or (rec.waves is not None and rec.waves <= d.waves))]
        if not covering:
            raise CopingLabelError(f"{path}: {ALONE_FIRST} {rec.label!r} serves "
                                   f"{rec.served_as!r}, which is not a did_nothing label "
                                   f"for every wave the entry names")
    # One label in two classes for an overlapping scope is a contradiction --
    # except filler + ALONE_FIRST, which is the slot-aware class's purpose.
    by_label: dict[str, list[CopingLabel]] = {}
    for rec in out:
        by_label.setdefault(rec.label, []).append(rec)
    for label, recs in by_label.items():
        for a in recs:
            for b in recs:
                if a is b or a.klass == b.klass:
                    continue
                if {a.klass, b.klass} == {"filler", ALONE_FIRST}:
                    continue
                if a.waves is None or b.waves is None or a.waves & b.waves:
                    raise CopingLabelError(f"{path}: {label!r} is both {a.klass} and "
                                           f"{b.klass} for an overlapping set of waves")
    return tuple(out)


@lru_cache(maxsize=None)
def _registry(country: str, root: str) -> tuple[CopingLabel, ...]:
    path = Path(root) / country / "_" / REGISTRY_FILE
    if not path.exists():
        return ()
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return _parse_registry(data, path)


def label_classes(country: str) -> tuple[CopingLabel, ...]:
    """Every registry entry for ``country`` (empty when it has no ``coping.yml``)."""
    return _registry(country, str(countries_root()))


def classes_for_wave(country: str, wave: Any) -> tuple[frozenset, frozenset]:
    """``(fillers, did_nothing)``: the exact labels of each class that apply to ``wave``."""
    filler, none = set(), set()
    for rec in label_classes(country):
        if rec.applies_to(wave) and rec.klass in ("filler", "did_nothing"):
            (filler if rec.klass == "filler" else none).add(rec.label)
    return frozenset(filler), frozenset(none)


def alone_first_for_wave(country: str, wave: Any) -> dict[str, str]:
    """``{label: served_as}`` of the slot-aware class that applies to ``wave``."""
    return {rec.label: rec.served_as for rec in label_classes(country)
            if rec.klass == ALONE_FIRST and rec.applies_to(wave)}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def coping_set(labels: Iterable[Any], *, filler: frozenset = frozenset(),
               did_nothing: frozenset = frozenset()) -> Any:
    """The served ``HowCopedSet`` value for one row's labels (``pd.NA`` if none).

    Fillers and missing codes never enter; repeats collapse (Uganda's repeated
    96 "Other (specify)" is one member); a "did nothing" label survives only
    when nothing else does.  Sorted, joined with ``MULTI_SEP``.
    """
    members = set()
    for value in labels:
        if _is_missing(value):
            continue
        label = str(value)
        if label in filler:
            continue
        if MULTI_SEP in label:
            raise CopingLabelError(
                f"coping label {label!r} contains {MULTI_SEP!r}, the HowCopedSet "
                f"separator; the set cannot represent it")
        members.add(label)
    if not members:
        return pd.NA
    real = members - did_nothing
    return MULTI_SEP.join(sorted(real or members))


def _wave_of_rows(df: pd.DataFrame) -> list[Any]:
    if "t" in (df.index.names or []):
        return list(df.index.get_level_values("t"))
    if "t" in df.columns:
        return list(df["t"])
    return [None] * len(df)


def normalise_coping(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """Apply the GH #1562 read-time rules to a ``shocks`` frame.

    0. A slot-aware label (``did_nothing_when_alone_first``) that is the FIRST
       position with nothing else recorded is served as its ``served_as``
       "did nothing" label in that position (so position and set agree, as for
       every lone "did nothing" answer); decided on the recorded positions,
       before step 1.  Elsewhere it is whatever its other classes say.
    1. A filler or missing code (per ``coping.yml``, per wave) is set NA in
       every ``HowCoped<n>`` position.
    2. ``HowCopedSet`` is the build's own set where the build wrote one for the
       row, else the positions (after 1); in either case the class rules of
       :func:`coping_set` are applied, so the rule is the same for both.

    Returns a new frame (``attrs`` carried); rows, index and every other column
    are untouched.  A row the rules leave with no non-NA content is kept, not
    dropped: this runs after ``_finalize_result``'s ``dropna(how='all')`` on
    purpose, because no ruling authorises deleting a recorded shock.
    Idempotent: a second pass returns the same frame.
    """
    positions = position_columns(df.columns)
    if not positions and SET_COLUMN not in df.columns:
        return df
    attrs = dict(df.attrs)
    out = df.copy()
    waves = _wave_of_rows(out)
    classes = {w: classes_for_wave(country, w) for w in dict.fromkeys(waves)}
    alone = {w: alone_first_for_wave(country, w) for w in classes}

    lone: list[Any] = [None] * len(out)
    if positions and any(alone.values()):
        first = out[positions[0]].tolist()
        rest = [out[col].tolist() for col in positions[1:]]
        for i, w in enumerate(waves):
            value = first[i]
            if _is_missing(value) or str(value) not in alone[w]:
                continue
            filler = classes[w][0]
            if all(_is_missing(r[i]) or str(r[i]) == str(value) or str(r[i]) in filler
                   for r in rest):
                lone[i] = alone[w][str(value)]

    for col in positions:
        filler_rows = [v is not None and not _is_missing(v) and str(v) in classes[w][0]
                       for w, v in zip(waves, out[col].tolist())]
        if any(filler_rows):
            if isinstance(out[col].dtype, pd.CategoricalDtype):
                out[col] = out[col].astype(object)
            out.loc[pd.Series(filler_rows, index=out.index).to_numpy(), col] = pd.NA

    is_lone = [x is not None for x in lone]
    if any(is_lone):
        col = positions[0]
        if isinstance(out[col].dtype, pd.CategoricalDtype):
            out[col] = out[col].astype(object)
        for k in positions[1:]:   # anything left there was the same label: not a strategy
            if isinstance(out[k].dtype, pd.CategoricalDtype):
                out[k] = out[k].astype(object)
            out.loc[pd.Series(is_lone, index=out.index).to_numpy(), k] = pd.NA
        out.loc[pd.Series(is_lone, index=out.index).to_numpy(), col] = [
            x for x in lone if x is not None]

    pos_values = [out[col].tolist() for col in positions]
    built = out[SET_COLUMN].tolist() if SET_COLUMN in out.columns else [None] * len(out)
    served = []
    for i, w in enumerate(waves):
        filler, none = classes[w]
        if not _is_missing(built[i]):
            labels = str(built[i]).split(MULTI_SEP)
        else:
            labels = [values[i] for values in pos_values]
        served.append(coping_set(labels, filler=filler, did_nothing=none))
    out[SET_COLUMN] = pd.array(served, dtype=pd.StringDtype())
    out.attrs = attrs
    return out
