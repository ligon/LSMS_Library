"""The recall record -- what reference period a food number actually covers.

``food_expenditures`` returns a number whose reference period was, until this
module, recorded nowhere.  Most of the corpus is **7 days asked once**; GhanaLSS
delivers **30 days** of coverage (six 5-day asks), Burkina Faso 2014 **28** (four
quarterly 7-day passages), Guatemala and Panama **15**.  Pooling GhanaLSS with
Ethiopia therefore compares a 30-day figure with a 7-day one, and nothing said
so.  GH #851.

*This module surfaces facts.  It transforms nothing.*  There is deliberately no
``period=`` kwarg, no scaling and no annualisation -- see "What this refuses to
do" below, which is a design position with evidence behind it, not an omission.

Shape and cardinality
---------------------
One record per ``(country, wave)``, stored in ``countries/{C}/_/recall.yml`` --
a sibling of ``population.yml``, read through :func:`paths.countries_root` so
``LSMS_COUNTRIES_ROOT`` is honoured, and *packaged*, so an installed wheel gets
the record too.  (The corpus-wide ``.coder/coverage/food_recall.csv`` is the
evidence base these files are generated from; it resolves via ``_repo_root()``
and so exists only in a source checkout.  It is not what the library reads.)

A ``.yml`` sibling moves no cache hash: ``Country._table_cache_hash`` hashes
``data_scheme.yml`` by name and the ``_/*`` glob admits only
``_BUILD_INPUT_SUFFIXES`` plus ``*.org``.  Measured for ``population.yml``; the
same holds here.

The four facts, and why each is separate
----------------------------------------
``recall_days``
    The length of ONE ask.  Not the exposure.
``n_asks``
    How many times the module is asked -- **asks, not interviewer visits**.
    GLSS7 fields seven visits and asks food at six of them (visit 1 is intake),
    so ``recall_days * n_asks`` = 6 * 5 = 30 days of coverage, where 7 * 5 = 35
    is merely the fieldwork cycle.  Getting this wrong makes the product column
    mean two different things in one country.
``within_wave_variation``
    The axis the window varies on *inside* one wave, when it does.  Nine cells
    across five countries have no single window: GhanaLSS 1991-92 varies by
    stratum (urban 3-day x 10 asks, rural 2-day x 7), GhanaLSS 2005-06 and
    Nigeria W4 by acquisition source ``s``, Burkina Faso 2021-22 across two
    unmarked periods, Guatemala and Panama by column.  Such a record carries the
    AXIS and leaves the numbers null -- filling either would be the error the
    record exists to flag.
``basis``
    The evidence rung, and the reason this is not just a number.  Same ladder as
    :mod:`lsms_library.capability`, for the same reason: a weak source must not
    be laundered into a fact.  ``questionnaire`` > ``variable-label`` >
    ``config-comment`` > ``filename`` > ``not-recorded``.  Only 8 of 34 stated
    windows in this corpus are questionnaire-grade.

``ask_count_basis`` records the other thing @ligon asked to be legible: whether
``n_asks`` was **measured** from the delivered data or is the schedule's
**declared** claim.  Where the survey published per-visit structure the count is
observable; where it did not -- which is most of the corpus -- we are relying on
what the manual says, and the consumer should be able to see which they have.

What this refuses to do, and why
--------------------------------
No ``period=``, no rate, no annualisation.  The obvious move -- divide by the
elapsed days -- is **refuted on this corpus**: measured within household on
GhanaLSS 1998-99, doubling the realised interval from 5 to 10 days moves reported
spend by +1.2% where proportionality predicts +100% (elasticity 0.018; item
counts identical at 18.8).  Reported spend does not respond to interval length,
so dividing by it does not normalise -- it injects fieldwork tempo.  Bin that
wave by measured exposure and total spend is flat while the implied daily rate
spans 2.7x, entirely from the denominator.

Scott and Amenuvegbe (1990) predicted the mechanism from an experiment
commissioned *for* the GLSS: beyond a threshold recall length respondents switch
from factual to *normative* reporting -- "what I usually buy" rather than "what I
bought".  See ``GhanaLSS/_/CONTENTS.org`` for the measurements and the
literature.  Any future rate must also carry an ask-order correction: the first
ask runs 1.84x (unaided) / 1.17x (diary-assisted) at *identical* interval
lengths.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from .paths import countries_root
from .yaml_utils import load_yaml

__all__ = [
    "RecallRecord", "recall_records", "records_for_frame", "attach",
    "BASIS_LADDER", "VARIATION_AXES", "ASK_COUNT_BASES",
]

#: Evidence rungs, strongest first.  Mirrors :mod:`lsms_library.capability`.
BASIS_LADDER = (
    "questionnaire",    # the instrument states it, quoted or transcribed
    "variable-label",   # a Stata variable label states it
    "config-comment",   # a maintainer asserted it in repo config or prose
    "filename",         # inferred from a source filename -- a GUESS
    "not-recorded",     # nobody has looked
)

#: Rungs that do NOT establish a window.  A record on one of these may still
#: carry numbers (someone's assertion), but a consumer is entitled to discount
#: them; ``not-recorded`` carries none by construction.
WEAK_BASES = frozenset({"filename", "not-recorded"})

#: The axis a window varies on *inside* one wave.
VARIATION_AXES = ("stratum", "by-s", "by-column", "by-item", "two-periods")

#: Whether ``n_asks`` was observed in the data or taken from the schedule.
ASK_COUNT_BASES = ("measured", "declared", "unknown")


@dataclass(frozen=True)
class RecallRecord:
    """What one ``(country, wave)`` food module's reference period is.

    Immutable: returned from an ``lru_cache``d loader, so a mutated record would
    leak into every later caller.  Same discipline as ``PopulationRecord``.
    """

    country: str
    wave: str
    recall_days: float | None = None
    n_asks: int | None = None
    ask_count_basis: str = "unknown"
    basis: str = "not-recorded"
    recall_verbatim: str | None = None
    evidence: str | None = None
    within_wave_variation: str | None = None
    n_items_delivered: int | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        if self.basis not in BASIS_LADDER:
            raise ValueError(
                f"{self.country} {self.wave}: basis {self.basis!r} is not one of "
                f"{BASIS_LADDER}.  The rung is what stops a filename guess being "
                f"read as a questionnaire fact; it may not be invented.")
        if self.ask_count_basis not in ASK_COUNT_BASES:
            raise ValueError(
                f"{self.country} {self.wave}: ask_count_basis "
                f"{self.ask_count_basis!r} not in {ASK_COUNT_BASES}")
        if (self.within_wave_variation is not None
                and self.within_wave_variation not in VARIATION_AXES):
            raise ValueError(
                f"{self.country} {self.wave}: within_wave_variation "
                f"{self.within_wave_variation!r} not in {VARIATION_AXES}")
        # A cell whose window varies inside the wave has no single window.
        # Carrying one would be exactly the error the axis exists to flag.
        if self.within_wave_variation and self.recall_days is not None:
            raise ValueError(
                f"{self.country} {self.wave}: within_wave_variation="
                f"{self.within_wave_variation!r} AND recall_days="
                f"{self.recall_days!r}.  A cell that varies inside the wave "
                f"must leave the number null and carry the axis instead.")
        # A rung that establishes nothing may not also assert a length.
        if self.basis == "not-recorded" and self.recall_days is not None:
            raise ValueError(
                f"{self.country} {self.wave}: basis='not-recorded' with "
                f"recall_days={self.recall_days!r}.  'Nobody has looked' and "
                f"'the window is N days' cannot both be true.")

    @property
    def total_exposure_days(self) -> float | None:
        """``recall_days * n_asks``, or None where either is unknown.

        Deliberately a property rather than a stored field: it is arithmetic on
        the other two, and storing it invites the three drifting apart.
        """
        if self.recall_days is None or self.n_asks is None:
            return None
        return self.recall_days * self.n_asks

    @property
    def is_recorded(self) -> bool:
        """True when *someone has looked*, whatever they concluded.

        Distinguishes "we know there is no statement" from "nobody has checked",
        the distinction whose absence produced the Albania unevidenced-absence
        claim (``docs/guide/coverage.md``).
        """
        return self.basis != "not-recorded"

    @property
    def is_strong(self) -> bool:
        """True when the window rests on more than an assertion or a guess."""
        return self.basis not in WEAK_BASES

    @classmethod
    def from_config(cls, country: str, wave: str, data: dict[str, Any]
                    ) -> "RecallRecord":
        def _num(key, cast):
            v = data.get(key)
            if v is None or v == "":
                return None
            return cast(v)

        return cls(
            country=country,
            wave=wave,
            recall_days=_num("recall_days", float),
            n_asks=_num("n_asks", int),
            ask_count_basis=data.get("ask_count_basis") or "unknown",
            basis=data.get("basis") or "not-recorded",
            recall_verbatim=data.get("recall_verbatim") or None,
            evidence=data.get("evidence") or None,
            within_wave_variation=data.get("within_wave_variation") or None,
            n_items_delivered=_num("n_items_delivered", int),
            notes=data.get("notes") or None,
        )


def recall_yml_path(country: str) -> Path:
    """Resolved via ``countries_root()`` so ``LSMS_COUNTRIES_ROOT`` is honoured."""
    return Path(countries_root()) / country / "_" / "recall.yml"


@lru_cache(maxsize=None)
def recall_records(country: str) -> dict[str, RecallRecord]:
    """``{wave: RecallRecord}`` for one country; empty when none is recorded.

    Cached, so records are immutable (see :class:`RecallRecord`).  Clear with
    ``recall_records.cache_clear()`` after editing config in a session.
    """
    path = recall_yml_path(country)
    if not path.exists():
        return {}
    data = load_yaml(path) or {}
    block = data.get("Recall") or {}
    out: dict[str, RecallRecord] = {}
    for wave, entry in block.items():
        if not isinstance(entry, dict):
            continue
        out[str(wave)] = RecallRecord.from_config(country, str(wave), entry)
    return out


def records_for_frame(df, country: str) -> dict[str, RecallRecord]:
    """The records covering the waves actually present in ``df``.

    Keyed on the frame's ``t`` level, like ``population.records_for_frame`` --
    a frame sliced to one wave should not advertise the whole country's records.
    """
    records = recall_records(country)
    if not records:
        return {}
    names = list(getattr(df.index, "names", []) or [])
    if "t" not in names:
        return dict(records)
    waves = {str(v) for v in df.index.get_level_values("t").unique()}
    return {w: r for w, r in records.items() if w in waves}


def attach(df, country: str) -> None:
    """Attach ``df.attrs['recall'] = {country: {wave: RecallRecord}}``.

    Never raises.  A metadata annotation that can break a data call is worse
    than no annotation -- the rule ``population.attach`` already follows, and
    the reason this is a config read rather than a join against
    ``interview_date`` (which recurses, and which four food countries do not
    have at all).
    """
    try:
        records = records_for_frame(df, country)
        if not records:
            return
        existing = df.attrs.get("recall")
        merged = dict(existing) if isinstance(existing, dict) else {}
        merged[country] = records
        df.attrs["recall"] = merged
    except Exception:                       # pragma: no cover - never fatal
        pass


def merge_attrs(frames) -> dict[str, dict[str, RecallRecord]]:
    """Union the ``recall`` attrs of several frames, for ``Feature()`` assembly.

    ``attrs`` survive an operation only when every input agrees, so a
    cross-country ``concat`` -- whose inputs differ *by design*, one record per
    country -- always lands in the ``{}`` case.  The re-attach is therefore
    load-bearing, not decorative (``CLAUDE.md`` §"Panel ID Transitive Chains").
    """
    out: dict[str, dict[str, RecallRecord]] = {}
    for f in frames:
        got = getattr(f, "attrs", {}).get("recall")
        if isinstance(got, dict):
            for c, recs in got.items():
                out.setdefault(c, {}).update(recs)
    return out
