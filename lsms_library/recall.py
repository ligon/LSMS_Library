"""The recall record -- what reference period a food number actually covers.

``food_expenditures`` returns a number whose reference period was, until this
module, recorded nowhere.  Most of the corpus is **7 days asked once**; GhanaLSS
delivers **30 days** of coverage (six 5-day asks), Burkina Faso 2014 **28** (four
quarterly 7-day passages), Guatemala and Panama **15**.  Pooling GhanaLSS with
Ethiopia therefore compares a 30-day figure with a 7-day one, and nothing said
so.  GH #851.

*This module surfaces facts.*  Phase I transformed nothing.  Phase II (GH #851,
ruling 2026-10-18) adds exactly one conversion, :func:`to_period`, behind
``food_expenditures(period=...)`` / ``food_quantities(period=...)``: it divides
by the instrument's DECLARED exposure, a constant per wave, and refuses any
wave whose window is not established.  It is still not the division "What this
refuses to do" below refutes -- that one divides by the REALISED interval.

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
No rate computed from the survey's own dates.  (``period=`` divides by the
declared ``recall_days * n_asks`` instead -- see :func:`to_period`; the
paragraphs below are why it must never be "improved" to use measured days.)
The obvious move -- divide by the
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

from functools import lru_cache
from pathlib import Path
from typing import Any

from .paths import countries_root
from .yaml_utils import load_yaml

__all__ = [
    "ATTRS_KEY",
    "RecallRecord", "recall_records", "records_for_frame", "attach",
    "merge_attrs",
    "BASIS_LADDER", "VARIATION_AXES", "ASK_COUNT_BASES",
    # Phase II (GH #851): the one conversion the record licenses.
    "PERIOD_ATTRS_KEY", "PERIOD_DAYS", "PERIOD_TABLES",
    "period_days", "period_refusal", "unavailable_reason", "to_period",
    "merge_period_attrs",
]

#: ``df.attrs`` key holding ``{country: {wave: RecallRecord}}``.
ATTRS_KEY = "recall"

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


class RecallRecord(dict):
    """What one ``(country, wave)`` food module's reference period is.

    Immutable: returned from an ``lru_cache``d loader, so a mutated record would
    leak into every later caller.  Same discipline as ``PopulationRecord``.

    Why this is a ``dict`` subclass and not a ``@dataclass(frozen=True)``
    ---------------------------------------------------------------------
    It was a frozen dataclass, and :func:`attach` put it on ``df.attrs``.  That
    broke every user's ``df.to_parquet()`` on an API frame -- 17 countries'
    ``housing()`` results, measured -- because ``pandas.io.parquet`` serialises
    ``df.attrs`` with a bare ``json.dumps`` (pandas 3.0.2, ``io/parquet.py:194``)
    and a dataclass is not JSON.  ``PopulationRecord`` hit the identical wall
    and its docstring states the two constraints a record in ``attrs`` must
    satisfy; this class ports that solution rather than re-deriving it:

    * ``json.dumps`` accepts a ``dict`` subclass, so ``to_parquet`` works;
    * equality is ``dict`` equality, **by value** -- load-bearing, because
      pandas propagates ``attrs`` only when every input compares equal
      (``core/generic.py``: ``obj.attrs == attrs``; CLAUDE.md §"Panel ID
      Transitive Chains").  A record comparing by identity would silently drop
      ``attrs`` on merges that preserve them today.  It also compares equal to
      the plain dict a parquet round-trip hands back, so a frame read from disk
      still agrees with a freshly built one;
    * ``rec.recall_days`` and ``rec["recall_days"]`` both work on the object in
      ``attrs``; the ``@property`` derivations (``total_exposure_days``,
      ``is_recorded``, ``is_strong``) are unchanged.

    What it gives up -- the dataclass's immutability -- is replaced: the
    mutating ``dict`` methods raise, ``__hash__`` is restored (a ``dict``
    subclass otherwise loses it), and ``__reduce__`` is defined because the
    default reconstruction replays items through ``__setitem__``, which the
    guard rejects, and pandas deepcopies ``attrs`` on every propagation.

    Mapping content.  A field whose value is ``None`` is **dropped from the
    mapping** (the ``PopulationRecord`` convention), so ``rec["recall_days"]``
    on a ``not-recorded`` cell is a ``KeyError`` while ``rec.recall_days`` is
    ``None``.  The two non-``None`` defaults -- ``basis="not-recorded"`` and
    ``ask_count_basis="unknown"`` -- are always **stored**, never merely
    returned by attribute lookup: they are what makes the ``not-recorded`` rung
    legible on a frame read back from parquet, where only the mapping survives.
    """

    #: Every field, in declaration order.  The one reading of the schema.
    _FIELDS: tuple[str, ...] = (
        "country", "wave", "recall_days", "n_asks", "ask_count_basis", "basis",
        "recall_verbatim", "evidence", "within_wave_variation",
        "n_items_delivered", "notes",
    )
    #: Values a field takes when the constructor is not given one.  ``None``
    #: means "absent"; the two strings are real rungs and are always stored.
    _DEFAULTS: dict[str, Any] = {
        "recall_days": None, "n_asks": None,
        "ask_count_basis": "unknown", "basis": "not-recorded",
        "recall_verbatim": None, "evidence": None,
        "within_wave_variation": None, "n_items_delivered": None,
        "notes": None,
    }
    _REQUIRED: tuple[str, ...] = ("country", "wave")

    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # ``dict(*args, **kwargs)`` rather than keyword-only: ``__reduce__``
        # rebuilds positionally from a plain mapping.
        given = dict(*args, **kwargs)
        unknown = sorted(set(given) - set(self._FIELDS))
        if unknown:
            raise TypeError(
                f"RecallRecord got unexpected field(s) {unknown}; "
                f"known fields are {list(self._FIELDS)}")
        missing = [f for f in self._REQUIRED if given.get(f) is None]
        if missing:
            raise TypeError(f"RecallRecord is missing required field(s) {missing}")
        data = {**self._DEFAULTS, **given}
        # Defaults apply BEFORE validation: a record built with no ``basis``
        # is validated as ``not-recorded``, exactly as the dataclass did.
        self._validate(data)
        # ``dict.__init__`` is C-level and bypasses the overridden
        # ``__setitem__``, so this is the one legal write.
        dict.__init__(self, {k: data[k] for k in self._FIELDS
                             if data.get(k) is not None})

    @staticmethod
    def _validate(d: dict[str, Any]) -> None:
        """The construction-time refusals, unchanged from the dataclass."""
        country, wave = d["country"], d["wave"]
        basis, ask_count_basis = d["basis"], d["ask_count_basis"]
        variation, recall_days = d["within_wave_variation"], d["recall_days"]
        if basis not in BASIS_LADDER:
            raise ValueError(
                f"{country} {wave}: basis {basis!r} is not one of "
                f"{BASIS_LADDER}.  The rung is what stops a filename guess being "
                f"read as a questionnaire fact; it may not be invented.")
        if ask_count_basis not in ASK_COUNT_BASES:
            raise ValueError(
                f"{country} {wave}: ask_count_basis "
                f"{ask_count_basis!r} not in {ASK_COUNT_BASES}")
        if variation is not None and variation not in VARIATION_AXES:
            raise ValueError(
                f"{country} {wave}: within_wave_variation "
                f"{variation!r} not in {VARIATION_AXES}")
        # A cell whose window varies inside the wave has no single window.
        # Carrying one would be exactly the error the axis exists to flag.
        if variation and recall_days is not None:
            raise ValueError(
                f"{country} {wave}: within_wave_variation="
                f"{variation!r} AND recall_days="
                f"{recall_days!r}.  A cell that varies inside the wave "
                f"must leave the number null and carry the axis instead.")
        # A rung that establishes nothing may not also assert a length.
        if basis == "not-recorded" and recall_days is not None:
            raise ValueError(
                f"{country} {wave}: basis='not-recorded' with "
                f"recall_days={recall_days!r}.  'Nobody has looked' and "
                f"'the window is N days' cannot both be true.")

    # -- attribute access ----------------------------------------------------
    def __getattr__(self, name: str) -> Any:
        # Only reached when normal lookup fails, so the ``@property``
        # derivations below take precedence.  An absent optional field reads
        # as ``None``, not ``AttributeError``.
        if name in self._FIELDS:
            return self.get(name)
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}")

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={self[k]!r}" for k in self._FIELDS if k in self)
        return f"RecallRecord({inner})"

    # -- immutability --------------------------------------------------------
    def _immutable(self, *args: Any, **kwargs: Any):
        raise TypeError(
            "RecallRecord is immutable: it is returned from an lru_cached "
            "loader and rides on df.attrs, so a mutation would be visible to "
            "every other reader. Build a new record, or take dict(record).")

    __setitem__ = _immutable
    __delitem__ = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable
    clear = _immutable
    __ior__ = _immutable

    def __setattr__(self, name: str, value: Any) -> None:
        # ``__slots__ = ()`` already refuses this with an AttributeError; say
        # why instead, with the same message the mapping guard uses.
        self._immutable()

    def __delattr__(self, name: str) -> None:
        self._immutable()

    def __hash__(self) -> int:                     # dict sets this to None
        return hash(tuple(sorted(self.items())))

    def __reduce__(self):
        """Pickle / deepcopy via ``__init__``, not by replaying ``__setitem__``."""
        return (self.__class__, (dict(self),))

    def to_dict(self) -> dict[str, Any]:
        """Plain-``dict`` copy; ``dict(record)`` means the same thing."""
        return dict(self)

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
        existing = df.attrs.get(ATTRS_KEY)
        merged = dict(existing) if isinstance(existing, dict) else {}
        merged[country] = records
        df.attrs[ATTRS_KEY] = merged
    except Exception:                       # pragma: no cover - never fatal
        pass


def merge_attrs(frames) -> dict[str, dict[str, RecallRecord]]:
    """Union the ``recall`` attrs of several frames, for ``Feature()`` assembly.

    ``attrs`` survive an operation only when every input agrees, so a
    cross-country ``concat`` -- whose inputs differ *by design*, one record per
    country -- always lands in the ``{}`` case.  The re-attach is therefore
    load-bearing, not decorative (``CLAUDE.md`` §"Panel ID Transitive Chains").
    Wired from ``Feature.__call__`` since GH #873; before that this function
    had no caller and ``attrs['recall']`` died at every multi-country call.

    Each element may be a **frame** or the bare ``attrs`` **mapping** captured
    from one.  ``Feature`` captures the mapping before assembly (the frame
    itself is reshaped by ``_harmonize_country_frame`` on the way), so both
    shapes have to work.
    """
    out: dict[str, dict[str, RecallRecord]] = {}
    for f in frames:
        got = (getattr(f, "attrs", f) or {}).get(ATTRS_KEY)
        if isinstance(got, dict):
            for c, recs in got.items():
                out.setdefault(c, {}).update(recs)
    return out


# ---------------------------------------------------------------------------
# Phase II: ``period=`` -- the one conversion the record licenses (GH #851)
# ---------------------------------------------------------------------------
#
# Ruling (@ligon, 2026-10-18): ship ``period=`` on ``food_expenditures`` /
# ``food_quantities`` under Contract B -- a typed raise from ``Country()``, a
# degrade plus warning from ``Feature()`` -- and refuse it on ``food_prices``.
#
# What it does is one division per wave: the derived tables sum ``visit``
# away, so a row's value covers the cell's TOTAL exposure, ``recall_days *
# n_asks`` (30 days for GLSS7's six five-day asks, 28 for Burkina Faso 2014's
# four 7-day passages, 7 for a single 7-day ask), and
#
#     value_in_period = value * PERIOD_DAYS[period] / total_exposure_days
#
# That is NOT the division the module docstring refutes.  The refuted move
# divides by the REALISED interval between visits, which injects fieldwork
# tempo (elasticity 0.018 on GLSS4); this divides by the DECLARED exposure of
# the instrument, a constant per wave, so it rescales every household of a
# wave by the same factor and cannot reorder them.  Charter §6
# (``.coder/charter-recall-windows.md``) records why the ask-order bias does
# not gate it: the arithmetic is correct GIVEN the total, and whether the
# total estimates consumption is a measurement question ``food_expenditures``
# already carries.  What the conversion does NOT do is equalise that bias --
# a single-ask 7-day survey is entirely first-ask, GhanaLSS's 30 days spread
# one first-ask premium (1.84x unaided / 1.17x diary-assisted) over six asks.
# Said in ``docs/guide/recall.md`` beside the kwarg; not a reason to withhold.

#: ``df.attrs`` key holding ``{country: {...conversion record...}}``.
PERIOD_ATTRS_KEY = "period"

#: Accepted ``period=`` values and their length in days.  Closed: one
#: spelling per period.  ``month`` and ``year`` use the Julian year
#: (365.25 days), the convention the only prior in-repo conversion stated
#: (``Togo/_/data_scheme.yml``, ``Expenditure * 365.25 / days``).
PERIOD_DAYS: dict[str, float] = {
    "day": 1.0,
    "week": 7.0,
    "month": 365.25 / 12,
    "year": 365.25,
}

#: The tables ``period=`` is defined on, and the FLOW columns it scales.
#: Deliberately excludes ``food_prices`` (a price is a rate per unit, not a
#: flow over time; see :func:`period_refusal`) and ``food_acquired`` (its
#: ``visit`` level is not summed, so a row covers ONE ask, not the cell's
#: exposure -- a different denominator, and not in Phase II's scope).
PERIOD_TABLES: dict[str, tuple[str, ...]] = {
    "food_expenditures": ("Expenditure",),
    "food_quantities": ("Quantity",),
}


def period_days(period: str) -> float:
    """Length of *period* in days; ``ValueError`` on anything else."""
    try:
        return PERIOD_DAYS[period]
    except (KeyError, TypeError):
        raise ValueError(
            f"period= must be one of {list(PERIOD_DAYS)} (or None for the "
            f"window the survey recorded); got {period!r}") from None


def period_refusal(table: str) -> str:
    """The ``TypeError`` message for ``period=`` on a table that refuses it.

    One message for ``Country`` and ``Feature`` so the two cannot drift.
    ``food_prices`` gets its own sentence, because it is the table someone
    will add ``period=`` to "for symmetry" unless the refusal says why.
    """
    head = (f"{table}() got an unexpected keyword argument 'period'; only "
            f"{sorted(PERIOD_TABLES)} accept it.")
    if table == "food_prices":
        return (head + "  A price is a rate per unit of the good, not a "
                "flow over time: it has no reference period to convert, and "
                "rescaling it by a recall window would change its meaning, "
                "not its period (GH #851).")
    if table == "food_acquired":
        return (head + "  food_acquired keeps the `visit` level, so a row "
                "covers ONE ask rather than the cell's total exposure; derive "
                "food_expenditures / food_quantities, which sum it away.")
    return head


def unavailable_reason(rec: RecallRecord | None) -> str | None:
    """Why *rec* does not establish a window ``period=`` may divide by.

    ``None`` means it does: ``total_exposure_days`` is known and rests on a
    rung stronger than a guess.  The order is the order a reader needs:

    1. no record at all -- the wave was never entered in ``recall.yml``;
    2. ``within_wave_variation`` -- there is no single window to divide by
       (GhanaLSS 1991-92 by stratum, Nigeria 2018Q3/2019Q1 by source), so
       no per-wave factor is correct for every row;
    3. a WEAK rung (``not-recorded``, ``filename``) -- ``not-recorded``
       carries no number by construction, and a ``filename`` window is a
       guess the record itself tells consumers to discount;
    4. a recorded rung whose ``recall_days`` or ``n_asks`` is blank.

    ``config-comment`` cells DO convert: the ruling counted them, and the
    rung rides on ``attrs['period']`` so a consumer can discount them.
    """
    if rec is None:
        return "no recall record for this wave"
    if rec.within_wave_variation:
        return (f"the window varies within the wave "
                f"(within_wave_variation={rec.within_wave_variation!r}), so "
                f"no single factor is right for every row")
    if not rec.is_strong:
        if rec.basis == "not-recorded":
            return "basis='not-recorded' -- nobody has read the questionnaire"
        return (f"basis={rec.basis!r} -- a guess, not evidence of the "
                f"window")
    missing = [f for f in ("recall_days", "n_asks") if getattr(rec, f) is None]
    if missing:
        return f"{' and '.join(missing)} not established (basis={rec.basis!r})"
    return None


def to_period(df, country: str, period: str, *, table: str,
              records: dict[str, RecallRecord] | None = None):
    """Rescale a derived food table from each wave's recorded window to *period*.

    Each row's flow column (``Expenditure`` / ``Quantity``, per
    :data:`PERIOD_TABLES`) is multiplied by ``PERIOD_DAYS[period] /
    total_exposure_days`` of its own wave ``t``.  Linear and per-``t``, so it
    commutes with the ``labels=`` re-aggregation, ``market=`` and
    ``numeraire=`` post-steps; the caller applies it before them.

    Strict, by contract: if ANY wave in *df* has no established window
    (:func:`unavailable_reason`), nothing is converted and
    :class:`~lsms_library.errors.PeriodUnavailableError` is raised naming
    every such wave and the convertible ones.  Converting the known waves and
    leaving the rest unscaled would return one column on two clocks; filling
    the unknown ones with NA would read as zero under ``sum()`` (GH #828).
    ``Feature`` degrades over the error at the wave grain.

    Returns a COPY carrying ``attrs[PERIOD_ATTRS_KEY] = {country: {...}}``:
    the target, its length in days, the scaled columns and, per wave, the
    exposure, factor, ``recall_days``, ``n_asks`` and both rungs -- so a
    ``config-comment`` window is visibly weaker than a ``questionnaire`` one
    on the frame itself.  Plain dicts and numbers only (``df.to_parquet``
    serialises ``attrs`` with a bare ``json.dumps``).

    *records* defaults to :func:`recall_records` for *country*; tests pass
    their own.
    """
    import pandas as pd

    from .errors import PeriodUnavailableError

    target = period_days(period)
    if table not in PERIOD_TABLES:
        raise TypeError(period_refusal(table))
    cols = [c for c in PERIOD_TABLES[table] if c in df.columns]
    if not cols:
        raise ValueError(
            f"{country}: {table}(period={period!r}) found none of the flow "
            f"columns {list(PERIOD_TABLES[table])} to rescale; got "
            f"{list(df.columns)}")
    names = list(df.index.names or [])
    if "t" not in names:
        raise ValueError(
            f"{country}: {table}(period={period!r}) needs a `t` index level "
            f"to look each row's window up by wave; got {names}")
    if records is None:
        records = recall_records(country)

    # ``t`` as python ``str`` (``pd.NA`` where null): record keys are strings,
    # and a categorical or nullable-string level must look up the same way.
    t_str = (df.index.get_level_values("t").astype("string")
             .to_numpy(dtype=object, na_value=pd.NA))
    factors: dict[str, float] = {}
    unavailable: dict[str, str] = {}
    for w in pd.unique(t_str):
        if w is None or w is pd.NA:
            unavailable["<NA>"] = "the row's wave `t` is missing (NaN key)"
            continue
        rec = records.get(str(w))
        why = unavailable_reason(rec)
        if why is not None:
            unavailable[str(w)] = why
        else:
            factors[str(w)] = target / rec.total_exposure_days
    if unavailable:
        raise PeriodUnavailableError(
            country=country, period=period, table=table,
            unavailable=unavailable, convertible=factors)

    out = df.copy()
    factor = pd.Series(t_str).map(factors).to_numpy(dtype=float)
    for c in cols:
        out[c] = out[c] * factor
    out.attrs = dict(df.attrs)
    record = {
        "period": period,
        "period_days": target,
        "columns": cols,
        "waves": {
            w: {
                "exposure_days": float(records[w].total_exposure_days),
                "factor": factors[w],
                "recall_days": float(records[w].recall_days),
                "n_asks": int(records[w].n_asks),
                "basis": records[w].basis,
                "ask_count_basis": records[w].ask_count_basis,
            }
            for w in sorted(factors)
        },
    }
    existing = out.attrs.get(PERIOD_ATTRS_KEY)
    merged = dict(existing) if isinstance(existing, dict) else {}
    merged[country] = record
    out.attrs[PERIOD_ATTRS_KEY] = merged
    return out


def merge_period_attrs(frames) -> dict[str, dict[str, Any]]:
    """Union the ``period`` attrs of several frames, for ``Feature()`` assembly.

    The twin of :func:`merge_attrs`, for the same reason: the cross-country
    ``concat`` disagrees by design (one record per country) and lands in the
    ``{}`` case, so ``feature._ATTRS_CARRIERS`` re-attaches this.  Elements
    may be frames or captured ``attrs`` mappings.
    """
    out: dict[str, dict[str, Any]] = {}
    for f in frames:
        got = (getattr(f, "attrs", f) or {}).get(PERIOD_ATTRS_KEY)
        if isinstance(got, dict):
            out.update(got)
    return out
