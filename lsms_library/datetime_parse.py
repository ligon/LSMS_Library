"""One build-time parser for every calendar quantity (GH #1045).

WHY THIS EXISTS.  A declared ``datetime`` column used to be built ad hoc in each
wave (87 ``to_datetime`` calls in 52 country files) and then GUESSED into a
datetime by the read-time cast in ``Country._finalize_result`` -- after the
waves were concatenated, when the information that says how a wave writes its
dates (its source file, its questionnaire) is gone.  That guess nulled 12,745 of
Mali's dates and 5,893 of Tanzania's on a warm read (GH #798); PR #1025 made the
guess safe, but it is still a guess made in the wrong place.

This module is the right place: the wave, at build time, with the format known.

* :func:`parse_datetime` -- components (``y``/``m``/``d`` [/``H``/``M``/``S``],
  partial allowed), Stata ``%td`` / ``%tc`` and SPSS numerics, strings with a
  ``format=`` or, without one, the PR #1025 evidence rule (year-first strings
  per value; year-last numeric strings settled per wave from the data, else
  ``NaT`` + :class:`DatetimeAmbiguityWarning`).  ``calendar`` is REQUIRED (no
  default); Ethiopian dates go through :mod:`lsms_library.calendars`.  Kinds:
  ``date`` (``datetime64[us]``), ``month`` (month-of-year, nullable ``Int64``
  1-12 -- not a date) and ``year`` (nullable ``Int64``).
* **Nothing is coerced silently.**  Every value lands in exactly one of
  ``parsed`` / ``missing`` / ``sentinel`` / ``out_of_range`` / ``incomplete`` /
  ``ambiguous`` / ``unparsed``; ``imputed_day`` / ``imputed_month`` (partial
  dates padded to day 1, a REGISTERED DERIVATION -- see below) and
  ``outside_window`` (outside a declared fieldwork window, value KEPT) are
  subsets of ``parsed``.  A sentinel is reported as a sentinel, never as a
  parse failure; month 13 or day 32 is counted, never rolled over.
* **The report survives the cache** (#323's lesson).  Reports are filed in
  ``_DATETIME_LEDGER`` under ``(country, table)``, stamped into the parquet's
  schema metadata (``lsms_datetime_audit``, beside ``lsms_grain_audit``) at the
  L2-wave and L2-country tiers -- and by a wave SCRIPT's own ``to_parquet(...,
  datetime_audit=datetime_reports(country, table))`` in its ``make``
  subprocess -- and replayed on every warm read (:func:`replay_datetime_audit`).
* **Its own lever**: ``LSMS_DATE_STRICT=1`` -> :class:`DatetimeParseError`.  An
  unparseable date is a different concern from an empty column
  (``LSMS_READ_STRICT``) or a destroyed row (``LSMS_GRAIN_STRICT``).
  Public accessor :func:`datetime_reports` (twin of ``grain_reports``).

PARTIAL DATES ARE A DERIVATION, NOT A PARSER OPTION (@ligon, 2026-10-18).  A
year-month is served as the first of the month and carries
``::<table>::date-day-imputed`` in the ``Derivation`` column; a year-only value
is served as 1 January and carries ``::<table>::date-month-day-imputed``.  Both are
registered in ``lsms_library/derivations.yml`` (basis ``modelling-choice``).
:func:`assign_datetime` writes the key, joining it with a key the row already
carries (``MULTI_SEP``).

CACHE COUPLING (decision 1).  The public entry points are tagged
``@build_transform(tables=CALENDAR_TABLES)`` and listed in
``_build_registry._EXCLUDED_CALLABLES``.  Both halves are needed: the tag folds
this module into exactly the tables that carry a calendar column, and the
exclusion stops the ALL-scoped ``Wave.grab_data`` (which calls
:func:`apply_datetime_specs`) from recursing into it and folding it into every
table in the corpus.  ``tests/test_gh1045_parse_datetime.py`` pins both, and
pins ``CALENDAR_TABLES`` to the set of tables declaring a calendar type in
``lsms_library/data_info.yml`` -- so a new calendar column cannot be parsed
here without its table re-warming on a parser edit.
"""
from __future__ import annotations

import os
import re
import unicodedata
import warnings
from typing import Any, Iterable, NamedTuple

import numpy as np
import pandas as pd

from ._build_registry import build_transform

__all__ = [
    "CALENDARS", "KINDS", "CALENDAR_TABLES", "COMPONENTS", "DERIVATION_NAMES",
    "derivation_key", "DatetimeWarning",
    "DatetimeCoercionWarning", "DatetimeAmbiguityWarning", "DatetimeParseWarning",
    "DatetimeStringAtReadWarning", "DatetimeParseError", "DatetimeSpecError",
    "ParsedDatetime", "parse_datetime", "assign_datetime",
    "spec_source_vars", "apply_datetime_specs", "datetime_reports",
    "replay_datetime_audit",
]

#: Calendars a spec may name.  No default, by decision (@ligon 2026-10-18):
#: a spec that does not say which calendar its source is in is refused.
CALENDARS = ("gregorian", "ethiopian")
#: ``date`` -> datetime64; ``month`` = month-of-year and ``year`` -> Int64.
KINDS = ("date", "month", "year")
#: Tables whose fingerprint folds this module.  Pinned by a test to the set of
#: tables declaring a calendar ``type`` in ``lsms_library/data_info.yml``.
CALENDAR_TABLES = ("interview_date",)
#: Component names a components spec may use.
COMPONENTS = ("y", "m", "d", "H", "M", "S")
#: ``type`` values in ``data_info.yml`` that make a column a calendar column.
CALENDAR_TYPES = frozenset({"datetime", "date", "timestamp", "month", "year"})

#: Names of the two partial-date derivations.  The registry KEY is
#: ``::<table>::<name>`` (framework level, scoped to the table: an empty table
#: slot would attach the entry to every table in the corpus), and
#: ``lsms_library/derivations.yml`` registers both names for every table in
#: :data:`CALENDAR_TABLES` -- pinned by a test.
DERIVATION_NAMES = {
    "imputed_day": "date-day-imputed",
    "imputed_month": "date-month-day-imputed",
}


def derivation_key(which: str, table: str | None) -> str:
    """``'::<table>::<name>'`` for ``which`` in :data:`DERIVATION_NAMES`."""
    return f"::{table or ''}::{DERIVATION_NAMES[which]}"

#: Ordered outcome categories.  The first seven partition the input; the last
#: three are subsets of ``parsed``.
_PARTITION = ("parsed", "missing", "sentinel", "out_of_range", "incomplete",
              "ambiguous", "unparsed")
_SUBSETS = ("imputed_day", "imputed_month", "outside_window")
#: Categories that make a report worth a warning.  ``missing`` (a blank source
#: value) and the imputations (disclosed by the ``Derivation`` key) are counted
#: but silent; everything else is information a user of the column needs.
_WARN = ("sentinel", "out_of_range", "incomplete", "ambiguous", "unparsed",
         "outside_window")

_DATE_STRICT_ENV = "LSMS_DATE_STRICT"
#: Plausible year range for a calendar year (inclusive).  A year outside it is
#: ``out_of_range`` -- counted, never re-centred (no two-digit-year guessing).
_YEAR_RANGE = (1800, 2100)

#: Read-time cast switch (GH #1045): when True, ``country._coerce_datetime_column``
#: warns on every STRING it receives for a declared datetime column.  OFF until
#: the string-storing countries are migrated, so it starts as a work queue rather
#: than a firehose.  Tested ON in tests/test_gh1045_parse_datetime.py.
READ_CAST_WARNS_ON_STRINGS = False


# ---------------------------------------------------------------------------
# Warning / error family.  ``DatetimeCoercionWarning`` and
# ``DatetimeAmbiguityWarning`` were defined in ``country.py`` by PR #1025 and
# are re-exported from there unchanged.
# ---------------------------------------------------------------------------

class DatetimeWarning(RuntimeWarning):
    """Base of the date-warning family (GH #1045).

    Two siblings under it, because they are two different events:
    :class:`DatetimeCoercionWarning` is the READ-TIME cast reporting what it
    could not parse (GH #798), :class:`DatetimeParseWarning` is the BUILD-TIME
    parse report (replayed warm).  Carries ``country``, ``table``, ``column``,
    ``count`` and ``examples`` as attributes so callers and tests can target it
    precisely, the way ``GrainCollapseWarning`` does.
    """

    def __init__(self, message: str, *, country: str | None = None,
                 table: str | None = None, column: str | None = None,
                 count: int = 0, examples: tuple = ()):
        super().__init__(message)
        self.country = country
        self.table = table
        self.column = column
        self.count = count
        self.examples = examples


class DatetimeCoercionWarning(DatetimeWarning):
    """The READ-TIME cast of a declared datetime column held values that did
    not parse (GH #798); see ``country._coerce_datetime_column``."""


class DatetimeAmbiguityWarning(DatetimeCoercionWarning):
    """Year-last numeric dates whose day/month order the data cannot settle.

    ``01/05/2019`` is 1 May or 5 January.  A group of year-last values is read
    day-first only when some value has a first field above 12 and none a
    second field above 12 (and month-first the other way round); otherwise --
    no evidence, or contradictory evidence -- the group is served ``NaT`` and
    this warning names it.  ``reason`` is ``'ambiguous'`` or ``'conflicting'``;
    ``wave`` is the ``t`` value, or ``None``.  (GH #798, @ligon 2026-10-18: a
    silent per-value guess was the one failure mode ruled out.)
    """

    def __init__(self, message: str, *, reason: str, wave=None, **kw):
        super().__init__(message, **kw)
        self.reason = reason
        self.wave = wave


class DatetimeParseWarning(DatetimeWarning):
    """A build-time parse report (GH #1045) with something to say.

    ``report`` is the report dict (see :func:`parse_datetime`); ``wave`` its
    wave.  Emitted at the cold build AND on every warm read that replays the
    report from the parquet's ``lsms_datetime_audit`` stamp.
    """

    def __init__(self, message: str, *, report: dict | None = None, **kw):
        super().__init__(message, **kw)
        self.report = report or {}
        self.wave = self.report.get("wave")


class DatetimeStringAtReadWarning(DatetimeCoercionWarning):
    """The read-time cast received strings for a declared datetime column.

    Fires only when :data:`READ_CAST_WARNS_ON_STRINGS` is on: once every
    country parses at build time, a string reaching the read path is a wave
    that has not been migrated.
    """


class DatetimeParseError(RuntimeError):
    """Raised instead of :class:`DatetimeParseWarning` under ``LSMS_DATE_STRICT``."""


class DatetimeSpecError(ValueError):
    """A ``datetime:`` spec (or a direct call) that cannot be honoured --
    no ``calendar``, an unknown kind/calendar/component, an unsupported
    combination.  Always raised: a malformed spec is a config bug."""


def _date_strict() -> bool:
    """Whether a warn-worthy parse report should RAISE.  Same ``{1,true,yes}``
    spelling as ``LSMS_READ_STRICT`` / ``LSMS_GRAIN_STRICT``; its own lever."""
    return os.environ.get(_DATE_STRICT_ENV, "").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# The ledger, the accessor, the emitter, the replay.
# ---------------------------------------------------------------------------

# Reports filed during this process, keyed by (country, table).  A process-local
# LOG, not a build constant: listed in _build_registry._EXCLUDED_CONSTANTS for
# the reason _GRAIN_LEDGER is (a fingerprint must not depend on when in a process
# it was first memoised).
_DATETIME_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def _ledger_key(report: dict) -> tuple[str, str]:
    return (str(report.get("country") or "?"), str(report.get("table") or "?"))


def _file_datetime_report(report: dict[str, Any]) -> None:
    existing = _DATETIME_LEDGER.setdefault(_ledger_key(report), [])
    if report not in existing:
        existing.append(report)


def _warn_worthy(report: dict[str, Any]) -> bool:
    return any(int(report.get(k) or 0) for k in _WARN)


def _format_report(report: dict[str, Any]) -> str:
    where = "/".join(str(x) for x in (report.get("country"), report.get("table"),
                                      report.get("wave"), report.get("column"))
                     if x is not None)
    counts = ", ".join(f"{k}={report.get(k)}" for k in _PARTITION + _SUBSETS
                       if report.get(k))
    ex = report.get("examples") or {}
    extxt = "; ".join(f"{k} e.g. {v}" for k, v in ex.items() if k in _WARN and v)
    origin = " (replayed from the parquet's lsms_datetime_audit stamp)" \
        if report.get("from_cache") else ""
    return (f"{where or 'datetime column'}: build-time date parse of "
            f"{report.get('n')} value(s) [{report.get('kind')}, "
            f"{report.get('calendar')}]: {counts}{origin}."
            + (f"  {extxt}." if extxt else "")
            + "  Nothing was coerced: out-of-range / unparsed / ambiguous / "
              "sentinel values are served NaT, out-of-window dates are kept.")


def _emit_datetime_report(report: dict[str, Any]) -> None:
    """Raise (strict) or warn (default).  The single choke point."""
    if not _warn_worthy(report):
        return
    msg = _format_report(report)
    if _date_strict():
        raise DatetimeParseError(msg)
    warnings.warn(DatetimeParseWarning(
        msg, report=report, country=report.get("country"),
        table=report.get("table"), column=report.get("column"),
        count=sum(int(report.get(k) or 0) for k in _WARN if k != "outside_window"),
        examples=tuple(v for k in _WARN for v in (report.get("examples") or {}).get(k, []))[:3]),
        stacklevel=3)


def _record_datetime_report(report: dict[str, Any]) -> None:
    _file_datetime_report(report)
    _emit_datetime_report(report)


def datetime_reports(country: str | None = None,
                     table: str | None = None) -> list[dict]:
    """Build-time date-parse reports filed during this process (GH #1045).

    Public read-only accessor -- the twin of ``country.grain_reports`` and
    ``null_read_audit.null_read_reports``.  A report replayed from a parquet's
    stamp carries ``from_cache=True``.  A wave script passes
    ``datetime_reports(country, table)`` to ``to_parquet(datetime_audit=...)``
    so its report travels with the wave parquet out of the ``make`` subprocess.
    """
    out: list[dict] = []
    for (c, t), reports in _DATETIME_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


def _clear_datetime_reports() -> None:
    """Drop every filed report.  For tests only."""
    _DATETIME_LEDGER.clear()


def replay_datetime_audit(reports: Any, country: str, table: str,
                          wave: str | None = None, waves=None) -> None:
    """Re-file and re-emit reports stamped into a parquet by its cold build.

    THE LINE THAT MAKES BUILD-TIME PARSING SAFE: without it the report is a
    cold-build-only event, and since practically all reads are warm the signal
    would disappear for everyone who did not do the build (#323).  ``wave``
    fills a report that names none and never overrides one.  ``waves``, when
    given, keeps only reports about those waves (or naming none): a reader
    served one wave must not be told -- or, under ``LSMS_DATE_STRICT``, fail --
    about another (a multi-round folder's parquet carries every round's).
    """
    if not isinstance(reports, list):
        return
    keep = None if waves is None else {str(w) for w in waves}
    for report in reports:
        if not isinstance(report, dict):
            continue
        if keep is not None and report.get("wave") is not None \
                and str(report.get("wave")) not in keep:
            continue
        report = {**report, "country": country, "table": table, "from_cache": True}
        if wave is not None and report.get("wave") is None:
            report["wave"] = wave
        _record_datetime_report(report)


def partial_date_inputs() -> pd.DataFrame:
    """``inputs`` callable of the two partial-date derivations
    (``lsms_library/derivations.yml``): one row per (country, table, wave,
    column) report filed in this process that padded at least one date, with
    the counts.  The raw component values themselves are the variables the
    report's column spec names; the parse of a built table files these reports
    (cold) or replays them from the parquet's stamp (warm), so call this after
    reading the table."""
    rows = [{k: r.get(k) for k in ("country", "table", "wave", "column", "n",
                                   "imputed_day", "imputed_month")}
            for r in datetime_reports()
            if int(r.get("imputed_day") or 0) or int(r.get("imputed_month") or 0)]
    return pd.DataFrame(rows, columns=["country", "table", "wave", "column", "n",
                                       "imputed_day", "imputed_month"])


def wave_datetime_audit(country: str, table: str, wave: str | None) -> list[dict] | None:
    """The reports THIS process's build of one wave folder filed, for stamping
    into that wave's L2 parquet.  Filtered on ``wave_folder`` (so a multi-round
    folder keeps each round's ``wave``); replayed reports are excluded -- a
    wave write is a cold rebuild, and what is true now is what it just filed."""
    if wave is None:
        return None
    out = [r for r in _DATETIME_LEDGER.get((country, table), [])
           if str(r.get("wave_folder", r.get("wave"))) == str(wave)
           and not r.get("from_cache")]
    return out or None


# ---------------------------------------------------------------------------
# Month names.  Keys are casefolded and accent-stripped; English, French,
# Portuguese and Spanish full names and three-letter forms.  Ethiopian names go
# through calendars.parse_ethiopian_month under calendar='ethiopian'.
# ---------------------------------------------------------------------------

def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", s.casefold())


_MONTH_NAMES: dict[str, int] = {}
for _i, _names in enumerate([
        ("january", "janvier", "janeiro", "enero"),
        ("february", "fevrier", "fevereiro", "febrero"),
        ("march", "mars", "marco", "marzo"),
        ("april", "avril", "abril"),
        ("may", "mai", "maio", "mayo"),
        ("june", "juin", "junho", "junio"),
        ("july", "juillet", "julho", "julio"),
        ("august", "aout", "agosto"),
        ("september", "septembre", "setembro", "septiembre", "setiembre"),
        ("october", "octobre", "outubro", "octubre"),
        ("november", "novembre", "novembro", "noviembre"),
        ("december", "decembre", "dezembro", "diciembre")], start=1):
    for _n in _names:
        _MONTH_NAMES[_n] = _i
        _MONTH_NAMES.setdefault(_n[:3], _i)
_MONTH_NAMES.update({"sept": 9, "juil": 7, "janv": 1, "fevr": 2, "avr": 4})
del _i, _names, _n

_ONE_INTEGER = re.compile(r"^\D*?(-?\d+(?:\.0+)?)\D*$")
_YEAR_LAST_DATE = re.compile(
    r'^\s*(\d{1,2})[-/.](\d{1,2})[-/.](\d{4}|\d{2})(?:[ T].*)?$')


def _blank(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _norm_sentinels(sentinels: Iterable) -> tuple[set, set]:
    nums, strs = set(), set()
    for s in sentinels or ():
        if isinstance(s, str):
            strs.add(s.strip())
            try:
                nums.add(float(s))
            except ValueError:
                pass
        else:
            nums.add(float(s))
    return nums, strs


def _is_sentinel(s: pd.Series, nums: set, strs: set) -> pd.Series:
    if not nums and not strs:
        return pd.Series(False, index=s.index)
    out = pd.Series(False, index=s.index)
    if strs:
        out |= s.map(lambda v: isinstance(v, str) and v.strip() in strs,
                     na_action="ignore").fillna(False).astype(bool)
    if nums:
        num = pd.to_numeric(s.map(lambda v: v if not isinstance(v, str) else v.strip()),
                            errors="coerce") if s.dtype == object else \
            pd.to_numeric(s, errors="coerce")
        out |= num.isin(nums).fillna(False).astype(bool)
    return out


def _component_numbers(s: pd.Series, which: str, calendar: str) -> tuple[pd.Series, pd.Series]:
    """Numeric value of one component, plus a mask of values that are present
    but not readable as one.  Strings: a month name (for ``m``), else the one
    integer the string holds (``'Année 2017'`` -> 2017, ``'2014.0'`` -> 2014);
    anything else is unreadable.  Categoricals are read by their labels."""
    if isinstance(s.dtype, pd.CategoricalDtype):
        s = s.astype(object)
    if pd.api.types.is_numeric_dtype(s.dtype) and not pd.api.types.is_bool_dtype(s.dtype):
        return pd.to_numeric(s, errors="coerce").astype("float64"), pd.Series(False, index=s.index)

    def one(v):
        if _blank(v):
            return np.nan
        if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool):
            return float(v)
        txt = str(v).strip()
        if which == "m":
            if calendar == "ethiopian":
                from .calendars import parse_ethiopian_month
                k = parse_ethiopian_month(txt)
                if k is not None:
                    return float(k)
            k = _MONTH_NAMES.get(_fold(txt))
            if k is not None:
                return float(k)
        m = _ONE_INTEGER.match(txt)
        return float(m.group(1)) if m else -np.inf     # -inf marks "unreadable"

    vals = s.map(one).astype("float64")
    bad = vals == -np.inf
    return vals.where(~bad), bad


def _groups(index: pd.Index, wave) -> pd.Series:
    """The wave label of every row: the ``t`` level when the index has one,
    else ``wave`` (possibly ``None``)."""
    if "t" in (index.names or []):
        return pd.Series(index.get_level_values("t"), index=index).astype(object)
    return pd.Series([wave] * len(index), index=index, dtype=object)


def _examples(raw, mask: pd.Series) -> list[str]:
    """Up to three distinct raw values under ``mask``.  ``raw`` is the input
    Series or components DataFrame (components joined with '/'); only the
    masked rows are stringified, so a 1M-row parse pays for a handful."""
    if not mask.any():
        return []
    sub = raw[mask.to_numpy()]
    out: list[str] = []
    for start in range(0, len(sub), 64):
        chunk = sub.iloc[start:start + 64]
        if isinstance(chunk, pd.DataFrame):
            vals = ["/".join("" if _blank(v) else str(v) for v in row)
                    for row in chunk.astype(object).itertuples(index=False)]
        else:
            vals = ["" if _blank(v) else str(v) for v in chunk.astype(object)]
        for v in vals:
            if v not in out:
                out.append(v)
                if len(out) == 3:
                    return out
    return out


class ParsedDatetime(NamedTuple):
    """What :func:`parse_datetime` returns."""
    values: pd.Series          #: datetime64[us] (kind date) or Int64
    derivation: pd.Series      #: registry key per row (object; NA where reported)
    reports: list              #: one report dict per wave


def _check_spec(calendar, kind, components, stata, spss, fmt):
    if calendar is None:
        raise DatetimeSpecError(
            "parse_datetime: `calendar` is required and has no default "
            f"(one of {CALENDARS}); a source whose calendar is not stated is refused "
            "(GH #1045 decision 4).")
    if calendar not in CALENDARS:
        raise DatetimeSpecError(f"parse_datetime: unknown calendar {calendar!r}; one of {CALENDARS}")
    if kind not in KINDS:
        raise DatetimeSpecError(f"parse_datetime: unknown kind {kind!r}; one of {KINDS}")
    if stata not in (None, "td", "tc"):
        raise DatetimeSpecError(f"parse_datetime: stata must be 'td' or 'tc', got {stata!r}")
    if components is not None:
        bad = sorted(set(components) - set(COMPONENTS))
        if bad:
            raise DatetimeSpecError(f"parse_datetime: unknown component(s) {bad}; allowed {COMPONENTS}")
        need = {"date": "y", "month": "m", "year": "y"}[kind]
        if need not in components:
            raise DatetimeSpecError(
                f"parse_datetime: a components {kind} needs a `{need}` component")
    if calendar == "ethiopian" and (stata or spss or fmt):
        raise DatetimeSpecError(
            "parse_datetime: stata/spss/format values are Gregorian encodings; "
            "an Ethiopian-calendar source must be given as components")


@build_transform(tables=list(CALENDAR_TABLES))
def parse_datetime(data, *, calendar: str | None = None, kind: str = "date",
                   format: str | None = None, stata: str | None = None,
                   spss: bool = False, sentinels: Iterable = (),
                   window: tuple | list | None = None, resolution: str = "us",
                   country: str | None = None, table: str | None = None,
                   column: str | None = None, wave: str | None = None,
                   wave_folder: str | None = None) -> ParsedDatetime:
    """Parse one calendar quantity at build time; report everything (GH #1045).

    ``data`` is a Series (one source column: strings, numerics, Stata/SPSS
    numerics, or datetime64) or a DataFrame of COMPONENT columns named from
    :data:`COMPONENTS` (``y``, ``m``, ``d``, ``H``, ``M``, ``S``).  Components may
    be numeric, numeric strings (``'2014.0'``), strings holding one integer
    (``'Année 2017'``), or -- for ``m`` -- month names (English, French,
    Portuguese, Spanish; Ethiopian under ``calendar='ethiopian'``).

    ``calendar`` is required.  ``kind='date'`` returns ``datetime64[us]``
    (``resolution='day'`` floors to midnight); ``'month'`` (month-of-year) and
    ``'year'`` return nullable ``Int64``.  ``sentinels`` are compared on the RAW
    values (any component equal to one makes that component a sentinel).
    ``window=[start, end]`` (inclusive) counts dates outside the fieldwork
    window as ``outside_window`` and KEEPS them.

    Partial components: ``y`` and ``m`` with no ``d`` -> the first of the month,
    keyed ``::<table>::date-day-imputed``; ``y`` alone -> 1 January, keyed
    ``::<table>::date-month-day-imputed``; any other gap (a ``d`` without an ``m``) is
    ``incomplete``.  Out-of-range components (month 13, day 32, 30 February,
    a year outside 1800-2100) are ``out_of_range`` -- counted, never rolled.

    Strings with ``format=`` are parsed with it; without one, year-first
    strings are parsed per value and year-last numeric strings follow the PR
    #1025 evidence rule per wave (``ambiguous`` when undecidable).

    Reports are per wave: the frame's ``t`` level when it has one, else
    ``wave``.  Each is filed (:func:`datetime_reports`) and, if it has
    something to say, warned (:class:`DatetimeParseWarning`) or raised under
    ``LSMS_DATE_STRICT=1`` (:class:`DatetimeParseError`).
    """
    components = list(data.columns) if isinstance(data, pd.DataFrame) else None
    _check_spec(calendar, kind, components, stata, spss, format)
    if resolution not in ("us", "day"):
        raise DatetimeSpecError(f"parse_datetime: resolution must be 'us' or 'day', got {resolution!r}")
    index = data.index
    n = len(index)
    if n == 0:
        empty = pd.Series([], index=index, dtype="datetime64[us]" if kind == "date" else "Int64")
        return ParsedDatetime(empty, pd.Series([], index=index, dtype=object), [])
    cat = pd.Series("parsed", index=index, dtype=object)
    imputed = pd.Series(pd.NA, index=index, dtype=object)
    nums, strs = _norm_sentinels(sentinels)
    groups = _groups(index, wave)
    examples: dict[str, pd.Series] = {}

    if components is not None:
        raw = data
        comp: dict[str, pd.Series] = {}
        present: dict[str, pd.Series] = {}
        sent_any = pd.Series(False, index=index)
        unread_any = pd.Series(False, index=index)
        for c in components:
            s = data[c]
            sent = _is_sentinel(s.astype(object) if isinstance(s.dtype, pd.CategoricalDtype) else s,
                                nums, strs)
            vals, unread = _component_numbers(s, c, calendar)
            vals = vals.where(~sent)
            comp[c] = vals
            present[c] = vals.notna()
            sent_any |= sent
            unread_any |= unread & ~sent
        allnull = ~pd.concat([present[c] | (data[c].map(lambda v: not _blank(v)))
                              for c in components], axis=1).any(axis=1)
        # A sentinel in ANY component makes the row a sentinel (served NaT):
        # "don't know" is a statement by the source, and padding it to day 1
        # would be a construction the spec did not ask for.  Only a genuinely
        # BLANK component is a partial date (decision 3).
        cat[allnull & ~sent_any] = "missing"
        cat[sent_any] = "sentinel"
        cat[unread_any & (cat == "parsed")] = "unparsed"
        y = comp.get("y", pd.Series(np.nan, index=index))
        m = comp.get("m", pd.Series(np.nan, index=index))
        d = comp.get("d", pd.Series(np.nan, index=index))
        ok = cat == "parsed"
        # Partial components (decision 3).
        if kind == "date":
            no_m, no_d = m.isna(), d.isna()
            cat[ok & y.isna()] = "incomplete"
            cat[ok & y.notna() & no_m & d.notna()] = "incomplete"
            ok = cat == "parsed"
            imputed[ok & ~no_m & no_d] = "imputed_day"
            imputed[ok & no_m & no_d] = "imputed_month"
            m = m.where(~(ok & no_m), 1.0)
            d = d.where(~(ok & no_d), 1.0)
        # Range checks: integral and inside the calendar's bounds.
        mmax = 13 if calendar == "ethiopian" else 12
        bounds = {"y": _YEAR_RANGE, "m": (1, mmax), "d": (1, 31),
                  "H": (0, 23), "M": (0, 59), "S": (0, 59)}
        filled = {"y": y, "m": m, "d": d}
        for c in components:
            if c in ("H", "M", "S"):
                filled[c] = comp[c]
        oor = pd.Series(False, index=index)
        for c, v in filled.items():
            if kind == "month" and c != "m":
                continue
            if kind == "year" and c != "y":
                continue
            lo, hi = bounds[c]
            has = v.notna()
            oor |= has & ((v != np.floor(v)) | (v < lo) | (v > hi))
        cat[ok & oor] = "out_of_range"
        imputed[cat != "parsed"] = pd.NA
        ok = cat == "parsed"
        if kind == "month":
            values = m.where(ok).round().astype("Int64")
        elif kind == "year":
            values = y.where(ok).round().astype("Int64")
        elif calendar == "ethiopian":
            from .calendars import ethiopian_to_gregorian
            out = pd.Series(pd.NaT, index=index, dtype="datetime64[us]")
            for ix in np.flatnonzero(ok.to_numpy()):
                try:
                    g = ethiopian_to_gregorian(int(y.iloc[ix]), int(m.iloc[ix]), int(d.iloc[ix]))
                except ValueError:
                    cat.iloc[ix] = "out_of_range"
                    imputed.iloc[ix] = pd.NA
                    continue
                out.iloc[ix] = pd.Timestamp(g)
            values = out
        else:
            parts = {"year": y.where(ok), "month": m.where(ok), "day": d.where(ok)}
            for c, name in (("H", "hour"), ("M", "minute"), ("S", "second")):
                if c in comp:
                    parts[name] = comp[c].where(ok).fillna(0)
            values = pd.to_datetime(pd.DataFrame(parts), errors="coerce")
            bad_combo = ok & values.isna()          # e.g. 30 February
            cat[bad_combo] = "out_of_range"
            imputed[bad_combo] = pd.NA
    else:
        s = data
        if isinstance(s.dtype, pd.CategoricalDtype):
            s = s.astype(object)
        raw = s
        sent = _is_sentinel(s, nums, strs)
        if pd.api.types.is_numeric_dtype(s.dtype) or pd.api.types.is_datetime64_any_dtype(s.dtype):
            blank = s.isna()
        else:
            blank = s.map(_blank).astype(bool)
        cat[blank & ~sent] = "missing"
        cat[sent] = "sentinel"
        live = cat == "parsed"
        if kind in ("month", "year"):
            which = "m" if kind == "month" else "y"
            vals, unread = _component_numbers(s.where(live), which, calendar)
            cat[live & unread] = "unparsed"
            lo, hi = (1, 13 if calendar == "ethiopian" else 12) if kind == "month" else _YEAR_RANGE
            live = cat == "parsed"
            cat[live & vals.notna() & ((vals != np.floor(vals)) | (vals < lo) | (vals > hi))] = "out_of_range"
            values = vals.where(cat == "parsed").round().astype("Int64")
        elif calendar == "ethiopian":
            raise DatetimeSpecError(
                "parse_datetime: an Ethiopian-calendar date must be given as components")
        elif pd.api.types.is_datetime64_any_dtype(s.dtype):
            values = pd.Series(s, index=index)
            if getattr(values.dt, "tz", None) is not None:
                values = values.dt.tz_localize(None)
        elif stata or spss:
            v = pd.to_numeric(s.where(live), errors="coerce")
            cat[live & v.isna()] = "unparsed"
            # Convert to seconds since 1960-01-01 and bound it BEFORE pandas
            # sees it: an absurd value (Stata day 1e8) must be counted
            # out_of_range, not raise OutOfBoundsDatetime for the column.
            if stata == "td":
                secs = v * 86400.0
            elif stata == "tc":
                secs = v / 1000.0
            else:   # SPSS: seconds since 14 Oct 1582
                secs = v - (pd.Timestamp("1960-01-01") - pd.Timestamp("1582-10-14")).total_seconds()
            lo_s = (pd.Timestamp(f"{_YEAR_RANGE[0]}-01-01") - pd.Timestamp("1960-01-01")).total_seconds()
            hi_s = (pd.Timestamp(f"{_YEAR_RANGE[1] + 1}-01-01") - pd.Timestamp("1960-01-01")).total_seconds()
            bad = v.notna() & ~((secs >= lo_s) & (secs < hi_s))
            cat[live & bad] = "out_of_range"
            secs = secs.where(~bad)
            values = pd.Timestamp("1960-01-01") + pd.to_timedelta(secs.round(6), unit="s")
        else:
            txt = s.where(live).map(lambda x: str(x).strip(), na_action="ignore")
            if format is not None:
                values = pd.to_datetime(txt, format=format, errors="coerce")
            else:
                parts = txt.astype(object).str.extract(_YEAR_LAST_DATE)
                year_last = live & parts[0].notna()
                # A two-digit year needs a century, and guessing one is the
                # kind of silent choice this module refuses: give a format=.
                two_digit = year_last & (parts[2].str.len() == 2)
                cat[two_digit] = "ambiguous"
                year_last = year_last & ~two_digit
                values = _mixed(txt.where(~year_last & (cat == "parsed")))
                if year_last.any():
                    a = pd.to_numeric(parts[0], errors="coerce")
                    b = pd.to_numeric(parts[1], errors="coerce")
                    for g in pd.unique(groups[year_last]):
                        msk = year_last & ((groups == g) if not _blank(g) else groups.isna())
                        day_ev, month_ev = bool((a[msk] > 12).any()), bool((b[msk] > 12).any())
                        if day_ev != month_ev:
                            values.loc[msk] = _mixed(txt[msk], dayfirst=day_ev)
                        else:
                            cat[msk] = "ambiguous"
            cat[(cat == "parsed") & values.isna()] = "unparsed"
        values = values.where(cat == "parsed")

    if kind == "date":
        values = pd.to_datetime(values, errors="coerce").astype("datetime64[us]")
        # The plausible-year bound applies to every input kind, not only to
        # components: '9999-12-31' is a sentinel spelled as a date.
        yr = values.dt.year
        cat[(cat == "parsed") & values.notna() & ((yr < _YEAR_RANGE[0]) | (yr > _YEAR_RANGE[1]))] = "out_of_range"
        imputed[cat != "parsed"] = pd.NA
        if resolution == "day":
            values = values.dt.floor("D")
        values = values.where(cat == "parsed")

    outside = pd.Series(False, index=index)
    if window is not None and kind == "date":
        lo, hi = (pd.Timestamp(window[0]), pd.Timestamp(window[1]))
        if pd.Timestamp(window[1]).normalize() == pd.Timestamp(window[1]):
            hi = hi + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        outside = values.notna() & ((values < lo) | (values > hi))
    elif window is not None:
        lo, hi = window
        outside = values.notna() & ((values < int(lo)) | (values > int(hi)))
        outside = outside.fillna(False).astype(bool)

    derivation = pd.Series(
        [derivation_key(k, table) if isinstance(k, str) and c == "parsed" else pd.NA
         for k, c in zip(imputed.tolist(), cat.tolist())], index=index, dtype=object)

    reports = []
    for g in pd.unique(groups) if n else [wave]:
        msk = (groups == g) if not _blank(g) else groups.isna()
        if not n:
            msk = pd.Series([], dtype=bool)
        rep: dict[str, Any] = {
            "site": "parse_datetime", "country": country, "table": table,
            "column": column, "wave": None if _blank(g) else str(g),
            "kind": kind, "calendar": calendar, "n": int(msk.sum()),
        }
        if wave_folder is not None:
            rep["wave_folder"] = str(wave_folder)
        elif wave is not None:
            rep["wave_folder"] = str(wave)
        exs = {}
        for k in _PARTITION:
            km = msk & (cat == k)
            rep[k] = int(km.sum())
            if k != "parsed" and rep[k]:
                exs[k] = _examples(raw, km)
        for k in ("imputed_day", "imputed_month"):
            rep[k] = int((msk & (imputed == k)).sum())
        rep["outside_window"] = int((msk & outside).sum())
        if rep["outside_window"]:
            exs["outside_window"] = _examples(values, msk & outside)
        if exs:
            rep["examples"] = exs
        reports.append(rep)
    for rep in reports:
        _record_datetime_report(rep)
    return ParsedDatetime(values, derivation, reports)


def _mixed(txt: pd.Series, dayfirst: bool = False) -> pd.Series:
    """Per-value parse; a value pandas refuses (e.g. a timezone mix) is NaT
    -- and so ``unparsed`` -- never a whole-column failure."""
    try:
        out = pd.to_datetime(txt, errors="coerce", format="mixed", dayfirst=dayfirst)
        if getattr(out.dt, "tz", None) is not None:
            out = out.dt.tz_localize(None)
        return out
    except (ValueError, TypeError):
        def one(v):
            if _blank(v):
                return pd.NaT
            try:
                ts = pd.Timestamp(pd.to_datetime(v, dayfirst=dayfirst))
            except (ValueError, TypeError):
                return pd.NaT
            return ts.tz_localize(None) if ts.tzinfo is not None else ts
        return pd.to_datetime(txt.map(one), errors="coerce")


@build_transform(tables=list(CALENDAR_TABLES))
def assign_datetime(df: pd.DataFrame, column: str, data, **kw) -> pd.DataFrame:
    """``df[column] = parse_datetime(data, column=column, **kw).values``, plus
    the partial-date ``Derivation`` key, joined (``MULTI_SEP``, sorted) with a
    key the row already carries.  Returns a new frame; ``df`` is not mutated."""
    from .derivations import COLUMN, union_keys
    if not data.index.equals(df.index):
        raise ValueError(
            f"assign_datetime({column!r}): `data` must carry exactly df's index, "
            "in the same order -- values are assigned by position, so a "
            "reordered index would put one row's date on another row.")
    res = parse_datetime(data, column=column, **kw)
    out = df.copy()
    out[column] = res.values.array
    keyed = res.derivation.notna()
    if keyed.any():
        new = res.derivation
        if COLUMN in out.columns:
            old = out[COLUMN].astype(object)
            merged = [union_keys([o, k]) if pd.notna(k) else o
                      for o, k in zip(old.tolist(), new.tolist())]
            out[COLUMN] = pd.Series(merged, index=out.index, dtype=object)
        else:
            out[COLUMN] = new.astype(object)
    return out


# ---------------------------------------------------------------------------
# The declarative spec (YAML path).  ``Wave.column_mapping`` calls
# :func:`spec_source_vars` for every dict-valued myvar carrying a ``datetime``
# key, extracts the source columns under temporary names, and ``Wave.grab_data``
# hands the extracted frame to :func:`apply_datetime_specs` before the wave's
# ``df_edit`` hook runs.
#
#   Int_t:
#     datetime: {y: s00q22y, m: s00q22m, d: s00q22j}   # or one column: s00q23a
#     calendar: gregorian                               # REQUIRED
#     format: '%Y-%m-%dT%H:%M:%S'   stata: td|tc   spss: true
#     sentinels: ['##N/A##', 9999]  window: [2014-07-01, 2015-03-31]
#     resolution: us|day            kind: date|month|year
# ---------------------------------------------------------------------------

_SPEC_KEYS = frozenset({"datetime", "calendar", "format", "stata", "spss",
                        "sentinels", "window", "resolution", "kind"})
_TMP_SEP = "\x1f"


@build_transform(tables=list(CALENDAR_TABLES))
def is_datetime_spec(value) -> bool:
    return isinstance(value, dict) and "datetime" in value


@build_transform(tables=list(CALENDAR_TABLES))
def spec_source_vars(column: str, spec: dict) -> dict[str, str]:
    """``{temporary column name: source variable}`` for one spec, validating it.

    Refuses (``DatetimeSpecError``) a spec with no ``calendar`` or an unknown
    key -- the spec is config, and a typo must not be read as "no option".
    """
    unknown = sorted(set(spec) - _SPEC_KEYS)
    if unknown:
        raise DatetimeSpecError(f"{column}: unknown datetime spec key(s) {unknown}; "
                                f"allowed {sorted(_SPEC_KEYS)}")
    src = spec["datetime"]
    comps = list(src) if isinstance(src, dict) else None
    _check_spec(spec.get("calendar"), spec.get("kind", "date"), comps,
                spec.get("stata"), spec.get("spss", False), spec.get("format"))
    if isinstance(src, dict):
        return {f"{column}{_TMP_SEP}{c}": str(v) for c, v in src.items()}
    if isinstance(src, str):
        return {f"{column}{_TMP_SEP}": src}
    raise DatetimeSpecError(f"{column}: `datetime:` must be a source variable or a "
                            f"{{component: variable}} mapping, got {src!r}")


@build_transform(tables=list(CALENDAR_TABLES))
def apply_datetime_specs(df: pd.DataFrame, specs: dict[str, dict], *,
                         country: str | None, table: str | None,
                         wave: str | None) -> pd.DataFrame:
    """Parse every spec'd column of an extracted wave frame; drop the temporary
    source columns.  The wave's ``t`` level (when present) names each report's
    wave; ``wave`` is the folder."""
    if not specs or not isinstance(df, pd.DataFrame):
        return df
    for column, spec in specs.items():
        src = spec["datetime"]
        if isinstance(src, dict):
            names = {f"{column}{_TMP_SEP}{c}": c for c in src}
            have = [k for k in names if k in df.columns]
            data = df[have].rename(columns=names)
        else:
            tmp = f"{column}{_TMP_SEP}"
            have = [tmp] if tmp in df.columns else []
            data = df[tmp] if have else pd.Series(pd.NA, index=df.index, dtype=object)
        kw = {k: spec[k] for k in ("calendar", "format", "stata", "spss",
                                   "sentinels", "window", "resolution", "kind")
              if k in spec}
        df = assign_datetime(df.drop(columns=have), column, data, country=country,
                             table=table, wave=wave, wave_folder=wave, **kw)
    return df
