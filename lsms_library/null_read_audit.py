"""The SILENT ALL-NULL READ -- a reader can return the right SHAPE with NO
CONTENT, and raise nothing.

Every guard the library had before this one checks *shape*:

* ``Country._assert_built_required_columns`` -- is the declared column
  **present**?
* ``Wave.grab_data``'s dropped-sub-df check (GH #515/#323) -- is the required
  column **present** after an optional sub-df was dropped?
* ``country._audit_index_collapse`` (GH #323) -- is the declared index
  **unique**?

None of them looks *inside* the column.  ``get_dataframe`` -- the only
sanctioned reader, and therefore the one chokepoint over the local / DVC /
WB-fallback paths -- did no post-read content validation at all.  So a parse
that produces a correctly-shaped frame of NaN sails through untouched:

* **Peru 1990** ``.SSP`` (SAS XPORT V5) read with ``pyreadstat.read_xport``:
  ``N00A.SSP`` comes back 1528x10 with **7 of 10 columns entirely NaN**, no
  exception (``pandas.read_sas`` reads all 10).  Measured here, not quoted.
* **GhanaLSS 1988-89** ``COMM.DAT``: the sister ``COMM.DCT`` declares 311
  fixed-width fields, the ``.DAT`` actually ships comma-delimited with a header
  row.  A fixed-width parse per the ``.DCT`` yields 86x311 with **254 of 311
  columns (82%) entirely NaN**, no exception.
* **Niger 2014-15** ``Latitude``: declared ``float`` in ``_/data_scheme.yml``
  and served 0-of-270 populated, passing every guard above.

THE TWO SITES, AND WHY THERE MUST BE TWO.  These three instances do not live at
one level and no single probe sees them all:

* **Site R** (:func:`audit_read`, called from ``local_tools.get_dataframe``)
  sees the raw frame the parser produced.  It catches the mis-parse class --
  Peru, Ghana -- *including in columns nothing has asked for yet*, which is the
  point: a wave is usually mis-parsed before anyone wires it.
* **Site B** (:func:`audit_declared_columns`, called from
  ``Country._finalize_result``) sees the DELIVERED table.  It catches Niger,
  which Site R is structurally blind to: Niger 2014-15's ``data_info.yml``
  declares no ``Latitude`` at all, so no read ever produces it -- the column
  materialises as NaN in the cross-wave concat.

This mirrors GH #323's Site 1 / Site 2 split deliberately: one learnable shape
beats two.  Same warn-by-default, same own-lever strict mode, same report dict,
same public accessor (:func:`null_read_reports`, the twin of
``country.grain_reports``).

WHY THE TRIGGER IS A FRACTION OF THE FRAME AND NOT "ANY ALL-NULL COLUMN".
Measured before it was chosen, over every source file the corpus declares --
2,264 declared reads, 1,337 of them actually held and read cold through
``get_dataframe`` (65,099 columns):

===========================================  =======
naive "any column 100% null after read"        887 cells in 153 files
...of which a table had actually ASKED for       3
frames where >=1/3 of columns are all-null       0   (corpus max 28.3%)
===========================================  =======

So the naive per-column form is a **firehose**: 884 of its 887 firings are raw
columns of a source file that nothing reads, and a warning nobody reads is
exactly how GH #323 survived its first fix.  The *frame-level* form separates
cleanly instead -- the corpus tops out at 28.3% (Albania 2004
``w3_hh_basic.dta``, 169 of 598 columns) while every known-bad parse sits at
40%-82%.  ``_NULL_FRACTION_TRIGGER`` is set in that gap.

WHAT THIS DOES NOT DO.  It does not change returned data -- not one value, not
one dtype.  It is a reporting layer, which is why its own edits are exempt from
the build fingerprint (see ``_build_registry._EXCLUDED_CALLABLES``).
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

__all__ = [
    "NullReadError",
    "NullReadWarning",
    "audit_read",
    "audit_declared_columns",
    "check_read",
    "check_declared_columns",
    "null_read_reports",
]


_READ_STRICT_ENV = "LSMS_READ_STRICT"

#: Fraction of a frame's columns that must be 100% null for :func:`audit_read`
#: to report.  Chosen from the corpus sweep documented in the module docstring:
#: the measured corpus maximum is 0.283 and every known-bad parse is >= 0.40, so
#: 1/3 fires on zero healthy reads today and on all four known-bad ones.  It is
#: a threshold on the *frame*, not on the column, precisely so that the 887
#: individually-empty raw columns scattered across healthy files stay silent.
_NULL_FRACTION_TRIGGER = 1.0 / 3.0


class NullReadError(RuntimeError):
    """A read (or a built table) came back structurally right and empty.

    Raised instead of the default warning when ``LSMS_READ_STRICT`` is set.
    """


class NullReadWarning(RuntimeWarning):
    """A read (or a built table) came back structurally right and empty.

    Its own class -- not a bare ``RuntimeWarning`` -- so callers, tests and CI
    can target it precisely:
    ``warnings.simplefilter("error", NullReadWarning)``.
    """


def _read_strict() -> bool:
    """Whether a null-content finding should RAISE rather than warn.

    Default is warn, for the same reason ``country._grain_strict`` defaults to
    warn: a guard that breaks a working corpus on the day it lands gets
    reverted, and a revert is how the bug class survives.  Its own lever rather
    than ``LSMS_GRAIN_STRICT`` -- a destroyed row and an empty column are
    different concerns and a maintainer must be able to ratchet them
    separately -- but identical spelling semantics, so knowing one is knowing
    both.
    """
    return os.environ.get(_READ_STRICT_ENV, "").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# Site R -- the reader.  What did the parser actually produce?
# ---------------------------------------------------------------------------

def _all_null_columns(df: pd.DataFrame) -> list[str]:
    """Columns of *df* in which every value is missing.

    ``isna`` and not a truthiness test: an empty *string* is a value the survey
    recorded, and calling it missing would be this module inventing a defect.
    """
    out: list[str] = []
    for col in df.columns:
        try:
            if df[col].isna().all():
                out.append(str(col))
        except (TypeError, ValueError):
            # An exotic dtype whose isna() cannot be reduced.  Not evidence of
            # emptiness -- say nothing rather than guess.
            continue
    return out


def _context_from_path(source: Any) -> tuple[str | None, str | None]:
    """Best-effort ``(country, wave)`` for a source path.

    The reader is handed a path, not a survey identity, so the identity has to
    be recovered from the path -- ``.../countries/{Country}/{wave}/Data/...``.
    Best-effort by design: a report that names only the file is still
    actionable, and a wrong guess would be worse than no guess.  Never raises.
    """
    try:
        from .paths import countries_root
        root = Path(countries_root()).resolve()
        parts = Path(str(source)).resolve().relative_to(root).parts
    except (ImportError, OSError, ValueError, TypeError):
        return None, None
    country = parts[0] if len(parts) > 0 else None
    wave = parts[1] if len(parts) > 1 and parts[1] not in ("_", "Data") else None
    return country, wave


def audit_read(df: Any, source: Any, *, country: str | None = None,
               wave: str | None = None) -> dict[str, Any] | None:
    """Measure whether a freshly-read frame is structurally right and empty.

    Returns ``None`` when the read looks healthy -- which, per the corpus sweep,
    is every held source file in the library today.  Otherwise a report dict.

    Deliberately silent on:

    * a 0-row frame -- every column of an empty table is vacuously all-null, and
      an empty extract is a different defect with a different fix;
    * a frame with no columns;
    * anything that is not a ``DataFrame`` (``get_dataframe`` can hand back
      other shapes on some paths).
    """
    if not isinstance(df, pd.DataFrame):
        return None
    nrow, ncol = df.shape
    if nrow == 0 or ncol == 0:
        return None
    allnull = _all_null_columns(df)
    frac = len(allnull) / ncol
    if frac < _NULL_FRACTION_TRIGGER:
        return None
    if country is None and wave is None:
        country, wave = _context_from_path(source)
    return {
        "site": "get_dataframe",
        "country": country,
        "wave": wave,
        "table": None,
        "source": str(source),
        "rows": int(nrow),
        "columns": int(ncol),
        "null_columns": allnull,
        "null_fraction": round(frac, 4),
    }


# ---------------------------------------------------------------------------
# Site B -- the delivered table.  Is a column the schema DECLARES empty?
# ---------------------------------------------------------------------------

def audit_declared_columns(df: Any, required: list[str], *, country: str,
                           table: str) -> list[dict[str, Any]]:
    """Measure which *required* declared columns of a built table hold nothing.

    Two grains, because they mean different things:

    * **whole** -- the column is 100% null across every wave.  For a column the
      country's ``data_scheme.yml`` declares REQUIRED that is a defect by
      construction: the schema says the country has this, and it does not.
    * **waves** -- the column is 100% null within one wave's ``t`` slice while
      other waves carry it.  This is the Niger 2014-15 ``Latitude`` shape, and
      it is invisible to every whole-table check.

    Only REQUIRED columns are examined.  ``optional: true`` is the author's own
    signed statement that the column may be absent for this country -- honouring
    it here is what keeps the signal readable, and it is the same reading
    ``_required_scheme_columns`` gives its two existing callers.
    """
    if not isinstance(df, pd.DataFrame) or df.empty or not required:
        return []
    cols = [c for c in required if c in df.columns]
    if not cols:
        return []

    reports: list[dict[str, Any]] = []
    present = df[cols].notna()
    whole = [c for c in cols if not bool(present[c].any())]

    per_wave: dict[str, list[str]] = {}
    partial = [c for c in cols if c not in whole]
    names = list(df.index.names or [])
    if partial and "t" in names:
        try:
            by_wave = present[partial].groupby(
                df.index.get_level_values("t"), observed=True).any()
        except (TypeError, ValueError, KeyError):
            by_wave = None
        if by_wave is not None:
            for t, row in by_wave.iterrows():
                empty = [c for c in partial if not bool(row[c])]
                if empty:
                    per_wave[str(t)] = empty

    for col in whole:
        reports.append({
            "site": "built-table",
            "country": country,
            "table": table,
            "column": col,
            "scope": "whole",
            "waves": None,
            "rows": int(len(df)),
        })
    by_col: dict[str, list[str]] = {}
    for t, empty in per_wave.items():
        for col in empty:
            by_col.setdefault(col, []).append(t)
    for col, waves in sorted(by_col.items()):
        reports.append({
            "site": "built-table",
            "country": country,
            "table": table,
            "column": col,
            "scope": "waves",
            "waves": sorted(waves),
            "rows": int(len(df)),
        })
    return reports


# ---------------------------------------------------------------------------
# Reporting -- the single choke point, mirroring country._emit_grain_report
# ---------------------------------------------------------------------------

def _format_null_read_report(report: dict[str, Any]) -> str:
    if report.get("site") == "get_dataframe":
        where = "/".join(str(x) for x in
                         (report.get("country"), report.get("wave")) if x)
        where = where or "?"
        shown = report["null_columns"][:12]
        more = len(report["null_columns"]) - len(shown)
        return (
            f"{where}: read of {report['source']} returned "
            f"{report['rows']:,} row(s) x {report['columns']} column(s) but "
            f"{len(report['null_columns'])} of those columns "
            f"({report['null_fraction']:.0%}) are 100% NULL: "
            f"{shown}{f' (+{more} more)' if more else ''}. "
            f"A frame of the right SHAPE with no CONTENT is the signature of a "
            f"reader that misread the file rather than of empty data -- a "
            f"fixed-width parse of a delimited file, or an XPORT reader "
            f"mis-striding the field table. Nothing raised, so this would "
            f"otherwise be served silently. Check which branch of "
            f"local_tools.read_file claimed this file and whether it is the "
            f"right one. Set {_READ_STRICT_ENV}=1 to make this fatal."
        )
    scope = report.get("scope")
    where = f"{report.get('country')}/{report.get('table')}"
    if scope == "whole":
        detail = (
            f"is 100% NULL across all {report['rows']:,} row(s) of the built "
            f"table -- every wave."
        )
    else:
        waves = ", ".join(report.get("waves") or [])
        detail = (
            f"is 100% NULL in wave(s) [{waves}] while other waves carry it."
        )
    return (
        f"{where}: required declared column '{report.get('column')}' {detail} "
        f"The column is PRESENT, so the shape guards "
        f"(Country._assert_built_required_columns, the dfs: sub-df check) all "
        f"pass; it just holds nothing. Either the wave's data_info.yml does not "
        f"wire the column (look for a sibling source file that carries it -- "
        f"that has been the answer more often than 'the survey never asked'), "
        f"or the source column it names is itself empty. If the data genuinely "
        f"does not exist for this country, mark the column `optional: true` in "
        f"{report.get('country')}/_/data_scheme.yml -- but note `optional:` is "
        f"COUNTRY-grain while this finding may be WAVE-grain, so do not reach "
        f"for it to silence a single wave. Set {_READ_STRICT_ENV}=1 to make "
        f"this fatal."
    )


# Reports filed during this process, keyed by (country, table).  A module-level
# ledger rather than ``df.attrs`` for the reason GH #323 documents: pandas drops
# ``attrs`` across merge/set_index/groupby, so routing a finding through attrs
# would silently lose exactly the findings this exists to surface.
_NULL_READ_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def _record_null_read_report(report: dict[str, Any]) -> None:
    """File a report for :func:`null_read_reports`, then emit it."""
    key = (str(report.get("country") or "?"), str(report.get("table") or "?"))
    existing = _NULL_READ_LEDGER.setdefault(key, [])
    if report not in existing:
        existing.append(report)
    _emit_null_read_report(report)


def _emit_null_read_report(report: dict[str, Any]) -> None:
    """Raise (strict) or warn (default).  The single choke point."""
    msg = _format_null_read_report(report)
    if _read_strict():
        raise NullReadError(msg)
    warnings.warn(msg, NullReadWarning, stacklevel=2)


def null_read_reports(country: str | None = None,
                      table: str | None = None) -> list[dict]:
    """Null-content reports filed during this process.

    Public read-only accessor -- the twin of ``country.grain_reports`` -- for
    tests, for ``bench/feature_audit``, and for a user who wants to assert that
    the frame they are about to analyse actually contains something.

    Site-R reports (from ``get_dataframe``) carry ``table=None``, so they are
    filed under the ``"?"`` table key and are returned by a ``country=``-only
    query.
    """
    out: list[dict[str, Any]] = []
    for (c, t), reports in _NULL_READ_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


# ---------------------------------------------------------------------------
# The two one-line entry points the framework calls.  Kept here rather than
# inlined at the call sites so that the whole audit -- predicate, threshold,
# message, ledger -- lives in one file, and so that editing any of it does not
# touch a build-path function's source (see the CACHE note below).
#
# CACHE.  ``_build_registry`` fingerprints the SOURCE of every build-path
# callable into the L2 parquet hash, and recurses into the lsms_library
# callables they reference.  Both entry points are therefore listed in
# ``_build_registry._EXCLUDED_CALLABLES``: they are pure reporting and provably
# cannot change a returned value, so folding them in would rebuild the entire
# corpus every time a warning's wording changed -- the same over-invalidation
# ``Country._finalize_result`` is excluded to avoid.  MEASURED, both ways: with
# the exclusion, editing this module moves no table's cache hash; without it,
# every table in every country moves.
# ---------------------------------------------------------------------------

def check_read(df: Any, source: Any) -> Any:
    """Site R.  Audit a freshly-read frame; warn (or raise) if it is empty.

    Returns *df* unchanged -- always, and by contract.  Call it for its effect,
    not its value.
    """
    try:
        report = audit_read(df, source)
    except NullReadError:
        raise
    except Exception as exc:                                   # noqa: BLE001
        # An instrument that fails silently and reports clean is the disease
        # this module exists to cure, so say so rather than swallow it.  A
        # broken audit must never break a read, though: the read is the job.
        warnings.warn(
            f"null-read audit could not run on {source}: "
            f"{type(exc).__name__}: {exc}. The read itself is unaffected, but "
            f"this file is NOT known to be well-parsed.",
            NullReadWarning, stacklevel=2)
        return df
    if report is not None:
        _record_null_read_report(report)
    return df


def check_declared_columns(df: Any, required: list[str], *, country: str,
                           table: str) -> Any:
    """Site B.  Audit a built table's required declared columns for emptiness.

    Returns *df* unchanged -- always, and by contract.
    """
    try:
        reports = audit_declared_columns(df, required, country=country,
                                         table=table)
    except NullReadError:
        raise
    except Exception as exc:                                   # noqa: BLE001
        warnings.warn(
            f"{country}/{table}: null-content audit of declared columns could "
            f"not run ({type(exc).__name__}: {exc}). The table itself is "
            f"unaffected, but its declared columns are NOT known to be "
            f"populated.",
            NullReadWarning, stacklevel=2)
        return df
    for report in reports:
        _record_null_read_report(report)
    return df


def _clear_null_read_reports() -> None:
    """Drop every filed report.  For tests only -- the ledger is process-wide
    and a test that asserts on it must not inherit another test's findings."""
    _NULL_READ_LEDGER.clear()
