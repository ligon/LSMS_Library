"""SITE I -- a NaN on a DECLARED INDEX LEVEL is a deferred silent deletion
(GH #847).

The library's content guards each check one thing and stop:

* ``Country._assert_built_required_columns`` / the ``dfs:`` sub-df check --
  is a declared COLUMN present?
* ``country._audit_index_collapse`` (GH #323) -- is the declared index
  **unique**?  It also counts the NaN-keyed rows its own collapse deletes
  (``nan_key_rows``), but only when a collapse actually runs.
* ``null_read_audit`` Sites R and B -- does a declared _column_ hold anything
  at all?
* ``quantity_audit`` Site Q (GH #857) -- is what it holds possible?

None of them looks at a PARTLY-null declared index level on the read path.
A row whose ``u`` is NaN is **served**: no guard fires, because the guards
look at columns, at uniqueness, and at values -- never at the keys.

WHAT HAPPENS NEXT, AND WHY THAT IS THE BUG.  The row vanishes in the FIRST
``groupby`` anything reaches -- pandas' ``groupby`` defaults to
``dropna=True``, so NaN keys are dropped, not merely merged:

* **in the framework**: ``_normalize_dataframe_index``'s
  ``groupby(level=...).first()`` / additive sum (Grain Collapse decision D2,
  2026-07-13, "delete-and-report").  Where a wave's index is non-unique the
  collapse runs ON the NaN-keyed rows and deletes them outright, counted by
  GH #323's audit.  This issue is the OTHER half: where the index is unique
  (Niger 2018-19's ``crop_production``), no collapse runs, the rows are served
  intact, and **nothing anywhere counts them**.
* **in the consumer's code**: any ``groupby(...)`` or ``pivot_table(...)`` the
  analyst runs silently drops the same rows, and the loss is attributed to
  their code, not to the table.

So whether a NaN-keyed defect is visible is decided by an accident of index
uniqueness: deleted-with-count on the collapse path, served-with-no-count at
the API.  Niger's own ``CONTENTS.org`` measured the difference: 2018-19
served all 441 of its NaN-``u`` ``crop_production`` rows because its
``(t, i, plot, crop, u)`` index happens to be unique; the other three waves'
indexes are non-unique by design (one ``(plot, crop)`` can carry several
reported harvest lines), so the collapse DELETED every NaN-keyed row before
anyone saw it.  GH #842's "441 rows" was what survived an accident; the wave
parquets held 5,663.

WHAT THIS DOES.  Counts, per level and per wave ``t``, the rows whose value
on a level the country's ``data_scheme.yml`` / canonical ``index_info``
declares is NaN; then **warns and names**.  That is all this module does, on
purpose:

* It **never fixes** -- filling a NaN key with a sentinel (Niger's
  ``'Unknown'``, landed at GH #842) or dropping-with-count is a country-level
  data decision.  A framework that silently fills invents a label; one that
  silently drops invents a deletion.  Either repairs the *symptom it was
  asked to report*.
* It **warns by default** and is made fatal by ``LSMS_READ_STRICT=1`` -- the
  SAME lever as ``null_read_audit`` (GH #699), because a NaN key is not a
  third kind of defect, it is the null-content class sitting on the index
  axis.  Pin by test; the lever must not ratchet into ``LSMS_GRAIN_STRICT``
  (destroyed rows) or ``LSMS_QUANTITY_STRICT`` (impossible values).
* **Deliberately no allowlist** -- the reason ``country._grain_strict`` gives:
  an allowlist is the same disease with a registry, and a legitimate NaN-key
  cell stays loud until the data or the config can say why.

THE TRIGGER, MEASURED (2026-09-13, ``bench/null_index_census.py`` -- reads
every warm L2-country parquet under ``data_root()`` and runs this module's own
:func:`audit_index_levels` with the same level-union the framework passes).
384 warm tables across the corpus:

* **19 cells fire**, over 14 countries.  The instances, every one of them a
  live defect this guard exists to name: **GhanaLSS ``food_acquired``
  u=13,380 rows** (2016-17 ``produced`` lines of Cassava / Maize / ... keyed
  with no unit), **GhanaLSS ``plot_features`` plot_id=13,944** (2012-13 +
  2016-17), **CotedIvoire ``crop_production`` u=6,365** (2018-19),
  **Malawi ``crop_production`` u=8,535 + plot_id=40**, **GhanaSPS
  ``food_acquired`` u=13,903** (2009-10 + 2017-18), plus a tail of 1-1,000-row
  cells (Niger's ``v=2`` 2011-12 -- the country fix at GH #842 covered the
  ``u`` axis, not the v-join's own sparse row).
* 61,351 (row, level) findings in total -- and 60,355 of them sit in the
  five cells above.  The signal is concentrated where it should be: a few
  real defects, not noise scattered over the corpus.

NO THRESHOLD and NO EXCLUSION were adopted.  The "any declared level NaN"
rule is already a work queue (19 cells, not hundreds), and every candidate
narrowing was measured and rejected:

* *excluding ``v``* would silence Niger's live 2011-12 instance -- the very
  class GH #847 names -- while saving nothing (only Azerbaijan's one
  1995 ``v`` row joins it);
* *excluding ``t``* has no corpus instance at all (0 cells carry a NaN ``t``
  level) -- the obvious worry, that the v-join or a mis-wired wave stamps a
  null wave label, is simply not what is broken;
* *a row-count floor* would silence Burkina Faso's one ``assets.j`` row and
  CotedIvoire's single ``pid`` while buying nothing the 19-cell count does
  not already give.

COST.  ``get_level_values(...).isna().sum()`` on the whole frame per declared
level: integer-code arithmetic behind a ``MultiIndex``, measured at
0.03-0.07 ms on Niger's 46k-row ``crop_production`` read and proportionally
on the largest frame in the corpus (GhanaLSS ``food_acquired``, 5.34M rows,
~0.4 ms).  Negligible beside the ~50 ms-2 s warm read it rides on; zero I/O.

CACHE.  Like ``null_read_audit`` this is pure reporting -- it provably returns
its input unchanged -- so its entry points are listed in
``_build_registry._EXCLUDED_CALLABLES`` and its ledger in
``_EXCLUDED_CONSTANTS``.  Measured both ways (stash-probe, 2026-09-13): with
the exclusions, adding this module and its call site moved **0 of 10** probed
``Country._table_cache_hash`` values (Niger x3, Uganda x3, GhanaLSS x2,
Malawi, Tanzania) and **0 of 3** ``build_transforms_fingerprint`` values
(``cluster_features``, ``food_acquired``, ``crop_production``).
"""

from __future__ import annotations

import os
import warnings
from typing import Any

import pandas as pd

__all__ = [
    "NullIndexKeyError",
    "NullIndexKeyWarning",
    "audit_index_levels",
    "check_index_levels",
    "null_index_reports",
]


_READ_STRICT_ENV = "LSMS_READ_STRICT"

#: How many (wave, level) findings one warning line carries before it says
#: "+N more".  The smallest wave is 33 of 40 countries' findings; a per-cell
#: line each is what a firehose LOOKS like, so findings merge into one line.
_MAX_NAMED_FINDINGS = 5


class NullIndexKeyError(RuntimeError):
    """A declared index level is present and partly empty.

    Raised instead of the default warning when ``LSMS_READ_STRICT`` is set --
    the SAME lever as ``null_read_audit`` (a NaN key is the null-content
    class sitting on the index axis; the three levers ratchet the destroyed,
    empty-column and impossible-value concerns, and the fourth folds into the
    second).
    """


class NullIndexKeyWarning(RuntimeWarning):
    """A declared index level is present and partly empty.

    Its own class -- not a bare ``RuntimeWarning`` -- so callers, tests and CI
    can target it precisely:
    ``warnings.simplefilter("error", NullIndexKeyWarning)``.
    """


def _read_strict() -> bool:
    """Whether a null-index-key finding should RAISE rather than warn.

    Warn by default, for the reason ``country._grain_strict`` and
    ``null_read_audit._read_strict`` both give: a guard that breaks a working
    corpus on the day it lands gets reverted, and a revert is how the bug
    class survives.  Shares ``LSMS_READ_STRICT`` with the all-null-column
    guard -- deliberately, because a NaN key on a declared level is the same
    null-content defect one axis over, not a fourth concern.
    """
    return os.environ.get(_READ_STRICT_ENV, "").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# The audit.  A row whose declared-index level is NaN is named per level,
# per wave.
# ---------------------------------------------------------------------------

def audit_index_levels(df: Any, declared: list[str] | tuple[str, ...],
                       *, country: str, table: str) -> list[dict[str, Any]]:
    """Counts NaN keys per declared level; one report per (wave, level).

    Returns ``[]`` on a frame with no declared level (or no levels named at
    all), on an empty frame, or -- the frequent, healthy case -- on a frame
    whose declared levels are all fully populated.

    The per-level ``n_nan`` is a strict count of dropped-key positions, one
    per row whose *that level* is NaN; a row NaN on two levels contributes
    twice.  ``waves`` carries the per-wave breakdown so a wave-gated fill
    (Niger GH #842) has per wave what it needs and a per-wave re-firing on
    the audit ledger settles to one report per wave anyway -- pandas'
    ``groupby`` drops NaN keys by default, so "count per wave" and "count
    per wave per level" differ only when a level is NaN in some waves and
    populated in others, and both readings are named.
    """
    if not isinstance(df, pd.DataFrame) or df.empty or not declared:
        return []
    levels = [lvl for lvl in declared if lvl in df.index.names]
    if not levels:
        return []

    names = list(df.index.names)
    has_t = "t" in names and len(names) > 1

    findings: list[dict[str, Any]] = []
    for level in levels:
        values = df.index.get_level_values(level)
        mask = values.isna()
        n_nan = int(mask.sum())
        if not n_nan:
            continue
        per_wave: dict[str, int] | None = None
        if has_t:
            try:
                t = df.index.get_level_values("t")
                series = pd.Series(mask, dtype=bool).groupby(t).sum()
                per_wave = {str(w): int(n)
                            for w, n in series.items() if int(n)}
            except (TypeError, ValueError, KeyError):
                per_wave = None
        findings.append({
            "level": level,
            "n_nan": n_nan,
            "waves": per_wave,
        })
    if not findings:
        return []
    return [{
        "site": "built-table-index",
        "country": country,
        "table": table,
        "rows": int(len(df)),
        "findings": findings,
    }]


# ---------------------------------------------------------------------------
# Reporting -- the single choke point, mirroring
# null_read_audit._emit_null_read_report
# ---------------------------------------------------------------------------

def _format_index_report(report: dict[str, Any]) -> str:
    findings = report["findings"]
    shown = findings[:_MAX_NAMED_FINDINGS]
    more = len(findings) - len(shown)
    listed = "; ".join(
        f"level '{f['level']}'" +
        (f" (wave(s) {list(f['waves'])} with {f['n_nan']:,} NaN row(s))"
         if f.get("waves") else f" (NaN on {f['n_nan']:,} of "
                                   f"{report['rows']:,} row(s))")
        for f in shown)
    return (
        f"{report['country']}/{report['table']}: declared index level(s) "
        f"carried NaN key(s): {listed}"
        f"{f' (+{more} more)' if more else ''}. "
        f"These row(s) are SERVED and will vanish in the first groupby "
        f"(pandas' groupby defaults to dropna=True): called a deletion when "
        f"_normalize_dataframe_index runs (GH #323's nan_key_rows), silent "
        f"data loss when only a consumer groupby runs (GH #847). Fix is "
        f"country-level (a sentinel like Niger's 'Unknown'/'Manquant' fixed "
        f"at GH #842 for the u axis), not this guard's. Set "
        f"{_READ_STRICT_ENV}=1 to make this fatal."
    )


# Reports filed during this process, keyed by (country, table).  A module-level
# ledger rather than ``df.attrs``, for the reason GH #323 documents: pandas
# drops ``attrs`` across merge/set_index/groupby, so routing a finding through
# attrs would silently lose exactly the findings this exists to surface.
_NULL_INDEX_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def _record_null_index_report(report: dict[str, Any]) -> None:
    """File a report for :func:`null_index_reports`, then emit it."""
    key = (str(report.get("country") or "?"), str(report.get("table") or "?"))
    existing = _NULL_INDEX_LEDGER.setdefault(key, [])
    if report not in existing:
        existing.append(report)
    _emit_null_index_report(report)


def _emit_null_index_report(report: dict[str, Any]) -> None:
    """Raise (strict) or warn (default).  The single choke point."""
    msg = _format_index_report(report)
    if _read_strict():
        raise NullIndexKeyError(msg)
    warnings.warn(msg, NullIndexKeyWarning, stacklevel=2)


def null_index_reports(country: str | None = None,
                       table: str | None = None) -> list[dict]:
    """NaN-index-key reports filed during this process.

    Public read-only accessor -- the twin of
    :func:`~lsms_library.null_read_audit.null_read_reports` and of
    ``country.grain_reports`` -- for tests, for ``bench/feature_audit``, and
    for the work queue on the repo dashboard.  Each report names the level(s)
    and, per wave, the count.
    """
    out: list[dict[str, Any]] = []
    for (c, t), reports in _NULL_INDEX_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


# ---------------------------------------------------------------------------
# The one-line entry point the framework calls.  See the CACHE note in the
# module docstring: this and :func:`audit_index_levels` are excluded from the
# build fingerprint because they provably cannot change a returned value.
# ---------------------------------------------------------------------------

def check_index_levels(df: Any, declared: list[str], *, country: str,
                       table: str) -> Any:
    """Site I.  Audit a built table's declared index levels for NaN keys.

    Returns *df* unchanged -- always, and by contract.  Call it for its
    effect, not its value.
    """
    if not declared:
        return df
    try:
        reports = audit_index_levels(df, declared, country=country,
                                     table=table)
    except NullIndexKeyError:
        raise
    except Exception as exc:                                   # noqa: BLE001
        # A broken audit must never break a read: the read is the job.  Say so
        # rather than swallow it -- the disease this module cures is a silent-
        # failure layer.
        warnings.warn(
            f"{country}/{table}: declared-index-level audit could not run "
            f"({type(exc).__name__}: {exc}). The table itself is unaffected, "
            f"but its declared index levels are NOT known to be populated.",
            NullIndexKeyWarning, stacklevel=2)
        return df
    for report in reports:
        _record_null_index_report(report)
    return df


def _clear_null_index_reports() -> None:
    """Drop every filed report.  For tests only -- the ledger is process-wide
    and a test that asserts on it must not inherit another test's findings."""
    _NULL_INDEX_LEDGER.clear()
