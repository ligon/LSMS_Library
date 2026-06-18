#!/usr/bin/env python
"""Deterministic wide-net scanner for the cross-country Feature() audit.

Phases 0-2 of the Feature-audit workflow (see ``bench/feature_audit/README.md``).
This script does *no* agentic judgement: it builds every ``Feature('foo')`` (and
a per-feature grid of ``Feature('foo')(**kwargs)``), runs the existing
``diagnostics.is_this_feature_sane`` per country plus a set of cross-country
*assembly invariants*, and writes one JSON record per check to ``results.jsonl``.

The warn/fail/error subset of that file is the candidate-finding list that the
agentic triage + red-team phases (the Workflow script) consume.  Every record
carries a stable ``fingerprint`` so the filing phase can dedup against GitHub.

Design notes
------------
* **No silent caps.**  Every (feature, country, kwargs) cell we *skip* is logged
  with a reason, so "covered everything" never reads as a lie.
* **Flag, don't judge.**  Row-count mismatches, ~100% NaN under a units mode,
  and reaggregation total drift are emitted as ``warn`` with the numbers
  attached.  Whether each is a real bug or by-design (units' documented
  "no silent fallback", the GH#501 additive-collapse, no-microdata countries)
  is the red-team's call, not ours.
* Couples to a few private helpers in ``lsms_library.feature`` /
  ``lsms_library.diagnostics`` (canonical index levels, property-feature set).
  Acceptable for an internal bench tool; they are stable within the repo.

Usage
-----
    # smoke test: one feature, two countries, warm cache
    python bench/feature_audit/scan.py --features food_prices \
        --countries Uganda Malawi --out /tmp/fa_smoke.jsonl

    # full deterministic sweep (heavy; cold for authority -> from a neutral CWD)
    LSMS_NO_CACHE=1 python bench/feature_audit/scan.py --features canonical \
        --out bench/feature_audit/results/$(date +%F)/results.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
import warnings
from dataclasses import asdict, dataclass, field
from multiprocessing import TimeoutError as MPTimeoutError, get_context
from typing import Any, Callable

# Pin per-process numerical thread pools to 1 BEFORE importing numpy/pandas.
# With N parallel build workers we want N cores doing N independent country
# builds, NOT one country fanning a BLAS op across all cores (oversubscription).
# setdefault: respect an explicit override from the environment.
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, "1")

import pandas as pd

import lsms_library as ll
from lsms_library import diagnostics
from lsms_library.diagnostics import (
    is_this_feature_sane,
    load_feature,
    _PROPERTY_FEATURES,
)
from lsms_library.feature import (
    _all_known_features,
    _canonical_index_levels,
    _DERIVED_SOURCE,
)

# ---------------------------------------------------------------------------
# Scope: which features, and which kwargs to vary per feature
# ---------------------------------------------------------------------------

# Curated canonical set (registered tables in data_info.yml Index Info + the
# runtime-derived tables).  `--features all` widens to _all_known_features().
CANONICAL_FEATURES = [
    "household_roster",
    "household_characteristics",
    "cluster_features",
    "sample",
    "food_acquired",
    "food_expenditures",
    "food_prices",
    "food_quantities",
    "plot_features",
    "interview_date",
    "shocks",
    "assets",
    "individual_education",
]

# Per-feature kwarg grid for Phase 2.  Each entry is ONE forwarded-kwarg call;
# the no-kwarg baseline is always exercised in Phase 1b.  Kept deliberately
# narrow to the documented "interesting" axes (units modes, Aggregate relabel,
# market widening, an age-cut) rather than a full Cartesian product.
KWARG_GRID: dict[str, list[dict[str, Any]]] = {
    "food_prices": (
        [{"units": u} for u in ("kgvalue", "unitvalue", "kgprice", "unitprice")]
        + [{"labels": "Aggregate"}, {"market": "Region"}]
    ),
    "food_quantities": (
        [{"units": u} for u in ("kgs", "units")]
        + [{"labels": "Aggregate"}, {"market": "Region"}]
    ),
    "food_expenditures": [{"labels": "Aggregate"}, {"market": "Region"}],
    "food_acquired": [{"labels": "Aggregate"}],
    "assets": [{"labels": "Aggregate"}],
    "household_characteristics": [{"age_cuts": [0, 5, 15, 60]}, {"market": "Region"}],
    "household_roster": [{"market": "Region"}],
}

# Columns that are additive across countries -> a relabel/reaggregate must
# CONSERVE their grand total (Price is per-unit and must NOT).
ADDITIVE_MEASURES = ("Quantity", "Expenditure")

# Countries with no source microdata in the repo (CLAUDE.md "Countries Without
# Microdata").  Their per-feature builds don't fail fast -- they spin in
# fallback paths (Armenia's "manual aggregation", Nepal's make loop) -- so one
# of them stalls the whole feature.  Skipped up front and logged transparently.
# Override with --include-no-microdata.
NO_MICRODATA = {"Nepal", "Armenia"}

# Hard ceiling on a single country build (seconds).  A legitimate cold
# food_acquired build for a big country is minutes, so this is generous; it only
# trips on a genuine hang, after which the straggler worker is force-killed.
DEFAULT_BUILD_TIMEOUT = 600

# Substrings that classify a captured warning into an issue class + a quick
# "expected?" hint for triage.  (severity, expected_by_default)
WARNING_TAXONOMY: list[tuple[str, str, bool]] = [
    ("cross-country index collapsed", "B", False),   # GH#325 assembly defect
    ("does not accept", "A", True),                  # kwarg not supported by a country
    ("No data for", "A", True),                      # empty per-country frame
    ("Failed to load", "A", True),                   # per-country build error (often no-microdata)
    ("duplicate", "B", False),                       # GH#323 dup-collapse
]


def _classify_warning(msg: str) -> tuple[str, bool]:
    for needle, sev, expected in WARNING_TAXONOMY:
        if needle.lower() in msg.lower():
            return sev, expected
    return "C", False  # unknown warning -> treat as a silent-corruption candidate


# ---------------------------------------------------------------------------
# Finding record
# ---------------------------------------------------------------------------

@dataclass
class Finding:
    phase: str
    feature: str
    check: str
    status: str                       # pass | warn | fail | error
    country: str | None = None        # None => cross-country assembly
    kwargs: dict[str, Any] = field(default_factory=dict)
    severity: str | None = None       # A (loud) | B (assembly) | C (silent semantic)
    expected: bool | None = None      # provisional "by-design?" hint for triage
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def fingerprint(self) -> str:
        kw = ",".join(f"{k}={self.kwargs[k]}" for k in sorted(self.kwargs))
        return f"{self.feature}|{self.country or 'ALL'}|{kw}|{self.check}"

    def to_record(self) -> dict[str, Any]:
        rec = asdict(self)
        rec["fingerprint"] = self.fingerprint
        return rec


# ---------------------------------------------------------------------------
# Warning-capturing build helper
# ---------------------------------------------------------------------------

def _build(fn: Callable[[], Any]) -> tuple[Any, list[str], str | None, float]:
    """Run *fn*, returning (result, warnings, error_repr, seconds).

    Never raises: a build exception is returned as ``error_repr`` so one bad
    cell can't abort the sweep.
    """
    t0 = time.perf_counter()
    captured: list[str] = []
    result = None
    err = None
    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        try:
            result = fn()
        except Exception as e:  # noqa: BLE001 - sweep must continue
            err = f"{type(e).__name__}: {e}"
        captured = [str(w.message) for w in wlist]
    return result, captured, err, time.perf_counter() - t0


def _warnings_to_findings(
    phase: str, feature: str, captured: list[str], kwargs: dict[str, Any]
) -> list[Finding]:
    out = []
    for msg in captured:
        sev, expected = _classify_warning(msg)
        out.append(Finding(
            phase=phase, feature=feature, check="runtime_warning",
            status="warn", kwargs=dict(kwargs), severity=sev, expected=expected,
            detail=msg,
        ))
    return out


# ---------------------------------------------------------------------------
# Phase 1a -- per-country sanity (no kwargs)
# ---------------------------------------------------------------------------

def _scan_one_country(feature: str, name: str) -> tuple[list[Finding], str, int | None]:
    """Build ONE (country, feature), run sanity, return (findings, name, rows).

    Module-level and never-raising so it is safe to ship to a ProcessPoolExecutor
    worker.  ``rows`` is None unless a non-empty DataFrame was built (it feeds the
    Phase-1b row-conservation invariant).  Side effect: populates the country's
    L2 cache, which is exactly the parallel cache-warming we want.
    """
    findings: list[Finding] = []
    try:
        result, captured, err, secs = _build(lambda: load_feature(ll.Country(name), feature))
        findings += _warnings_to_findings("1a-sanity", feature, captured, {})

        if err is not None:
            findings.append(Finding(
                phase="1a-sanity", feature=feature, country=name, check="build",
                status="error", severity="A", expected=False, detail=err,
                metrics={"seconds": round(secs, 2)},
            ))
            return findings, name, None

        if feature in _PROPERTY_FEATURES:
            # panel_ids / updated_ids return dicts -> record load, skip df checks.
            n = len(result) if hasattr(result, "__len__") else 0
            findings.append(Finding(
                phase="1a-sanity", feature=feature, country=name,
                check="build", status="pass", detail=f"property loaded ({n} entries)",
                metrics={"entries": n, "seconds": round(secs, 2)},
            ))
            return findings, name, None

        if not isinstance(result, pd.DataFrame) or result.empty:
            findings.append(Finding(
                phase="1a-sanity", feature=feature, country=name, check="build",
                status="fail", severity="A", expected=False,
                detail="empty / non-DataFrame result",
            ))
            return findings, name, None

        report = is_this_feature_sane(result, name, feature)
        for c in report.checks:
            if c.status == "pass":
                continue
            findings.append(Finding(
                phase="1a-sanity", feature=feature, country=name, check=c.name,
                status=c.status, severity="B" if c.status == "fail" else "C",
                expected=False, detail=c.message,
            ))
        return findings, name, len(result)
    except Exception as e:  # noqa: BLE001 - a worker must never crash the pool
        findings.append(Finding(
            phase="1a-sanity", feature=feature, country=name, check="worker",
            status="error", severity="A", expected=False,
            detail=f"worker crashed: {type(e).__name__}: {e}",
        ))
        return findings, name, None


def phase1a_country_sanity(
    feature: str, countries: list[str], jobs: int = 1,
    build_timeout: int = DEFAULT_BUILD_TIMEOUT,
) -> tuple[list[Finding], dict[str, int]]:
    """Build each country's table alone and run is_this_feature_sane.

    ``countries`` is assumed already filtered of no-microdata countries by the
    caller (``run``), since Phase 1b/2 share that filter.  With ``jobs > 1`` the
    per-country builds fan out across a ``multiprocessing`` pool — safe because
    v0.7.3 reads are lock-free (direct S3) and each country writes a distinct L2
    cache path, so concurrent builds of different countries don't contend.  Each
    build is bounded by ``build_timeout`` and a straggler is force-killed via
    ``pool.terminate()`` so one hang can't block the feature.

    Returns (findings, per_country_rowcount); the row counts feed the Phase-1b
    conservation invariant.
    """
    findings: list[Finding] = []
    rows: dict[str, int] = {}
    targets = countries

    if jobs > 1 and len(targets) > 1:
        # fork: workers inherit the already-imported package + the BLAS=1 env,
        # so no re-import cost and no thread oversubscription.
        pool = get_context("fork").Pool(processes=min(jobs, len(targets)))
        try:
            ars = {c: pool.apply_async(_scan_one_country, (feature, c)) for c in targets}
            for c, ar in ars.items():
                try:
                    f_list, name, n = ar.get(timeout=build_timeout)
                    findings += f_list
                    if n is not None:
                        rows[name] = n
                except MPTimeoutError:
                    findings.append(Finding(
                        phase="1a-sanity", feature=feature, country=c,
                        check="build_timeout", status="error", severity="A",
                        expected=False,
                        detail=f"build exceeded {build_timeout}s — killed (likely hang)",
                    ))
                except Exception as e:  # noqa: BLE001 - worker died unexpectedly
                    findings.append(Finding(
                        phase="1a-sanity", feature=feature, country=c,
                        check="worker", status="error", severity="A", expected=False,
                        detail=f"worker died: {type(e).__name__}: {e}",
                    ))
        finally:
            pool.terminate()   # force-kill any straggler still spinning
            pool.join()
    else:
        for name in targets:
            f_list, name, n = _scan_one_country(feature, name)
            findings += f_list
            if n is not None:
                rows[name] = n
    return findings, rows


# ---------------------------------------------------------------------------
# Phase 1b -- cross-country assembly invariants (no kwargs)
# ---------------------------------------------------------------------------

def _assembly_invariants(
    phase: str, feature: str, result: Any, per_country_rows: dict[str, int],
    kwargs: dict[str, Any], check_rows: bool = True,
) -> list[Finding]:
    """Cross-country assembly invariants.

    We deliberately do NOT assert exact index-name equality: ``Feature``
    legitimately widens the canonical index (a ``currency`` level for monetary
    tables under the default ``currency='index'``, an ``m`` level under
    ``market=``) and ``market`` drops ``v``.  Reproducing that logic here would
    just re-implement ``__call__``.  Instead we check the two things that are
    unambiguously wrong: an UNNAMED level (full or partial GH#325 collapse) and
    a MISSING canonical core level.
    """
    findings: list[Finding] = []
    canon = _canonical_index_levels(feature)

    if not isinstance(result, pd.DataFrame) or result.empty:
        findings.append(Finding(
            phase=phase, feature=feature, check="assembly_nonempty",
            status="fail", severity="B", expected=False, kwargs=dict(kwargs),
            detail="Feature() returned empty / non-DataFrame",
        ))
        return findings

    names = list(result.index.names)

    # (1) any unnamed index level -> full (GH#325) or partial collapse
    if None in names:
        findings.append(Finding(
            phase=phase, feature=feature, check="index_unnamed_level",
            status="fail", severity="B", expected=False, kwargs=dict(kwargs),
            detail=f"cross-country index has unnamed level(s): {names}",
            metrics={"names": names, "nrows": len(result)},
        ))

    # (2) a canonical core level went missing (market legitimately drops v)
    core = set(canon)
    if kwargs.get("market") is not None:
        core.discard("v")
    missing = core - set(names)
    if missing:
        findings.append(Finding(
            phase=phase, feature=feature, check="missing_canonical_level",
            status="warn", severity="B", expected=False, kwargs=dict(kwargs),
            detail=f"canonical level(s) {sorted(missing)} absent from index {names}",
            metrics={"missing": sorted(missing), "names": names},
        ))

    # (3) market widening actually added m
    if kwargs.get("market") is not None:
        ok_m = "m" in names
        findings.append(Finding(
            phase=phase, feature=feature, check="market_adds_m",
            status="pass" if ok_m else "fail",
            severity=None if ok_m else "B", expected=None if ok_m else False,
            kwargs=dict(kwargs),
            detail=f"m in index = {ok_m}; index = {names}",
        ))

    # (4) row-count conservation vs sum of per-country builds.  Only meaningful
    # for the no-kwarg assembly pass: kwarg variants (units modes especially)
    # legitimately change cardinality, so a delta there is kwarg semantics, not
    # an assembly defect -- captured separately in phase2_kwargs.
    if check_rows and per_country_rows:
        present = [c for c in per_country_rows if c in result.index.get_level_values("country").unique()] \
            if "country" in names else list(per_country_rows)
        expected_rows = sum(per_country_rows[c] for c in present)
        got = len(result)
        if got < expected_rows:
            findings.append(Finding(
                phase=phase, feature=feature, check="row_conservation",
                status="warn", severity="B", expected=False, kwargs=dict(kwargs),
                detail=(f"assembled {got} rows < sum of per-country {expected_rows} "
                        f"({expected_rows - got} dropped via harmonize/collapse — "
                        f"triage losslessness)"),
                metrics={"assembled": got, "sum_parts": expected_rows,
                         "dropped": expected_rows - got},
            ))

    # (5) columns that went entirely NaN after the concat
    for col in result.columns:
        if result[col].isna().all():
            findings.append(Finding(
                phase=phase, feature=feature, country=None, check="all_nan_column",
                status="warn", severity="B", expected=False, kwargs=dict(kwargs),
                detail=f"column {col!r} is entirely NaN after assembly",
                metrics={"column": col},
            ))
    return findings


def phase1b_assembly(feature: str, countries: list[str], per_country_rows: dict[str, int]) -> tuple[list[Finding], Any]:
    if feature in _PROPERTY_FEATURES:
        return [], None  # properties don't assemble cross-country
    result, captured, err, secs = _build(lambda: ll.Feature(feature)(countries))
    findings = _warnings_to_findings("1b-assembly", feature, captured, {})
    if err is not None:
        findings.append(Finding(
            phase="1b-assembly", feature=feature, check="assembly_build",
            status="error", severity="B", expected=False, detail=err,
        ))
        return findings, None
    findings += _assembly_invariants("1b-assembly", feature, result, per_country_rows, {})
    return findings, result


# ---------------------------------------------------------------------------
# Phase 2 -- kwarg cross-product
# ---------------------------------------------------------------------------

def _grand_totals(df: pd.DataFrame) -> dict[str, float]:
    out = {}
    for col in ADDITIVE_MEASURES:
        if col in df.columns:
            out[col] = float(pd.to_numeric(df[col], errors="coerce").sum())
    return out


def _nan_fractions(df: pd.DataFrame) -> dict[str, float]:
    out = {}
    for col in df.select_dtypes(include="number").columns:
        out[col] = round(float(df[col].isna().mean()), 4)
    return out


def _scan_one_kwarg(feature: str, countries: list[str], kwargs: dict[str, Any],
                    base_totals: dict[str, float], base_nan: dict[str, float],
                    base_len: int | None,
                    per_country_rows: dict[str, int]) -> list[Finding]:
    """Build ONE Feature(feature)(countries, **kwargs) and check it against the
    no-kwarg baseline summaries.  Module-level + never-raising so it ships to a
    pool worker; baseline *summaries* (totals/nan-fracs/len) are passed instead
    of the baseline DataFrame, which is too big to pickle and not needed here.
    """
    findings: list[Finding] = []
    try:
        result, captured, err, secs = _build(lambda: ll.Feature(feature)(countries, **kwargs))
        findings += _warnings_to_findings("2-kwargs", feature, captured, kwargs)
        if err is not None:
            findings.append(Finding(
                phase="2-kwargs", feature=feature, check="kwarg_build",
                status="error", severity="C", expected=False, kwargs=dict(kwargs),
                detail=err,
            ))
            return findings

        findings += _assembly_invariants("2-kwargs", feature, result,
                                          per_country_rows, kwargs, check_rows=False)
        if not isinstance(result, pd.DataFrame) or result.empty:
            return findings

        # kwarg changing cardinality vs the no-kwarg assembled baseline is a
        # semantics signal (e.g. units price-modes drop unreported-price rows).
        if base_len is not None and len(result) != base_len:
            delta = len(result) - base_len
            findings.append(Finding(
                phase="2-kwargs", feature=feature, check="kwarg_changes_rowcount",
                status="warn", severity="C", expected=None, kwargs=dict(kwargs),
                detail=(f"row count {base_len} -> {len(result)} ({delta:+d}) "
                        f"under this kwarg vs no-kwarg baseline"),
                metrics={"baseline": base_len, "got": len(result), "delta": delta},
            ))

        # units mode that blows NaN-rate from <99% to ~100% on a value column
        if "units" in kwargs:
            for col, frac in _nan_fractions(result).items():
                base = base_nan.get(col, 0.0)
                if frac > 0.99 and base <= 0.99:
                    findings.append(Finding(
                        phase="2-kwargs", feature=feature, check="units_all_nan",
                        status="warn", severity="C", expected=None, kwargs=dict(kwargs),
                        detail=(f"units={kwargs['units']!r}: {col!r} is {frac:.1%} NaN "
                                f"(baseline {base:.1%}) — by-design no-fallback or bug?"),
                        metrics={"column": col, "nan_frac": frac, "baseline_nan_frac": base},
                    ))

        # labels='Aggregate' (reaggregate) must conserve additive grand totals
        if kwargs.get("labels") == "Aggregate" and base_totals:
            for col, base_tot in base_totals.items():
                got = _grand_totals(result).get(col)
                if got is None or base_tot == 0:
                    continue
                drift = abs(got - base_tot) / abs(base_tot)
                if drift > 1e-3:
                    findings.append(Finding(
                        phase="2-kwargs", feature=feature, check="reaggregate_conservation",
                        status="warn", severity="C", expected=False, kwargs=dict(kwargs),
                        detail=(f"labels='Aggregate' changed {col} grand total by "
                                f"{drift:.2%} ({base_tot:.0f} -> {got:.0f})"),
                        metrics={"column": col, "baseline": base_tot, "got": got,
                                 "rel_drift": drift},
                    ))
        return findings
    except Exception as e:  # noqa: BLE001 - worker must not crash the pool
        findings.append(Finding(
            phase="2-kwargs", feature=feature, check="worker", status="error",
            severity="C", expected=False, kwargs=dict(kwargs),
            detail=f"worker crashed: {type(e).__name__}: {e}",
        ))
        return findings


def phase2_kwargs(feature: str, countries: list[str], baseline: Any,
                  per_country_rows: dict[str, int], jobs: int = 1,
                  build_timeout: int = DEFAULT_BUILD_TIMEOUT) -> list[Finding]:
    """Exercise the per-feature kwarg grid.  Each variant is an independent warm
    Feature() rebuild + transform, so with ``jobs > 1`` the variants run
    concurrently in a pool — this is the dominant cost on the food features
    (6 units/label/market passes over ~40 countries back-to-back when serial).
    """
    grid = KWARG_GRID.get(feature, [])
    if not grid:
        return []
    base_totals = _grand_totals(baseline) if isinstance(baseline, pd.DataFrame) else {}
    base_nan = _nan_fractions(baseline) if isinstance(baseline, pd.DataFrame) else {}
    base_len = len(baseline) if isinstance(baseline, pd.DataFrame) else None

    if jobs > 1 and len(grid) > 1:
        findings: list[Finding] = []
        pool = get_context("fork").Pool(processes=min(jobs, len(grid)))
        try:
            ars = [(kw, pool.apply_async(_scan_one_kwarg,
                    (feature, countries, kw, base_totals, base_nan, base_len, per_country_rows)))
                   for kw in grid]
            for kw, ar in ars:
                try:
                    findings += ar.get(timeout=build_timeout)
                except MPTimeoutError:
                    findings.append(Finding(
                        phase="2-kwargs", feature=feature, check="kwarg_timeout",
                        status="error", severity="C", expected=False, kwargs=dict(kw),
                        detail=f"kwarg build exceeded {build_timeout}s — killed",
                    ))
                except Exception as e:  # noqa: BLE001
                    findings.append(Finding(
                        phase="2-kwargs", feature=feature, check="worker",
                        status="error", severity="C", expected=False, kwargs=dict(kw),
                        detail=f"worker died: {type(e).__name__}: {e}",
                    ))
        finally:
            pool.terminate()
            pool.join()
        return findings

    findings = []
    for kw in grid:
        findings += _scan_one_kwarg(feature, countries, kw, base_totals, base_nan,
                                    base_len, per_country_rows)
    return findings


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def resolve_features(spec: list[str]) -> list[str]:
    if spec == ["canonical"]:
        return list(CANONICAL_FEATURES)
    if spec == ["all"]:
        return sorted(_all_known_features())
    return spec


def declaring_countries(feature: str, country_filter: list[str] | None) -> list[str]:
    declared = ll.Feature(feature).countries
    if country_filter:
        keep = [c for c in declared if c in country_filter]
        return keep
    return declared


def run(features: list[str], country_filter: list[str] | None,
        phases: set[str], limit_countries: int | None,
        out_path: str, jobs: int = 1,
        build_timeout: int = DEFAULT_BUILD_TIMEOUT,
        include_no_microdata: bool = False) -> None:
    """Stream one JSON record per check to *out_path* as each feature completes.

    Writing incrementally (and flushing per feature) means a multi-hour sweep is
    crash-safe and tail-able for live progress, instead of producing nothing
    until the very end.
    """
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    by_status: dict[str, int] = {}
    by_sev: dict[str, int] = {}
    n_records = 0
    n_skips = 0

    with open(out_path, "w", encoding="utf-8") as fh:
        def emit(findings: list[Finding]) -> None:
            nonlocal n_records
            for f in findings:
                fh.write(json.dumps(f.to_record()) + "\n")
                n_records += 1
                by_status[f.status] = by_status.get(f.status, 0) + 1
                if f.status in ("warn", "fail", "error"):
                    by_sev[f.severity or "?"] = by_sev.get(f.severity or "?", 0) + 1
            fh.flush()

        def skip(feature: str, reason: str) -> None:
            nonlocal n_skips
            fh.write(json.dumps({"phase": "0-scope", "status": "skip",
                                 "feature": feature, "reason": reason}) + "\n")
            fh.flush()
            n_skips += 1

        for idx, feature in enumerate(features, 1):
            try:
                countries = declaring_countries(feature, country_filter)
            except ValueError as e:  # unknown feature name
                skip(feature, str(e))
                continue
            if limit_countries:
                countries = countries[:limit_countries]
            # Drop known no-microdata countries BEFORE any phase: Feature() (used
            # by Phase 1b/2) has no skip list and would spin on them in the main
            # process.  Logged per (feature, country) so coverage stays honest.
            excluded = set() if include_no_microdata else NO_MICRODATA
            dropped = [c for c in countries if c in excluded]
            countries = [c for c in countries if c not in excluded]
            for c in dropped:
                skip(feature, f"{c}: known no-source-data country (CLAUDE.md)")
            if not countries:
                skip(feature, "no declaring country in filter")
                continue

            t0 = time.perf_counter()
            print(f"[{idx}/{len(features)} {feature}] {len(countries)} countries"
                  f"{f' (skipped {len(dropped)})' if dropped else ''}: "
                  f"{', '.join(countries)}", file=sys.stderr, flush=True)

            per_country_rows: dict[str, int] = {}
            baseline = None
            before = n_records
            if "1" in phases:
                f1a, per_country_rows = phase1a_country_sanity(
                    feature, countries, jobs, build_timeout)
                emit(f1a)
                f1b, baseline = phase1b_assembly(feature, countries, per_country_rows)
                emit(f1b)

            if "2" in phases:
                if baseline is None and feature not in _PROPERTY_FEATURES:
                    baseline, *_ = _build(lambda: ll.Feature(feature)(countries))
                emit(phase2_kwargs(feature, countries, baseline, per_country_rows,
                                   jobs, build_timeout))

            print(f"    done in {time.perf_counter() - t0:.0f}s  "
                  f"(+{n_records - before} records)", file=sys.stderr, flush=True)

    print("\n=== feature-audit scan summary ===", file=sys.stderr)
    print(f"features: {len(features)}  records: {n_records}  skips: {n_skips}",
          file=sys.stderr)
    print(f"by status: {by_status}", file=sys.stderr)
    print(f"candidate findings (warn/fail/error) by severity class: {by_sev}",
          file=sys.stderr)
    print(f"-> {out_path}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--features", nargs="+", default=["canonical"],
                    help="feature names, or 'canonical' (default) / 'all'")
    ap.add_argument("--countries", nargs="*", default=None,
                    help="restrict to these countries (intersect with declarers)")
    ap.add_argument("--phases", default="1,2",
                    help="comma list of phases to run (1,2). default 1,2")
    ap.add_argument("--limit-countries", type=int, default=None,
                    help="cap countries per feature (smoke testing)")
    ap.add_argument("--out", default="bench/feature_audit/results.jsonl")
    ap.add_argument("--jobs", "-j", type=int, default=1,
                    help="parallel Phase-1a country builds (process pool). "
                         "Size to PHYSICAL cores (e.g. 24 on a 32-core node). "
                         "default 1 = sequential")
    ap.add_argument("--build-timeout", type=int, default=DEFAULT_BUILD_TIMEOUT,
                    help=f"per-country build hard ceiling, seconds "
                         f"(default {DEFAULT_BUILD_TIMEOUT}); straggler is killed")
    ap.add_argument("--include-no-microdata", action="store_true",
                    help=f"don't skip the known no-source-data countries "
                         f"({', '.join(sorted(NO_MICRODATA))})")
    args = ap.parse_args(argv)

    features = resolve_features(args.features)
    phases = set(args.phases.replace(" ", "").split(","))
    skipped = "none" if args.include_no_microdata else ", ".join(sorted(NO_MICRODATA))
    print(f"feature-audit scan: {len(features)} features, jobs={args.jobs}, "
          f"BLAS threads/worker={os.environ.get('OMP_NUM_THREADS')}, "
          f"build_timeout={args.build_timeout}s, skip_no_microdata=[{skipped}]",
          file=sys.stderr)
    run(features, args.countries, phases, args.limit_countries, args.out, args.jobs,
        args.build_timeout, args.include_no_microdata)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
