"""Country × Feature × Wave coverage / readiness model.

The importable core behind ``bench/matrix.py`` and the ``ll.coverage()`` reader.
Composes existing machinery — it does **not** reinvent sanity checks or the
declared matrix:

- axes / declared coverage -> :mod:`lsms_library.catalog` + ``Country.waves`` +
  ``Wave.data_scheme`` (config only; no builds, no auth);
- per-wave readiness -> :func:`diagnostics.load_feature` builds the
  country-level table once; we slice it on the ``t`` index level and run the
  existing :func:`diagnostics.is_this_feature_sane` per wave.

See `.coder/charter-coverage-matrix.md` and `.coder/ledger/coverage-matrix.md`.

Tier ladder (worst→best; each derived from existing machinery only):

=====================  ====================================================
tier                   meaning
=====================  ====================================================
n/a                    no per-wave readiness applies: a country-level-only
                       feature, or a table with no ``t`` axis
blocked                we WANT it, we KNOW where it is, and we CANNOT get it —
                       the acquisition channel is broken (Nepal: NSO site down;
                       Albania 1996).  A LIVE gap, not a settled one: visible,
                       out of the work queue, and RE-PROBED on every refresh.
                       See :func:`load_blocked`.
not-asked              adjudicated: the instrument genuinely never asked.
                       Closed.  *Requires evidence* (:func:`load_verdicts`).
asked-not-distributed  adjudicated: the instrument DID ask, but the shipped
                       extract does not carry the variables.  An acquisition
                       problem, not a config one.  *Requires evidence.*
absent                 feature applies to the country but its source is not
                       declared for this wave — and the gap is UN-ADJUDICATED
                       (or adjudicated ``todo``/``unsure``, which stay open).
                       This is the live queue.
declared               source present for the wave; readiness not assessed
                       (coverage-only run)
dropped                source declared for the wave but the wave is missing /
                       empty in the built table (the silent-drop bug class)
broken                 the country-level build raised, or the feature is empty
builds                 wave slice is non-empty but ``SanityReport.ok is False``
sane                   wave slice is non-empty and ``SanityReport.ok is True``
                       — i.e. the automated checks passed.  NOT a claim that
                       any human has looked at a single number.
blessed                ``sane`` **and** a human read the numbers and believed
                       them (git-tracked ``blessed.csv``).  Required for any
                       cell feeding published analysis.
=====================  ====================================================

Two rules the tiers exist to enforce, both learned the hard way:

- *An unevidenced negative is unfalsifiable, and therefore permanent whether or
  not it is true.*  So a closing verdict without evidence is refused outright.
- *A regrade is not a fix.*  When a change alters how cells are GRADED rather
  than what they CONTAIN, first inventory what the old grading was catching by
  accident (see GH #591, #592).
"""
from __future__ import annotations

import os
import warnings
from functools import lru_cache
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Tier ladder
# ---------------------------------------------------------------------------
TIER_ORDER = [
    "n/a", "not-asked", "asked-not-distributed", "blocked", "unconfigured",
    "absent", "declared", "dropped", "broken", "builds", "sane", "blessed",
]
# Worst → best, for rolling several wave tiers up into one grid cell. The most
# actionable (a real defect) sorts first so problems surface in the summary.
#
# ``absent`` (= an *un-adjudicated* gap) sorts as actionable, because it is the
# live queue.  ``not-asked`` and ``asked-not-distributed`` are *adjudicated* and
# sort with ``n/a`` at the quiet end: they are settled, not pending.
# `blocked` sorts BELOW the actionable defects (nobody can close it by trying
# harder) but ABOVE the settled tiers -- it is a gap we still WANT, and it must
# never read as "nothing to see here".  That is precisely the mistake `n/a` made.
ROLLUP_PRIORITY = [
    "broken", "dropped", "builds", "unconfigured", "absent", "declared",
    "sane", "blessed", "blocked", "asked-not-distributed", "not-asked", "n/a",
]
COLUMNS = ["country", "feature", "wave", "tier", "coverage", "n_rows", "detail"]

# ---------------------------------------------------------------------------
# Absent-cell verdicts (GH #593)
# ---------------------------------------------------------------------------
# ``absent`` means only "this feature is not declared for this wave".  It
# conflates states that could not be more different, and until they are
# separated the number can never reach zero:
#
#   todo                   the data IS there; nobody has written the config.
#                          Real, actionable work.  Stays ``absent`` in the grid
#                          (it is the live queue) but is now *sized and sourced*.
#   asked-not-distributed  the instrument DID ask, but the shipped extract does
#                          not carry the variables.  An ACQUISITION problem, not
#                          a config one -- a different queue entirely.
#   not-asked              the instrument genuinely never asked.  Closed forever.
#   unsure                 a required check could not be run.  STAYS in the queue,
#                          and records why.  Silence is never evidence.
#
# A 38-cell pilot (2026-07-11) returned 25 todo / 13 unsure / **0 not-asked** --
# so ``absent`` is not hiding "the survey never asked", it is hiding a backlog.
# ``not-asked`` may be near-empty; the mechanism earns its keep by making every
# probe TERMINATE in a recorded, re-checkable verdict rather than evaporating.
#
# The evidence bar is non-negotiable, because a verdict here is a PERMANENT,
# UNSUPERVISED write.  An unevidenced negative is unfalsifiable, and therefore
# forever.  This is not hypothetical: ``Albania/_/data_scheme.yml`` asserted
# "earlier waves have no shocks module" -- and Albania 2005's
# ``migrationE_cl.dta`` carries ``m6e_q00 = 'Type of Shock Code'`` with ten shock
# types.  The claim was wrong, nobody could catch it, and it suppressed work.
#
# See ``docs/guide/coverage.md`` and
# ``slurm_logs/DESIGN_coverage_discipline_2026-07-11.org``.
ADJUDICATED_TIERS = {"not-asked", "asked-not-distributed"}
VERDICTS = {"todo", "asked-not-distributed", "not-asked", "unsure"}
# Verdicts that CLOSE a cell (change its tier away from plain ``absent``).
# ``todo`` and ``unsure`` deliberately do not: they are still open work.
_VERDICT_TO_TIER = {
    "not-asked": "not-asked",
    "asked-not-distributed": "asked-not-distributed",
}


# ---------------------------------------------------------------------------
# Snapshot / blessing file locations
# ---------------------------------------------------------------------------
def _repo_root() -> Path:
    """Repo root for a dev/worktree checkout (parent of the package dir)."""
    return Path(__file__).resolve().parent.parent


def default_snapshot_path() -> Path:
    """Resolve the status snapshot CSV (env → CWD → repo root)."""
    env = os.environ.get("LSMS_COVERAGE_SNAPSHOT")
    if env:
        return Path(env)
    cwd = Path.cwd() / ".coder" / "coverage" / "latest.csv"
    if cwd.exists():
        return cwd
    return _repo_root() / ".coder" / "coverage" / "latest.csv"


def default_blessed_path() -> Path:
    return _repo_root() / ".coder" / "coverage" / "blessed.csv"


def default_verdicts_path() -> Path:
    return _repo_root() / ".coder" / "coverage" / "absent_verdicts.csv"


def load_verdicts(path: Path | None = None) -> dict[tuple[str, str, str], dict]:
    """Adjudications of ``absent`` cells, keyed ``(country, feature, wave)``.

    CSV with header
    ``country,feature,wave,verdict,checks_run,evidence,adjudicated_by,date``
    (``wave`` blank for country-level features).  Missing file → empty dict.

    ``verdict`` must be one of :data:`VERDICTS`.  A row is IGNORED (with a
    warning) when its verdict is unknown, or when it claims a closing verdict
    (``not-asked`` / ``asked-not-distributed``) with an **empty ``evidence``**
    field.

    That last rule is the point of the whole mechanism.  A closing verdict is a
    permanent, unsupervised write: it removes a cell from the work queue with
    nobody reviewing it.  An unevidenced negative cannot be challenged, and is
    therefore permanent whether or not it is true.  So we simply do not accept
    one -- an evidence-free close is not a close, it is a red cell that lies.
    """
    path = path or default_verdicts_path()
    if not Path(path).exists():
        return {}
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    out: dict[tuple[str, str, str], dict] = {}
    for _, r in df.iterrows():
        key = (r.get("country", ""), r.get("feature", ""), r.get("wave", ""))
        verdict = (r.get("verdict", "") or "").strip()
        if verdict not in VERDICTS:
            warnings.warn(
                f"absent_verdicts: ignoring {key} -- unknown verdict "
                f"{verdict!r} (expected one of {sorted(VERDICTS)})",
                stacklevel=2,
            )
            continue
        if verdict in _VERDICT_TO_TIER and not (r.get("evidence", "") or "").strip():
            warnings.warn(
                f"absent_verdicts: ignoring {key} -- verdict {verdict!r} closes "
                "the cell permanently and so REQUIRES a non-empty `evidence` "
                "field.  An unevidenced negative is unfalsifiable.",
                stacklevel=2,
            )
            continue
        out[key] = dict(r)
    return out


def _absent_tier(country, feature, wave, verdicts) -> tuple[str, str]:
    """Grade a not-declared cell: ``(tier, detail)``.

    The single place ``absent`` is decided.  Without an adjudication the cell is
    plain ``absent`` -- an *open, un-triaged* gap.  With one it may be closed
    (``not-asked`` / ``asked-not-distributed``); ``todo`` and ``unsure`` stay
    ``absent`` because they are still work, but carry their evidence forward so
    the next person does not re-derive it.
    """
    v = verdicts.get((country, feature, str(wave) if wave is not None else ""))
    if not v:
        return "absent", "source not declared for wave"
    verdict = v.get("verdict", "")
    detail = f"{verdict}: {v.get('evidence', '')}".strip().rstrip(":")
    return _VERDICT_TO_TIER.get(verdict, "absent"), detail


def load_blessed(path: Path | None = None) -> set[tuple[str, str, str]]:
    """Return the git-tracked set of blessed ``(country, feature, wave)`` cells.

    CSV with header ``country,feature,wave`` (``wave`` blank for country-level
    features). Missing file → empty set.
    """
    path = path or default_blessed_path()
    if not Path(path).exists():
        return set()
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return {
        (r.get("country", ""), r.get("feature", ""), r.get("wave", ""))
        for _, r in df.iterrows()
    }


def unconfigured_countries(countries_root) -> dict[str, int]:
    """Country dirs with downloaded microdata but **no** ``_/data_scheme.yml``.

    Returns ``{country: n_waves_with_data}``.

    :func:`catalog.countries` admits a directory only if it carries a
    ``_/data_scheme.yml``, so these are invisible to ``Country()``, to
    ``Feature()``, and — until now — to this matrix.  As of 2026-07-11 that hid
    **ten in-remit countries and ~35 waves of already-downloaded microdata**
    (Afghanistan, Bosnia-Herzegovina, Brazil, Bulgaria, Kyrgyz Republic,
    Nicaragua, Panama, Peru, Rwanda, Tanzania_Kegera).

    A denominator that omits the work you have not started is not a denominator,
    so the matrix now reports them as ``unconfigured`` rather than not at all.

    A directory counts only if at least one of its wave subdirs has a ``Data/``
    (with or without files — DVC sidecars alone are enough, since the blobs are
    fetched lazily).  That keeps stray non-country dirs out.
    """
    out: dict[str, int] = {}
    try:
        root = Path(countries_root())
    except Exception:  # noqa: BLE001
        return out
    for p in sorted(root.iterdir()):
        if not p.is_dir() or p.name.startswith((".", "_")):
            continue
        if (p / "_" / "data_scheme.yml").exists():
            continue                                   # configured; not our problem
        n = sum(1 for w in p.iterdir() if w.is_dir() and (w / "Data").is_dir())
        if n:
            out[p.name] = n
    return out


# Kinds of blocker.  Absorbed from the June-2026 obtainability audit (GH #552),
# which independently reached this axis; recording it as STRUCTURE rather than
# prose, because the kinds route to completely different people:
#
#   nso-redirect  the WB catalogues it but redirects to a national statistics
#                 office, and that office's channel is broken/unreachable.
#                 (Nepal NSO; Jamaica STATIN; Ecuador INEC.)  -> chase a human.
#   fee-gated     obtainable, for money and permission.  (Vietnam GSO: USD
#                 200-2000 + GSO consent.)  -> a BUDGET decision, not an
#                 engineering one.  Never queue this for an RA.
#   unconfirmed   we believe it is blocked but have not proven it; it may in
#                 fact be a plain obtainable TODO.  (Albania 1996.)
#                 -> the honest state when you have not actually checked.
#
# "The site is down" and "it costs $2,000" are not the same blocker, and
# flattening them is how a money question silently becomes an engineering
# backlog item that nobody can ever close.
BLOCKER_KINDS = {"nso-redirect", "fee-gated", "unconfirmed", "licensed", "other"}


def default_blocked_path() -> Path:
    return _repo_root() / ".coder" / "coverage" / "blocked_sources.csv"


def load_blocked(path: Path | None = None) -> dict[tuple[str, str], dict]:
    """Sources we WANT, KNOW where to get, and CANNOT obtain — keyed ``(country, wave)``.

    CSV header:
    ``country,wave,catalog_id,blocker,probe,last_checked,unblock,recorded_by``

    This is the state between ``missing-wave`` (obtainable — go run ``add-wave``)
    and "does not exist".  The data exists, the provenance is known, and the
    *acquisition channel* is broken:

    - **Nepal** (3 waves) — NSO Nepal hosts the microdata, not the WB; the NSO
      site is unreachable and no human contact has been reached.
    - **Albania 1996** — directory and WB catalog entry (2311) both exist; zero
      data files.

    Why it is NOT any tier we already had
    -------------------------------------
    ``n/a`` says *"no readiness applies"* — it reads as *nothing to see here*,
    and hides a gap we actively want.  (Nepal was graded ``n/a`` between
    2026-07-12 and this commit.  That was wrong, and it was mine.)
    ``not-asked`` / ``none`` are **terminal**; this is not.  ``todo`` would put
    it in an RA's queue, where they would spend a day rediscovering that the NSO
    site is down.

    ``blocked`` is *visible* (it counts against us) but *not actionable by us* —
    nobody closes it by trying harder.

    The load-bearing part: ``probe`` + the recheck
    ---------------------------------------------
    A blocker is a **condition that changes**.  A broken website heals; a dead
    contact gets replaced.  *"We hope we'll find it someday" is not a plan — a
    scheduled recheck is.*  So every row carries a ``probe`` (the URL to retry)
    and a ``last_checked``, and the existing ``bin/coverage_refresh.sbatch``
    re-probes them and opens a GitHub issue when one comes back.  Without that,
    this tier is just a nicer word for *forgotten*.

    A row with no ``blocker`` or no ``probe`` is IGNORED (with a warning): an
    un-probeable blocker cannot be rechecked, so it would silently become the
    permanent close it is explicitly not.
    """
    path = path or default_blocked_path()
    if not Path(path).exists():
        return {}
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    out: dict[tuple[str, str], dict] = {}
    for _, r in df.iterrows():
        key = (r.get("country", ""), r.get("wave", ""))
        if not (r.get("blocker", "") or "").strip():
            warnings.warn(f"blocked_sources: ignoring {key} — no `blocker` recorded; "
                          "an unexplained block is indistinguishable from neglect.",
                          stacklevel=2)
            continue
        if not (r.get("probe", "") or "").strip():
            warnings.warn(f"blocked_sources: ignoring {key} — no `probe` URL, so it can "
                          "never be rechecked and would silently become permanent.",
                          stacklevel=2)
            continue
        out[key] = dict(r)
    return out


@lru_cache(maxsize=1)
def countries_without_microdata() -> dict[str, str]:
    """Countries whose config exists but whose source microdata is not in-repo.

    Declared in the canonical ``lsms_library/data_info.yml`` (config, not code —
    GH #436), mirroring the "Countries Without Microdata" table in CLAUDE.md.

    Their features *cannot* build.  That is a fact about data availability, not
    a defect, so the matrix grades their cells ``n/a`` rather than ``broken``.
    Before this, **all 8 ``broken`` cells in the cube were these two countries**
    (Armenia ×2, Nepal ×6) — which meant the most alarming tier in the ladder
    contained nothing actionable, and a genuine build failure would have been
    lost among them.

    Returns ``{country: reason}``.
    """
    try:
        from importlib.resources import files
        from .yaml_utils import load_yaml
        with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as fh:
            info = load_yaml(fh)
        section = (info or {}).get("Countries Without Microdata", {}) or {}
        return dict(section.get("countries", {}) or {})
    except Exception:  # noqa: BLE001 — a missing/old data_info.yml must not break grading
        return {}


# ---------------------------------------------------------------------------
# Lazy bundle of lsms_library symbols (avoids import cycles at package init)
# ---------------------------------------------------------------------------
def _env():
    from . import catalog
    from .country import Country, JSON_CACHE_METHODS
    from .diagnostics import is_this_feature_sane, load_feature
    from .feature import _DERIVED_SOURCE
    from .paths import countries_root
    return {
        "catalog": catalog, "Country": Country,
        "COUNTRY_LEVEL_ONLY": frozenset(JSON_CACHE_METHODS),
        "is_this_feature_sane": is_this_feature_sane, "load_feature": load_feature,
        "DERIVED_SOURCE": dict(_DERIVED_SOURCE), "countries_root": countries_root,
    }


# ---------------------------------------------------------------------------
# Cell + grading helpers
# ---------------------------------------------------------------------------
def _cell(country, feature, wave, tier, coverage, n_rows=None, detail=""):
    return {
        "country": country, "feature": feature,
        "wave": wave if wave is not None else "",
        "tier": tier, "coverage": coverage,
        "n_rows": n_rows if n_rows is not None else pd.NA,
        "detail": detail,
    }


def wave_available_features(wave, derived_source: dict) -> set[str]:
    """Tables available at a wave = ``Wave.data_scheme`` lifted by derived map."""
    base = set(wave.data_scheme)
    for derived, source in derived_source.items():
        if source in base:
            base.add(derived)
    return base


def tier_from_report(report) -> str:
    """Map a ``SanityReport`` to the builds↔sane boundary (reuse ``.ok``)."""
    return "sane" if report.ok else "builds"


def _report_summary(report) -> str:
    fails = [c.name for c in report.errors]
    warns = [c.name for c in report.warnings]
    bits = []
    if fails:
        bits.append("fail: " + ",".join(fails))
    if warns:
        bits.append("warn: " + ",".join(warns))
    return "; ".join(bits) or "all checks pass"


def _has_country_level_feature(country: str, feature: str, countries_root) -> bool:
    d = countries_root() / country / "_"
    return (d / f"{feature}.py").exists() or (d / f"{feature}.json").exists()


def _safe_build(co, feature, env):
    """Build a country-level feature, capturing warnings/exceptions.

    Returns ``(df_or_None, err_str_or_None)``. Mirrors ``scan.py:_build``
    (thinner; we don't need the per-warning findings here).
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            return env["load_feature"](co, feature), None
        except Exception as e:  # noqa: BLE001 — any build failure is a 'broken' cell
            return None, f"{type(e).__name__}: {e}"


def grade_feature(country_name, feature, waves, co, env, *,
                  avail_by_wave=None, blessed=frozenset(), verdicts=None,
                  blocked=None, readiness=True) -> list[dict]:
    """Per-wave cells for one ``(country, feature)``.

    Builds the country-level table at most once (``readiness=True``) and grades
    each wave by slicing on ``t`` and reusing ``is_this_feature_sane``.

    ``avail_by_wave`` maps wave -> available-feature set; :func:`build_matrix`
    precomputes it once per country (it is identical across that country's
    features). When ``None`` we compute it here, keeping the function
    self-contained for tests / one-off calls.
    """
    if avail_by_wave is None:
        derived_source = env["DERIVED_SOURCE"]
        avail_by_wave = {}
        for w in waves:
            try:
                avail_by_wave[w] = wave_available_features(co[w], derived_source)
            except Exception:  # noqa: BLE001 — a broken wave declares nothing
                avail_by_wave[w] = set()
    covered: dict[str, bool] = {w: feature in avail_by_wave[w] for w in waves}
    verdicts = {} if verdicts is None else verdicts
    blocked = {} if blocked is None else blocked

    def _absent(w):
        """The single place an un-declared cell is graded (see _absent_tier)."""
        return _absent_tier(country_name, feature, w, verdicts)

    def _blocked_cell(w):
        """`blocked` if this wave's SOURCE is unobtainable, else None.

        Blockedness is a property of the *source*, not of a build -- we do not
        need to try (and fail) to build Nepal to know that NSO's site is down.
        So this is checked FIRST, and it applies to the cheap coverage-only layer
        too.  (It did not, briefly, which meant `make matrix-coverage` reported
        `blocked=0` while four sources were plainly blocked.)
        """
        blk = blocked.get((country_name, str(w)))
        if not blk:
            return None
        kind = blk.get("blocker_kind", "") or "other"
        return _cell(country_name, feature, w, "blocked", "declared",
                     detail=f"blocked[{kind}]: {blk.get('blocker','')} "
                            f"[probe {blk.get('probe','')}; "
                            f"last checked {blk.get('last_checked','?')}]")

    if not readiness:
        out = []
        for w in waves:
            blk = _blocked_cell(w)
            if blk is not None:
                out.append(blk)
                continue
            if covered[w]:
                out.append(_cell(country_name, feature, w, "declared", "declared",
                                 detail="coverage-only run"))
            else:
                tier, detail = _absent(w)
                out.append(_cell(country_name, feature, w, tier, "absent",
                                 detail=detail))
        return out

    df, err = _safe_build(co, feature, env)
    if err is not None or df is None or len(df) == 0:
        why = err or "feature built empty"
        # A country with no microdata in the repo cannot build.  That is a data-
        # availability fact, not a defect -- so it is not `broken`.  But WHICH
        # non-defect it is matters, and `blocked` OUTRANKS `n/a`:
        #
        #   blocked : we WANT this, we KNOW where it is, we CANNOT get it (the
        #             acquisition channel is broken).  A live gap.  Re-probed.
        #   n/a     : nothing to see here.
        #
        # Nepal was graded `n/a` (my error, 2026-07-12).  `n/a` says "don't look",
        # which buried a gap we actively want -- the same lumping bug this module
        # exists to prevent, committed by its author.  A blocked source must stay
        # VISIBLE.
        no_data = countries_without_microdata().get(country_name)
        out = []
        for w in waves:
            if not covered[w]:
                tier, detail = _absent(w)
                out.append(_cell(country_name, feature, w, tier, "absent",
                                 detail=detail))
                continue
            blk = _blocked_cell(w)
            if blk is not None:
                out.append(blk)
                continue
            if no_data:
                out.append(_cell(country_name, feature, w, "n/a", "declared",
                                 detail=f"no microdata in repo: {no_data}"))
                continue
            out.append(_cell(country_name, feature, w, "broken", "declared",
                             detail=why))
        return out

    names = list(df.index.names or [])
    if "t" not in names:
        rep = env["is_this_feature_sane"](df, country_name, feature)
        base = tier_from_report(rep)
        cells = []
        for w in waves:
            if covered[w]:
                cells.append(_cell(country_name, feature, w, "n/a", "declared",
                                   detail=f"no per-wave (t) axis; "
                                          f"country-level grade={base}"))
            else:
                tier, detail = _absent(w)
                cells.append(_cell(country_name, feature, w, tier, "absent",
                                   detail=detail))
        cells.append(_cell(country_name, feature, None, base, "declared",
                           n_rows=len(df), detail=_report_summary(rep)))
        return cells

    t_values = {str(v) for v in df.index.get_level_values("t").unique()}
    t_level = df.index.get_level_values("t").astype(str)

    # Columns populated SOMEWHERE in the country frame.  Exempt these from the
    # per-wave ``no_all_null_columns`` check: it is a *country-level* check, and
    # a question simply not fielded in a given wave is legitimately all-null in
    # that wave's slice.  Without this, such cells were graded ``builds`` (a
    # defect) rather than ``sane`` -- 128 of the 138 ``builds`` cells in the
    # 2026-06-26 snapshot were this artefact, and *zero* of the columns they
    # flagged were all-null country-wide.
    #
    # A column that IS all-null across the whole country stays non-optional and
    # still (correctly) fails -- we are relaxing the slice, not the country.
    #
    # NB: this deliberately hides *wave-level* column gaps from the sanity
    # grade, because a sanity grade is the wrong instrument for them.  Two such
    # gaps that this exposure was catching only by accident were inventoried as
    # GH #591 (Nigeria food_prices) and GH #592 (18 waves with no GPS) BEFORE
    # this change landed, so the regrade cannot bury them.
    populated = {c for c in df.columns if df[c].notna().any()}

    cells = []
    for w in waves:
        blk = _blocked_cell(w)
        if blk is not None:
            cells.append(blk)
            continue
        if not covered[w]:
            tier, detail = _absent(w)
            cells.append(_cell(country_name, feature, w, tier, "absent",
                               detail=detail))
            continue
        if str(w) not in t_values:
            cells.append(_cell(country_name, feature, w, "dropped", "declared",
                               detail="declared for wave but absent from built t-index"))
            continue
        sl = df[t_level == str(w)]
        if len(sl) == 0:
            cells.append(_cell(country_name, feature, w, "dropped", "declared",
                               detail="declared, present in t-index but 0 rows"))
            continue
        rep = env["is_this_feature_sane"](sl, country_name, feature,
                                          extra_optional=populated)
        tier = tier_from_report(rep)
        if tier == "sane" and (country_name, feature, str(w)) in blessed:
            tier = "blessed"
        cells.append(_cell(country_name, feature, w, tier, "declared",
                           n_rows=len(sl), detail=_report_summary(rep)))
    return cells


def grade_country_level(country_name, feature, co, env, *,
                        blessed=frozenset(), verdicts=None, blocked=None,
                        readiness=True) -> dict:
    """Grade a country-level-only feature (panel_ids / updated_ids): wave=None."""
    if not readiness:
        return _cell(country_name, feature, None, "n/a", "n/a",
                     detail="country-level feature; readiness skipped")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            val = env["load_feature"](co, feature)
    except Exception as e:  # noqa: BLE001
        # A blocked source cannot build, and that is not a defect.  Same rule as
        # the per-wave path: `blocked` outranks `n/a`, because it is a gap we
        # still WANT and must stay visible.
        blk = (blocked or {}).get((country_name, ""))
        if blk:
            return _cell(country_name, feature, None, "blocked", "declared",
                         detail=f"blocked: {blk.get('blocker','')} "
                                f"[probe {blk.get('probe','')}]")
        no_data = countries_without_microdata().get(country_name)
        if no_data:
            return _cell(country_name, feature, None, "n/a", "declared",
                         detail=f"no microdata in repo: {no_data}")
        return _cell(country_name, feature, None, "broken", "declared",
                     detail=f"{type(e).__name__}: {e}")
    n = len(val) if hasattr(val, "__len__") else 0
    if n == 0:   # avoid `not val` — ambiguous if a future feature returns a frame
        tier, detail = _absent_tier(country_name, feature, None,
                                    verdicts or {})
        if detail == "source not declared for wave":   # un-adjudicated
            detail = "empty / no entries"
        return _cell(country_name, feature, None, tier, "absent", detail=detail)
    tier = "blessed" if (country_name, feature, "") in blessed else "sane"
    return _cell(country_name, feature, None, tier, "declared",
                 n_rows=n, detail=f"{n} entries")


# ---------------------------------------------------------------------------
# Whole-matrix build
# ---------------------------------------------------------------------------
def build_matrix(countries=None, features=None, *, readiness=True,
                 blessed=None, verdicts=None, blocked=None,
                 log=lambda _m: None) -> pd.DataFrame:
    """Build the full (country × feature × wave) status table.

    Parameters
    ----------
    countries, features : list[str] | None
        Restrict the sweep; ``None`` = all.
    readiness : bool
        If ``False``, emit only the cheap coverage layer (no builds, no auth).
    blessed : set | None
        Blessed-cell set; defaults to :func:`load_blessed`.
    log : callable
        Progress sink (one line per country).
    """
    env = _env()
    Country = env["Country"]
    catalog = env["catalog"]
    country_level_only = env["COUNTRY_LEVEL_ONLY"]
    countries_root = env["countries_root"]
    if blessed is None:
        blessed = load_blessed()
    if verdicts is None:
        verdicts = load_verdicts()
    if blocked is None:
        blocked = load_blocked()
    if countries is None:
        countries = catalog.countries()
    feat_filter = set(features) if features else None

    rows: list[dict] = []

    # Country dirs holding downloaded microdata but NO config are invisible to
    # `catalog.countries()` (it requires `_/data_scheme.yml`), so the matrix has
    # never reported them at all -- 10 in-remit countries and ~35 waves of
    # paid-for data that simply did not appear.  Surface them as `unconfigured`:
    # visibly red, not absent from the report.  A denominator that omits the work
    # you have not started is not a denominator.
    # A blocked source we want but do NOT model as a wave must still appear.
    # `Albania/1996` is exactly this: the directory and its WB catalog entry
    # (2311) exist, but it has no `_/` and no `Data/`, so `Country.waves` never
    # enumerates it and every per-wave loop skips it silently.  The blocked list
    # is AUTHORITATIVE about sources we want -- it does not defer to whether the
    # model currently happens to know about them.  (Same invisibility bug as the
    # `unconfigured` countries below, one level down.)
    modelled: set[tuple[str, str]] = set()

    for c, nwaves in unconfigured_countries(countries_root).items():
        if countries is not None and c not in countries:
            continue
        rows.append(_cell(c, "<country>", None, "unconfigured", "absent",
                          n_rows=nwaves,
                          detail=f"microdata present ({nwaves} waves) but no "
                                 f"_/data_scheme.yml — country not configured"))

    for c in countries:
        try:
            co = Country(c, preload_panel_ids=False)
            waves = list(co.waves)               # also populates wave_folder_map
            feats = sorted(set(co.data_scheme) - country_level_only)
            # Per-wave availability is identical across a country's features;
            # compute it ONCE here (cheap config reads) rather than re-deriving
            # it per feature inside grade_feature.
            avail_by_wave = {}
            for w in waves:
                try:
                    avail_by_wave[w] = wave_available_features(co[w], env["DERIVED_SOURCE"])
                except Exception:  # noqa: BLE001 — a broken wave declares nothing
                    avail_by_wave[w] = set()
        except Exception as e:  # noqa: BLE001
            log(f"  [skip] {c}: {type(e).__name__}: {e}")
            rows.append(_cell(c, "<country>", None, "broken", "declared",
                              detail=f"country load failed: {type(e).__name__}: {e}"))
            continue

        modelled.update((c, str(w)) for w in waves)

        sel = [f for f in feats if (feat_filter is None or f in feat_filter)]
        log(f"  {c}: {len(waves)} waves × {len(sel)} features"
            + ("" if readiness else " (coverage only)"))
        for f in sel:
            rows.extend(grade_feature(c, f, waves, co, env,
                                      avail_by_wave=avail_by_wave,
                                      blessed=blessed, verdicts=verdicts,
                                      blocked=blocked,
                                      readiness=readiness))

        for clf in country_level_only:
            if feat_filter is not None and clf not in feat_filter:
                continue
            if _has_country_level_feature(c, clf, countries_root):
                rows.append(grade_country_level(c, clf, co, env,
                                                blessed=blessed, verdicts=verdicts,
                                      blocked=blocked,
                                      readiness=readiness))

    for (bc, bw), blk in (blocked or {}).items():
        if countries is not None and bc not in countries:
            continue
        if (bc, str(bw)) in modelled:
            continue                       # already graded per-wave above
        rows.append(_cell(bc, "<wave>", bw, "blocked", "declared",
                          detail=f"blocked, and NOT MODELLED as a wave "
                                 f"(no _/ config): {blk.get('blocker','')} "
                                 f"[probe {blk.get('probe','')}]"))

    df = pd.DataFrame(rows, columns=COLUMNS)
    df["tier"] = pd.Categorical(df["tier"], categories=TIER_ORDER, ordered=True)
    return df


def save_snapshot(df: pd.DataFrame, path: Path | None = None, *,
                  merge: bool = True) -> Path:
    """Write the status table to the git-tracked snapshot CSV.

    ``merge=True`` (default) **upserts** ``df``'s cells into the existing
    snapshot on the ``(country, feature, wave)`` key, leaving every other cell
    untouched.  ``merge=False`` replaces the file wholesale.

    Why merge is the default
    ------------------------
    A *scoped* run (``make matrix C="Uganda"``) grades only some cells.  Writing
    that result wholesale silently **destroys** every cell it did not grade --
    and ``docs/guide/coverage.md`` documents exactly that as the "spot refresh"
    procedure, immediately followed by ``git add .coder/coverage/latest.csv &&
    git commit``.  Following the guide verbatim therefore committed a 67-cell
    snapshot over the authoritative 1849-cell one.  (Observed 2026-07-12.)

    A partial measurement must never be able to erase a complete one.  A full
    sweep still rewrites everything it grades, so the authoritative run is
    unaffected; it simply no longer depends on *not* being scoped.
    """
    path = Path(path) if path is not None else default_snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["tier"] = out["tier"].astype(str)

    if merge and path.exists():
        try:
            prev = pd.read_csv(path, dtype=str, keep_default_na=False)
        except Exception:  # noqa: BLE001 — an unreadable snapshot is simply replaced
            prev = None
        if prev is not None and not prev.empty:
            key = ["country", "feature", "wave"]
            fresh = out.copy()
            for k in key:                      # align dtypes for the anti-join
                fresh[k] = fresh[k].astype(str)
            keep = prev.merge(fresh[key].drop_duplicates(), on=key,
                              how="left", indicator=True)
            keep = keep[keep["_merge"] == "left_only"].drop(columns="_merge")
            out = pd.concat([keep, fresh], ignore_index=True)

    out = out.reindex(columns=COLUMNS)
    out.sort_values(["country", "feature", "wave"]).to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Public reader
# ---------------------------------------------------------------------------
def coverage(refresh: bool | str = False, *, countries=None, features=None,
             snapshot: Path | None = None) -> pd.DataFrame:
    """Return the country × feature × wave status table.

    Parameters
    ----------
    refresh : bool | str, default False
        - ``False`` — load the committed snapshot (the last ``make matrix``
          readiness run). Raises ``FileNotFoundError`` with guidance if absent.
        - ``"coverage"`` (or ``True``) — recompute the **live coverage layer**
          fresh from config (no builds, no auth). Always current; no readiness
          tiers.
        - ``"readiness"`` — recompute the **full** matrix in-process (heavy;
          builds every cell). Mostly for tests / one-offs.
    countries, features : list[str] | None
        Restrict a refresh; ignored when reading the snapshot.
    snapshot : Path | None
        Override the snapshot location (env ``LSMS_COVERAGE_SNAPSHOT`` also works).

    Returns
    -------
    pandas.DataFrame  (columns: country, feature, wave, tier, coverage, n_rows, detail)
    """
    if refresh is not False:
        if refresh in ("coverage", True):
            return build_matrix(countries, features, readiness=False)
        if refresh == "readiness":
            return build_matrix(countries, features, readiness=True)
        raise ValueError(
            "refresh must be False, True, 'coverage', or 'readiness'; "
            f"got {refresh!r}"
        )

    path = Path(snapshot) if snapshot is not None else default_snapshot_path()
    if not path.exists():
        raise FileNotFoundError(
            f"No coverage snapshot at {path}. Run `make matrix` (or "
            "`ll.coverage(refresh='coverage')` for a build-free coverage view)."
        )
    df = pd.read_csv(path, dtype={"wave": str}, keep_default_na=False)
    if "tier" in df.columns:
        df["tier"] = pd.Categorical(df["tier"], categories=TIER_ORDER, ordered=True)
    return df
