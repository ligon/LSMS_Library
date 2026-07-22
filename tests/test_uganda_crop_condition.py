"""Regression tests for the ``condition`` index level on Uganda ``crop_production``.

GH #323 / #637.  The UNPS post-harvest module asks quantity, unit AND
condition/state as one compound question, so the same plot-crop-season is
reported once per condition.  ``crop_production`` was keyed
``(t, i, plot, j, u, season)`` with no level for the condition, so those
records collided and the de-duplication block at the end of
``uganda.crop_production_for_wave`` SUMMED them — adding dry weight to
fresh weight.

These tests are written to FAIL on the pre-fix tree:

* ``test_condition_is_an_index_level``          — the level did not exist.
* ``test_declared_index_carries_condition``     — ``data_scheme.yml`` did not
  declare it, and ``_normalize_dataframe_index`` drops undeclared levels.
* ``test_fresh_and_dry_no_longer_collide``      — the 2011-12 coffee record was
  one 340 kg row instead of 240 kg dry + 100 kg fresh.
* ``test_harvest_conditions_table_exists``      — the mapping table did not exist.

A second family of tests (added after the adversarial review of PR #649) guards
the *other* failure mode — not "the level is missing" but "the level is there
and silently wrong".  ``crop_production_for_wave`` used to fall through a quiet
``in df5.columns`` guard, so a mis-typed ``CROP_COLMAPS`` condition column
bucketed a whole wave-season into ``unknown_condition`` with every test green:

* ``test_crop_colmap_columns_resolve_in_source``   — a NAMED column must exist.
* ``test_sentinel_share_bounded_per_wave_season``  — per-cell sentinel ceiling.
* ``test_condition_varies_within_every_wave_season`` — per-cell variety floor.

The first catches a name that does not resolve; the other two catch a name that
resolves to the wrong column.  Their thresholds are measured, not guessed —
provenance is in the constants at the top of this module.

Schema rules are READ from ``lsms_library/data_info.yml`` and from Uganda's
``_/data_scheme.yml`` / ``_/categorical_mapping.org``, never hardcoded here
(CLAUDE.md, "Canonical Schema").
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml
from importlib.resources import files

from lsms_library.paths import countries_root

TABLE = "crop_production"
LEVEL = "condition"
SENTINEL = "unknown_condition"

# --- ceilings for the wave-season invariants (see the tests that use them) ---
#
# Both numbers come from ONE cold rebuild (isolated LSMS_DATA_DIR with only
# dvc-cache symlinked in, LSMS_COUNTRIES_ROOT pinned to the branch worktree,
# Uganda cache physically removed), 2026-07-21, 133 683 rows.
#
# Observed `unknown_condition` share by (t, season), worst first:
#     2009-10 A 25.72%   2009-10 B 20.46%   2010-11 A 7.10%   2013-14 B 4.35%
#     2011-12 A  3.61%   2011-12 B  3.44%   2010-11 B 0.97%   2013-14 A 0.28%
#     2015-16 A  0.22%   2018-19 A  0.13%   2019-20 A 0.12%   2015-16 B 0.03%
#     2019-20 B  0.04%   2018-19 B  0.00%
# The two leaders are 2009-10's seasons, which ship no value labels at all;
# every labelled wave-season is under 4.4%.  The ceiling is set at 40% — well
# clear of the worst honest cell, and far under the ~100% that a wave-season
# collapses to when its condition column is mis-wired (the failure this
# guards: the red-team mutation of CROP_COLMAPS['2018-19']['A'] took that
# cell from 0.13% to 7 153 / 7 153 = 100%).
#
# This ceiling is NOT sufficient on its own, and the threshold is not tuned to
# pretend otherwise.  Measured counter-example: pointing 2018-19 season A's
# condition at `s5aq06f_1` (the harvest month — a plausible 6f/6b fat-finger)
# leaves the sentinel share at 33.7%, UNDER this ceiling, while the cell
# decodes only 2 real conditions.  That is what the variety floor below is
# for; the two invariants are kept because neither alone suffices.
MAX_SENTINEL_SHARE = 0.40
# Observed distinct NON-sentinel conditions by (t, season): min 18 (2010-11 B),
# max 20, every cell >= 18.  A mis-wired column yields 0-2.  Floor at 10.
MIN_DISTINCT_CONDITIONS = 10


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    Tests that read the RAW Stata sources need this; without credentials they
    fail with ``NoCredentialsError`` regardless of whether the logic is right.
    The CI ``unit-tests`` job sets ``LSMS_SKIP_AUTH=1`` and carries no secrets,
    so they must silent-skip there and run in ``data-tests``.

    (Mirrors the identical helper in ``tests/test_declared_spellings.py`` /
    ``tests/test_canonical_shape_via_cache_miss.py``; that duplication is the
    house pattern until a shared ``conftest.py`` lands.)
    """
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


# ---------------------------------------------------------------------------
# config fixtures (cheap — no data build)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def canonical_vocabulary() -> set[str]:
    """Canonical `condition` values, from data_info.yml Columns.

    `spellings` is an inverse dict; "the canonical values are simply
    spellings.keys()" (data_info.yml, Columns preamble).
    """
    with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    col = info.get("Columns", {}).get(TABLE, {}).get(LEVEL)
    assert isinstance(col, dict), (
        f"data_info.yml declares no Columns.{TABLE}.{LEVEL} entry; the canonical "
        f"{LEVEL} vocabulary has no home"
    )
    spellings = col.get("spellings") or {}
    assert spellings, f"Columns.{TABLE}.{LEVEL} declares an empty spellings dict"
    return set(spellings)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


@pytest.fixture(scope="module")
def uganda_scheme() -> dict:
    path = countries_root() / "Uganda" / "_" / "data_scheme.yml"
    with open(path, encoding="utf-8") as f:
        return yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]


@pytest.fixture(scope="module")
def harvest_conditions_table() -> pd.DataFrame:
    """The `harvest_conditions` Code -> Preferred Label table."""
    from lsms_library.local_tools import df_from_orgfile

    path = countries_root() / "Uganda" / "_" / "categorical_mapping.org"
    return df_from_orgfile(str(path), name="harvest_conditions")


# ---------------------------------------------------------------------------
# built-table fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def crop_production() -> pd.DataFrame:
    """The built table.

    Skips only when the build could not have succeeded *for environmental
    reasons* — no S3 credentials, hence no source data.  With credentials
    present a build failure is a DEFECT and is re-raised, so that e.g. a
    mis-wired `CROP_COLMAPS` entry (which now raises `CropColmapError`) turns
    this module red instead of quietly reporting eight skips.  Turning a
    config bug into a skip is the same silent-failure mode these tests exist
    to close.
    """
    try:
        import lsms_library as ll

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country("Uganda", preload_panel_ids=False,
                            verbose=False).crop_production()
    except Exception as exc:
        if _aws_creds_available():
            raise
        pytest.skip(f"Uganda crop_production unavailable (no S3 creds): {exc!r}")
    if df is None or df.empty:
        pytest.skip("Uganda crop_production built empty")
    return df


# ---------------------------------------------------------------------------
# config-level tests (run without any data)
# ---------------------------------------------------------------------------

def test_declared_index_carries_condition(uganda_scheme):
    """`_normalize_dataframe_index` DROPS index levels the scheme does not
    declare, so an undeclared `condition` would be a silent no-op that still
    sums fresh onto dry."""
    declared = uganda_scheme["index"]
    assert LEVEL in [tok.strip() for tok in declared.strip("() ").split(",")], (
        f"Uganda data_scheme.yml declares {TABLE} index {declared!r} without "
        f"{LEVEL!r}; country.py::_normalize_dataframe_index would drop the level"
    )


def test_harvest_conditions_table_exists(harvest_conditions_table):
    t = harvest_conditions_table
    assert "Preferred Label" in t.columns, t.columns.tolist()
    assert len(t) >= 20, f"harvest_conditions has only {len(t)} rows"
    assert t["Preferred Label"].is_unique, "duplicate Preferred Labels"


def test_harvest_conditions_labels_are_canonical(harvest_conditions_table,
                                                 canonical_vocabulary):
    """Every label Uganda emits must be a declared canonical value, so the
    cross-country vocabulary in data_info.yml is not silently bypassed."""
    labels = {str(v).strip() for v in harvest_conditions_table["Preferred Label"]
              if pd.notna(v) and str(v).strip() not in ("", "---")}
    unknown = labels - canonical_vocabulary
    assert not unknown, (
        f"harvest_conditions emits labels absent from data_info.yml "
        f"Columns.{TABLE}.{LEVEL}.spellings: {sorted(unknown)}"
    )


def _crop_colmaps():
    import sys

    sys.path.insert(0, str(countries_root() / "Uganda" / "_"))
    try:
        from uganda import CROP_COLMAPS
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"cannot import uganda module: {exc!r}")
    return CROP_COLMAPS


def test_every_wave_sources_a_condition_column():
    """Each wave/season condition slot must name a condition column; a `None`
    would silently sentinel the whole wave."""
    missing = []
    for wave, seasons in _crop_colmaps().items():
        for season in ("A", "B"):
            cm = seasons.get(season)
            if not cm:
                continue
            for n, cond in enumerate(cm["conditions"]):
                if not cond.get("condition"):
                    missing.append(f"{wave}/{season}[{n}]")
    assert not missing, f"condition column unwired for: {missing}"


# ---------------------------------------------------------------------------
# the colmap must RESOLVE, not merely be non-empty  (GH #323 red-team F4)
# ---------------------------------------------------------------------------
#
# `test_every_wave_sources_a_condition_column` above checks only that a name is
# written down.  A name that does not exist in the source file used to fall
# through a silent `in df5.columns` guard, so a typo'd condition column
# sentinelled an entire wave-season to `unknown_condition` and every test
# passed.  Proven by mutation: typo CROP_COLMAPS['2018-19']['A']'s condition
# to `a5aq6b_TYPO`, clear the Uganda cache, and 2018-19 season A went from 20
# distinct conditions to `unknown_condition` on 7 153 of 7 153 rows with 10/10
# tests green.
#
# Two independent guards close it, because they catch different mistakes:
#   * this test catches a name that does NOT resolve (typo / dropped column);
#   * the wave-season invariants below catch a name that DOES resolve but is
#     the wrong column — measured with `condition: 's5aq06f_1'` (the harvest
#     month, a plausible 6f/6b fat-finger): the build succeeds, this test
#     passes, and the variety floor is what fails.

_MODULE_FILES = {"A": "agsec5a.dta", "B": "agsec5b.dta"}


def _source_path(wave: str, basename: str) -> Path | None:
    """Locate a wave's raw module by basename, case- and subdir-insensitively.

    Derived from the shipped `.dvc` sidecars rather than hardcoded, so it
    follows 2019-20's `Data/Agric/agsec5a.dta` without a special case.
    """
    root = Path(str(countries_root())) / "Uganda" / wave / "Data"
    for side in root.rglob("*.dvc"):
        if side.name[: -len(".dvc")].lower() == basename:
            return side.with_suffix("")
    return None


@pytest.mark.slow
@pytest.mark.skipif(
    not _aws_creds_available(),
    reason="reads the raw UNPS Stata modules (DVC -> S3); no credentials in the "
           "`unit-tests` CI job (LSMS_SKIP_AUTH=1). Runs in `data-tests`.",
)
def test_crop_colmap_columns_resolve_in_source():
    """Every column CROP_COLMAPS *names* must exist in the file it is read from.

    `None` remains the declared, auditable way to say "this wave records no
    such column" (2018-19 season A genuinely has no harvest unit).  A NAME is a
    claim about the source file, and this test checks it.
    """
    from lsms_library.local_tools import get_dataframe

    colmaps = _crop_colmaps()
    bad, checked = [], 0
    for wave, seasons in colmaps.items():
        for season, basename in _MODULE_FILES.items():
            cm = seasons.get(season)
            if not cm:
                continue
            path = _source_path(wave, basename)
            assert path is not None, f"no {basename} under Uganda/{wave}/Data"
            try:
                df = get_dataframe(str(path), convert_categoricals=False)
            except Exception as exc:  # pragma: no cover - environment-dependent
                pytest.skip(f"cannot read {path}: {exc!r}")
            cols = set(df.columns)
            for key in ("hhid", "parcel", "plot", "crop"):
                name = cm.get(key)
                if name is not None:
                    checked += 1
                    if name not in cols:
                        bad.append(f"{wave}/{season} {key}={name!r}")
            for n, cond in enumerate(cm["conditions"]):
                for key in ("qty", "unit", "condition", "qty_sold",
                            "value_sold", "month"):
                    name = cond.get(key)
                    if name is not None:
                        checked += 1
                        if name not in cols:
                            bad.append(f"{wave}/{season}[{n}] {key}={name!r}")
    assert checked > 100, f"only {checked} colmap entries checked; harness broke"
    assert not bad, (
        "CROP_COLMAPS names source columns that do not exist. Each one is "
        "SILENT in the output — a bad `condition` sentinels a whole wave-"
        "season to unknown_condition, a bad `qty` drops the slot entirely:\n  "
        + "\n  ".join(bad)
    )


# ---------------------------------------------------------------------------
# built-table tests
# ---------------------------------------------------------------------------

def test_condition_is_an_index_level(crop_production):
    assert LEVEL in list(crop_production.index.names), (
        f"crop_production index is {list(crop_production.index.names)}; harvest "
        f"records differing only by condition collide and are summed (GH #323)"
    )


def test_condition_level_has_no_nulls(crop_production):
    """A null index key is SILENTLY DROPPED by the duplicate collapse's
    `groupby(level=...)` (pandas default dropna=True), so the level must carry
    a sentinel string instead."""
    values = crop_production.index.get_level_values(LEVEL)
    assert int(pd.isna(values).sum()) == 0


def test_condition_values_are_canonical(crop_production, canonical_vocabulary):
    observed = set(crop_production.index.get_level_values(LEVEL).unique())
    unknown = {v for v in observed if pd.notna(v)} - canonical_vocabulary
    assert not unknown, (
        f"crop_production emits {LEVEL} values not declared in data_info.yml: "
        f"{sorted(unknown)}"
    )


def test_index_is_unique(crop_production):
    assert crop_production.index.is_unique


def test_fresh_and_dry_no_longer_collide(crop_production):
    """The measured GH #323 example: one household's coffee off one plot,
    reported twice in the same month and unit — 240 kg dry and 100 kg fresh.
    Pre-fix these summed to a single 340 kg row, which is not a quantity of
    anything."""
    f = crop_production.reset_index()
    sel = f[(f["t"] == "2011-12") & (f["i"] == "1033000506")
            & (f["plot"] == "1033000506-1-6") & (f["j"] == "Coffee")]
    # Deliberately NOT `pytest.skip` on empty: this is the PR's single most
    # important regression test, and a skip-on-empty escape hatch would let an
    # id remapping silently disarm it rather than fail it.
    assert not sel.empty, (
        "2011-12 HH 1033000506 plot -1-6 Coffee is missing from the build; the "
        "GH #323 regression example can no longer be checked. If the household "
        "id scheme changed, RE-DERIVE the example — do not delete the test."
    )

    qty = sorted(float(x) for x in sel["Quantity"])
    assert qty == [100.0, 240.0], (
        f"expected the dry (240) and fresh (100) coffee records to stay "
        f"separate, got quantities {qty}"
    )
    assert sel[LEVEL].nunique() == 2, sel[LEVEL].tolist()
    assert {"dried_in_pods", "fresh_in_pods"} <= set(sel[LEVEL])


def test_condition_actually_separates_records(crop_production):
    """Sanity: the level does real work — thousands of plot-crop-unit-season
    groups carry more than one condition.  A level that never varies would
    pass every test above while fixing nothing."""
    f = crop_production.reset_index()
    keys = [k for k in ["t", "i", "plot", "j", "u", "season"] if k in f.columns]
    n_split = int((f.groupby(keys, dropna=False)[LEVEL].nunique() > 1).sum())
    assert n_split > 1000, (
        f"only {n_split} plot-crop-unit-season groups carry >1 condition; "
        f"expected ~3200 (2026-07-21 measurement)"
    )


# ---------------------------------------------------------------------------
# per-(wave, season) invariants  (GH #323 red-team F4)
# ---------------------------------------------------------------------------
#
# The panel-wide checks above are all insensitive to ONE broken wave-season:
# `test_condition_actually_separates_records`' > 1000 threshold survives losing
# any single cell, and `test_condition_values_are_canonical` passes because
# `unknown_condition` IS canonical.  These two are per-cell, so a single
# mis-wired colmap entry cannot hide behind the other thirteen.

def _by_wave_season(crop_production):
    f = crop_production.reset_index()
    assert {"t", "season"} <= set(f.columns), list(f.columns)
    return f.groupby(["t", "season"])


def test_sentinel_share_bounded_per_wave_season(crop_production):
    """No wave-season may be mostly `unknown_condition`.

    A condition column that resolves but is the WRONG column decodes to
    nothing and buckets the whole cell into the sentinel.  Ceiling and its
    provenance: see MAX_SENTINEL_SHARE at the top of this module.
    """
    share = _by_wave_season(crop_production)[LEVEL].apply(
        lambda s: float((s == SENTINEL).mean()))
    over = share[share > MAX_SENTINEL_SHARE]
    assert over.empty, (
        f"{LEVEL} is >{MAX_SENTINEL_SHARE:.0%} `{SENTINEL}` for:\n"
        + "\n".join(f"  {t}/{s}: {v:.1%}" for (t, s), v in over.items())
        + f"\nWorst honest cell measured 2026-07-21 was 2009-10/A at 25.7%. "
          f"Check the `condition` entry in CROP_COLMAPS for these waves — it "
          f"is the column that silently sentinels a whole cell when wrong."
    )


def test_condition_varies_within_every_wave_season(crop_production):
    """Every wave-season must decode a real spread of conditions.

    Floor and its provenance: see MIN_DISTINCT_CONDITIONS at the top of this
    module (measured min was 18 distinct non-sentinel values per cell).
    """
    n = _by_wave_season(crop_production)[LEVEL].apply(
        lambda s: int(s[s != SENTINEL].nunique()))
    thin = n[n < MIN_DISTINCT_CONDITIONS]
    assert thin.empty, (
        f"fewer than {MIN_DISTINCT_CONDITIONS} distinct non-sentinel {LEVEL} "
        f"values for:\n"
        + "\n".join(f"  {t}/{s}: {v}" for (t, s), v in thin.items())
        + "\nEvery wave-season carried >= 18 on 2026-07-21; a cell that "
          "collapses to 0 or 1 means its condition column is mis-wired."
    )
