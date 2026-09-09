"""Regression tests for Uganda 2018-19's harvest UNIT wiring (GH #842).

Before this landed, ``Country('Uganda').crop_production()`` served
``u='Unknown'`` on **all 14 194 rows** of wave 2018-19 — both seasons, 100% —
because ``uganda.CROP_COLMAPS['2018-19']`` declared ``unit: None`` for each.
``None`` is the library's declared, auditable way to say "this wave records no
such column" (``uganda._require``), so writing it is a *claim about the
survey*.  For season B the claim was false: ``AGSEC5B.dta`` ships ``a5bq6b``,
populated on every one of its 7 041 rows.

**Why the columns cannot be identified by their labels.**  The 2018-19 files
and the questionnaire disagree three ways.  The form prints the harvest columns
in a *different order on each visit* — Section 5A (p. 13) is Unit=6b, Qty=6a,
Condition=6c; Section 5B (p. 24) is Qty=6a, Condition=6b, Unit=6c — and the
shipped data contradicts the form on **both** visits.  AGSEC5A goes further and
carries *two* columns both titled "6c. Condition / state".  So these tests, like
the ``condition`` ones next door, assert against the **value vocabulary**:
``harvest_units`` (46 codes) versus ``harvest_conditions`` (20 codes).  The 11
codes the two schemes share are excluded from the discriminating statistics, so
the measure is one-sided and cannot be fooled by overlap.

The tests are written to FAIL on the pre-fix tree:

* ``test_2018_19_season_B_serves_a_real_unit``  — was 7 041/7 041 ``Unknown``.
* ``test_2018_19_B_unit_vocabulary_is_canonical`` — was the bare sentinel.
* ``test_kg_factor_is_carried_for_2018_19``     — the column did not exist.

and two that guard the *opposite* failure — someone "fixing" season A, or the
next wave, by wiring a column that is not a unit:

* ``test_season_A_really_has_no_unit_column``   — proves the ``None`` claim by
  value range, so it stays a survey fact rather than an unexamined default.
  (Its discriminator is deliberately *not* set membership: ``harvest_units``
  contains 1 = Kg and 2 = Gram, so every yes/no field in the file is "100%
  unit codes".  See ``MIN_DISTINCT_UNIT_ONLY_CODES``.)
* ``test_wired_unit_agrees_with_the_reported_kg_factor`` — the strongest check
  here: the household-reported kg factor and the weight parsed out of the wired
  unit's own label are *independent*, and they must agree.

**Mutation-proved.**  Reverting the one key
``CROP_COLMAPS['2018-19']['B']['conditions'][0]['unit']`` to ``None`` and
rebuilding cold turns 5 of these 8 tests red (the two config/source tests, the
two built-table tests, and the kg-factor agreement test); the three that stay
green are the season-A and ``KgFactor`` ones, which that mutation does not
touch.

Vocabularies are READ from Uganda's ``_/categorical_mapping.org`` and the
schema from ``_/data_scheme.yml``, never hardcoded (CLAUDE.md, "Canonical
Schema").
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

WAVE = "2018-19"
TABLE = "crop_production"
UNIT_SENTINEL = "Unknown"

# Measured cold on 2026-09-09 (private LSMS_DATA_DIR, LSMS_COUNTRIES_ROOT
# pinned to the branch worktree), 133 683 rows corpus-wide:
#
#   2018-19 season B  u='Unknown'  7 041 -> 0        distinct u  1 -> 30
#   2018-19 season A  u='Unknown'  7 153 -> 7 153    (no unit column exists)
#   rows, and sum(Quantity/Quantity_sold/Value_sold), unchanged to the digit.
#
# a5bq6b is non-null on all 7 041 season-B rows and every one of its 30 codes
# is in `harvest_units`, so the post-fix sentinel count is exactly 0.  The
# assertion is therefore == 0, not a share: a threshold here would be a place
# for a regression to hide.
N_5B_ROWS = 7041

# What actually distinguishes a Uganda harvest-unit column from any other
# small-integer column, measured on both 2018-19 files (2026-09-09).
#
# Set membership alone is NOT enough, and getting this wrong is the obvious
# trap: `harvest_units` contains codes 1 (Kg) and 2 (Gram), so EVERY yes/no
# question in the file is "100% inside the unit scheme, 100% unit-only".  A
# first draft of this module flagged `s5aq06a_1_1` ("were the decisions made
# by a single household member?", values {1, 2}) and `s5aq05_2` (constant 2)
# as candidate unit columns in AGSEC5A.
#
# A real unit column uses the SPARSE HIGH RANGE of the table -- sacks (9-13),
# basins (22), baskets (37-40), bunches (67-69), heaps (90-92) -- which no
# yes/no field can reach, and it uses many of them.  Measured:
#
#   column          distinct  distinct unit-only  share of rows with code > 45
#   a5bq6b (unit)         30                  21                        0.231
#   s5aq06a_1_1 (y/n)      2                   2                        0.000
#   s5bq06a_1_1 (y/n)      2                   2                        0.000
#   s5aq05_2 (const)       1                   1                        0.000
#
# 45 is the largest non-`99` code in `harvest_conditions`, so "> 45" means
# "in unit territory no condition scheme reaches".  The thresholds sit in the
# gap between 21 and 2, and between 0.231 and 0.000; they are not tuned.
MIN_DISTINCT_UNIT_ONLY_CODES = 5
MIN_SHARE_ABOVE_CONDITION_RANGE = 0.05
TOP_CONDITION_CODE = 45


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    Mirrors the identical helper in ``tests/test_uganda_crop_condition.py`` —
    that duplication is the house pattern until a shared ``conftest.py`` lands.
    The CI ``unit-tests`` job sets ``LSMS_SKIP_AUTH=1`` and carries no secrets,
    so data tests must silent-skip there and run in ``data-tests``.
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


needs_data = pytest.mark.skipif(
    not _aws_creds_available(),
    reason="reads the raw UNPS Stata modules (DVC -> S3); no credentials in the "
           "`unit-tests` CI job (LSMS_SKIP_AUTH=1). Runs in `data-tests`.",
)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def harvest_units() -> dict[int, str]:
    """`harvest_units` Code -> Preferred Label, from categorical_mapping.org."""
    from lsms_library.local_tools import df_from_orgfile

    path = countries_root() / "Uganda" / "_" / "categorical_mapping.org"
    t = df_from_orgfile(str(path), name="harvest_units")
    return {int(c): str(v) for c, v in zip(t["Code"], t["Preferred Label"])
            if pd.notna(c) and pd.notna(v)}


@pytest.fixture(scope="module")
def harvest_conditions() -> dict[int, str]:
    from lsms_library.local_tools import df_from_orgfile

    path = countries_root() / "Uganda" / "_" / "categorical_mapping.org"
    t = df_from_orgfile(str(path), name="harvest_conditions")
    return {int(c): str(v) for c, v in zip(t["Code"], t["Preferred Label"])
            if pd.notna(c) and pd.notna(v)}


@pytest.fixture(scope="module")
def uganda_scheme() -> dict:
    path = countries_root() / "Uganda" / "_" / "data_scheme.yml"
    with open(path, encoding="utf-8") as f:
        return yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]


@pytest.fixture(scope="module")
def colmap_2018_19() -> dict:
    import sys

    d = str(countries_root() / "Uganda" / "_")
    if d not in sys.path:
        sys.path.insert(0, d)
    try:
        from uganda import CROP_COLMAPS
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"cannot import uganda module: {exc!r}")
    return CROP_COLMAPS[WAVE]


@pytest.fixture(scope="module")
def agsec5(request):
    """Raw AGSEC5A / AGSEC5B for 2018-19, uncategoricalised (codes as codes)."""
    from lsms_library.local_tools import get_dataframe

    out = {}
    for season, base in (("A", "AGSEC5A"), ("B", "AGSEC5B")):
        path = countries_root() / "Uganda" / WAVE / "Data" / f"{base}.dta"
        try:
            out[season] = get_dataframe(str(path), convert_categoricals=False)
        except Exception as exc:  # pragma: no cover - environment-dependent
            pytest.skip(f"cannot read {path}: {exc!r}")
    return out


@pytest.fixture(scope="module")
def crop_production() -> pd.DataFrame:
    """The built table.

    Skips only when the build could not have succeeded for ENVIRONMENTAL
    reasons (no S3 credentials, hence no source data).  With credentials a
    build failure is a DEFECT and is re-raised — a mis-wired `CROP_COLMAPS`
    raises `CropColmapError`, and turning that into a skip is the same silent
    failure these tests exist to close.
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


def _unit_column_profile(codes: pd.Series, harvest_units, harvest_conditions):
    """The three statistics that identify a harvest-unit column by VALUE.

    Returns ``(all_in_units, n_distinct_unit_only, share_above_condition_range)``.
    See the constants at the top of this module for why set membership alone
    is not sufficient.
    """
    unit_only = set(harvest_units) - set(harvest_conditions)
    all_in_units = bool(codes.isin(list(harvest_units)).all())
    n_unit_only = len(set(codes.unique()) & unit_only)
    share_high = float((codes > TOP_CONDITION_CODE).mean())
    return all_in_units, n_unit_only, share_high


def _wave_slice(df: pd.DataFrame, season: str | None = None) -> pd.DataFrame:
    r = df.reset_index()
    r = r[r["t"] == WAVE]
    if season is not None:
        r = r[r["season"] == season]
    return r


# ---------------------------------------------------------------------------
# config-level tests (no data build)
# ---------------------------------------------------------------------------

def test_season_B_declares_a_unit_column(colmap_2018_19):
    """The whole defect in one line: season B's `unit` was `None`."""
    unit = colmap_2018_19["B"]["conditions"][0]["unit"]
    assert unit is not None, (
        "CROP_COLMAPS['2018-19']['B'] declares `unit: None`, which claims the "
        "survey records no harvest unit for season B. AGSEC5B ships `a5bq6b`, "
        "populated on all 7 041 rows (GH #842). `None` is a survey fact and "
        "must not be used as a default."
    )


def test_kg_factor_is_declared_optional_in_the_scheme(uganda_scheme):
    """`KgFactor` must be declared, and declared `optional`.

    It is wired for 2018-19 only, so it is legitimately all-NaN in the other
    six waves; without `optional: true` the all-null sanity check would fire
    on them (CLAUDE.md, "The Silent All-Null Read", Site B).
    """
    col = uganda_scheme.get("KgFactor")
    assert isinstance(col, dict), (
        "Uganda data_scheme.yml declares no `KgFactor` entry for "
        f"{TABLE}; the survey-reported kg rate has no home"
    )
    assert col.get("type") == "float", f"KgFactor declared {col.get('type')!r}"
    assert col.get("optional") is True, (
        "KgFactor is wired for 2018-19 only and is all-NaN in the other six "
        "waves, so it MUST be `optional: true` or Site B of the null-read "
        "audit fires on every one of them."
    )


# ---------------------------------------------------------------------------
# source-level tests — identify the columns by VALUE, never by label
# ---------------------------------------------------------------------------

@pytest.mark.slow
@needs_data
def test_wired_unit_column_holds_unit_codes(colmap_2018_19, agsec5,
                                            harvest_units, harvest_conditions):
    """Season B's wired `unit` must hold UNIT codes, judged one-sidedly.

    The two schemes share 11 codes, so this asserts on the *unit-only* codes
    (in `harvest_units`, not in `harvest_conditions`) — a statistic the
    condition column cannot produce by accident.  Measured 2026-09-09:
    `a5bq6b` is 72.4% unit-only and 0.0% condition-only; the rival `a5bq6c` is
    0.0% unit-only and 56.4% condition-only.
    """
    cond_only = set(harvest_conditions) - set(harvest_units)
    name = colmap_2018_19["B"]["conditions"][0]["unit"]
    codes = pd.to_numeric(agsec5["B"][name], errors="coerce").dropna()
    codes = codes[codes == codes.round()].astype(int)
    assert len(codes) > 0.9 * N_5B_ROWS, f"{name} unexpectedly sparse"

    all_in, n_unit_only, share_high = _unit_column_profile(
        codes, harvest_units, harvest_conditions)
    frac_cond_only = codes.isin(list(cond_only)).mean()
    assert all_in, (
        f"{name} has codes outside `harvest_units`: "
        f"{sorted(set(codes) - set(harvest_units))}"
    )
    assert n_unit_only >= MIN_DISTINCT_UNIT_ONLY_CODES, (
        f"{name} uses only {n_unit_only} distinct unit-only code(s) (21 "
        f"measured) — it is probably not the unit column. Identify by value "
        f"range, never by the variable label: this file's labels and the "
        f"questionnaire disagree with each other (GH #842)."
    )
    assert share_high >= MIN_SHARE_ABOVE_CONDITION_RANGE, (
        f"{name} puts only {share_high:.1%} of rows above code "
        f"{TOP_CONDITION_CODE} (23.1% measured); sacks, bunches and baskets "
        f"live up there and no condition scheme reaches them"
    )
    assert frac_cond_only == 0.0, (
        f"{name} matches condition-only codes on {frac_cond_only:.1%} of rows"
    )


@pytest.mark.slow
@needs_data
def test_season_A_really_has_no_unit_column(colmap_2018_19, agsec5,
                                            harvest_units, harvest_conditions):
    """`unit: None` for season A is a MEASURED survey fact, and stays one.

    AGSEC5A carries two columns both titled "6c. Condition / state"; a future
    reader may be tempted to wire one of them as the unit.  This proves no
    column in the file can be a unit: every numeric-code column is either
    outside `harvest_units` entirely or matches no unit-only code.
    """
    assert colmap_2018_19["A"]["conditions"][0]["unit"] is None, (
        "CROP_COLMAPS['2018-19']['A'] now names a unit column. AGSEC5A ships "
        "none — see the value evidence below and _/CONTENTS.org (GH #842). If "
        "a genuine unit column has been found, update this test with its "
        "measurement rather than deleting it."
    )
    df = agsec5["A"]
    plausible = []
    for col in df.columns:
        codes = pd.to_numeric(df[col], errors="coerce").dropna()
        codes = codes[codes == codes.round()].astype(int)
        if len(codes) < 0.5 * len(df):
            continue                       # too sparse to be the unit
        all_in, n_unit_only, share_high = _unit_column_profile(
            codes, harvest_units, harvest_conditions)
        if (all_in
                and n_unit_only >= MIN_DISTINCT_UNIT_ONLY_CODES
                and share_high >= MIN_SHARE_ABOVE_CONDITION_RANGE):
            plausible.append(f"{col} ({n_unit_only} unit-only codes, "
                             f"{share_high:.1%} above {TOP_CONDITION_CODE})")
    assert not plausible, (
        "AGSEC5A (2018-19) does after all contain column(s) that look like a "
        f"harvest unit: {plausible}. GH #842 measured that it does not — both "
        "a5aq6b and a5aq6c are 100% inside the 20-code condition scheme with "
        "0% unit-only codes, and no other column reaches the sack/bunch/basket "
        "range. Re-measure before changing the colmap."
    )


# ---------------------------------------------------------------------------
# built-table tests
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_2018_19_season_B_serves_a_real_unit(crop_production, agsec5):
    """No `u='Unknown'` in 2018-19 season B where the source records a unit.

    `a5bq6b` is non-null on all 7 041 rows and every code it uses is in
    `harvest_units`, so the sentinel count must be exactly 0 — not "small".
    """
    r = _wave_slice(crop_production, "B")
    assert len(r) > 0, f"{WAVE} season B built empty"
    n_unknown = int((r["u"] == UNIT_SENTINEL).sum())
    raw_populated = int(pd.to_numeric(agsec5["B"]["a5bq6b"],
                                      errors="coerce").notna().sum())
    assert raw_populated == N_5B_ROWS, (
        f"AGSEC5B a5bq6b populated on {raw_populated} rows, expected "
        f"{N_5B_ROWS}; the source changed and this test's premise with it"
    )
    assert n_unknown == 0, (
        f"{WAVE} season B serves u='{UNIT_SENTINEL}' on {n_unknown} of "
        f"{len(r)} rows while AGSEC5B records a unit on all {raw_populated}. "
        f"That is GH #842 regressing: check "
        f"CROP_COLMAPS['{WAVE}']['B']['conditions'][0]['unit']."
    )


@pytest.mark.slow
def test_2018_19_B_unit_vocabulary_is_canonical(crop_production, harvest_units):
    """Every `u` served for 2018-19 is a `harvest_units` Preferred Label.

    (Or the `Unknown` sentinel, which season A legitimately carries.)  Measured
    corpus-wide 2026-09-09: the delivered vocabulary is EXACTLY the 46 Preferred
    Labels plus `Unknown`, in every wave — so a value outside it means a code
    leaked through unmapped.
    """
    r = _wave_slice(crop_production)
    served = set(r["u"].dropna().astype(str))
    allowed = set(harvest_units.values()) | {UNIT_SENTINEL}
    assert served <= allowed, (
        f"{WAVE} serves u values outside `harvest_units` + "
        f"'{UNIT_SENTINEL}': {sorted(served - allowed)}"
    )
    season_b = set(_wave_slice(crop_production, "B")["u"].astype(str))
    assert len(season_b) > 10, (
        f"{WAVE} season B decodes only {len(season_b)} distinct unit(s) "
        f"({sorted(season_b)}); 30 were measured on 2026-09-09. A cell that "
        f"collapses to one value is the pre-#842 symptom."
    )


@pytest.mark.slow
def test_kg_factor_is_carried_for_2018_19(crop_production):
    """`KgFactor` reaches the table for 2018-19, and only for 2018-19.

    It is the survey's own kg-per-unit rate (UNPS q6d).  A reported 0 means
    "not recorded", not "weighs nothing", so it must be NaN — a stored 0 would
    zero out the row's kilograms for whoever multiplies by it.
    """
    assert "KgFactor" in crop_production.columns, (
        "crop_production has no KgFactor column; 2018-19 season A has no unit "
        "at all, so the reported factor is the only kg basis it has (GH #842)"
    )
    r = crop_production.reset_index()
    kgf = pd.to_numeric(r["KgFactor"], errors="coerce")
    assert not ((kgf <= 0).any()), (
        "KgFactor holds non-positive values; 0 means 'not recorded' in UNPS "
        "q6d and must be stored NaN"
    )
    here = kgf[r["t"] == WAVE]
    assert here.notna().mean() > 0.95, (
        f"KgFactor is non-null on only {here.notna().mean():.1%} of {WAVE} "
        f"rows; 99.7% were measured on 2026-09-09"
    )
    # Wired for 2018-19 only, deliberately -- see _/CONTENTS.org. If another
    # wave is wired later this assertion should be RELAXED with its own
    # before/after, not deleted.
    elsewhere = kgf[r["t"] != WAVE]
    assert elsewhere.notna().sum() == 0, (
        "KgFactor is populated outside 2018-19. That may be correct, but "
        "wiring a wave whose kilograms already resolve through the unit label "
        "MOVES numbers and needs its own before/after (_/CONTENTS.org)."
    )


@pytest.mark.slow
def test_wired_unit_agrees_with_the_reported_kg_factor(crop_production):
    """The two kg routes are independent, so agreement validates the wiring.

    One route is a number the household reported (`KgFactor`); the other is a
    weight parsed out of the wired unit's own `harvest_units` label by
    ``transformations._kg_factor_series``.  Had `a5bq6b` been the wrong column
    they could not agree.  Measured 2026-09-09 on the 1 922 season-B rows where
    both exist: median ratio 1.000, p25 = p75 = 1.000, 95.0% within +/-20%.
    """
    from lsms_library.transformations import _kg_factor_series

    label_factor = _kg_factor_series(crop_production)
    r = crop_production.reset_index()
    mask = ((r["t"] == WAVE) & (r["season"] == "B")).to_numpy()
    reported = pd.to_numeric(r["KgFactor"], errors="coerce").to_numpy()[mask]
    from_label = pd.Series(label_factor.to_numpy()[mask])
    both = pd.DataFrame({"rep": reported, "lab": from_label}).dropna()
    both = both[both["lab"] > 0]
    assert len(both) > 500, (
        f"only {len(both)} season-B rows have both a reported and a "
        f"label-derived kg factor; 1 922 were measured -- the harness broke"
    )
    ratio = both["rep"] / both["lab"]
    assert 0.9 < float(ratio.median()) < 1.1, (
        f"reported kg factor and the wired unit's label-implied factor "
        f"disagree: median ratio {float(ratio.median()):.3f} over {len(both)} "
        f"rows. These are INDEPENDENT sources; a systematic gap means the "
        f"`unit` column in CROP_COLMAPS['{WAVE}']['B'] is the wrong column."
    )
    close = float(((ratio > 0.8) & (ratio < 1.25)).mean())
    assert close > 0.8, (
        f"only {close:.1%} of rows agree within +/-20% (95.0% measured); the "
        f"unit wiring or the factor column has changed"
    )
