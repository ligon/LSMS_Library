"""Regression tests for Uganda ``crop_production.intercropped`` -- GH #872.

Until 2026-09-12 every wave's ``CROP_COLMAPS[t]['intercrop']['flag']``
pointed at the **seed-use** question ("Did you use any seed/seedlings for
this crop?", ``{1: Yes, 2: No}``: ``a4aq3`` in 2011-12, ``a4aq16`` in
2013-14/2015-16, ``s4aq16`` in 2018-19/2019-20).  The recode is
``2 -> True``, so a served ``intercropped=True`` meant "did **NOT** use
seed" -- it said nothing about the crop stand, and agreed with the real
crop-stand answer on 48.5-52.5% of rows (a coin flip).

The cropping-system question was in the SAME file in every wave:

===============  ==========  =========================================
wave             column      value labels
===============  ==========  =========================================
2009-10,2010-11  ``a4aq7``   {1 Pure Stand, 2 Inter cropped}
2011-12..2015-16 ``a4aq8``   {1 Pure Stand, 2 Mixed Stand}
2018-19,2019-20  ``s4aq08``  {1 Pure Stand, 2 Mixed Stand}
===============  ==========  =========================================

The same question is asked separately in EACH season's roster, and until
the season-B follow-up the season-A answer was served for season-B harvest
rows too -- contradicting AGSEC4B on 27.7% of comparable rows (12,855 of
46,418) and turning 14,671 honest NAs into values.  ``intercrop_B`` wires
season B to its own roster:

===============  ===========  =========================================
wave             column       file
===============  ===========  =========================================
2009-10,2010-11  ``a4bq7``    AGSEC4B, {1 Pure stand, 2 Inter cropped}
2011-12..2015-16 ``a4bq8``    AGSEC4B, {1 Pure Stand, 2 Mixed Stand}
2018-19,2019-20  ``s4bq08``   AGSEC4B, {1 Pure Stand, 2 Mixed Stand}
===============  ===========  =========================================

Three test families:

* ``test_colmap_*`` -- config pins.  No data, always run.  These are the
  load-bearing ones: the defect was a config value, not logic.
* ``test_recode_*`` -- synthetic ``crop_production_for_wave`` calls that
  pin ``{1 -> False, 2 -> True, NA -> NA}``.
* ``test_raw_*`` (data-gated) -- read the REAL AGSEC4A of every wave and
  assert the wired column is the crop-stand one by its Stata VALUE LABELS,
  and that it carries no code outside ``{1, 2, NA}`` (so the unconditional
  ``int(c) == 2`` cannot turn a sentinel into "pure stand").  Skipped
  without S3 credentials, mirroring ``tests/test_uganda_99999.py``.

Deliberately no test reads the warm shared cache through ``Country()``.
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

# The crop-stand column each wave must be wired to.
CROP_STAND = {
    "2009-10": "a4aq7",
    "2010-11": "a4aq7",
    "2011-12": "a4aq8",
    "2013-14": "a4aq8",
    "2015-16": "a4aq8",
    "2018-19": "s4aq08",
    "2019-20": "s4aq08",
}

# The seed-use columns the flag used to point at.  None of these may ever
# be an intercrop flag again.
SEED_USE = {"a4aq3", "a4aq16", "s4aq16", "a4aq10", "s4aq10"}

# Where each wave's AGSEC4A lives, relative to countries_root()/Uganda.
AGSEC4A = {
    "2009-10": "2009-10/Data/AGSEC4A.dta",
    "2010-11": "2010-11/Data/AGSEC4A.dta",
    "2011-12": "2011-12/Data/AGSEC4A.dta",
    "2013-14": "2013-14/Data/AGSEC4A.dta",
    "2015-16": "2015-16/Data/AGSEC4A.dta",
    "2018-19": "2018-19/Data/AGSEC4A.dta",
    "2019-20": "2019-20/Data/Agric/agsec4a.dta",
}

# The SECOND season's roster and its crop-stand column.
CROP_STAND_B = {
    "2009-10": "a4bq7",
    "2010-11": "a4bq7",
    "2011-12": "a4bq8",
    "2013-14": "a4bq8",
    "2015-16": "a4bq8",
    "2018-19": "s4bq08",
    "2019-20": "s4bq08",
}

SEED_USE_B = {"a4bq3", "a4bq16", "s4bq16", "a4bq10", "s4bq10"}

AGSEC4B = {
    "2009-10": "2009-10/Data/AGSEC4B.dta",
    "2010-11": "2010-11/Data/AGSEC4B.dta",
    "2011-12": "2011-12/Data/AGSEC4B.dta",
    "2013-14": "2013-14/Data/AGSEC4B.dta",
    "2015-16": "2015-16/Data/AGSEC4B.dta",
    "2018-19": "2018-19/Data/AGSEC4B.dta",
    "2019-20": "2019-20/Data/Agric/agsec4b.dta",
}

# Which colmap block and source file each season is read from.
SEASON = {"A": ("intercrop", CROP_STAND, SEED_USE, AGSEC4A),
          "B": ("intercrop_B", CROP_STAND_B, SEED_USE_B, AGSEC4B)}


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.  (Duplicated from
    ``tests/test_uganda_99999.py``; the house pattern until a shared
    ``conftest.py`` lands.)"""
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
    not _aws_creds_available(), reason="no S3/DVC credentials for raw AGSEC4A"
)


@pytest.fixture(scope="module")
def uganda_module():
    uganda_dir = str(countries_root() / "Uganda" / "_")
    sys.path.insert(0, uganda_dir)
    try:
        import uganda
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"cannot import uganda module: {exc!r}")
    return uganda


@pytest.fixture()
def in_2009_10_wave_dir():
    """``crop_production_for_wave`` resolves ``categorical_mapping.org`` via
    CWD-relative candidate dirs, exactly as when the real wave script runs
    from its own ``_/``.  Reproduce that CWD."""
    wave_dir = countries_root() / "Uganda" / "2009-10" / "_"
    prev = os.getcwd()
    os.chdir(wave_dir)
    try:
        yield
    finally:
        os.chdir(prev)


# ---------------------------------------------------------------------------
# config pins
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("season", ["A", "B"])
def test_colmap_wires_the_crop_stand_column(uganda_module, season):
    """Every wave's intercrop flag is THE crop-stand column for that wave and
    that SEASON -- AGSEC4A's for season A, AGSEC4B's for season B."""
    block, expected_by_wave, _, _ = SEASON[season]
    for wave, expected in expected_by_wave.items():
        ic = uganda_module.CROP_COLMAPS[wave].get(block)
        assert ic is not None, f"{wave}: {block} block is None"
        assert ic.get("flag") == expected, (
            f"{wave} season {season}: {block} flag is {ic.get('flag')!r}, expected "
            f"{expected!r} (the cropping-system question) -- GH #872"
        )


@pytest.mark.parametrize("season", ["A", "B"])
def test_colmap_never_wires_a_seed_use_column(uganda_module, season):
    """The specific regression: no wave may point an intercrop flag at a
    seed question again.  Stated as a denylist as well as the allowlist
    above, because the failure mode was a *plausible-looking* column in the
    same file with the same {1, 2} codes."""
    block, _, denied, _ = SEASON[season]
    for wave, colmap in uganda_module.CROP_COLMAPS.items():
        ic = colmap.get(block)
        if not ic:
            continue
        assert ic.get("flag") not in denied, (
            f"{wave} season {season}: {block} flag {ic['flag']!r} is a SEED "
            "question, not the cropping system -- GH #872"
        )


def test_colmap_each_season_reads_its_own_roster(uganda_module):
    """Season B must never be wired to a season-A column.  This is the
    season-B regression stated directly: the two rosters ask the same
    question of the same plot in different seasons, and their answers differ
    on 23.3-31.2% of plots, so borrowing A's answer for B is wrong on about
    one row in four -- and wrong in a way that LOOKS authoritative."""
    for wave, colmap in uganda_module.CROP_COLMAPS.items():
        a = colmap["intercrop"]["flag"]
        b = colmap["intercrop_B"]["flag"]
        assert a != b, f"{wave}: both seasons read {a!r}"
        assert "4a" in a.lower() and "4b" in b.lower(), (
            f"{wave}: season A reads {a!r} and season B {b!r}; each season must "
            "read its own roster's column -- GH #872"
        )


def test_colmap_every_wave_populates_intercropped(uganda_module):
    """All seven waves ship a crop-stand question in BOTH seasons' rosters,
    so no wave-season may serve NA for want of a wired column.  2009-10 and
    2010-11 were 100% NaN before #872; season B was borrowed from season A
    before the season-B follow-up."""
    assert set(uganda_module.CROP_COLMAPS) == set(CROP_STAND) == set(CROP_STAND_B), (
        "a wave was added to or removed from CROP_COLMAPS: decide its intercrop "
        "wiring for BOTH seasons (the crop-stand question in its AGSEC4A and "
        "AGSEC4B) and add it to CROP_STAND / CROP_STAND_B / AGSEC4A / AGSEC4B "
        "here -- GH #872"
    )
    for wave, colmap in uganda_module.CROP_COLMAPS.items():
        for block in ("intercrop", "intercrop_B"):
            ic = colmap.get(block)
            assert ic and ic.get("flag"), f"{wave}: no {block} flag wired"
            for key in ("hhid", "parcel", "plot"):
                assert ic.get(key), f"{wave}: {block} has no {key!r} join key"


# ---------------------------------------------------------------------------
# the recode, on synthetic rows
# ---------------------------------------------------------------------------

def _frames(uganda_module, stand_codes):
    """A minimal AGSEC5A + AGSEC4A pair, one plot per household, whose
    crop-stand codes are ``stand_codes``."""
    crop_code = next(iter(uganda_module._crop_label_map()))
    unit_code = next(iter(uganda_module._harvest_unit_map()))
    cond_code = next(iter(uganda_module._harvest_condition_map()))
    n = len(stand_codes)
    hhids = [f"100000{i:04d}" for i in range(n)]
    df5a = pd.DataFrame({
        "HHID": hhids,
        "a5aq1": [1] * n, "a5aq3": [1] * n, "a5aq5": [crop_code] * n,
        "a5aq6a": [5.0] * n, "a5aq6c": [unit_code] * n, "a5aq6b": [cond_code] * n,
        "a5aq7a": [np.nan] * n, "a5aq8": [np.nan] * n,
    })
    df4a = pd.DataFrame({
        "HHID": hhids,
        "a4aq2": [1] * n, "a4aq4": [1] * n, "a4aq6": [crop_code] * n,
        "a4aq7": stand_codes,
    })
    return df5a, df4a


COLMAP_2009_10 = {
    "A": {
        "hhid": "HHID", "parcel": "a5aq1", "plot": "a5aq3", "crop": "a5aq5",
        "conditions": [{
            "qty": "a5aq6a", "unit": "a5aq6c", "condition": "a5aq6b",
            "qty_sold": "a5aq7a", "value_sold": "a5aq8", "month": None,
        }],
    },
    "B": None,
    "intercrop": {"hhid": "HHID", "parcel": "a4aq2", "plot": "a4aq4",
                  "flag": "a4aq7", "crop": "a4aq6"},
}


def test_recode_pure_stand_is_false_mixed_is_true(uganda_module, in_2009_10_wave_dir):
    """{1 Pure Stand -> False, 2 Mixed/Inter cropped -> True}.  The recode
    itself is unchanged by #872 -- it was always right for the crop-stand
    codes and always wrong-by-meaning for the seed-use ones."""
    df5a, df4a = _frames(uganda_module, [1, 2, 1, 2])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave(
            "2009-10", df5a, None, df4a, COLMAP_2009_10)
    got = out.reset_index().set_index("i")["intercropped"]
    assert got.loc["1000000000"] is False or got.loc["1000000000"] == False  # noqa: E712
    assert bool(got.loc["1000000001"]) is True
    assert bool(got.loc["1000000003"]) is True
    assert not bool(got.loc["1000000002"])


def test_recode_missing_code_stays_na(uganda_module, in_2009_10_wave_dir):
    """A plot with no crop-stand answer serves NA, not False.  The lookup is
    only written for ``pd.notna(c)``; this pins that a blank never silently
    becomes "pure stand"."""
    df5a, df4a = _frames(uganda_module, [2, np.nan])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave(
            "2009-10", df5a, None, df4a, COLMAP_2009_10)
    got = out.reset_index().set_index("i")["intercropped"]
    assert bool(got.loc["1000000000"]) is True
    assert pd.isna(got.loc["1000000001"]), "missing crop stand must stay NA"


# ---------------------------------------------------------------------------
# season B reads its OWN roster
# ---------------------------------------------------------------------------

def _season_b_frames(uganda_module, a_codes, b_codes):
    """An AGSEC5A/5B pair over the same plots, plus a 4A roster answering
    ``a_codes`` and a 4B roster answering ``b_codes`` for those same plots."""
    crop_code = next(iter(uganda_module._crop_label_map()))
    unit_code = next(iter(uganda_module._harvest_unit_map()))
    cond_code = next(iter(uganda_module._harvest_condition_map()))
    n = len(a_codes)
    assert len(b_codes) == n
    hhids = [f"100000{i:04d}" for i in range(n)]

    def harvest(prefix):
        return pd.DataFrame({
            "HHID": hhids,
            f"{prefix}q1": [1] * n, f"{prefix}q3": [1] * n,
            f"{prefix}q5": [crop_code] * n,
            f"{prefix}q6a": [5.0] * n, f"{prefix}q6c": [unit_code] * n,
            f"{prefix}q6b": [cond_code] * n,
            f"{prefix}q7a": [np.nan] * n, f"{prefix}q8": [np.nan] * n,
        })

    roster = lambda codes, q: pd.DataFrame({          # noqa: E731
        "HHID": hhids, f"a4{q}q2": [1] * n, f"a4{q}q4": [1] * n,
        f"a4{q}q6": [crop_code] * n, f"a4{q}q7": codes,
    })
    return (harvest("a5a"), harvest("a5b"),
            roster(a_codes, "a"), roster(b_codes, "b"))


def _season_b_colmap(with_b_block=True):
    cm = {
        "A": {
            "hhid": "HHID", "parcel": "a5aq1", "plot": "a5aq3", "crop": "a5aq5",
            "conditions": [{"qty": "a5aq6a", "unit": "a5aq6c", "condition": "a5aq6b",
                            "qty_sold": "a5aq7a", "value_sold": "a5aq8",
                            "month": None}],
        },
        "B": {
            "hhid": "HHID", "parcel": "a5bq1", "plot": "a5bq3", "crop": "a5bq5",
            "conditions": [{"qty": "a5bq6a", "unit": "a5bq6c", "condition": "a5bq6b",
                            "qty_sold": "a5bq7a", "value_sold": "a5bq8",
                            "month": None}],
        },
        "intercrop": {"hhid": "HHID", "parcel": "a4aq2", "plot": "a4aq4",
                      "flag": "a4aq7", "crop": "a4aq6"},
    }
    if with_b_block:
        cm["intercrop_B"] = {"hhid": "HHID", "parcel": "a4bq2", "plot": "a4bq4",
                             "flag": "a4bq7", "crop": "a4bq6"}
    return cm


def test_season_b_reads_agsec4b_not_agsec4a(uganda_module, in_2009_10_wave_dir):
    """THE season-B regression.  The two plots below are answered oppositely
    in the two rosters; season A must serve 4A's answer and season B must
    serve 4B's.  Until this was wired, both seasons served 4A -- wrong on
    27.7% of comparable rows corpus-wide (12,855 of 46,418), because a plot
    really is planted differently in the second season."""
    df5a, df5b, df4a, df4b = _season_b_frames(uganda_module, [1, 2], [2, 1])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave(
            "2009-10", df5a, df5b, df4a, _season_b_colmap(), df4b=df4b)
    got = out.reset_index().set_index(["i", "season"])["intercropped"]
    assert not bool(got.loc[("1000000000", "A")]), "season A must read a4aq7"
    assert bool(got.loc[("1000000000", "B")]), "season B must read a4bq7, not a4aq7"
    assert bool(got.loc[("1000000001", "A")]), "season A must read a4aq7"
    assert not bool(got.loc[("1000000001", "B")]), "season B must read a4bq7, not a4aq7"


def test_season_b_without_a_roster_is_na_never_borrowed(uganda_module,
                                                        in_2009_10_wave_dir):
    """A season with no roster serves NA -- it must NOT fall back to the
    other season's answer.  NA is the honest value; borrowing produces a
    number that looks measured and is wrong about one time in four."""
    df5a, df5b, df4a, _ = _season_b_frames(uganda_module, [2, 2], [1, 1])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave(
            "2009-10", df5a, df5b, df4a, _season_b_colmap(with_b_block=False))
    got = out.reset_index().set_index(["i", "season"])["intercropped"]
    assert bool(got.loc[("1000000000", "A")])
    assert pd.isna(got.loc[("1000000000", "B")]), (
        "season B with no intercrop_B block must be NA, not season A's answer")


def test_season_b_frame_missing_is_na(uganda_module, in_2009_10_wave_dir):
    """Same rule when the block is declared but the FILE is unavailable:
    ``df4b=None`` serves NA for season B, never season A's value."""
    df5a, df5b, df4a, _ = _season_b_frames(uganda_module, [2, 2], [1, 1])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave(
            "2009-10", df5a, df5b, df4a, _season_b_colmap(), df4b=None)
    got = out.reset_index().set_index(["i", "season"])["intercropped"]
    assert bool(got.loc[("1000000000", "A")])
    assert pd.isna(got.loc[("1000000000", "B")])


@needs_data
@pytest.mark.parametrize("wave", sorted(CROP_STAND))
def test_raw_the_two_seasons_genuinely_disagree(uganda_module, wave):
    """Not a wiring check but the JUSTIFICATION for one: on plots rostered in
    both seasons the crop stand differs on 23.3-31.2% of plots, every wave.
    If a future re-release made these agree, borrowing would be harmless and
    this test should be revisited rather than deleted -- it is the evidence
    the season split rests on."""
    from lsms_library.local_tools import get_dataframe

    def lookup(path, block):
        df = get_dataframe(str(countries_root() / "Uganda" / path),
                           convert_categoricals=False)
        ic = uganda_module.CROP_COLMAPS[wave][block]
        hh = uganda_module._format_agsec_hhid(df[ic["hhid"]], wave)
        pa = df[ic["parcel"]].apply(uganda_module.format_id)
        pl = df[ic["plot"]].apply(uganda_module.format_id)
        code = pd.to_numeric(df[ic["flag"]], errors="coerce")
        return {k: int(c) == 2 for k, c in zip(zip(hh, pa, pl), code)
                if pd.notna(c)}

    a = lookup(AGSEC4A[wave], "intercrop")
    b = lookup(AGSEC4B[wave], "intercrop_B")
    common = set(a) & set(b)
    assert len(common) > 1000, f"{wave}: only {len(common)} plots in both rosters"
    disagree = sum(1 for k in common if a[k] != b[k]) / len(common)
    assert 0.15 < disagree < 0.45, (
        f"{wave}: seasons disagree on {disagree:.1%} of {len(common)} shared "
        "plots; measured 23.3-31.2% in 2026-09 -- if this has genuinely moved, "
        "re-measure before changing the wiring")


# ---------------------------------------------------------------------------
# the raw source: the wired column really is the crop-stand question
# ---------------------------------------------------------------------------

@needs_data
@pytest.mark.parametrize("season", ["A", "B"])
@pytest.mark.parametrize("wave", sorted(CROP_STAND))
def test_raw_wired_column_is_value_labelled_pure_vs_mixed(uganda_module, wave, season):
    """Identify the column by its Stata VALUE LABELS, not by its name: code 1
    must be a pure stand and code 2 a mixed/inter-cropped one.  A rename in a
    future re-release would break the name pin above and this one together,
    which is the point -- the meaning is what matters."""
    import pyreadstat
    from lsms_library import data_access

    block, _, _, files = SEASON[season]
    path = countries_root() / "Uganda" / files[wave]
    local = data_access.get_data_file(str(path))
    _, meta = pyreadstat.read_dta(str(local), metadataonly=True)
    col = uganda_module.CROP_COLMAPS[wave][block]["flag"]
    labelset = meta.variable_to_label.get(col)
    assert labelset, f"{wave} {season}: {col} carries no value labels"
    labels = {int(k): str(v).lower() for k, v in meta.value_labels[labelset].items()}
    assert "pure" in labels.get(1, ""), f"{wave} {season}: {col} code 1 is {labels.get(1)!r}"
    assert ("mixed" in labels.get(2, "") or "inter" in labels.get(2, "")), (
        f"{wave} {season}: {col} code 2 is {labels.get(2)!r}")
    assert "seed" not in str(meta.column_names_to_labels.get(col, "")).lower(), (
        f"{wave} {season}: {col} is a seed question -- GH #872")


@needs_data
@pytest.mark.parametrize("season", ["A", "B"])
@pytest.mark.parametrize("wave", sorted(CROP_STAND))
def test_raw_wired_column_has_no_sentinel_codes(uganda_module, wave, season):
    """``crop_production_for_wave`` recodes ANY non-NA code as
    ``int(c) == 2``, so a 0/9/99 "don't know" would be served as False
    (pure stand).  Measured 2026-09-12: all seven waves take values in
    {1, 2, NA} only.  If a future re-release adds a code, this fails and
    the recode must gain an explicit map rather than silently mis-serve."""
    from lsms_library.local_tools import get_dataframe

    block, _, _, files = SEASON[season]
    path = countries_root() / "Uganda" / files[wave]
    df = get_dataframe(str(path), convert_categoricals=False)
    col = uganda_module.CROP_COLMAPS[wave][block]["flag"]
    codes = pd.to_numeric(df[col], errors="coerce").dropna().unique()
    assert set(codes) <= {1, 2}, f"{wave} {season}: {col} carries codes {sorted(codes)}"
