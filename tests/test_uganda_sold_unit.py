"""Regression tests for ``Unit_sold`` / ``Condition_sold`` on Uganda ``crop_production``.

GH #824.  UNPS asks question 7 as a compound question -- "how much of the
harvest was SOLD, in what CONDITION and in what UNIT" -- so the sold quantity
carries its OWN unit code (q7c) and its OWN condition code (q7b), distinct
from the harvest unit (q6c) and harvest condition (q6b) that the table keys
on.  ``CROP_COLMAPS`` wired neither, so ``Value_sold / Quantity_sold`` was a
price denominated in a unit the row did not carry while ``u`` advertised the
harvest unit.  Measured on the built table (2026-09-10, isolated
``LSMS_DATA_DIR``): the two units differ on 3.9% of the 40,083 rows with a
positive ``Quantity_sold`` that know both units, and on the 565 disagreeing
priced rows whose two labels both name a weight in KILOGRAMS the two readings
differ by a median factor of 50 -- >= 100x on 47.4% of them.  Same pathology Malawi closed in ``85947ff5``; different
shape, because Uganda's sale is on the harvest row and needs no merge.

The consumer rule is NOT "always divide by Unit_sold".  Where the two unit
reports disagree the row is a data-quality FLAG: measured, ``Unit_sold`` is
the closer denominator on only 40.5% of the disagreeing priced rows (price
test) and 42.7% under the household's own reported sold-kg factor, so those
rows are AMBIGUOUS and neither label may be used to form a price without
further evidence.  See ``lsms_library/data_info.yml`` and CONTENTS.org.

Uganda is a SCRIPT-PATH table, so these tests deliberately avoid the warm
``Country()`` cache: the fixture-level family builds synthetic frames and the
data-gated family calls ``uganda.crop_production_for_wave`` on the RAW Stata
modules, exactly as ``tests/test_uganda_99999.py`` does and for the same
reason (the shared cache is pre-fix until Uganda is re-warmed after merge).

Which column is the unit and which the condition is decided by the survey's
own VALUE-LABEL vocabulary and VALUE RANGE -- never by the variable label,
and never by copying a Stata do-file.  ``EPAR_UW_Uganda_UNPS_W7.do:520-523``
is the counter-example: on the 2018-19 HARVEST side it assigns a condition
column to ``unit_code_harv``, and because all 20 condition codes also occur
as unit codes in EPAR's own conversion table the merge succeeds on a
condition code.  ``test_2018_19_season_a_sold_column_is_a_unit_by_its_values``
pins that the same trap does NOT recur on the sold side, on that wave's own
numbers.

Schema rules are READ from ``lsms_library/data_info.yml`` and Uganda's
``_/data_scheme.yml`` / ``_/categorical_mapping.org``, never hardcoded
(CLAUDE.md, "Canonical Schema").
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from importlib.resources import files

from lsms_library.paths import countries_root

TABLE = "crop_production"
UNIT_SOLD = "Unit_sold"
CONDITION_SOLD = "Condition_sold"

#: Waves that ship a q7b (sold condition) column.  2011-12 does NOT -- the
#: questionnaire asks it (Uganda2011-12AgricultureQuestionnaire.pdf prints
#: "7a Qty | 7b Condition/State Code | 7c Unit Code" on p.13 for section 5A
#: and p.21 for 5B), so this is `asked-not-distributed`, and the colmap says
#: so with an explicit ``None`` rather than by omission.
WAVES_WITHOUT_SOLD_CONDITION = {"2011-12"}

#: 2018-19 AGSEC5A's sold-unit column, and the numbers that identify it.
#: Measured 2026-09-09 on the raw file: 2,734 non-null, 19 distinct, range
#: 1-92, 84.8% of values are codes that occur ONLY in `harvest_units`, 0.0%
#: are codes that occur ONLY in `harvest_conditions`, and it carries the full
#: 40-label `harvest_units` value-label set under the variable label
#: "7c. Unit".  Its sibling `s5aq07b_1` is the mirror image: 100% inside
#: `harvest_conditions`, 60.6% condition-only, 0.0% unit-only.  Contrast the
#: same file's HARVEST side, which has no unit column at all (GH #842).
W2018_A_UNIT_SOLD = "s5aq07c_1"
W2018_A_CONDITION_SOLD = "s5aq07b_1"
MIN_UNIT_ONLY_SHARE = 0.60      # observed 0.848; a condition column scores 0.000
MAX_CONDITION_ONLY_SHARE = 0.10  # observed 0.000; a condition column scores 0.606


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    (Mirrors the identical helper in ``tests/test_uganda_99999.py`` et al.;
    duplication is the house pattern until a shared ``conftest.py`` lands.)
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
def in_wave_dir():
    """CWD-relative resolution of ``categorical_mapping.org``.

    ``crop_production_for_wave`` resolves its crop / unit / condition label
    tables through ``local_tools.get_categorical_mapping``'s CWD-relative
    candidate dirs, exactly as when the real wave script runs.  Yields a
    callable that chdirs into a given wave's ``_/``.
    """
    prev = os.getcwd()
    def _cd(wave):
        os.chdir(countries_root() / "Uganda" / wave / "_")
    try:
        yield _cd
    finally:
        os.chdir(prev)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


@pytest.fixture(scope="module")
def uganda_scheme() -> dict:
    path = countries_root() / "Uganda" / "_" / "data_scheme.yml"
    with open(path, encoding="utf-8") as f:
        return yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]


@pytest.fixture(scope="module")
def canonical_columns() -> dict:
    with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    return info.get("Columns", {}).get(TABLE, {})


# ---------------------------------------------------------------------------
# config-level: the columns are declared, everywhere they should be and
# nowhere by accident
# ---------------------------------------------------------------------------

def _slots(colmap, season):
    cm = colmap.get(season)
    return list(cm.get("conditions", [])) if cm else []


def test_every_wave_season_declares_a_sold_unit(uganda_module):
    """Every UNPS wave asks q7c; none may inherit "no sold unit" by omission."""
    for wave, colmap in uganda_module.CROP_COLMAPS.items():
        for season in ("A", "B"):
            for n, slot in enumerate(_slots(colmap, season)):
                assert "unit_sold" in slot, (
                    f"CROP_COLMAPS[{wave!r}][{season!r}]['conditions'][{n}] "
                    f"declares no 'unit_sold' key.  Every UNPS wave asks q7c; "
                    f"omitting the key is indistinguishable from forgetting it."
                )
                assert slot["unit_sold"], (
                    f"{wave}/{season} slot {n} declares unit_sold=None -- no "
                    f"Uganda wave-season is known to lack a q7c column"
                )


def test_sold_condition_declared_or_explicitly_absent(uganda_module):
    """``condition_sold`` must be NAMED or explicitly ``None`` -- never absent.

    A missing key and ``None`` behave identically at build time, which is
    exactly why the distinction has to be enforced here: ``None`` is a
    survey FACT (2011-12 ships no 7b column) that a reader can audit, while
    a missing key is silence.  Same argument as ``CropColmapError``'s.
    """
    for wave, colmap in uganda_module.CROP_COLMAPS.items():
        for season in ("A", "B"):
            for n, slot in enumerate(_slots(colmap, season)):
                assert "condition_sold" in slot, (
                    f"CROP_COLMAPS[{wave!r}][{season!r}]['conditions'][{n}] "
                    f"declares no 'condition_sold' key"
                )
                if wave in WAVES_WITHOUT_SOLD_CONDITION:
                    assert slot["condition_sold"] is None, (
                        f"{wave} ships no q7b column in either AGSEC5 file; "
                        f"the colmap must say so with None"
                    )
                else:
                    assert slot["condition_sold"], (
                        f"{wave}/{season} slot {n} declares condition_sold=None, "
                        f"but only {sorted(WAVES_WITHOUT_SOLD_CONDITION)} is known "
                        f"to lack a q7b column.  If a new wave genuinely lacks "
                        f"one, record the questionnaire evidence in "
                        f"Uganda/_/CONTENTS.org and add it to the set above."
                    )


def test_2018_19_season_a_sold_unit_column_is_pinned(uganda_module):
    """2018-19 AGSEC5A HAS a sold unit even though it has no harvest unit.

    The harvest side of this file ships two columns both titled
    "6c. Condition / state" and no unit at all (GH #842), so ``u`` is
    ``'Unknown'`` on all 7,153 rows.  The SOLD side is not like that:
    ``s5aq07c_1`` is a genuine unit column, so a farmgate price for this
    wave-season is reachable.  Pinned by name here and by its VALUES in the
    data-gated test below.
    """
    slot = uganda_module.CROP_COLMAPS["2018-19"]["A"]["conditions"][0]
    assert slot["unit_sold"] == W2018_A_UNIT_SOLD
    assert slot["condition_sold"] == W2018_A_CONDITION_SOLD
    assert slot["unit"] is None, (
        "2018-19 season A's HARVEST unit is still declared absent (GH #842); "
        "if that changed, re-read the #842 entry before touching this test"
    )


def test_data_scheme_declares_both_columns_optional(uganda_scheme):
    for col in (UNIT_SOLD, CONDITION_SOLD):
        assert col in uganda_scheme, (
            f"Uganda data_scheme.yml does not declare {TABLE}.{col}; an "
            f"undeclared column is dropped by Country._finalize_result"
        )
        spec = uganda_scheme[col]
        assert isinstance(spec, dict) and spec.get("optional") is True, (
            f"{col} must be `optional: true`: 2011-12 has no sold-condition "
            f"column at all, and a row with no sale has no sold unit, so the "
            f"null-content guard (Site B) would otherwise fire on honest data"
        )


def test_canonical_schema_declares_the_sold_unit(canonical_columns):
    assert UNIT_SOLD in canonical_columns, (
        "lsms_library/data_info.yml declares no canonical crop_production."
        f"{UNIT_SOLD}; the cross-country meaning of the column has no home"
    )
    spec = canonical_columns[UNIT_SOLD]
    assert spec.get("optional") is True
    assert spec.get("type") == "str"
    note = spec.get("note") or ""
    assert "Value_sold" in note and "Quantity_sold" in note, (
        "the canonical note must state the consumer rule -- Value_sold / "
        "Quantity_sold is a price per Unit_sold, not per u"
    )


def test_sold_condition_shares_the_condition_vocabulary(canonical_columns):
    """One vocabulary, one source of truth.

    ``Condition_sold``'s ``spellings`` block is a YAML ALIAS of
    ``condition``'s (``&crop_condition_vocabulary``), so the two cannot
    drift.  This test fails if someone replaces the alias with a copy that
    then diverges.
    """
    assert canonical_columns[CONDITION_SOLD]["spellings"] == \
        canonical_columns["condition"]["spellings"]


# ---------------------------------------------------------------------------
# fixture-level: canonicalisation, NA (never a sentinel), conflict blanking
# ---------------------------------------------------------------------------

def _frame(uganda_module, *, unit_sold_codes, condition_sold_codes=None,
           hhids=None, n=None):
    """A minimal AGSEC5A-shaped frame carrying a sold quantity and value."""
    crop_map = uganda_module._crop_label_map()
    unit_map = uganda_module._harvest_unit_map()
    condition_map = uganda_module._harvest_condition_map()
    crop_code = next(iter(crop_map))
    unit_code = next(iter(unit_map))
    condition_code = next(iter(condition_map))
    n = n if n is not None else len(unit_sold_codes)
    if condition_sold_codes is None:
        condition_sold_codes = [condition_code] * n
    if hhids is None:
        hhids = [f"100000{i:04d}" for i in range(n)]
    return pd.DataFrame({
        "HHID": hhids,
        "a5aq1": [1] * n,
        "a5aq3": [1] * n,
        "a5aq5": [crop_code] * n,
        "a5aq6a": [10.0] * n,
        "a5aq6b": [condition_code] * n,
        "a5aq6c": [unit_code] * n,
        "a5aq7a": [3.0] * n,
        "a5aq7b": condition_sold_codes,
        "a5aq7c": unit_sold_codes,
        "a5aq8": [1500.0] * n,
    })


def _colmap(*, unit_sold="a5aq7c", condition_sold="a5aq7b"):
    return {
        "A": {"hhid": "HHID", "parcel": "a5aq1", "plot": "a5aq3", "crop": "a5aq5",
              "conditions": [{"qty": "a5aq6a", "unit": "a5aq6c",
                              "condition": "a5aq6b", "qty_sold": "a5aq7a",
                              "unit_sold": unit_sold,
                              "condition_sold": condition_sold,
                              "value_sold": "a5aq8", "month": None}]},
        "intercrop": None,
    }


def test_sold_unit_and_condition_are_canonicalised(uganda_module, in_wave_dir):
    """Codes go through the SAME two org tables ``u`` and ``condition`` use."""
    in_wave_dir("2013-14")
    unit_map = uganda_module._harvest_unit_map()
    condition_map = uganda_module._harvest_condition_map()
    ucodes = list(unit_map)[:3]
    ccodes = list(condition_map)[:3]
    df5a = _frame(uganda_module, unit_sold_codes=ucodes, condition_sold_codes=ccodes)
    out = uganda_module.crop_production_for_wave("2013-14", df5a, None, None, _colmap())
    assert list(out[UNIT_SOLD]) == [unit_map[c] for c in ucodes]
    assert list(out[CONDITION_SOLD]) == [condition_map[c] for c in ccodes]
    # and they are the SAME strings the index levels carry
    assert set(out[UNIT_SOLD]) <= set(unit_map.values())
    assert set(out[CONDITION_SOLD]) <= set(condition_map.values())


def test_unmapped_sold_code_is_NA_not_a_sentinel(uganda_module, in_wave_dir):
    """A code outside the labelled scheme becomes NA.

    ``u`` carries ``'Unknown'`` and ``condition`` ``'unknown_condition'``
    ONLY because they are declared INDEX levels and a null index key is
    deleted outright by ``groupby(dropna=True)``.  These are columns; NA is
    safe and says something a category cannot ("no sold unit recorded").
    """
    in_wave_dir("2013-14")
    bogus = 999999
    df5a = _frame(uganda_module, unit_sold_codes=[bogus],
                  condition_sold_codes=[bogus])
    out = uganda_module.crop_production_for_wave("2013-14", df5a, None, None, _colmap())
    assert out[UNIT_SOLD].isna().all()
    assert out[CONDITION_SOLD].isna().all()
    assert "Unknown" not in set(out[UNIT_SOLD].dropna())
    assert uganda_module._CONDITION_UNKNOWN not in set(out[CONDITION_SOLD].dropna())
    # the harvest levels DO keep their sentinels -- the asymmetry is deliberate
    assert out.index.get_level_values("condition").notna().all()


def test_wave_without_a_sold_condition_column_yields_all_NA(uganda_module, in_wave_dir):
    """``condition_sold: None`` (2011-12) must not raise and must not invent."""
    in_wave_dir("2013-14")
    unit_map = uganda_module._harvest_unit_map()
    df5a = _frame(uganda_module, unit_sold_codes=list(unit_map)[:2])
    out = uganda_module.crop_production_for_wave(
        "2011-12", df5a, None, None, _colmap(condition_sold=None))
    assert out[CONDITION_SOLD].isna().all()
    assert out[UNIT_SOLD].notna().all()


def test_named_but_absent_sold_column_raises(uganda_module, in_wave_dir):
    """A typo must be loud -- the whole point of ``CropColmapError``."""
    in_wave_dir("2013-14")
    unit_map = uganda_module._harvest_unit_map()
    df5a = _frame(uganda_module, unit_sold_codes=list(unit_map)[:1])
    with pytest.raises(uganda_module.CropColmapError):
        uganda_module.crop_production_for_wave(
            "2013-14", df5a, None, None, _colmap(unit_sold="a5aq7z"))


def test_duplicate_rows_disagreeing_on_the_sold_unit_are_blanked(uganda_module,
                                                                 in_wave_dir):
    """A summed ``Quantity_sold`` across two sold units is in no unit.

    The de-duplication block sums ``Quantity_sold`` over rows sharing the
    whole declared index.  Where those rows report DIFFERENT sold units the
    sum has no single denominator, so ``reduce_to_agreed(on_conflict='na')``
    blanks the label rather than picking a winner -- and warns.  Nothing is
    dropped: the row and its summed quantities are served as before.
    """
    in_wave_dir("2013-14")
    unit_map = uganda_module._harvest_unit_map()
    u1, u2 = list(unit_map)[:2]
    # two rows, SAME household/parcel/plot/crop/harvest-unit/harvest-condition
    df5a = _frame(uganda_module, unit_sold_codes=[u1, u2], hhids=["1000000001"] * 2)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = uganda_module.crop_production_for_wave(
            "2013-14", df5a, None, None, _colmap())
    assert len(out) == 1, "the two source rows share the whole declared index"
    assert out[UNIT_SOLD].isna().all(), (
        "disagreeing sold units must blank the label, not pick the first"
    )
    assert float(out["Quantity_sold"].iloc[0]) == 6.0, (
        "the pre-existing sum is unchanged -- only the label is withheld"
    )
    assert any("grain conflict" in str(w.message) for w in caught), (
        "the blanking must be loud (GrainConflictWarning)"
    )


def test_agreeing_duplicate_rows_keep_the_label(uganda_module, in_wave_dir):
    """Agreement is lossless and silent -- the other half of the contract."""
    in_wave_dir("2013-14")
    unit_map = uganda_module._harvest_unit_map()
    u1 = list(unit_map)[0]
    df5a = _frame(uganda_module, unit_sold_codes=[u1, u1], hhids=["1000000001"] * 2)
    out = uganda_module.crop_production_for_wave(
        "2013-14", df5a, None, None, _colmap())
    assert len(out) == 1
    assert out[UNIT_SOLD].iloc[0] == unit_map[u1]


# ---------------------------------------------------------------------------
# data-gated: the column CHOICE, on the real files' own numbers
# ---------------------------------------------------------------------------

def _raw(wave, filename, subdir="Data"):
    from lsms_library.local_tools import get_dataframe
    path = countries_root() / "Uganda" / wave / subdir / filename
    try:
        return get_dataframe(str(path), convert_categoricals=False)
    except Exception as exc:
        if _aws_creds_available():
            raise
        pytest.skip(f"raw {wave}/{filename} unavailable (no S3 creds): {exc!r}")


def _codes(df, col):
    return pd.to_numeric(df[col], errors="coerce").dropna().astype(int)


def test_2018_19_season_a_sold_column_is_a_unit_by_its_values(uganda_module):
    """The evidence for ``s5aq07c_1``, re-derived rather than copied.

    The discriminating statistics are the share of values that are codes
    occurring ONLY in ``harvest_units`` and ONLY in ``harvest_conditions``.
    A unit column scores high on the first and zero on the second; the
    file's condition column (``s5aq07b_1``) scores the mirror image.  This
    is the test that would fail if someone wired the sold unit the way
    ``EPAR_UW_Uganda_UNPS_W7.do:520-523`` wires the harvest unit.
    """
    df = _raw("2018-19", "AGSEC5A.dta")
    os.chdir(countries_root() / "Uganda" / "2018-19" / "_")
    try:
        units = set(uganda_module._harvest_unit_map())
        conds = set(uganda_module._harvest_condition_map())
    finally:
        os.chdir(Path(__file__).parent.parent)
    unit_only, cond_only = units - conds, conds - units

    s = _codes(df, W2018_A_UNIT_SOLD)
    assert len(s) > 2000, f"{W2018_A_UNIT_SOLD} unexpectedly sparse ({len(s)})"
    assert s.isin(unit_only).mean() >= MIN_UNIT_ONLY_SHARE
    assert s.isin(cond_only).mean() <= MAX_CONDITION_ONLY_SHARE

    c = _codes(df, W2018_A_CONDITION_SOLD)
    assert c.isin(conds).mean() > 0.99, "7b must be entirely inside the condition scheme"
    assert c.isin(unit_only).mean() <= MAX_CONDITION_ONLY_SHARE
    assert c.isin(cond_only).mean() > MAX_CONDITION_ONLY_SHARE, (
        "7b must be distinguishable FROM a unit column, not merely compatible "
        "with the condition scheme (the 11 shared codes are compatible with both)"
    )


def test_2013_14_variable_label_swap_is_confined_to_the_harvest_pair(uganda_module):
    """2013-14 AGSEC5A's swap is a HARVEST-side defect only.

    In that one file ``a5aq6b`` is titled "Unit of quantity" but carries
    condition value labels and ``a5aq6c`` the reverse (Uganda/_/CONTENTS.org).
    The SOLD pair in the SAME file is labelled correctly, and this test says
    so on the values: 7b is inside the condition scheme, 7c is a unit.  Do
    not generalise the swap.
    """
    df = _raw("2013-14", "AGSEC5A.dta")
    os.chdir(countries_root() / "Uganda" / "2013-14" / "_")
    try:
        units = set(uganda_module._harvest_unit_map())
        conds = set(uganda_module._harvest_condition_map())
    finally:
        os.chdir(Path(__file__).parent.parent)
    b, c = _codes(df, "a5aq7b"), _codes(df, "a5aq7c")
    assert b.isin(conds).mean() > 0.99            # observed 1.000
    assert b.isin(units - conds).mean() <= 0.02   # observed 0.000
    assert c.isin(units - conds).mean() >= 0.60   # observed 0.761
    # ... and the HARVEST pair really is the swapped one, so the two claims
    # are tested together rather than one being taken on trust.
    assert _codes(df, "a5aq6b").isin(conds).mean() > 0.95
    assert _codes(df, "a5aq6c").isin(units - conds).mean() >= 0.60


def test_2011_12_ships_no_sold_condition_column():
    """The colmap's ``condition_sold: None`` is a survey fact, not an omission."""
    for filename, prefix in (("AGSEC5A.dta", "a5aq"), ("AGSEC5B.dta", "a5bq")):
        df = _raw("2011-12", filename)
        assert f"{prefix}7b" not in df.columns, (
            f"2011-12 {filename} now ships {prefix}7b; wire it and update "
            f"WAVES_WITHOUT_SOLD_CONDITION"
        )
        assert f"{prefix}7c" in df.columns, "the sold UNIT is present in 2011-12"


def test_2010_11_season_a_sold_unit_is_capped_at_code_20():
    """A known, measured extract defect -- pinned so it cannot be forgotten.

    ``a5aq7c`` in 2010-11 AGSEC5A carries no code above 20: 2,115 non-null
    against 3,662 for the same file's 7b, and among rows with a positive
    sold quantity P(sold unit null | harvest unit code >= 21) = 0.973 versus
    0.026 for harvest codes below 21.  So ``Unit_sold`` is legitimately NA
    on most of that cell's sales, and its non-nullity is NOT random -- a
    consumer must never read the NA as "sold in the harvest unit".
    2010-11 AGSEC5B is unaffected (range 0-500).
    """
    a = _raw("2010-11", "AGSEC5A.dta")
    sold = _codes(a, "a5aq7c")
    assert sold.max() <= 20, (
        "2010-11 AGSEC5A a5aq7c is no longer capped at 20 -- the World Bank "
        "may have re-released the wave; re-measure and update CONTENTS.org"
    )
    qty = pd.to_numeric(a["a5aq7a"], errors="coerce")
    harv = pd.to_numeric(a["a5aq6c"], errors="coerce")
    unit = pd.to_numeric(a["a5aq7c"], errors="coerce")
    sel = qty > 0
    assert unit[sel & (harv >= 21)].isna().mean() > 0.90
    assert unit[sel & (harv < 21)].isna().mean() < 0.10

    b = _raw("2010-11", "AGSEC5B.dta")
    assert _codes(b, "a5bq7c").max() > 20, "season B is not capped"


@pytest.mark.parametrize("wave,filename,unit_col,subdir", [
    ("2013-14", "AGSEC5A.dta", "a5aq7c", "Data"),
    ("2018-19", "AGSEC5A.dta", "s5aq07c_1", "Data"),
    ("2019-20", "agsec5a.dta", "s5aq07c_1", "Data/Agric"),
])
def test_built_wave_carries_a_populated_sold_unit(uganda_module, wave, filename,
                                                  unit_col, subdir):
    """End-to-end on the RAW module, bypassing the Country() cache entirely.

    Measured 2026-09-09 (isolated ``LSMS_DATA_DIR``): ``Unit_sold`` is
    non-null on 91.9-99.8% of every wave-season's sale rows except 2010-11
    season A (57.2%, the capped column above).  The floor here is 0.80,
    which every cell in this parametrisation clears by a wide margin and a
    mis-wired column would not.
    """
    df5a = _raw(wave, filename, subdir)
    prev = os.getcwd()
    os.chdir(countries_root() / "Uganda" / wave / "_")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = uganda_module.crop_production_for_wave(
                wave, df5a, None, None, uganda_module.CROP_COLMAPS[wave])
    finally:
        os.chdir(prev)
    assert UNIT_SOLD in out.columns and CONDITION_SOLD in out.columns
    sale = out[out["Quantity_sold"].fillna(0) > 0]
    assert len(sale) > 500, f"{wave} season A has too few sale rows to judge"
    share = sale[UNIT_SOLD].notna().mean()
    assert share >= 0.80, (
        f"{wave} season A Unit_sold non-null on only {share:.1%} of sale rows; "
        f"{unit_col} may be mis-wired"
    )
    # the sold unit is NOT simply a copy of the harvest unit
    known = sale[sale[UNIT_SOLD].notna()]
    known = known[known.index.get_level_values("u") != "Unknown"]
    if len(known):
        differs = (known[UNIT_SOLD].astype(str)
                   != known.index.get_level_values("u").astype(str)).mean()
        assert differs > 0, (
            f"{wave} season A: Unit_sold never differs from u, which would mean "
            f"the colmap is reading the harvest unit twice"
        )


# ---------------------------------------------------------------------------
# data-gated: the shipped COUNTS, pinned
# ---------------------------------------------------------------------------

#: Panel figures shipped as prose in lsms_library/data_info.yml,
#: Uganda/_/data_scheme.yml, Uganda/_/CONTENTS.org and the ledger.  Measured
#: 2026-09-10 on a cold build in an isolated LSMS_DATA_DIR.  The 2026-09-09
#: version of those files shipped 40,183 / 169 / 95.1%, which did not
#: reproduce (the first counted non-sale rows, the second counted non-priced
#: rows, the third was a transcription error).  Pinned here so canonical
#: prose cannot drift from the table again.
SALE_ROWS_KNOWING_BOTH = 40_083         # Quantity_sold > 0, u != Unknown, Unit_sold notna
AGREEMENT_SHARE = 0.9606                # Unit_sold == u on those rows
LARGEST_DISAGREEMENT_CELL = 162         # PRICED rows, u='Sack (100 kgs)' Unit_sold='Kg'
W2009_10_A_COVERAGE = 0.968             # Unit_sold non-null / sale rows


@pytest.fixture(scope="module")
def built_table():
    """The whole built table, from the isolated build.

    Unlike the per-wave tests above this one goes through ``Country()``, so
    it is the only test here that can see the cache.  It skips (rather than
    fails) without S3 credentials, per the house pattern.
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
    return df.reset_index()


def test_shipped_counts_reproduce(built_table):
    """Every number this branch put into canonical prose, re-derived here.

    The definitions are load-bearing and are stated in the constants above:
    a "sale row" is ``Quantity_sold > 0``; "knows both" additionally requires
    ``u != 'Unknown'`` and a non-null ``Unit_sold``; the largest-cell count is
    over PRICED rows (``Value_sold > 0`` as well).
    """
    d = built_table
    sale = d[d["Quantity_sold"].fillna(0) > 0]
    both = sale[(sale["Unit_sold"].notna()) & (sale["u"] != "Unknown")]
    assert len(both) == SALE_ROWS_KNOWING_BOTH, (
        f"rows knowing both units moved: {len(both)} vs "
        f"{SALE_ROWS_KNOWING_BOTH}; update every place the number is quoted "
        f"(data_info.yml, Uganda/_/data_scheme.yml, CONTENTS.org, the ledger)"
    )
    agree = (both["Unit_sold"] == both["u"]).mean()
    assert abs(agree - AGREEMENT_SHARE) < 0.0005, f"agreement share {agree:.4f}"

    a = sale[(sale["t"] == "2009-10") & (sale["season"] == "A")]
    cov = a["Unit_sold"].notna().mean()
    assert abs(cov - W2009_10_A_COVERAGE) < 0.001, (
        f"2009-10 season A coverage {cov:.3f}"
    )

    priced = d[(d["Quantity_sold"].fillna(0) > 0) & (d["Value_sold"].fillna(0) > 0)]
    cell = priced[(priced["u"] == "Sack (100 kgs)")
                  & (priced["Unit_sold"] == "Kg")]
    assert len(cell) == LARGEST_DISAGREEMENT_CELL, (
        f"largest disagreement cell {len(cell)} vs {LARGEST_DISAGREEMENT_CELL}"
    )


def test_the_largest_disagreement_cell_is_priced_like_a_sack(built_table):
    """The measurement that overturned the original consumer rule.

    On the 162 priced rows where the harvest unit says `Sack (100 kgs)` and
    the sold unit says `Kg`, the median `Value_sold / Quantity_sold` is a
    SACK price (~34,000 UShs), not a kilogram price (~500-1,700 for these
    crops).  Dividing by `Unit_sold` there overstates by ~70x -- which is
    why `data_info.yml` no longer says "NEVER per u" and instead calls a
    disagreeing row ambiguous.  If this assertion ever flips, the canonical
    consumer rule has to be revisited, not the test.
    """
    d = built_table
    priced = d[(d["Quantity_sold"].fillna(0) > 0) & (d["Value_sold"].fillna(0) > 0)].copy()
    priced["p"] = priced["Value_sold"] / priced["Quantity_sold"]
    cell = priced[(priced["u"] == "Sack (100 kgs)") & (priced["Unit_sold"] == "Kg")]
    agreeing_kg = priced[(priced["u"] == "Kg") & (priced["Unit_sold"] == "Kg")]
    assert cell["p"].median() > 10 * agreeing_kg["p"].median(), (
        "the disagreement cell is no longer priced like a container; the "
        "consumer rule in data_info.yml assumes it is"
    )
