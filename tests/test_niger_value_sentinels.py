"""No served Niger quantity or sale value is an all-nines "don't know" code -- GH #865.

The ECVMA harvest / sale modules record "ne sait pas" as an unlabelled
all-nines number, so ``convert_categoricals`` cannot see it and it was served
as a harvest of 999,999 units (Site Q, GH #857, reported it and by contract
never NaNs).  The codes differ by wave and by column -- 2011-12 999999 on
the quantities and 9999999 on the sale value; 2014-15 9999 and 9999999;
2021-22 ``plot_inputs`` 9999 -- and each WAVE script strips its own literal
codes through ``niger._na_exact`` (never a range; the sibling of Uganda's
``qty_sentinel`` in GH #861).  See ``Niger/_/CONTENTS.org``, "The VALUE
sentinel".

Two families, split as in ``tests/test_niger_u_sentinel.py``:

* ``TestStripIsDeclared`` needs NO microdata: it pins the literal codes in
  each wave script and that the strip is exact (a real 9,998 or 10,000
  survives).
* ``TestNothingServedIsACode`` builds the real tables and is skipped when
  Niger is unavailable.

Raw counts (``get_dataframe(..., convert_categoricals=False)``, 2026-09-24):
2011-12 ``as02eq07a`` 999999 x11, ``as02eq12a`` 999999 x3, ``as02eq13``
9999999 x2; 2014-15 ``AS02EQ07A`` 9999 x23, ``AS02EQ12A`` 9999 x7,
``AS02EQ13`` 9999999 x7; 2021-22 ``s16bq03a`` 9999 x1.
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.paths import countries_root

COUNTRY = "Niger"

#: (wave, script) -> the literal codes the script must declare, per name.
DECLARED = {
    ("2011-12", "crop_production.py"): {"QTY_SENTINELS": (999999,),
                                        "VALUE_SENTINELS": (9999999,)},
    ("2014-15", "crop_production.py"): {"QTY_SENTINELS": (9999,),
                                        "VALUE_SENTINELS": (9999999,)},
    ("2021-22", "plot_inputs.py"): {"QTY_SENTINELS": (9999,)},
}

#: Every code any Niger wave uses on these columns; a served value equal to
#: one of them, in ANY wave, is the bug returning.
ALL_CODES = (9999, 999999, 9999999)

#: Raw occurrences per (wave, column), so a regression in the source read
#: cannot hide inside a served-side check that passes vacuously.
RAW_COUNTS = {
    ("2011-12", "ecvmaas2e_p2.dta", "as02eq07a", 999999): 11,
    ("2011-12", "ecvmaas2e_p2.dta", "as02eq12a", 999999): 3,
    ("2011-12", "ecvmaas2e_p2.dta", "as02eq13", 9999999): 2,
    ("2014-15", "ECVMA2_AS2E1P2.dta", "AS02EQ07A", 9999): 23,
    ("2014-15", "ECVMA2_AS2E2P2.dta", "AS02EQ12A", 9999): 7,
    ("2014-15", "ECVMA2_AS2E2P2.dta", "AS02EQ13", 9999999): 7,
    ("2021-22", "s16b_me_ner2021.dta", "s16bq03a", 9999): 1,
}
RAW_DIRS = {
    "2011-12": "NER_2011_ECVMA_v01_M_Stata8",
    "2014-15": "NER_2014_ECVMA-II_v02_M_STATA8",
    "2021-22": "",
}


def _root() -> Path:
    return countries_root() / COUNTRY


def _aws_creds_available() -> bool:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (Path(__file__).parent.parent
                  / "lsms_library" / "countries" / ".dvc" / "s3_creds")
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


needs_data = pytest.mark.skipif(
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


def _module_constants(path: Path) -> dict:
    """Top-level ``NAME = (...)`` literals of a wave script, read with ``ast``
    (a wave script runs data reads at import, so it is never imported)."""
    out = {}
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    try:
                        out[t.id] = ast.literal_eval(node.value)
                    except ValueError:
                        pass
    return out


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestStripIsDeclared:

    @pytest.mark.parametrize("wave,script", sorted(DECLARED))
    def test_wave_script_declares_its_own_literal_codes(self, wave, script):
        consts = _module_constants(_root() / wave / "_" / script)
        for name, codes in DECLARED[(wave, script)].items():
            assert consts.get(name) == codes, (
                f"{wave}/{script}: {name} is {consts.get(name)!r}, expected {codes}")
        src = (_root() / wave / "_" / script).read_text(encoding="utf-8")
        assert "_na_exact(" in src, f"{wave}/{script} does not call _na_exact"

    def test_the_strip_is_exact_not_a_range(self):
        sys.path.insert(0, str(_root() / "_"))
        try:
            from niger import _na_exact
        finally:
            sys.path.pop(0)
        s = pd.Series([9999, 9998, 10000, 999999, 1000000, 9999999, None, 0.5])
        out = _na_exact(s, (9999, 999999, 9999999))
        assert list(out.isna()) == [True, False, False, True, False, True, True, False]
        assert out.dtype == "Float64"
        # A code not listed for this wave is NOT stripped.
        assert _na_exact(pd.Series([9999.0]), (999999,)).iloc[0] == 9999.0

    def test_no_strip_lives_in_the_shared_tail(self):
        """The literals belong to the wave scripts (one wave's hash), not to
        ``_finish_crop_production`` (all 18 Niger tables)."""
        src = (_root() / "_" / "niger.py").read_text(encoding="utf-8")
        start = src.index("def _finish_crop_production")
        end = src.index("\ndef ", start + 1)
        body = src[start:end]
        assert "_na_exact" not in body and "999999" not in body, (
            "sentinel strip found inside _finish_crop_production")


# ---------------------------------------------------------------------------
# built tables
# ---------------------------------------------------------------------------

@needs_data
class TestNothingServedIsACode:

    @pytest.fixture(scope="class")
    def country(self):
        import lsms_library as ll
        return ll.Country(COUNTRY)

    @pytest.mark.parametrize("key", sorted(RAW_COUNTS))
    def test_raw_source_still_carries_the_code(self, key):
        """The measurement behind the strip: if the raw count changes the
        strip is stripping something else (or a re-release moved the code)."""
        from lsms_library.local_tools import get_dataframe
        wave, fname, col, code = key
        path = _root() / wave / "Data" / RAW_DIRS[wave] / fname
        df = get_dataframe(str(path), convert_categoricals=False)
        n = int((pd.to_numeric(df[col], errors="coerce") == code).sum())
        assert n == RAW_COUNTS[key], f"{wave} {col} == {code}: {n} raw rows"

    @pytest.mark.parametrize("table,columns", [
        ("crop_production", ["Quantity", "Quantity_sold", "Value_sold"]),
        ("plot_inputs", ["Quantity", "Quantity_purchased"]),
    ])
    def test_no_served_value_equals_a_code(self, country, table, columns):
        df = getattr(country, table)()
        for col in columns:
            hits = df[col].isin(ALL_CODES)
            n = int(hits.sum())
            assert n == 0, (
                f"{table}.{col}: {n} served rows equal a sentinel code, by wave "
                f"{df[hits].groupby(level='t').size().to_dict()}")
