"""Niger 2011-12 ``wage_work`` reads the 30-day question ``ms04q01`` -- GH #882.

ECVMA 2011-12 asks wage participation twice: ``ms04q01`` ("in the last 30
days?") and ``ms04q02`` ("in the last 12 months?").  ``ms04q02`` is a
FOLLOW-UP, asked only when ``ms04q01`` = No: it is NA for every one of the
1,174 people who did wage work in the last 30 days, and no row is yes on both.
Reading it (as the World Bank's ``NER_ECVMA1.do:1378`` does) served
``wage_work`` NA for every current wage worker and True for 269 people who
worked for a wage in the year but not in the last month.

Ruling @ligon 2026-11-03 (option B): ``wage_work`` reads ``ms04q01``, so all
three 2011-12 dummies (``farm_work`` ms04q03, ``SOB_work`` ms04q05,
``wage_work`` ms04q01) share one 30-day reference period and one universe.

Measured, cold private roots: 2011-12 ``wage_work`` True / False / NA
269 / 18,529 / 6,327 -> 1,174 / 18,798 / 5,153; every other column and every
other wave frame-equal.

The pins test the RULE (the skip pattern in the source, the served value
equal to ``ms04q01`` person by person, the shared NA universe), so a future
"restore WB parity" edit fails here rather than only moving a count.
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

COUNTRY = "Niger"
WAVE = "2011-12"
SCRIPT = countries_root() / COUNTRY / WAVE / "_" / "people_last7days.py"
SOURCE = (countries_root() / COUNTRY / WAVE / "Data" / "NER_2011_ECVMA_v01_M_Stata8"
          / "ecvmaind_p1p2.dta")


def _aws_creds_available() -> bool:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_shared_conftest", Path(__file__).with_name("conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


needs_data = pytest.mark.skipif(
    not SCRIPT.exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


# ---------------------------------------------------------------------------
# mechanism -- no microdata
# ---------------------------------------------------------------------------

def test_script_reads_the_30_day_question():
    src = SCRIPT.read_text(encoding="utf-8")
    assert "wage_work = _yn_bool(src['ms04q01'])" in src
    assert "_yn_bool(src['ms04q02'])" not in src, (
        "ms04q02 is the 12-month follow-up asked only when ms04q01 = No "
        "(GH #882); reading it NAs every current wage worker")


# ---------------------------------------------------------------------------
# the source and the served table
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def source():
    return get_dataframe(str(SOURCE), convert_categoricals=False)


@pytest.fixture(scope="module")
def served():
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY).people_last7days()
    return df.xs(WAVE, level="t")


@needs_data
def test_ms04q02_is_a_follow_up_to_ms04q01(source):
    """The reason for the rule: q02 is asked only when q01 = No."""
    q01, q02 = source["ms04q01"], source["ms04q02"]
    assert q02.notna().sum() > 0
    assert (q01[q02.notna()] == 2).all(), "ms04q02 answered where ms04q01 is not No"
    assert q02[q01 == 1].isna().all(), "ms04q02 asked of a 30-day wage worker"


@needs_data
def test_served_wage_work_is_ms04q01_person_by_person(source, served):
    """Served wage_work equals ms04q01 (1 -> True, 2 -> False, else NA)."""
    sys.path.insert(0, str(countries_root() / COUNTRY / "_"))
    try:
        import niger
    finally:
        sys.path.pop(0)
    expected = pd.DataFrame({
        "i": source["hid"].apply(lambda x: niger.i(x) if pd.notna(x) else pd.NA).astype("string"),
        "pid": source["ms01q00"].apply(format_id).astype("string"),
        "expected": niger._yn_bool(source["ms04q01"]).values,
    }).dropna(subset=["i", "pid"])
    got = served.reset_index()[["i", "pid", "wage_work"]].astype({"i": "string", "pid": "string"})
    m = got.merge(expected, on=["i", "pid"], how="left", validate="one_to_one",
                  indicator=True)
    assert (m["_merge"] == "both").all(), "a served person is missing from the source"
    same = (m["wage_work"] == m["expected"]).fillna(False) | (m["wage_work"].isna() & m["expected"].isna())
    assert bool(same.all()), f"{int((~same).sum())} served rows differ from ms04q01"
    assert int(m["wage_work"].eq(True).sum()) > 0


@needs_data
def test_the_three_dummies_share_one_universe(served):
    na = served["wage_work"].isna()
    assert (na == served["farm_work"].isna()).all()
    assert (na == served["SOB_work"].isna()).all()
