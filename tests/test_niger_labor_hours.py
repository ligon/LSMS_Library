"""Niger `people_last7days` hours are an annual-average WEEK -- GH #877.

2011-12's wave script built `farm_hrs` / `SB_hrs` / `wage_hrs` as
``(months * days * hours) / 52``.  Its own Stata labels (French *and* the
English-labelled copy) and questionnaire ``ECVMA_Quest_MEN_P1_V10_ENG.pdf``
4.29-4.31 say ``ms04q31`` is days per **WEEK**, so ``d * h`` is already usual
weekly hours and the ``/52`` delivered ``(months/52) x`` that -- 0.23x for a
full-year job, and ~4.3x below 2014-15's dimensionally coherent
``(m * w * d * h) / 52``.  The fix imputes the exact calendar 52/12
weeks-per-month that 2011-12 does not ask for, i.e. divides by 12.

The World Bank's own cleaning code has the same error
(``NER_ECVMA1.do:1413``), so this is a deliberate parity departure and the
pins below exist to stop a future "restore WB parity" edit from reinstating it
silently.

A second defect of the same issue is pinned here too: every time-use input
declares a ``manquant`` code in the file's own Stata value labels
(``ms04q29/q30/q55/q56`` -> 99, ``ms04q31/q57`` -> 9; 2014-15 likewise), and
``convert_categoricals=False`` handed those back as quantities -- 99 hours a
day, 9 days in a week -- which the formula then multiplied.  They are NA'd
per variable, read from the labels at build time.  Per variable and never
blanket: 610 people genuinely report ``ms04q30 == 9`` hours a day, and that
variable's missing code is 99.

MEASURED, warm rebuild on the branch that introduced them (2026-09-12):

    wave                 sum hours   rows > 0   mean h/worker      max
    2011-12 before #877    79,691.9    11,353          7.019   1,710.2
    2011-12 after         328,476.7    11,350         28.941     238.0
    2014-15 before #877   221,526.4    10,103         21.927     148.6
    2014-15 after         221,516.7    10,102         21.928     148.6

The 2014-15 / 2011-12 per-worker ratio moves 3.124 -> 0.758.  The residual
1.32x is real and explained in ``Niger/_/CONTENTS.org``: 2014-15's reported
weeks-per-month is 4 for 86% of jobs, not 4.333, plus composition.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from lsms_library.paths import countries_root

COUNTRY = "Niger"
SCRIPT = countries_root() / COUNTRY / "2011-12" / "_" / "people_last7days.py"

HOURS_COLS = ["farm_hrs", "SB_hrs", "wage_hrs"]


def _load_niger_module():
    """Import ``Niger/_/niger.py`` by path (it is not on an import path)."""
    import importlib.util

    path = countries_root() / COUNTRY / "_" / "niger.py"
    spec = importlib.util.spec_from_file_location("_niger_for_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# mechanism -- no microdata, no credentials
# ---------------------------------------------------------------------------

class TestFormulaPinnedInSource:
    """The 2011-12 script must not go back to the WB's ``/52``."""

    def test_weeks_per_month_is_the_exact_calendar_value(self):
        src = SCRIPT.read_text(encoding="utf-8")
        assert "WEEKS_PER_MONTH = 52 / 12" in src, (
            "2011-12 people_last7days.py must impute 52/12 weeks per month; "
            "see GH #877 and Niger/_/CONTENTS.org"
        )
        assert "hrs = (m * WEEKS_PER_MONTH * d * h) / 52" in src
        assert "hrs = (m * d * h) / 52" not in src, (
            "the WB formula (NER_ECVMA1.do:1413) treats ms04q31 as days per "
            "MONTH; it is days per WEEK.  GH #877."
        )

    def test_2014_15_keeps_its_reported_weeks_term(self):
        other = (countries_root() / COUNTRY / "2014-15" / "_"
                 / "people_last7days.py").read_text(encoding="utf-8")
        assert "hrs = (m * w * d * h) / 52" in other, (
            "2014-15 asks weeks-per-month (MS04Q26) and must use it rather "
            "than the imputed 52/12"
        )

    def test_both_waves_na_the_declared_missing_code(self):
        """Neither wave may feed a raw value-label code into the formula."""
        for wave in ("2011-12", "2014-15"):
            src = (countries_root() / COUNTRY / wave / "_"
                   / "people_last7days.py").read_text(encoding="utf-8")
            assert "_num_no_declared_missing" in src, (
                f"{wave} must NA each time-use input's declared `manquant` "
                "code before multiplying it (GH #877)"
            )
            assert "get_categorical_mapping" in src, (
                f"{wave} must read the missing code from the .dta value "
                "labels, not hardcode it"
            )

    def test_the_mask_is_per_variable_not_blanket(self):
        """The real helper, on a synthetic frame -- no microdata needed.

        A blanket mask of 9 would delete the 610 people who genuinely report
        ``ms04q30 == 9`` hours a day; 9 is ``manquant`` only where the
        variable's own value labels say so.
        """
        import pandas as pd

        niger = _load_niger_module()
        src = pd.DataFrame({
            "days": [7, 9, 3, 99],       # declares 9 == manquant
            "hours": [9, 8, 99, 4],      # declares 99 == manquant
            "weeks": [4, 9, 99, 2],      # no value labels at all
        })
        labels = {"days": {9: "manquant"}, "hours": {99: "Manquant"}}

        days = niger._num_no_declared_missing(src, "days", labels)
        hours = niger._num_no_declared_missing(src, "hours", labels)
        weeks = niger._num_no_declared_missing(src, "weeks", labels)

        assert list(days.isna()) == [False, True, False, False], (
            "9 must be NA in `days` (its declared code) and 99 must NOT be -- "
            "the mask is per variable"
        )
        assert list(hours.isna()) == [False, False, True, False], (
            "the genuine 9-hour day must survive; 99 is this variable's code"
        )
        assert list(weeks.isna()) == [False] * 4, (
            "a variable with no value labels is returned unmasked "
            "(2014-15 MS04Q26)"
        )



# ---------------------------------------------------------------------------
# served values -- needs the microdata
# ---------------------------------------------------------------------------

def _aws_creds_available() -> bool:
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
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


@pytest.fixture(scope="module")
def per_worker_mean():
    import lsms_library as ll

    df = ll.Country(COUNTRY).people_last7days().reset_index()
    df["tot"] = df[HOURS_COLS].sum(axis=1)
    out = {}
    for t, sub in df.groupby("t"):
        pos = sub.loc[sub["tot"] > 0, "tot"]
        out[str(t)] = float(pos.mean()) if len(pos) else float("nan")
    return out


@needs_data
class TestServedHours:

    def test_the_two_ecvma_waves_are_within_a_factor_of_two(self, per_worker_mean):
        """Before #877 the ratio was 3.12; it is now 0.72.

        The pin is deliberately loose (the residual 1.39x is a real,
        documented difference between the two instruments), but 3x apart is
        the formula bug returning.
        """
        a, b = per_worker_mean["2011-12"], per_worker_mean["2014-15"]
        ratio = max(a, b) / min(a, b)
        assert ratio < 2.0, (
            f"2011-12 mean {a:.3f} h/worker vs 2014-15 {b:.3f} "
            f"(ratio {ratio:.3f}); GH #877 restored this to 1.32"
        )

    def test_2011_12_mean_weekly_hours_is_plausible(self, per_worker_mean):
        # 7.02 h/wk (the pre-#877 figure) is not a plausible mean working
        # week.  Measured after the fix: 28.941.
        assert 20.0 < per_worker_mean["2011-12"] < 45.0

    def test_no_row_carries_a_declared_missing_code_as_hours(self):
        """The 7,410 h/week row is gone; the survivors are two-job sums."""
        import lsms_library as ll

        df = ll.Country(COUNTRY).people_last7days().reset_index()
        tot = df[HOURS_COLS].sum(axis=1)
        t = df["t"].astype(str)
        assert tot.max() < 300.0, (
            f"max weekly hours {tot.max():.1f}; before #877 it was 7,410.7, "
            "driven by ms04q55 == 99 / ms04q56 == 99 (declared `manquant`)"
        )
        assert int((tot[t == "2011-12"] > 168).sum()) == 3
        assert int((tot[t == "2014-15"] > 168).sum()) == 0

    def test_2014_15_mean_is_the_measured_value(self, per_worker_mean):
        # 21.927 before #877, 21.928 after: the wave's only declared-missing
        # code present is MS04Q27 == 9, on a single row.
        assert per_worker_mean["2014-15"] == pytest.approx(21.928, abs=0.01)
