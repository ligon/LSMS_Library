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

MEASURED, warm rebuild on the branch that introduced them (2026-09-12):

    wave              sum hours    rows > 0   mean hours per worker
    2011-12 before      79,691.9     11,353    7.019
    2011-12 after      345,331.4     11,353   30.418
    2014-15 (unchanged) 221,526.4    10,103   21.927

The 2014-15 / 2011-12 per-worker ratio moves 3.124 -> 0.721.  The residual
1.39x is real and explained in ``Niger/_/CONTENTS.org``: 2014-15's reported
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

    def test_the_two_spellings_are_the_same_number(self):
        # (m * 52/12 * d * h) / 52  ==  (m * d * h) / 12, exactly the
        # relationship the docstring claims.
        m, d, h = 7.0, 5.0, 6.0
        assert (m * (52 / 12) * d * h) / 52 == pytest.approx((m * d * h) / 12)

    def test_2014_15_keeps_its_reported_weeks_term(self):
        other = (countries_root() / COUNTRY / "2014-15" / "_"
                 / "people_last7days.py").read_text(encoding="utf-8")
        assert "hrs = (m * w * d * h) / 52" in other, (
            "2014-15 asks weeks-per-month (MS04Q26) and must use it rather "
            "than the imputed 52/12"
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
            f"(ratio {ratio:.3f}); GH #877 restored this to 1.39"
        )

    def test_2011_12_mean_weekly_hours_is_plausible(self, per_worker_mean):
        # 7.02 h/wk (the pre-#877 figure) is not a plausible mean working week.
        assert 20.0 < per_worker_mean["2011-12"] < 45.0

    def test_2014_15_is_unchanged_by_877(self, per_worker_mean):
        assert per_worker_mean["2014-15"] == pytest.approx(21.927, abs=0.01)
