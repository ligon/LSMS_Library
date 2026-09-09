"""``visit`` on GhanaLSS ``food_acquired`` is the CALENDAR VISIT, not the source
question number.

GLSS numbers its two food modules from different origins.  Section 8H opens
with two screeners (``s8hq1`` "HH consume any home produce", ``s8hq2`` "No. of
months eat home produce"), so its per-visit columns start at q3; Section 9B has
no screener and starts at q1.  Both are asked at the same visits.  Every wave
from 1998-99 on says so in its own Stata variable labels -- GLSS4's ``s9bq1``
is "2nd visit amount spent" and its ``s8hq3`` is "2nd visit units consumed".

Until 2026-09-09 all five multi-visit wave scripts wrote the *question number*
into ``visit``.  A purchase and the own-production consumed at the same visit
therefore sat two apart; ``visit`` was not sliceable across ``s``; and the
extreme values (1, 2 purchased-only and 7, 8 produced-only) read as structural
missingness rather than as a labelling offset.  Nothing caught it because the
derived tables (``food_expenditures`` &c.) group by index level *name* and sum
``visit`` away, so every downstream number was right while the level itself was
wrong.  That is exactly the shape of defect this module exists to pin.

Two tiers, following ``test_ghanalss_food_label_canonical``:

* **Static** (``TestScripts``) -- reads the wave scripts as text.  No cache, no
  microdata, milliseconds.  Catches a re-introduction of the identity mapping,
  which is the regression that actually happened.
* **Data-gated** (``TestDelivered``) -- the real assertion, on the built table;
  skipped when the GhanaLSS cache is cold.
"""
import re

import pytest

from lsms_library.paths import countries_root

COUNTRY = 'GhanaLSS'

# Per wave: the calendar visits each module is fielded at, read off that wave's
# own Stata variable labels (see GhanaLSS/_/CONTENTS.org, "The repeated-visit
# design, wave by wave").  1991-92 is deliberately absent: its source carries no
# variable labels, so its offsets are unverified and its script is unchanged.
EXPECTED = {
    '1998-99': {'purchased': set(range(2, 8)),  'produced': set(range(2, 8))},
    '2005-06': {'purchased': set(range(2, 12)), 'produced': set(range(3, 12))},
    '2012-13': {'purchased': set(range(2, 8)),  'produced': set(range(2, 8))},
    '2016-17': {'purchased': set(range(2, 8)),  'produced': set(range(2, 8))},
}

# The exact expressions that wrote a bare question number into `visit`.  Each is
# the pre-fix line from that wave's script.
IDENTITY_PATTERNS = {
    '1998-99': [r"\{f's9bq\{v\}':\s*v\s+for v in range",
                r"\{f's8hq\{v\}':\s*v\s+for v in range"],
    '2005-06': [r"\{f's9bq\{v\}':\s*f'Expenditure_v\{v\}'",
                r"\{f's8hq\{v\}':\s*f'Quantity_v\{v\}'"],
    '2012-13': [r"di\['visit'\]\s*=\s*i\s*$",
                r'\{f"s8hq\{i\}":\s*f"Quantity_\{i\}"'],
    '2016-17': [r'_stack_visits\(num9b,\s*None,\s*range\(',
                r'_stack_visits\(num8h,\s*cat8h,\s*range\('],
}


def _script(wave):
    return (countries_root() / COUNTRY / wave / '_' / 'food_acquired.py').read_text()


class TestScripts:
    """Static. Always runs."""

    @pytest.mark.parametrize('wave', sorted(IDENTITY_PATTERNS))
    def test_no_bare_question_number_written_into_visit(self, wave):
        src = _script(wave)
        back = [p for p in IDENTITY_PATTERNS[wave]
                if re.search(p, src, re.MULTILINE)]
        assert not back, (
            f'{COUNTRY} {wave}/_/food_acquired.py writes the source QUESTION '
            f'NUMBER into `visit` again ({back}).  8H q1/q2 are screeners, so '
            f'its visit columns start at q3 while 9B starts at q1 -- writing '
            f'the stem puts a purchase and the own-production from the SAME '
            f'visit two apart.  Map through the calendar visit the wave\'s own '
            f'Stata labels give; see GhanaLSS/_/CONTENTS.org.')

    @pytest.mark.parametrize('wave', sorted(EXPECTED))
    def test_mapping_is_documented_in_the_script(self, wave):
        """The offset must carry its evidence, not just be correct."""
        src = _script(wave).lower()
        evidence = r'2nd visit|second visit|2\.\.7|2-7|2\.\.11'
        assert 'visit' in src and re.search(evidence, src), (
            f'{COUNTRY} {wave}: the question-number -> visit mapping is not '
            f'documented in the script.  Quote the source variable label, so '
            f'the next reader can check it against the .dta rather than trust it.')


@pytest.fixture(scope='module')
def visits_by_wave_and_source():
    """{wave: {s: set(visit)}} from the delivered table, or skip if cold."""
    try:
        import lsms_library as ll
        fa = ll.Country(COUNTRY).food_acquired()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} food_acquired not buildable here: {e}')
    if 'visit' not in (fa.index.names or []):
        pytest.skip(f'{COUNTRY} food_acquired has no `visit` level here')
    t = fa.index.get_level_values('t').astype(str)
    s = fa.index.get_level_values('s').astype(str)
    # `visit` is int in most waves and str in 1998-99 (a separate, pre-existing
    # inconsistency); compare on the integer value, which is what it means.
    v = fa.index.get_level_values('visit')
    out = {}
    for wave, src, visit in zip(t, s, v):
        try:
            visit = int(visit)
        except (TypeError, ValueError):
            continue
        out.setdefault(wave, {}).setdefault(src, set()).add(visit)
    return out


@pytest.mark.slow
class TestDelivered:
    """End-to-end. Skipped when the GhanaLSS cache is cold."""

    @pytest.mark.parametrize('wave', sorted(EXPECTED))
    def test_visits_match_the_questionnaire(self, wave, visits_by_wave_and_source):
        got = visits_by_wave_and_source.get(wave)
        if not got:
            pytest.skip(f'{wave} contributed no rows here')
        for source, expected in EXPECTED[wave].items():
            assert got.get(source) == expected, (
                f'{COUNTRY} {wave} s={source!r}: delivered visits '
                f'{sorted(got.get(source, []))} != the {sorted(expected)} the '
                f'wave\'s Stata labels name.')

    @pytest.mark.parametrize('wave', ['1998-99', '2012-13', '2016-17'])
    def test_both_modules_share_one_visit_axis(self, wave,
                                               visits_by_wave_and_source):
        """The point of the fix: `visit` slices across `s`.

        Only for the waves where both modules ARE fielded at the same visits.
        2005-06 is excluded on purpose -- there purchases start at the 2nd
        visit and own production at the 3rd, and that asymmetry is real.
        """
        got = visits_by_wave_and_source.get(wave)
        if not got:
            pytest.skip(f'{wave} contributed no rows here')
        assert got.get('purchased') == got.get('produced'), (
            f'{COUNTRY} {wave}: purchased visits {sorted(got.get("purchased", []))} '
            f'!= produced visits {sorted(got.get("produced", []))}.  Both modules '
            f'are asked at the same visits in this wave, so a mismatch means a '
            f'question number is being written into `visit` again.')

    def test_no_visit_one(self, visits_by_wave_and_source):
        """Visit 1 is intake: roster + diary training, no consumption.

        A `visit == 1` row means an off-by-one crept back in.
        """
        offenders = {w: sorted(s for s, vs in by.items() if 1 in vs)
                     for w, by in visits_by_wave_and_source.items()
                     if w in EXPECTED and any(1 in vs for vs in by.values())}
        assert not offenders, (
            f'{COUNTRY}: consumption recorded at visit 1 in {offenders}.  The '
            f'first visit collects the roster and trains the diary keeper; the '
            f'consumption modules start at the 2nd visit.')
