"""``visit`` on GhanaLSS ``food_acquired`` is an INTEGER CALENDAR VISIT.

Two defects, one level.  The second was found because the first was fixed.

**The calendar visit, not the source question number.**

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

**One integer type across all seven waves.**  Also fixed 2026-09-09.  The level
carried a different PYTHON TYPE per wave -- int in five waves, the strings
``'2'``..``'7'`` in 1998-99, and the English sentence ``'since last visit'`` in
1988-89.  The country-level concat therefore produced an object level of mixed
int/str, on which ``food_acquired().xs(2, level='visit')`` returned 752,970 rows
and SILENTLY OMITTED 1998-99's 104,351 -- a 12.2% undercount of visit-2 rows,
with no exception and no warning.  Both 1980s waves are single-recall and now
carry the library's convention for that, ``visit = 1`` (cf.
``build_transforms.add_visit_level``); it is unambiguous because every
multi-visit round numbers from 2, visit 1 being an intake call that collects no
consumption.

Nothing in the framework polices this: ``_enforce_canonical_dtypes`` iterates
``df.columns`` only, so no index level is dtype-checked anywhere.  And a scan of
the warm caches cannot find it either -- pyarrow launders a mixed object level to
``str`` on write, so the mixed level exists only on the cold BUILD path (measured
2026-09-09: 193 warm L2-country parquets, 543 index levels, zero mixed).  Hence
this module, and hence the STATIC tier below, which reads the scripts rather than
the delivered table.

Two tiers, following ``test_ghanalss_food_label_canonical``:

* **Static** (``TestScripts``) -- reads the wave scripts as text.  No cache, no
  microdata, milliseconds.  Catches a re-introduction of the identity mapping,
  which is the regression that actually happened.
* **Data-gated** (``TestDelivered``) -- the real assertion, on the built table;
  skipped when the GhanaLSS cache is cold.
"""
import re
from importlib.resources import files

import numpy as np
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = 'GhanaLSS'

# Per wave: the calendar visits each module is fielded at, read off that wave's
# own Stata variable labels (see GhanaLSS/_/CONTENTS.org, "The repeated-visit
# design, wave by wave").  1991-92 is deliberately absent: its source carries no
# labels").  1991-92 has no variable labels; its mapping comes from the
# questionnaire (G3QPartB.pdf: eleven date boxes 1st..11th, 9B q1 = "since my
# FIRST visit") and the interviewer's manual, corroborated by the visit dates
# the survey recorded in S0B.DTA.  Its 8 rural / 11 urban split shows as
# purchased rows thinning after visit 8, not as a shorter visit range.
EXPECTED = {
    '1991-92': {'purchased': set(range(2, 12)), 'produced': set(range(2, 12))},
    '1998-99': {'purchased': set(range(2, 8)),  'produced': set(range(2, 8))},
    '2005-06': {'purchased': set(range(2, 12)), 'produced': set(range(3, 12))},
    '2012-13': {'purchased': set(range(2, 8)),  'produced': set(range(2, 8))},
    '2016-17': {'purchased': set(range(2, 8)),  'produced': set(range(2, 8))},
}

# The two 1980s rounds ask the module ONCE ("since my last visit"), so they get
# the library's single-recall convention, `visit = 1` -- an INTEGER, like every
# other wave.  Held separately from EXPECTED because the multi-visit assertions
# (no visit 1; both modules on one axis) are about renumbered waves and would
# read backwards here.
SINGLE_RECALL = {
    '1987-88': {'purchased': {1}, 'produced': {1}},
    '1988-89': {'purchased': {1}, 'produced': {1}},
}

ALL_WAVES = {**SINGLE_RECALL, **EXPECTED}

# The exact expressions that wrote a bare question number into `visit`.  Each is
# the pre-fix line from that wave's script.
IDENTITY_PATTERNS = {
    '1991-92': [r'f"Purchased_v\{i\}":\(f"s9bq\{i\}"',
                r'f"Produced_v\{i\}":\(f"s8hq\{i\}"',
                # the stem range that dropped s9bq1 -- 16.2% of the wave's
                # recorded food purchase expenditure
                r'for i in range\(2,\s*11\)'],
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
        # Each wave must quote the source that fixes ITS mapping -- a Stata
        # variable label for 1998-99 onward, the questionnaire for 1991-92.
        evidence = {
            '1991-92': r'first visit|2nd\.\.11th',
            '1998-99': r'2nd visit|2\.\.7',
            '2005-06': r'second visit|2\.\.11|3rd visit',
            '2012-13': r'2nd visit|2\.\.7',
            '2016-17': r'2nd visit|2\.\.7',
        }[wave]
        assert 'visit' in src and re.search(evidence, src), (
            f'{COUNTRY} {wave}: the question-number -> visit mapping is not '
            f'documented in the script.  Quote the source variable label, so '
            f'the next reader can check it against the .dta rather than trust it.')

    @pytest.mark.parametrize('wave', sorted(SINGLE_RECALL))
    def test_single_recall_waves_write_the_integer_one(self, wave):
        """`VISIT = 1`, not a sentence, not '1'.

        1988-89 wrote ``VISIT = 'since last visit'`` until 2026-09-09.  That is
        a description of the recall WINDOW, which is prose about the instrument
        and belongs in CONTENTS.org; an index level holds a value.
        """
        src = _script(wave)
        m = re.search(r'^VISIT\s*=\s*(.+?)\s*(?:#.*)?$', src, re.MULTILINE)
        assert m, (f'{COUNTRY} {wave}/_/food_acquired.py no longer defines a '
                   f'module-level VISIT constant; if the stamp moved, move this '
                   f'assertion with it rather than deleting it.')
        assert m.group(1) == '1', (
            f'{COUNTRY} {wave}: VISIT = {m.group(1)}, not the integer 1.  A '
            f'single-recall wave takes `visit = 1` -- the convention '
            f'build_transforms.add_visit_level exists to stamp -- so that one '
            f'integer type spans every wave of the country.  A string here '
            f'(worst, an English sentence, which no coercion recovers) makes '
            f'the country-level concat a mixed object level, on which a slice '
            f'or a join silently returns a subset.')

    def test_no_wave_stringifies_visit(self):
        """`visit` is never cast to str on its way into the index.

        1998-99 finished with ``.astype(int).astype(str)``, which made it the
        one multi-visit wave delivering '2'..'7' against everyone else's 2..7.
        The ``.astype(int)`` is fine and stays -- the melt can leave the column
        object-typed; it is the trailing ``.astype(str)`` that is the bug.
        """
        offenders = {}
        for wave in sorted(ALL_WAVES):
            src = _script(wave)
            hits = re.findall(r"^.*\bvisit\b.*\.astype\(\s*(?:str|'str'|\"str\"|"
                              r"pd\.StringDtype\(\))\s*\).*$", src, re.MULTILINE)
            hits = [h.strip() for h in hits if not h.strip().startswith('#')]
            if hits:
                offenders[wave] = hits
        assert not offenders, (
            f'{COUNTRY}: `visit` is cast to str in {offenders}.  It is an '
            f'integer recall occasion in every wave; a per-wave type is a '
            f'silent-slice hazard, not a formatting choice.')


@pytest.fixture(scope='module')
def delivered():
    """The built ``food_acquired``, or skip if it cannot be built here."""
    try:
        import lsms_library as ll
        fa = ll.Country(COUNTRY).food_acquired()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} food_acquired not buildable here: {e}')
    if 'visit' not in (fa.index.names or []):
        pytest.skip(f'{COUNTRY} food_acquired has no `visit` level here')
    return fa


@pytest.fixture(scope='module')
def visit_types(delivered):
    """The distinct PYTHON types on the `visit` level, per wave.

    Kept separate from ``visits_by_wave_and_source`` on purpose: that fixture
    now insists on integers, so it would *raise* rather than report, and the
    type table is the thing worth naming in a failure message.
    """
    t = delivered.index.get_level_values('t').astype(str)
    v = delivered.index.get_level_values('visit')
    out = {}
    for wave, visit in zip(t, v):
        out.setdefault(wave, set()).add(type(visit).__name__)
    return out


@pytest.fixture(scope='module')
def visits_by_wave_and_source(delivered):
    """{wave: {s: set(visit)}} from the delivered table.

    Until 2026-09-09 this did ``try: int(visit) except: continue``, which
    SILENTLY SKIPPED 1988-89 -- the one wave whose value ('since last visit')
    no coercion recovers, and therefore the one wave most in need of covering.
    A fixture that drops what it cannot parse tests only the rows that were
    already fine.  It now insists on an integer and says which wave broke it.
    """
    t = delivered.index.get_level_values('t').astype(str)
    s = delivered.index.get_level_values('s').astype(str)
    v = delivered.index.get_level_values('visit')
    out = {}
    for wave, src, visit in zip(t, s, v):
        if isinstance(visit, bool) or not isinstance(visit, (int, np.integer)):
            raise AssertionError(
                f'{COUNTRY} {wave}: `visit` is {visit!r} ({type(visit).__name__}), '
                f'not an integer.  A level whose Python type differs by wave '
                f'makes the country-level concat an object level of mixed type: '
                f'`xs(2, level="visit")` then silently omits the string waves '
                f'and `xs("2", ...)` silently omits the int ones.  Single-recall '
                f'waves get the integer 1 (build_transforms.add_visit_level); '
                f'renumbered waves get the calendar visit.')
        out.setdefault(wave, {}).setdefault(src, set()).add(int(visit))
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

        A `visit == 1` row in a RENUMBERED wave means an off-by-one crept back
        in.  The two 1980s waves are excluded: they ask the module once and are
        *supposed* to be 1 (see ``test_visit_one_means_single_recall``, which is
        the other half of this and the reason 1 is a safe value to give them).
        """
        offenders = {w: sorted(s for s, vs in by.items() if 1 in vs)
                     for w, by in visits_by_wave_and_source.items()
                     if w in EXPECTED and any(1 in vs for vs in by.values())}
        assert not offenders, (
            f'{COUNTRY}: consumption recorded at visit 1 in {offenders}.  The '
            f'first visit collects the roster and trains the diary keeper; the '
            f'consumption modules start at the 2nd visit.')


@pytest.mark.slow
class TestVisitTypeDelivered:
    """One integer type, all seven waves.  Skipped when the cache is cold."""

    def test_level_is_one_integer_type(self, visit_types):
        """The whole point: no wave may differ from the others in TYPE.

        Reported per wave rather than as a single dtype, because the failure
        this pins is precisely a per-wave difference -- and because the
        aggregate dtype LIES on a warm read: pyarrow coerces a mixed object
        level to `str` on write, so the L2-country parquet comes back
        homogeneously stringy no matter what the build produced.
        """
        seen = {w: sorted(ts) for w, ts in sorted(visit_types.items())}
        bad = {w: ts for w, ts in seen.items()
               if ts != ['int'] and ts != ['int64']}
        assert not bad, (
            f'{COUNTRY} food_acquired `visit` is not integer in {bad} '
            f'(all waves: {seen}).  Mixed types on one index level: '
            f'`xs(2, level="visit")` silently omits the string waves, '
            f'`xs("2", ...)` silently omits the int ones, and a merge on an '
            f'Int64 key matches zero rows for the string side with no error.')

    @pytest.mark.parametrize('wave', sorted(ALL_WAVES))
    def test_every_wave_is_covered(self, wave, visits_by_wave_and_source):
        """Every wave must actually reach the assertions.

        The old fixture skipped a row it could not ``int()``, so 1988-89 -- the
        only broken wave -- contributed nothing and the suite passed green on a
        table it had not looked at.  This makes the absence of a wave a failure
        rather than a silence.
        """
        assert wave in visits_by_wave_and_source, (
            f'{COUNTRY} {wave} contributed no rows to the delivered table.  If '
            f'that is genuinely expected, say why here; do not let a wave drop '
            f'out of the fixture unremarked.')

    @pytest.mark.parametrize('wave', sorted(SINGLE_RECALL))
    def test_single_recall_waves_deliver_exactly_visit_one(
            self, wave, visits_by_wave_and_source):
        """The 1980s waves, which the old fixture never reached.

        ``TestDelivered.test_visits_match_the_questionnaire`` covers the five
        renumbered waves; this is the same assertion for the two that ask the
        module once, on both `s` sides.
        """
        got = visits_by_wave_and_source[wave]
        for source, expected in SINGLE_RECALL[wave].items():
            assert got.get(source) == expected, (
                f'{COUNTRY} {wave} s={source!r}: delivered visits '
                f'{sorted(got.get(source, []))} != {sorted(expected)}.  A '
                f'single-recall wave carries the integer 1 and nothing else.')

    def test_visit_one_means_single_recall(self, visits_by_wave_and_source):
        """`visit == 1` identifies the single-recall waves, and only those.

        This is what makes 1 a safe value to stamp on 1987-88 / 1988-89 rather
        than an ambiguity: every renumbered wave starts at 2 because visit 1 is
        an intake call collecting no consumption, so the two readings of 1
        cannot collide.  Verified against the data, not assumed.
        """
        with_one = {w for w, by in visits_by_wave_and_source.items()
                    if any(1 in vs for vs in by.values())}
        assert with_one == set(SINGLE_RECALL), (
            f'{COUNTRY}: `visit == 1` appears in {sorted(with_one)}, but the '
            f'single-recall waves are {sorted(SINGLE_RECALL)}.  If a renumbered '
            f'wave has grown a visit 1, the convention no longer distinguishes '
            f'"asked once" from "first calendar visit" and the stamp on the '
            f'1980s waves must be reconsidered -- not silently kept.')


class TestCanonicalDeclaration:
    """Config-only.  `visit` has a written type contract; nothing coerces it."""

    @staticmethod
    def _canonical():
        with open(files('lsms_library') / 'data_info.yml', encoding='utf-8') as f:
            return yaml.safe_load(f)

    @pytest.mark.parametrize('table', ['food_acquired', 'interview_date'])
    def test_visit_declares_int(self, table):
        entry = ((self._canonical().get('Columns') or {})
                 .get(table, {}).get('visit'))
        assert isinstance(entry, dict) and entry.get('type') == 'int', (
            f'data_info.yml declares no `Columns.{table}.visit: type: int`.  '
            f'`visit` is an index level and index levels are unpoliced, so this '
            f'declaration is where the contract lives -- the same role '
            f'`crop_production.condition` plays for its vocabulary.')

    @pytest.mark.parametrize('table', ['food_acquired', 'interview_date'])
    def test_visit_is_not_required(self, table):
        """It is a level, not a column; `required` would be read as a column."""
        entry = ((self._canonical().get('Columns') or {})
                 .get(table, {}).get('visit')) or {}
        assert not entry.get('required'), (
            f'data_info.yml marks {table}.visit `required`.  '
            f'`test_schema_consistency` reads `required` as "every country '
            f'declaring this table must have this COLUMN", and `visit` is an '
            f'index level that only repeated-recall countries carry at all.')

    def test_dtype_enforcement_still_skips_index_levels(self):
        """Pins the decision, so a later change to it is deliberate.

        Declaring the type does NOT make it coerced, and that is on purpose:
        `_enforce_canonical_dtypes`'s int path is
        `pd.to_numeric(errors='coerce')`, which on an index level converts an
        unparseable value to <NA> -- a row that is served and then deleted by
        the first `groupby`, counted nowhere.  Applied to the 2026-09-09 defect
        it would have silently dropped all 72,649 of 1988-89's rows instead of
        surfacing them.  If index-level enforcement is ever added it must be a
        loud CHECK on the build path, not a coercion on the read path.
        """
        import pandas as pd

        from lsms_library.country import _enforce_canonical_dtypes

        df = pd.DataFrame(
            {'Quantity': [1.0, 2.0]},
            index=pd.MultiIndex.from_tuples(
                [('1988-89', 'since last visit'), ('1998-99', '2')],
                names=['t', 'visit']))
        _enforce_canonical_dtypes(df, 'food_acquired')
        got = list(df.index.get_level_values('visit'))
        assert got == ['since last visit', '2'], (
            f'_enforce_canonical_dtypes now rewrites the `visit` index level '
            f'({got}).  That is a real design change, not a fix: on this input '
            f'a numeric coercion yields <NA> on a DECLARED INDEX LEVEL, which '
            f'is served and then deleted by the first groupby with nothing '
            f'reported.  Read data_info.yml `Columns > food_acquired > visit` '
            f'before keeping it.')
