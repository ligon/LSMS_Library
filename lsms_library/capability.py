"""What an instrument MEASURES -- recorded when we acquire it, not rediscovered later.

The coverage matrix grades a cell ``absent`` when a feature's source is not
declared for a wave.  ``absent`` conflates two states that could not be more
different (see :mod:`lsms_library.coverage_matrix`):

* ``todo``       -- the data IS there; nobody wrote the config.  Real work.
* ``not-asked``  -- the instrument genuinely never asked.  Closed forever.

Today the only way to tell them apart is to *probe* an already-absent cell:
a label sweep, a sibling-wave differential, and a questionnaire read.  That
probe is expensive, and it is run months after acquisition -- to rediscover a
fact that was knowable **the day the survey was acquired**.

Consider South Africa's General Household Survey.  It has no consumption
module.  When it lands, ``South Africa / food_acquired / 2015`` will grade
``absent``, someone will file it as a gap, and a probe will eventually
establish that the GHS does not ask about food acquisition.  We knew that
before we downloaded it.

So: **capability, recorded at acquisition time, pre-populates the verdict.**
A :class:`SeriesCapability` says what a survey series can and cannot populate.
Its ``lacks`` set maps directly onto ``absent`` cells that need no probe.

    The probe sweep then only has to adjudicate cells we inherited *without* a
    capability record -- a shrinking set, not a permanent tax.

Why this is not a licence to write ``not-asked`` from a catalog blurb
---------------------------------------------------------------------

It would be very easy to turn this into the Albania mistake with better
paperwork.  Albania's ``data_scheme.yml`` asserted "earlier waves have no
shocks module"; Albania 2005's ``migrationE_cl.dta`` carries ``m6e_q00 =
'Type of Shock Code'`` with ten shock types.  The claim was false, nothing
recorded *how* it was reached, and it silently suppressed work.

A ``not-asked`` verdict is a **permanent, unsupervised write**.  Our own
standard (``docs/guide/coverage.md``) is unambiguous: **C4 -- the questionnaire
-- is mandatory before any permanent close**, because *absence in the shipped
extract is not absence in the instrument*.  Only the questionnaire separates
``not-asked`` from ``asked-not-distributed``, and those route to entirely
different queues.

A capability asserted from **catalog metadata alone is not C4.**  It is a
topic list and an abstract written by a cataloguer.  It is good evidence for
*where to look*; it is not evidence that a question was never asked.

So a capability record carries an explicit **validation level** -- exactly the
distinction ``PROVENANCE_VALIDATION: content-validated | catalog-only`` draws
for provenance -- and *only the top rung may close a cell*:

===========================  ==============================  ==================
validation                   how it was established          proposes
===========================  ==============================  ==================
``catalog-only``             WB catalog topics / abstract    ``unsure``
``data-validated``           C1 label sweep over the         ``unsure``
                             shipped extract came back
                             negative
``questionnaire-validated``  C4: the questionnaire was read  ``not-asked``
===========================  ==============================  ==================

``data-validated`` deliberately does **not** close either: a negative label
sweep is exactly as consistent with ``asked-not-distributed`` as with
``not-asked``.  That is the whole reason C4 exists.

``unsure`` keeps the cell in the queue and records *why* -- so the record is
still worth writing.  It converts "nobody has looked at this" into "we have a
catalog-level expectation, unconfirmed", which is a strictly better starting
point for the probe, and it can never silently close anything.

Upgrading a record from ``catalog-only`` to ``questionnaire-validated`` (after
an RA reads the PDF) is what turns an ``unsure`` into a closing ``not-asked``.
That upgrade is the one place a human is required, and it is a *small, bounded*
human step per **series** -- not per cell.
"""

from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass, field

# --- Validation levels -----------------------------------------------------

CATALOG_ONLY = "catalog-only"
DATA_VALIDATED = "data-validated"
QUESTIONNAIRE_VALIDATED = "questionnaire-validated"

VALIDATION_LEVELS = (CATALOG_ONLY, DATA_VALIDATED, QUESTIONNAIRE_VALIDATED)

# The ONLY validation level permitted to close a cell.  C4 (the questionnaire)
# is mandatory before a permanent close -- see the module docstring, and
# ``docs/guide/coverage.md``.  Do not add to this set without reading both.
CLOSING_VALIDATIONS = frozenset({QUESTIONNAIRE_VALIDATED})

# Which of the four coverage checks each validation level corresponds to.
# ``catalog-only`` ran NONE of them: the WB catalog is not one of the checks.
_CHECKS_RUN = {
    CATALOG_ONLY: "",
    DATA_VALIDATED: "C1",
    QUESTIONNAIRE_VALIDATED: "C1;C4",
}

# Verdict proposed for a feature in ``lacks``, by validation level.
_VERDICT = {
    CATALOG_ONLY: "unsure",
    DATA_VALIDATED: "unsure",
    QUESTIONNAIRE_VALIDATED: "not-asked",
}


@dataclass(frozen=True)
class SeriesCapability:
    """What one survey *series* can and cannot populate.

    Keyed by series, not by wave: an instrument's modules are a property of the
    questionnaire, which is stable across a series by construction.  (A series
    that changes its modules mid-run needs two records; none do today.)
    """

    country: str
    series: str                       # idno series token: GHS, IES, ILCS, ...
    provides: tuple[str, ...]         # canonical feature names it CAN populate
    lacks: tuple[str, ...]            # canonical feature names it CANNOT
    validation: str
    evidence: str                     # cite the WB record / questionnaire page
    recorded: str | None = None
    note: str | None = None

    def __post_init__(self):
        if self.validation not in VALIDATION_LEVELS:
            raise ValueError(
                f"{self.country}/{self.series}: unknown validation level "
                f"{self.validation!r}; expected one of {VALIDATION_LEVELS}")
        if self.lacks and not self.evidence.strip():
            raise ValueError(
                f"{self.country}/{self.series}: a `lacks` claim REQUIRES "
                "evidence.  An unevidenced negative is unfalsifiable.")
        overlap = set(self.provides) & set(self.lacks)
        if overlap:
            raise ValueError(
                f"{self.country}/{self.series}: {sorted(overlap)} is in both "
                "`provides` and `lacks`.")

    @property
    def closes(self) -> bool:
        """True iff this record is strong enough to permanently close a cell."""
        return self.validation in CLOSING_VALIDATIONS

    def verdict_for(self, feature: str) -> str | None:
        """The verdict this record proposes for *feature*, or None.

        ``None`` means the record says nothing about this feature -- which is
        NOT the same as saying the survey has it.  Silence is never evidence.
        """
        if feature not in self.lacks:
            return None
        return _VERDICT[self.validation]

    @property
    def checks_run(self) -> str:
        return _CHECKS_RUN[self.validation]


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------
#
# Populate this when a series is ACQUIRED (`add-wave`), from the questionnaire
# where possible.  Every `lacks` entry must cite its evidence.
#
# The records below are all `catalog-only`, and therefore all propose `unsure`,
# NOT `not-asked`.  They are deliberately left at the weak rung: nobody has read
# a questionnaire yet, and none of these series is even acquired.  Upgrading
# South Africa's GHS to `questionnaire-validated` -- one RA, one PDF -- is what
# would convert its 21 x N `food_acquired` cells from `unsure` into a closing
# `not-asked`.

_SERIES_CAPABILITY: dict[tuple[str, str], SeriesCapability] = {

    ("South Africa", "GHS"): SeriesCapability(
        country="South Africa", series="GHS",
        provides=("household_roster", "household_characteristics", "housing",
                  "individual_education", "employment"),
        lacks=("food_acquired",),
        validation=CATALOG_ONLY,
        evidence=(
            "WB catalog id 2773 (ZAF_2015_GHS_v01_M): topic tags are "
            "'employment', 'unemployment', 'LABOUR AND EMPLOYMENT', "
            "'DEMOGRAPHY AND POPULATION' -- no consumption or expenditure "
            "topic; zero consumption/expenditure tokens anywhere in the "
            "record (which is LARGER than the IES record, so the absence is "
            "not metadata sparsity).  Abstract: 'an omnibus household-based "
            "instrument'.  Stats SA's consumption instruments are the IES and "
            "LCS, not the GHS."),
        note=("CATALOG-ONLY: proposes `unsure`, NOT `not-asked`.  Read the GHS "
              "questionnaire to upgrade to questionnaire-validated (C4) and "
              "close these cells."),
        recorded="2026-07-12",
    ),

    ("South Africa", "IES"): SeriesCapability(
        country="South Africa", series="IES",
        provides=("household_roster", "food_acquired", "nonfood_expenditures",
                  "income"),
        lacks=(),
        validation=CATALOG_ONLY,
        evidence=(
            "WB catalog id 8219 (ZAF_2022-2023_IES_v01_M): topic tags "
            "'Acquisitions', 'Income', 'Expenditure', 'Consumption'.  Abstract: "
            "'collects detailed data on acquisitions, consumption, spending, "
            "and income among South African households'."),
        recorded="2026-07-12",
    ),

    ("South Africa", "LCS"): SeriesCapability(
        country="South Africa", series="LCS",
        provides=("household_roster", "food_acquired", "nonfood_expenditures",
                  "housing"),
        lacks=(),
        validation=CATALOG_ONLY,
        evidence=(
            "WB catalog id 2882 (ZAF_2014_LCS_v02_M): diary + expenditure "
            "language throughout the record; the LCS is Stats SA's "
            "living-conditions/expenditure-diary instrument."),
        recorded="2026-07-12",
    ),

    ("Armenia", "ILCS"): SeriesCapability(
        country="Armenia", series="ILCS",
        provides=("household_roster", "food_acquired", "nonfood_expenditures",
                  "housing", "income"),
        lacks=(),
        validation=CATALOG_ONLY,
        evidence=(
            "WB catalog id 2950 (ARM_2001_ILCS_v02_M) and siblings: budget / "
            "consumption / diary language throughout the record."),
        recorded="2026-07-12",
    ),
}


def capability(country: str, series: str) -> SeriesCapability | None:
    """The capability record for a series, or ``None`` if none is recorded.

    ``None`` means *we have not written one down* -- never "the survey has
    everything".  Absence of a record is absence of knowledge.
    """
    return _SERIES_CAPABILITY.get((country, series))


_SERIES_TOKEN_RE = re.compile(r"^[A-Z]{3}_[^_]+_([A-Za-z0-9-]+)_")


def series_of(idno: str) -> str | None:
    """Extract the series token from a WB ``idno``.

    ``ZAF_2015_GHS_v01_M`` -> ``GHS``;  ``ARM_2001_ILCS_v02_M`` -> ``ILCS``.
    """
    m = _SERIES_TOKEN_RE.match(str(idno))
    return m.group(1) if m else None


def capability_for_idno(country: str, idno: str) -> SeriesCapability | None:
    """Capability record for the series that *idno* belongs to."""
    series = series_of(idno)
    return capability(country, series) if series else None


def proposed_absent_verdicts(country: str, series: str, waves,
                             adjudicated_by: str = "capability-record",
                             date: str | None = None) -> list[dict]:
    """Rows this capability record proposes for ``absent_verdicts.csv``.

    One row per ``(feature in lacks) x wave``, in the exact schema
    ``load_verdicts()`` reads:
    ``country,feature,wave,verdict,checks_run,evidence,adjudicated_by,date``.

    The verdict is derived from the record's *validation level*, never chosen by
    the caller -- which is the safety property.  A ``catalog-only`` record
    cannot emit a closing verdict even if someone wants it to: it emits
    ``unsure``, and :func:`coverage_matrix.load_verdicts` leaves the cell
    ``absent`` (i.e. in the work queue).

    Returns ``[]`` when the series has no record, or lacks nothing.  This
    function does NOT write the file; the caller decides.
    """
    cap = capability(country, series)
    if cap is None:
        return []
    date = date or _dt.date.today().isoformat()
    rows = []
    for feature in cap.lacks:
        verdict = cap.verdict_for(feature)
        if verdict is None:      # pragma: no cover -- lacks implies a verdict
            continue
        evidence = cap.evidence
        if not cap.closes:
            # Make the weakness legible IN the row, not just in this module.
            evidence = f"[{cap.validation}; C4 NOT run] {evidence}"
        for wave in waves:
            rows.append({
                "country": country,
                "feature": feature,
                "wave": str(wave),
                "verdict": verdict,
                "checks_run": cap.checks_run,
                "evidence": evidence,
                "adjudicated_by": adjudicated_by,
                "date": date,
            })
    return rows


def audit() -> list[str]:
    """Complaints about the registry.  Empty list = healthy.

    The invariant that matters: **no ``catalog-only`` record may propose a
    closing verdict.**  Asserted in the test-suite over the whole registry, so
    a future edit cannot quietly promote a catalog blurb into a permanent close.
    """
    problems = []
    for (country, series), cap in _SERIES_CAPABILITY.items():
        for feature in cap.lacks:
            verdict = cap.verdict_for(feature)
            if verdict == "not-asked" and not cap.closes:
                problems.append(
                    f"{country}/{series}: proposes a CLOSING verdict from "
                    f"validation={cap.validation!r} -- only "
                    f"{sorted(CLOSING_VALIDATIONS)} may close a cell.")
            if verdict == "not-asked" and "C4" not in cap.checks_run:
                problems.append(
                    f"{country}/{series}: proposes `not-asked` without C4 "
                    "(the questionnaire).  C4 is mandatory before any "
                    "permanent close.")
    return problems
