"""Capability records pre-populate `absent` verdicts -- without closing cells on metadata.

The mechanism (GH #597): record what an instrument *measures* when we acquire
it, so its `absent` cells are explained from birth instead of being probed
months later to rediscover a fact we already knew.

The danger is obvious and these tests exist to make it impossible: a capability
asserted from a **catalog blurb** must never become a permanent `not-asked`.
That is the Albania mistake with better paperwork -- Albania's `data_scheme.yml`
asserted "earlier waves have no shocks module" while `migrationE_cl.dta`
carried ten shock types, and nobody could catch it because nothing recorded how
the claim was reached.

Our standard: **C4 (the questionnaire) is mandatory before any permanent
close**, because absence in the shipped extract is not absence in the
instrument.  So only `questionnaire-validated` closes; `catalog-only` and
`data-validated` propose `unsure` and leave the cell in the queue.
"""

import pytest

from lsms_library import capability as cap
from lsms_library.capability import (
    CATALOG_ONLY,
    CLOSING_VALIDATIONS,
    DATA_VALIDATED,
    QUESTIONNAIRE_VALIDATED,
    SeriesCapability,
    proposed_absent_verdicts,
    series_of,
)
from lsms_library.coverage_matrix import VERDICTS, load_verdicts


# ---------------------------------------------------------------------------
# THE invariant
# ---------------------------------------------------------------------------

class TestCatalogMetadataCannotCloseACell:
    """The whole game.  If these fail, we are closing cells on a blurb."""

    def test_only_the_questionnaire_closes(self):
        assert CLOSING_VALIDATIONS == {QUESTIONNAIRE_VALIDATED}

    @pytest.mark.parametrize("level", [CATALOG_ONLY, DATA_VALIDATED])
    def test_weak_validation_proposes_unsure_never_not_asked(self, level):
        c = SeriesCapability(
            country="X", series="S", provides=(), lacks=("food_acquired",),
            validation=level, evidence="some evidence")
        assert c.verdict_for("food_acquired") == "unsure"
        assert not c.closes

    def test_questionnaire_validation_proposes_not_asked(self):
        c = SeriesCapability(
            country="X", series="S", provides=(), lacks=("food_acquired",),
            validation=QUESTIONNAIRE_VALIDATED,
            evidence="Questionnaire p.14: no consumption module.")
        assert c.verdict_for("food_acquired") == "not-asked"
        assert c.closes
        assert "C4" in c.checks_run

    def test_data_validation_alone_does_not_close(self):
        """A negative label sweep is as consistent with `asked-not-distributed`
        as with `not-asked`.  That is precisely why C4 exists."""
        c = SeriesCapability(
            country="X", series="S", provides=(), lacks=("food_acquired",),
            validation=DATA_VALIDATED,
            evidence="C1 label sweep over all 12 .dta: no food/expenditure labels")
        assert c.verdict_for("food_acquired") == "unsure"
        assert c.checks_run == "C1"          # C4 did NOT run

    def test_the_whole_registry_is_clean(self):
        """No shipped record may propose a close it has not earned."""
        assert cap.audit() == []

    def test_registry_records_are_all_catalog_only_today(self):
        """Nobody has read a questionnaire yet -- so nothing may close yet.

        This test is expected to CHANGE when an RA validates a series; that is
        the point.  It documents that today's records are the weak rung.
        """
        for (country, series), c in cap._SERIES_CAPABILITY.items():
            if c.lacks:
                assert not c.closes, (
                    f"{country}/{series} claims to close cells; if a "
                    "questionnaire really was read, update this test.")


# ---------------------------------------------------------------------------
# Evidence is not optional
# ---------------------------------------------------------------------------

class TestEvidenceDiscipline:

    def test_a_lacks_claim_without_evidence_is_rejected(self):
        with pytest.raises(ValueError, match="unfalsifiable"):
            SeriesCapability(country="X", series="S", provides=(),
                             lacks=("food_acquired",), validation=CATALOG_ONLY,
                             evidence="   ")

    def test_unknown_validation_level_is_rejected(self):
        with pytest.raises(ValueError, match="validation"):
            SeriesCapability(country="X", series="S", provides=(), lacks=(),
                             validation="vibes", evidence="e")

    def test_a_feature_cannot_be_both_provided_and_lacked(self):
        with pytest.raises(ValueError, match="both"):
            SeriesCapability(country="X", series="S",
                             provides=("food_acquired",),
                             lacks=("food_acquired",),
                             validation=CATALOG_ONLY, evidence="e")

    def test_silence_is_not_evidence_of_presence(self):
        """A record that says nothing about a feature says NOTHING -- it does
        not assert the survey has it."""
        c = cap.capability("South Africa", "GHS")
        assert c.verdict_for("shocks") is None      # not in `lacks`
        assert "shocks" not in c.provides           # ... and not claimed either

    def test_no_record_returns_none_not_a_guess(self):
        assert cap.capability("Atlantis", "XYZ") is None
        assert proposed_absent_verdicts("Atlantis", "XYZ", ["2020"]) == []


# ---------------------------------------------------------------------------
# The rows it emits, and how the real loader treats them
# ---------------------------------------------------------------------------

class TestProposedVerdictRows:

    def test_series_token_extraction(self):
        assert series_of("ZAF_2015_GHS_v01_M") == "GHS"
        assert series_of("ARM_2001_ILCS_v02_M") == "ILCS"
        assert series_of("ZAF_2022-2023_IES_v01_M") == "IES"
        assert series_of("not-an-idno") is None

    def test_ghs_emits_one_unsure_row_per_wave(self):
        rows = proposed_absent_verdicts(
            "South Africa", "GHS", ["2015", "2016", "2018"])
        assert len(rows) == 3
        for r in rows:
            assert r["feature"] == "food_acquired"
            assert r["verdict"] == "unsure"          # NOT not-asked
            assert r["checks_run"] == ""             # C4 did not run
            assert r["evidence"].startswith("[catalog-only; C4 NOT run]")
            assert "2773" in r["evidence"]           # cites the WB record

    def test_a_series_that_lacks_nothing_emits_nothing(self):
        assert proposed_absent_verdicts("South Africa", "IES", ["2022"]) == []
        assert proposed_absent_verdicts("Armenia", "ILCS", ["2001"]) == []

    def test_rows_use_the_verdict_vocabulary_the_matrix_understands(self):
        rows = proposed_absent_verdicts("South Africa", "GHS", ["2015"])
        assert {r["verdict"] for r in rows} <= VERDICTS

    def test_emitted_rows_survive_the_real_loader_and_do_NOT_close(self, tmp_path):
        """End-to-end: write the proposed rows, read them back through the
        production `load_verdicts()`, and confirm the cell stays OPEN."""
        import csv

        from lsms_library.coverage_matrix import _absent_tier

        rows = proposed_absent_verdicts("South Africa", "GHS", ["2015", "2016"])
        path = tmp_path / "absent_verdicts.csv"
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "country", "feature", "wave", "verdict", "checks_run",
                "evidence", "adjudicated_by", "date"])
            w.writeheader()
            w.writerows(rows)

        verdicts = load_verdicts(path)
        assert len(verdicts) == 2

        # The cell is graded `absent` -- i.e. it STAYS IN THE WORK QUEUE --
        # but now carries the reason forward so the probe does not start cold.
        tier, detail = _absent_tier(
            "South Africa", "food_acquired", "2015", verdicts)
        assert tier == "absent", "a catalog-only record must NOT close a cell"
        assert detail.startswith("unsure:")
        assert "catalog-only" in detail

    def test_upgrading_to_questionnaire_validated_closes_the_cell(self, tmp_path):
        """The upgrade path: one RA, one PDF, and the cells close for real."""
        import csv

        from lsms_library.coverage_matrix import _absent_tier

        upgraded = SeriesCapability(
            country="South Africa", series="GHS",
            provides=("household_roster",), lacks=("food_acquired",),
            validation=QUESTIONNAIRE_VALIDATED,
            evidence="GHS 2015 questionnaire (Stats SA), sections 1-8: no "
                     "food acquisition or expenditure module.")
        rows = [{
            "country": "South Africa", "feature": "food_acquired",
            "wave": "2015", "verdict": upgraded.verdict_for("food_acquired"),
            "checks_run": upgraded.checks_run, "evidence": upgraded.evidence,
            "adjudicated_by": "ra", "date": "2026-07-12"}]
        path = tmp_path / "v.csv"
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader(); w.writerows(rows)

        verdicts = load_verdicts(path)
        tier, _ = _absent_tier("South Africa", "food_acquired", "2015", verdicts)
        assert tier == "not-asked"        # closed, on questionnaire evidence

    def test_an_evidence_free_close_is_refused_by_the_loader(self, tmp_path):
        """Belt and braces: even if someone hand-writes a closing row with no
        evidence, `load_verdicts()` throws it out."""
        import csv
        path = tmp_path / "v.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["country", "feature", "wave", "verdict", "checks_run",
                        "evidence", "adjudicated_by", "date"])
            w.writerow(["South Africa", "food_acquired", "2015", "not-asked",
                        "C4", "", "someone", "2026-07-12"])
        with pytest.warns(UserWarning, match="unfalsifiable"):
            assert load_verdicts(path) == {}
