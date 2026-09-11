"""The recall record -- Phase I of GH #851.

``food_expenditures`` returned a number whose reference period was recorded
nowhere.  Most of the corpus is 7 days asked once; GhanaLSS delivers 30 days of
coverage, Burkina Faso 2014 28, Guatemala and Panama 15.  This module pins that
the record surfacing those facts is *complete, honest about its evidence, and
transforms nothing*.

Two tiers, following ``test_ghanalss_food_label_canonical``:

* **Config-only** (everything but ``TestDelivered``) -- reads the packaged YAML
  and nothing else.  No cache, no microdata, milliseconds.  This is the tier
  that matters: it is the one that runs in CI.
* **Data-gated** (``TestDelivered``) -- that ``attrs`` actually arrive on a
  built table; skipped when the cache is cold.
"""
import csv
import warnings
from pathlib import Path

import pytest

from lsms_library.recall import (
    ASK_COUNT_BASES, BASIS_LADDER, VARIATION_AXES, RecallRecord,
    recall_records, records_for_frame, attach, merge_attrs,
)
from lsms_library.paths import countries_root

#: Countries carrying a generated `_/recall.yml`.
COUNTRIES = sorted(
    p.parent.parent.name
    for p in Path(countries_root()).glob("*/_/recall.yml"))


def _all_records():
    for c in COUNTRIES:
        for wave, r in recall_records(c).items():
            yield c, wave, r


class TestVocabulary:
    """The fields whose whole job is to stop a weak source becoming a fact."""

    def test_countries_have_records(self):
        assert COUNTRIES, "no _/recall.yml found -- did promotion run?"

    @pytest.mark.parametrize("field,allowed", [
        ("basis", BASIS_LADDER),
        ("ask_count_basis", ASK_COUNT_BASES),
    ])
    def test_values_are_in_vocabulary(self, field, allowed):
        bad = {f"{c} {w}": getattr(r, field)
               for c, w, r in _all_records() if getattr(r, field) not in allowed}
        assert not bad, f"{field} outside {allowed}: {bad}"

    def test_variation_axes_are_in_vocabulary(self):
        bad = {f"{c} {w}": r.within_wave_variation
               for c, w, r in _all_records()
               if r.within_wave_variation is not None
               and r.within_wave_variation not in VARIATION_AXES}
        assert not bad, f"unknown within_wave_variation: {bad}"


class TestHonesty:
    """The invariants that make a number in this table worth reading."""

    def test_not_recorded_asserts_nothing(self):
        """'Nobody has looked' and 'the window is N days' cannot both be true.

        This is the Albania unevidenced-absence failure in its recall form: a
        claim nobody can trace, which is therefore permanent whether or not it
        is right.
        """
        bad = {f"{c} {w}": r.recall_days for c, w, r in _all_records()
               if r.basis == "not-recorded" and r.recall_days is not None}
        assert not bad, f"not-recorded cells asserting a window: {bad}"

    def test_varying_cells_carry_the_axis_not_a_number(self):
        """A cell whose DELIVERED window differs inside the wave gets no number.

        GhanaLSS 1991-92 is 2 days x 7 asks rural and 3 x 10 urban; either
        number alone misreports the other stratum by ~2x.
        """
        bad = {f"{c} {w}": (r.within_wave_variation, r.recall_days, r.n_asks)
               for c, w, r in _all_records()
               if r.within_wave_variation and
               (r.recall_days is not None or r.n_asks is not None)}
        assert not bad, f"varying cells carrying a single number: {bad}"

    def test_every_stated_window_cites_its_evidence(self):
        bad = [f"{c} {w}" for c, w, r in _all_records()
               if r.recall_verbatim and not r.evidence]
        assert not bad, f"a stated window with no citation: {bad}"

    def test_exposure_is_derived_never_stored(self):
        """`total_exposure_days` is a property, so the three cannot drift apart."""
        for c, w, r in _all_records():
            if r.recall_days is not None and r.n_asks is not None:
                assert r.total_exposure_days == r.recall_days * r.n_asks
            else:
                assert r.total_exposure_days is None, f"{c} {w}"

    def test_records_are_immutable(self):
        c, w, r = next(_all_records())
        with pytest.raises(Exception):
            r.recall_days = 999          # frozen dataclass


class TestConstructionRefusesContradictions:
    """The record refuses to be built wrong, not merely audited afterwards."""

    def test_rejects_unknown_basis(self):
        with pytest.raises(ValueError, match="not one of"):
            RecallRecord(country="X", wave="1", basis="vibes")

    def test_rejects_number_on_a_varying_cell(self):
        with pytest.raises(ValueError, match="must leave the number null"):
            RecallRecord(country="X", wave="1", basis="questionnaire",
                         within_wave_variation="stratum", recall_days=7)

    def test_rejects_window_on_a_not_recorded_cell(self):
        with pytest.raises(ValueError, match="cannot both be true"):
            RecallRecord(country="X", wave="1", basis="not-recorded",
                         recall_days=7)


class TestCoverage:
    """Completeness against the evidence base, and the size of the gap."""

    def test_every_csv_row_promoted(self):
        """The packaged YAML must not silently lose a row of the evidence base."""
        csv_path = (Path(__file__).resolve().parent.parent
                    / ".coder" / "coverage" / "food_recall.csv")
        if not csv_path.exists():
            pytest.skip("evidence CSV is source-checkout only")
        want = {(r["country"], r["wave"]) for r in csv.DictReader(csv_path.open())}
        got = {(c, w) for c, w, _ in _all_records()}
        missing = want - got
        assert not missing, f"CSV rows that reached no recall.yml: {sorted(missing)}"

    def test_the_gap_is_visible_not_hidden(self):
        """Roughly half the corpus is `not-recorded`, and that must stay legible.

        Not a threshold to defend -- a canary.  If this ever reads 0 without a
        deliberate questionnaire sweep, someone has filled the table by
        inference, which is the failure mode the ladder exists to prevent.
        """
        recs = list(_all_records())
        unrecorded = [f"{c} {w}" for c, w, r in recs if not r.is_recorded]
        assert unrecorded, (
            "no cell is `not-recorded` -- either a real sweep happened (update "
            "this test) or the table was filled by inference")
        assert len(unrecorded) < len(recs), "no cell has a window at all"


class TestNoTransformation:
    """Phase I ships facts.  It must not have grown a transformation."""

    def test_no_period_kwarg_appeared(self):
        """`period=` is deferred and gated on the ask-order bias (#851).

        Reported spend does not respond to the realised interval (elasticity
        0.018 on GLSS4), so a rate would inject fieldwork tempo rather than
        normalise.  If someone adds `period=`, this test should be deleted
        deliberately, with that evidence answered.
        """
        import inspect
        from lsms_library import transformations as T
        for fn in (T.food_expenditures_from_acquired,
                   T.food_quantities_from_acquired,
                   T.food_prices_from_acquired):
            params = inspect.signature(fn).parameters
            assert "period" not in params, (
                f"{fn.__name__} grew a `period=` kwarg; Phase I forbids it "
                f"until the ask-order bias is adjudicated -- see #851")


class TestAttach:
    """`attrs` plumbing, without needing a build."""

    def test_attach_is_never_fatal(self):
        import pandas as pd
        df = pd.DataFrame({"x": [1]})
        attach(df, "NoSuchCountry")            # must not raise
        assert "recall" not in df.attrs

    def test_records_for_frame_filters_on_t(self):
        import pandas as pd
        c = "Ethiopia"
        recs = recall_records(c)
        if not recs:
            pytest.skip("Ethiopia has no record")
        wave = sorted(recs)[0]
        df = pd.DataFrame({"v": [1]},
                          index=pd.MultiIndex.from_tuples([(wave, "i1")],
                                                          names=["t", "i"]))
        got = records_for_frame(df, c)
        assert set(got) == {wave}, "a one-wave frame advertised other waves"

    def test_merge_attrs_unions_countries(self):
        import pandas as pd
        a, b = pd.DataFrame({"x": [1]}), pd.DataFrame({"x": [2]})
        attach(a, "Ethiopia"); attach(b, "GhanaLSS")
        merged = merge_attrs([a, b])
        assert {"Ethiopia", "GhanaLSS"} <= set(merged)


@pytest.mark.slow
class TestDelivered:
    """End-to-end. Skipped when the cache is cold."""

    def test_attrs_arrive_on_a_built_table(self):
        try:
            import lsms_library as ll
            df = ll.Country("Ethiopia").food_acquired()
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f"Ethiopia food_acquired not buildable here: {e}")
        assert "recall" in df.attrs, "no recall record on a built food table"
        assert "Ethiopia" in df.attrs["recall"]
        waves = set(df.index.get_level_values("t").astype(str))
        assert set(df.attrs["recall"]["Ethiopia"]) <= waves, (
            "the record advertises waves the frame does not contain")

    def test_population_record_still_attaches(self):
        """The new attach must not have displaced the old one."""
        try:
            import lsms_library as ll
            df = ll.Country("Ethiopia").food_acquired()
        except Exception as e:                # pragma: no cover
            pytest.skip(f"not buildable here: {e}")
        assert "population" in df.attrs
