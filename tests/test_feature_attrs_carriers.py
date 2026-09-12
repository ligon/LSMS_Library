"""`Feature()` carries EVERY attrs record across the concat -- GH #873.

`attrs` survive an operation only when every input agrees; any disagreement --
including one side having none -- yields `{}`.  Cross-country assembly is a
`pd.concat` over frames whose records differ BY DESIGN (one per country), so it
lands in the `{}` case on every multi-country call, and the re-attach in
`Feature.__call__` is the only thing keeping the record alive.

That re-attach existed for `population` (GH #603) and for nothing else:
`recall.merge_attrs` (GH #851) and `derivations.merge_attrs`
(`SkunkWorks/derived_values.org` §"One level up") were both written, both
documented as load-bearing, and both had NO caller -- so `attrs['recall']` and
`attrs['derivations']` died silently at exactly the pooling case they were
created to disclose (a 7-day figure beside a 30-day one).

These tests mirror `tests/test_population.py::TestFeatureDoesNotFence` for the
two carriers that were dropped, and pin the registry itself so a fourth carrier
is one line rather than a fourth silent drop.

Two tiers, as in `test_recall_record.py`:

* **monkeypatched** -- a fake `Country` returning real 2-row frames carrying
  real records.  No cache, no microdata, milliseconds.
* **data-gated** (`TestDelivered`) -- the assembly the issue names,
  `Feature('food_acquired')(['GhanaLSS', 'Guatemala'])`; skipped when the cache
  is cold.
"""
import warnings

import pandas as pd
import pytest

import sys

import lsms_library.derivations
import lsms_library.feature
import lsms_library.population
import lsms_library.recall
from lsms_library.population import PopulationHeterogeneityWarning

# `lsms_library.recall` and `.derivations` are SHADOWED on the package by
# same-named public callables (`ll.recall(...)`, `ll.derivations(...)`), so
# `from lsms_library import recall` hands back the function, not the module.
# Reach for the modules themselves.
D = sys.modules["lsms_library.derivations"]
F = sys.modules["lsms_library.feature"]
P = sys.modules["lsms_library.population"]
R = sys.modules["lsms_library.recall"]


# ---------------------------------------------------------------------------
# the registry itself
# ---------------------------------------------------------------------------

class TestTheRegistry:
    """A carrier is data, not code -- that is the whole point of GH #873."""

    def test_all_three_carriers_are_registered(self):
        assert [c.name for c in F._ATTRS_CARRIERS] == \
            ["population", "recall", "derivations"]

    def test_feature_did_not_import_the_SHADOWING_callables(self):
        """`lsms_library` binds public callables over two of its own submodule
        names (`ll.recall`, `ll.derivations`), and `from package import name`
        prefers the package attribute.  `feature.py` must name the symbols it
        wants, or a reordering of `__init__` would silently hand it a
        function -- which is exactly what this test file's `sys.modules`
        lookup at the top exists to dodge."""
        import lsms_library as ll
        assert callable(ll.recall) and not hasattr(ll.recall, "merge_attrs")
        assert callable(ll.derivations)
        for carrier in F._ATTRS_CARRIERS[1:]:
            assert callable(carrier.attach)
        assert F._ATTRS_CARRIERS[1].keys == (R.ATTRS_KEY,)
        assert F._ATTRS_CARRIERS[2].keys == (D.ATTRS_KEY,)

    def test_the_captured_keys_are_exactly_the_carriers_keys(self):
        """The capture at the top of the country loop and the re-attach after
        the concat must read the SAME key list, or a record is captured and
        never re-attached (or vice versa)."""
        assert set(F._CARRIED_ATTRS_KEYS) == {
            k for c in F._ATTRS_CARRIERS for k in c.keys}
        assert set(F._CARRIED_ATTRS_KEYS) == {
            P.ATTRS_KEY, P.ATTRS_RESOLUTION_KEY, R.ATTRS_KEY, D.ATTRS_KEY}

    def test_each_carrier_delegates_to_its_own_modules_merge(self):
        """The union rule belongs to the module that defines the record.
        `feature.py` must not grow a second, drifting copy of it."""
        assert F._ATTRS_CARRIERS[0].attach is P.merge_attrs
        # recall / derivations return the union rather than writing it, so they
        # are wrapped -- but the wrapper closes over the module's own function.
        for carrier, merge in ((F._ATTRS_CARRIERS[1], R.merge_attrs),
                               (F._ATTRS_CARRIERS[2], D.merge_attrs)):
            closed_over = {c.cell_contents for c in carrier.attach.__closure__}
            assert merge in closed_over, \
                f"{carrier.name} does not call its module's merge_attrs"

    def test_an_empty_union_writes_no_key(self):
        """"No record" and "an empty record" must not become the same thing."""
        df = pd.DataFrame({"x": [1]})
        F._merge_into("whatever", lambda sources: {})(df, [])
        assert "whatever" not in df.attrs

    @pytest.mark.parametrize("mod", [R, D])
    def test_merge_attrs_accepts_a_bare_attrs_mapping(self, mod):
        """`Feature` captures the mapping, not the frame (the frame is reshaped
        by `_harmonize_country_frame` on the way to the concat), so both shapes
        have to work -- and the frame shape is what the pre-#873 unit tests
        exercise, so it must keep working too."""
        payload = {mod.ATTRS_KEY: {"Someland": {"1999": {"rows": 1}}}}
        frame = pd.DataFrame({"x": [1]})
        frame.attrs = dict(payload)
        assert mod.merge_attrs([payload]) == mod.merge_attrs([frame])
        assert set(mod.merge_attrs([payload])) == {"Someland"}


# ---------------------------------------------------------------------------
# the drop the issue reports, without needing microdata
# ---------------------------------------------------------------------------

class _FakeCountry:
    """Minimal stand-in: a real 2-row frame carrying all three records.

    Deliberately the same shape as `test_population.py::_FakeCountry`, with the
    recall and derivations records added -- the population assertions here are
    the control: they passed before #873 and must still pass.
    """

    _WAVE = {"Liberia": "2018-19", "Benin": "2018-19", "Togo": "2018"}

    def __init__(self, name):
        self.name = name

    def household_roster(self, **kwargs):
        t = self._WAVE[self.name]
        df = pd.DataFrame({"t": [t, t], "i": ["1", "2"], "Age": [30, 40]}
                          ).set_index(["t", "i"])
        P.attach(df, self.name)
        # Real records for the other two carriers, hand-built: neither module
        # has config for these countries, and the point here is the plumbing.
        df.attrs[R.ATTRS_KEY] = {self.name: {t: R.RecallRecord(
            country=self.name, wave=t,
            recall_days=30 if self.name == "Liberia" else 7,
            basis="questionnaire", evidence=f"{self.name} test fixture")}}
        df.attrs[D.ATTRS_KEY] = {self.name: {
            f"{self.name}::household_roster::fixture":
                {"rows": 2, "in": "household_roster"}}}
        return df


@pytest.fixture
def two_country_feature(monkeypatch):
    import lsms_library as ll
    monkeypatch.setattr(ll, "Country", lambda name, **kw: _FakeCountry(name))
    f = F.Feature("household_roster")
    object.__setattr__(f, "_countries", ["Liberia", "Benin"])
    return f


class TestEveryCarrierSurvivesAssembly:

    def test_all_three_records_arrive_on_the_assembled_frame(
            self, two_country_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = two_country_feature(["Liberia", "Benin"], currency=None)
        assert len(out) == 4                       # nothing fenced
        for key in (P.ATTRS_KEY, R.ATTRS_KEY, D.ATTRS_KEY):
            assert key in out.attrs, f"{key} died at the concat (GH #873)"
            assert set(out.attrs[key]) == {"Liberia", "Benin"}, \
                f"{key} lost a country"

    def test_the_recall_record_still_names_its_window(self, two_country_feature):
        """The disclosure the record exists for: a 30-day figure pooled beside
        a 7-day one, and the pool says so."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = two_country_feature(["Liberia", "Benin"], currency=None)
        windows = {c: next(iter(recs.values())).recall_days
                   for c, recs in out.attrs[R.ATTRS_KEY].items()}
        assert windows == {"Liberia": 30, "Benin": 7}

    def test_the_derivations_summary_points_down_at_its_table(
            self, two_country_feature):
        """At the Feature level the summary is a POINTER, not a row label --
        the design decided 2026-09-11 (`derived_values.org` §"One level up")."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = two_country_feature(["Liberia", "Benin"], currency=None)
        summ = out.attrs[D.ATTRS_KEY]["Benin"]
        assert set(summ) == {"Benin::household_roster::fixture"}
        assert summ["Benin::household_roster::fixture"]["in"] == "household_roster"

    def test_no_warning_fires_for_recall_or_derivations(
            self, two_country_feature):
        """Declined 2026-09-11: at this level the attrs summary IS the signal;
        a warning would repeat it as noise.  Population still warns (its
        heterogeneity report is a different decision, #603)."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            two_country_feature(["Liberia", "Benin"], currency=None)
        msgs = [str(w.message) for w in caught]
        assert not [m for m in msgs
                    if "recall" in m.lower() or "derivation" in m.lower()], msgs
        assert len([w for w in caught
                    if issubclass(w.category,
                                  PopulationHeterogeneityWarning)]) == 1

    def test_the_data_is_untouched_by_all_three(self, two_country_feature,
                                                monkeypatch):
        """Metadata only: strip every record from the per-country frames and the
        returned data must be value-identical."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rich = two_country_feature(["Liberia", "Benin"], currency=None)

            class _Bare(_FakeCountry):
                def household_roster(self, **kwargs):
                    df = super().household_roster(**kwargs)
                    df.attrs = {}
                    return df

            import lsms_library as ll
            monkeypatch.setattr(ll, "Country", lambda name, **kw: _Bare(name))
            bare = two_country_feature(["Liberia", "Benin"], currency=None)
        pd.testing.assert_frame_equal(rich, bare)
        assert not {P.ATTRS_KEY, R.ATTRS_KEY, D.ATTRS_KEY} & set(bare.attrs)

    def test_a_country_with_no_record_does_not_break_the_others(
            self, monkeypatch):
        import lsms_library as ll

        class _Half(_FakeCountry):
            def household_roster(self, **kwargs):
                df = super().household_roster(**kwargs)
                if self.name == "Benin":
                    for k in (R.ATTRS_KEY, D.ATTRS_KEY):
                        df.attrs.pop(k, None)
                return df

        monkeypatch.setattr(ll, "Country", lambda name, **kw: _Half(name))
        f = F.Feature("household_roster")
        object.__setattr__(f, "_countries", ["Liberia", "Benin"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = f(["Liberia", "Benin"], currency=None)
        assert set(out.attrs[R.ATTRS_KEY]) == {"Liberia"}
        assert set(out.attrs[D.ATTRS_KEY]) == {"Liberia"}
        assert set(out.attrs[P.ATTRS_KEY]) == {"Liberia", "Benin"}

    def test_only_the_KEPT_countries_reach_the_metadata(self, monkeypatch):
        """A frame dropped before the concat is not in the answer, so it must
        not be in the answer's metadata -- the same rule the population
        re-attach already obeyed, now applied to every carrier."""
        import lsms_library as ll
        monkeypatch.setattr(ll, "Country",
                            lambda name, **kw: _FakeCountry(name))
        f = F.Feature("household_roster")
        object.__setattr__(f, "_countries", ["Liberia", "Benin", "Togo"])
        captured = {n: {k: {n: {"x": 1}} for k in (R.ATTRS_KEY, D.ATTRS_KEY)}
                    for n in ("Liberia", "Benin", "Togo")}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = f(["Liberia", "Benin"], currency=None)
            # Re-run the hook by hand with a THIRD country in the capture.
            out.attrs.clear()
            f._attach_carried_attrs(out, captured)
        for key in (R.ATTRS_KEY, D.ATTRS_KEY):
            assert set(out.attrs[key]) == {"Liberia", "Benin"}, \
                f"{key} advertised a country the frame does not contain"

    def test_a_broken_carrier_warns_and_does_not_suppress_the_others(
            self, two_country_feature, monkeypatch):
        """A metadata annotation must never break a data call, and one broken
        record must not take the other two down with it."""
        def _boom(frames):
            raise RuntimeError("deliberate")
        monkeypatch.setattr(R, "merge_attrs", _boom)
        # The registry closes over the module function, so rebuild it.
        monkeypatch.setattr(F, "_ATTRS_CARRIERS", tuple(
            c._replace(attach=F._merge_into(R.ATTRS_KEY, _boom))
            if c.name == "recall" else c
            for c in F._ATTRS_CARRIERS))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            out = two_country_feature(["Liberia", "Benin"], currency=None)
        assert len(out) == 4                            # data unaffected
        assert R.ATTRS_KEY not in out.attrs
        assert set(out.attrs[P.ATTRS_KEY]) == {"Liberia", "Benin"}
        assert set(out.attrs[D.ATTRS_KEY]) == {"Liberia", "Benin"}
        assert [m for m in (str(w.message) for w in caught)
                if "could not attach the recall record" in m], \
            "a failed carrier must be LOUD"


# ---------------------------------------------------------------------------
# the assembly the issue names
# ---------------------------------------------------------------------------

class TestDelivered:
    """End-to-end, on real data.  Skipped when the cache is cold."""

    PAIR = ["GhanaLSS", "Guatemala"]

    @pytest.fixture(scope="class")
    def assembled(self):
        try:
            import lsms_library as ll
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return ll.Feature("food_acquired")(self.PAIR)
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f"{self.PAIR} food_acquired not buildable here: {e}")

    def test_recall_arrives_for_both_countries(self, assembled):
        """The pooling case the record was created for: GhanaLSS delivers 30
        days of coverage, Guatemala 15 -- in one frame, and it says so."""
        assert R.ATTRS_KEY in assembled.attrs, \
            "attrs['recall'] died at the Feature concat (GH #873)"
        assert set(assembled.attrs[R.ATTRS_KEY]) == set(self.PAIR)
        windows = {c: {r.recall_days for r in recs.values()}
                   for c, recs in assembled.attrs[R.ATTRS_KEY].items()}
        assert windows["GhanaLSS"] != windows["Guatemala"], \
            f"expected mixed recall windows in this pool, got {windows}"

    def test_derivations_arrives_for_the_country_that_has_one(self, assembled):
        """GhanaLSS is the only country with a `_/derivations.yml` today, so a
        summary naming it is the whole signal; Guatemala legitimately has none."""
        assert D.ATTRS_KEY in assembled.attrs, \
            "attrs['derivations'] died at the Feature concat (GH #873)"
        assert "GhanaLSS" in assembled.attrs[D.ATTRS_KEY]

    def test_the_population_record_still_arrives(self, assembled):
        """The control: generalising the hook must not have cost the carrier it
        was generalised from."""
        assert set(assembled.attrs[P.ATTRS_KEY]) == set(self.PAIR)
