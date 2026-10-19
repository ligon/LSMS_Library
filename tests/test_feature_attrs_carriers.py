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
import lsms_library.transformations
from lsms_library.population import PopulationHeterogeneityWarning

# `lsms_library.recall` and `.derivations` are SHADOWED on the package by
# same-named public callables (`ll.recall(...)`, `ll.derivations(...)`), so
# `from lsms_library import recall` hands back the function, not the module.
# Reach for the modules themselves.
D = sys.modules["lsms_library.derivations"]
F = sys.modules["lsms_library.feature"]
P = sys.modules["lsms_library.population"]
R = sys.modules["lsms_library.recall"]
S = sys.modules["lsms_library.sampling"]
T = sys.modules["lsms_library.transformations"]


# ---------------------------------------------------------------------------
# the registry itself
# ---------------------------------------------------------------------------

class TestTheRegistry:
    """A carrier is data, not code -- that is the whole point of GH #873."""

    def test_every_carrier_is_registered(self):
        # `labels_unmatched` added for GH #787 item 3; `unit_kg` for GH #931;
        # `sampling` for GH #793.
        assert [c.name for c in F._ATTRS_CARRIERS] == \
            ["population", "recall", "derivations", "labels_unmatched",
             "unit_kg", "sampling"]

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
        assert F._ATTRS_CARRIERS[3].keys == ("labels_unmatched",)
        assert F._ATTRS_CARRIERS[1].keys == (R.ATTRS_KEY,)
        assert F._ATTRS_CARRIERS[2].keys == (D.ATTRS_KEY,)
        assert F._ATTRS_CARRIERS[4].keys == (T.UNIT_KG_ATTRS_KEY,)

    def test_the_captured_keys_are_exactly_the_carriers_keys(self):
        """The capture at the top of the country loop and the re-attach after
        the concat must read the SAME key list, or a record is captured and
        never re-attached (or vice versa)."""
        assert set(F._CARRIED_ATTRS_KEYS) == {
            k for c in F._ATTRS_CARRIERS for k in c.keys}
        assert set(F._CARRIED_ATTRS_KEYS) == {
            P.ATTRS_KEY, P.ATTRS_RESOLUTION_KEY, R.ATTRS_KEY, D.ATTRS_KEY,
            "labels_unmatched", T.UNIT_KG_ATTRS_KEY, S.ATTRS_KEY,
            S.ATTRS_RESOLUTION_KEY}

    def test_each_carrier_delegates_to_its_own_modules_merge(self):
        """The union rule belongs to the module that defines the record.
        `feature.py` must not grow a second, drifting copy of it."""
        assert F._ATTRS_CARRIERS[0].attach is P.merge_attrs
        assert F._ATTRS_CARRIERS[4].attach is S.merge_attrs
        # recall / derivations return the union rather than writing it, so they
        # are wrapped -- but the wrapper closes over the module's own function.
        from lsms_library.country import merge_label_pass_through_reports
        for carrier, merge in ((F._ATTRS_CARRIERS[1], R.merge_attrs),
                               (F._ATTRS_CARRIERS[2], D.merge_attrs),
                               (F._ATTRS_CARRIERS[3],
                                merge_label_pass_through_reports),
                               (F._ATTRS_CARRIERS[4], T.merge_unit_kg_attrs)):
            closed_over = {c.cell_contents for c in carrier.attach.__closure__}
            assert merge in closed_over, \
                f"{carrier.name} does not call its module's merge_attrs"

    def test_an_empty_union_writes_no_key(self):
        """"No record" and "an empty record" must not become the same thing."""
        df = pd.DataFrame({"x": [1]})
        F._merge_into("whatever", lambda sources: {})(df, [])
        assert "whatever" not in df.attrs

    def test_unit_kg_merge_accepts_a_bare_attrs_mapping(self):
        """Same two shapes for the GH #931 record."""
        payload = {T.UNIT_KG_ATTRS_KEY: {"Someland": {"quintal": 100.0}}}
        frame = pd.DataFrame({"x": [1]})
        frame.attrs = dict(payload)
        assert T.merge_unit_kg_attrs([payload]) == \
            T.merge_unit_kg_attrs([frame]) == {"Someland": {"quintal": 100.0}}

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
# labels_unmatched (GH #787 item 3) and units_unavailable (GH #828)
# ---------------------------------------------------------------------------

def _report(n):
    """A Country-level ``labels_unmatched`` report: FLAT, no country in it."""
    return {"labels": "Aggregate", "column": "Aggregate", "n_unmatched": n,
            "n_values": 10, "rows": 2 * n, "row_share": n / 10,
            "examples": [f"food{k}" for k in range(n)]}


class _FoodCountry:
    """Fake country whose ``food_prices`` behaves like the real one on the two
    read-path contracts: ``labels=`` (pass-through report, or Contract-B
    refusal) and ``units=`` (Contract-B refusal when ``Price`` is absent).

    The signature names ``labels`` and ``units`` on purpose: ``Feature`` only
    forwards a kwarg the country's method accepts (``_method_parameters``).
    """

    #: name -> unmatched count (0 = fully matched, None = LabelUnavailable)
    UNMATCHED = {"Liberia": 3, "Benin": 0, "Togo": None}
    #: countries whose food_acquired carries no Price
    NO_PRICE = {"Togo"}

    def __init__(self, name):
        self.name = name

    def food_prices(self, labels=None, units=None, **kwargs):
        from lsms_library.errors import (LabelUnavailableError,
                                         UnitsUnavailableError)
        if units in {"unitprice", "kgprice"} and self.name in self.NO_PRICE:
            raise UnitsUnavailableError(
                units=units, missing=["Price"]).for_country(self.name)
        n = self.UNMATCHED[self.name]
        if labels is not None and n is None:
            raise LabelUnavailableError(f"{self.name}: no {labels!r} column")
        idx = pd.MultiIndex.from_tuples(
            [("2018", "c1", "h1", "Rice", "Kg", "purchased"),
             ("2018", "c1", "h2", "Rice", "Kg", "purchased")],
            names=["t", "v", "i", "j", "u", "s"])
        df = pd.DataFrame({"Price": [1.0, 2.0]}, index=idx)
        if labels is not None and n:
            df.attrs["labels_unmatched"] = _report(n)
        return df


@pytest.fixture
def food_feature(monkeypatch):
    import lsms_library as ll
    monkeypatch.setattr(ll, "Country", lambda name, **kw: _FoodCountry(name))
    f = F.Feature("food_prices")
    object.__setattr__(f, "_countries", ["Liberia", "Benin", "Togo"])
    return f


class TestLabelsUnmatchedSurvivesAssembly:
    """GH #787 item 3: the per-country pass-through report used to die at the
    cross-country concat (two countries' reports differ, so `attrs` -> `{}`)."""

    def test_two_countries_one_unmatched_carries_that_entry_keyed(
            self, food_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Liberia", "Benin"], labels="Aggregate",
                               currency=None)
        assert set(out.index.get_level_values("country")) == {"Liberia",
                                                             "Benin"}
        # Keyed by country at the Feature level; Benin matched everything and
        # so contributes no entry -- "no record" is not "an empty record".
        assert out.attrs["labels_unmatched"] == {"Liberia": _report(3)}

    def test_both_unmatched_both_carried(self, food_feature, monkeypatch):
        monkeypatch.setitem(_FoodCountry.UNMATCHED, "Benin", 5)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Liberia", "Benin"], labels="Aggregate",
                               currency=None)
        assert out.attrs["labels_unmatched"] == {"Liberia": _report(3),
                                                 "Benin": _report(5)}

    def test_a_contract_B_dropped_country_carries_none(self, food_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Liberia", "Benin", "Togo"],
                               labels="Aggregate", currency=None)
        assert out.attrs["labels_unavailable"] == ["Togo"]
        assert "Togo" not in out.attrs["labels_unmatched"]
        assert set(out.attrs["labels_unmatched"]) == {"Liberia"}

    def test_a_single_country_is_keyed_too(self, food_feature):
        """A one-frame concat PRESERVES attrs, so the flat Country report
        would leak through un-keyed if the re-attach did not overwrite it."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Liberia"], labels="Aggregate", currency=None)
        assert out.attrs["labels_unmatched"] == {"Liberia": _report(3)}

    def test_no_labels_no_key(self, food_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Liberia", "Benin"], currency=None)
        assert "labels_unmatched" not in out.attrs


class TestUnitsUnavailableDegrades:
    """GH #828: a country whose food_acquired has no `Price` used to return an
    empty frame under `units='unitprice'|'kgprice'`, and Feature concatenated
    the empties silently.  It now degrades like `labels=` (Contract B)."""

    @pytest.mark.parametrize("units", ["unitprice", "kgprice"])
    def test_dropped_with_one_warning_and_an_attrs_marker(self, food_feature,
                                                          units):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            out = food_feature(["Liberia", "Benin", "Togo"], units=units,
                               currency=None)
        assert set(out.index.get_level_values("country")) == {"Liberia",
                                                             "Benin"}
        assert out.attrs["units_unavailable"] == ["Togo"]
        msgs = [str(w.message) for w in caught
                if "unavailable" in str(w.message)]
        assert len(msgs) == 1, msgs
        assert "Togo" in msgs[0] and "Price" in msgs[0] and units in msgs[0]
        assert not [m for m in (str(w.message) for w in caught)
                    if m.startswith("Failed to load")], \
            "a units= refusal must not be reported as a build failure"

    def test_every_country_dropped_returns_empty_with_the_marker(
            self, food_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Togo"], units="unitprice", currency=None)
        assert out.empty
        assert out.attrs["units_unavailable"] == ["Togo"]

    def test_a_mode_whose_inputs_exist_drops_nobody(self, food_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = food_feature(["Liberia", "Togo"], units="unitvalue",
                               currency=None)
        assert "units_unavailable" not in out.attrs
        assert set(out.index.get_level_values("country")) == {"Liberia",
                                                             "Togo"}


# ---------------------------------------------------------------------------
# GH #931: the declared unit masses survive assembly, per country
# ---------------------------------------------------------------------------

class _CropCountry:
    """Fake ``crop_production``: one quintal row per country, each country
    declaring its OWN quintal -- the GH #919 case (100 kg as the metric
    centner, 45.36 kg as 100 lb).  The record is the shape
    ``Country._finalize_result`` stamps."""

    QUINTAL_KG = {"Liberia": 100.0, "Benin": 45.36}

    def __init__(self, name):
        self.name = name

    def crop_production(self, **kwargs):
        idx = pd.MultiIndex.from_tuples(
            [("2018-19", "c1", f"{self.name}-h1", "p1", "Maize", "Quintal"),
             ("2018-19", "c1", f"{self.name}-h2", "p1", "Maize", "Kg")],
            names=["t", "v", "i", "plot_id", "j", "u"])
        df = pd.DataFrame({"Quantity": [2.0, 10.0]}, index=idx)
        df.attrs[T.UNIT_KG_ATTRS_KEY] = {
            self.name: {"quintal": self.QUINTAL_KG[self.name]}}
        return df


@pytest.fixture
def crop_feature(monkeypatch):
    import lsms_library as ll
    monkeypatch.setattr(ll, "Country", lambda name, **kw: _CropCountry(name))
    f = F.Feature("crop_production")
    object.__setattr__(f, "_countries", ["Liberia", "Benin"])
    return f


class TestUnitKgSurvivesAssembly:
    """Without the carrier the two countries' records disagree, the concat
    hands back ``{}``, and ``harvest_kg`` on a ``Feature`` frame applies no
    declaration at all -- silently, which is the state GH #931 ends."""

    def test_both_countries_tables_arrive_keyed(self, crop_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = crop_feature(["Liberia", "Benin"], currency=None)
        assert out.attrs[T.UNIT_KG_ATTRS_KEY] == {
            "Liberia": {"quintal": 100.0}, "Benin": {"quintal": 45.36}}

    def test_each_row_is_served_its_own_countrys_quintal(self, crop_feature):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = crop_feature(["Liberia", "Benin"], currency=None)
            f = T.harvest_kg_factors(out)
        country = f.index.get_level_values("country")
        u = f.index.get_level_values("u")
        quintal = f[u == "Quintal"]
        got = dict(zip(country[u == "Quintal"], quintal["kg_per_unit"]))
        assert got == {"Liberia": 100.0, "Benin": 45.36}
        assert set(quintal["KgFactorSource"]) == {"metric"}
        assert f.attrs["kg_factor_sources"]["declared"] == 2
        assert f.attrs["kg_factor_declared_from"] == "attrs"


# ---------------------------------------------------------------------------
# the assembly the issue names
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    """End-to-end, on real data.  Skipped when the cache is cold.

    Marked slow: GhanaLSS `food_acquired` is the corpus's largest built table
    (5.26M rows, a ~3-minute warm read), and GhanaLSS is not optional here --
    it is the only country with a `_/derivations.yml`, so it is the only pair
    that can exercise the derivations carrier on real data at all.
    """

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
