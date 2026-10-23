"""The sampling record: config -> ``Country.sampling`` / ``df.attrs``.  GH #793.

How a sample was DRAWN, recorded per ``(country, wave)`` in
``countries/{C}/_/sampling.yml`` and read by :mod:`lsms_library.sampling` --
the sibling of the population record (``tests/test_population.py``), and
pinned the same way, for the same reasons:

1. **The tag travels with its provenance.**  ``design_tag`` is an editorial
   reading; a design record without ``source_type`` and ``confidence`` is
   refused.
2. **The vocabulary is FROZEN from the Uganda exemplar, as-is** (@ligon,
   2026-10-18).  Pinned in both directions: nothing the shipped files use is
   outside it, and nothing in it is unused -- so the freeze can neither be
   widened by a typo in a ``.yml`` nor silently carry dead words.
3. **``derived-from-data`` is never readable as documentation** (the issue's
   own acceptance test).
4. **The record rides on ``attrs`` and survives ``Feature()``**, through the
   carrier registry (``feature._ATTRS_CARRIERS``), and never changes data.
"""

import copy
import pickle
import re
import sys
import warnings

import pandas as pd
import pytest

from lsms_library.sampling import SamplingRecord, sampling_records

S = sys.modules["lsms_library.sampling"]


def design(**kw):
    """A minimal valid design block."""
    block = {"design_tag": "two-stage-stratified-cluster",
             "source_type": "local-documentation", "confidence": "high"}
    block.update(kw)
    return block


def rec(country="A", wave="1", **kw):
    return SamplingRecord.from_config(country, wave, design(**kw))


# A record with nested mappings AND lists -- the shape population never had.
NESTED = dict(
    stages=2, stage_units=["enumeration-area", "household"],
    stratification={"variable": "stratum", "basis": "region-x-urban-rural",
                    "explicit_labels": ["Kampala", "Other Urban"],
                    "implicit": None, "_status": "x"},
    selection={"psu": None, "ssu": "simple-random", "pps": None},
    oversamples=[{"what": "w", "status": "attested", "treatment": None}],
    panel={"role": "baseline", "rotation": None},
)


def _frame_with_record(country="Uganda", waves=("2009-10", "2010-11")):
    df = pd.DataFrame({"t": list(waves), "i": ["1", "2"], "x": [1, 2]}
                      ).set_index(["t", "i"])
    S.attach(df, country)
    return df


# ---------------------------------------------------------------------------
# 1. the triple travels together
# ---------------------------------------------------------------------------

class TestTheTagNeverTravelsAlone:

    @pytest.mark.parametrize("omit", ["design_tag", "source_type", "confidence"])
    def test_a_design_record_missing_any_of_the_three_is_rejected(self, omit):
        block = design(stages=2)
        block.pop(omit)
        with pytest.raises(ValueError, match="travel together"):
            SamplingRecord.from_config("Uganda", "2009-10", block)

    @pytest.mark.parametrize("field,bad", [
        ("design_tag", "two-stage"),                  # not the exemplar's word
        ("design_tag", S.UNRECORDED_TAG),             # library sentinel only
        ("source_type", "questionnaire"),
        ("confidence", "certain"),
        ("documentation_status", "boilerplate"),
    ])
    def test_every_controlled_field_is_validated(self, field, bad):
        block = design(**{field: bad})
        with pytest.raises(ValueError, match="controlled vocabulary"):
            SamplingRecord.from_config("Uganda", "2009-10", block)

    def test_an_oversample_status_is_validated(self):
        block = design(oversamples=[{"what": "x", "status": "suspected"}])
        with pytest.raises(ValueError, match="controlled vocabulary"):
            SamplingRecord.from_config("Uganda", "2009-10", block)

    @pytest.mark.parametrize("block", [
        design(n_psu=322),                                     # a DERIVED count
        design(stratification={"variable": "s", "count": 6}),
        design(selection={"psu": None, "probability": "pps"}),
        design(panel={"role": "panel", "attrition": 0.1}),
        design(oversamples=[{"what": "x", "status": "attested", "weight": 2}]),
    ])
    def test_an_unknown_field_is_refused_at_every_nesting_level(self, block):
        """The freeze is of field NAMES too.  `n_psu` is exactly the derived
        quantity the exemplar's header keeps out of this file."""
        with pytest.raises(ValueError, match="unknown"):
            SamplingRecord.from_config("Uganda", "2009-10", block)

    def test_a_non_json_value_is_refused_at_load_not_at_to_parquet(self):
        import datetime
        with pytest.raises(ValueError, match="JSON"):
            SamplingRecord.from_config(
                "Uganda", "2009-10", design(notes=datetime.date(2026, 1, 1)))


# ---------------------------------------------------------------------------
# 2. derived-from-data is not documentation
# ---------------------------------------------------------------------------

class TestDerivedFromDataIsNeverDocumentation:
    """The acceptance test #793 asked for, made structural."""

    def test_documented_is_true_only_for_a_document(self):
        assert rec(source_type="local-documentation").documented
        assert rec(source_type="wb-catalog").documented
        assert not rec(source_type="derived-from-data",
                       confidence="low").documented
        assert not rec(source_type="not-found", confidence="low").documented
        assert not SamplingRecord.unrecorded("A", "1").documented

    @pytest.mark.parametrize("source_type", ["derived-from-data", "not-found"])
    @pytest.mark.parametrize("claim", [
        {"design_statement": "Within each stratum ..."},
        {"documentation_status": "wave-specific"},
    ])
    def test_a_non_document_may_not_carry_a_document_claim(self, source_type,
                                                          claim):
        with pytest.raises(ValueError, match="never be readable as documentation"):
            SamplingRecord.from_config(
                "A", "1", design(source_type=source_type, confidence="low",
                                 **claim))

    def test_every_shipped_documented_record_is_a_document(self):
        import lsms_library as ll
        for name in ll.countries():
            for r in sampling_records(name).values():
                if r.design_tag is not None and not r.documented:
                    assert r.design_statement is None, (name, r.wave)
                    assert r.documentation_status is None, (name, r.wave)


# ---------------------------------------------------------------------------
# 3. the freeze, pinned in both directions
# ---------------------------------------------------------------------------

def _shipped():
    import lsms_library as ll
    return [r for name in ll.countries()
            for r in sampling_records(name).values()]


class TestTheVocabularyIsFrozenFromUganda:

    def test_the_three_named_vocabularies_are_exactly_the_exemplars(self):
        """#793: "freeze the vocabulary (design_tag / status /
        documentation_status)" -- as-is, so exactly Uganda's values."""
        uga = sampling_records("Uganda").values()
        assert {r.design_tag for r in uga} == S.DESIGN_TAGS
        assert {r.documentation_status for r in uga} == S.DOCUMENTATION_STATUSES
        assert {o["status"] for r in uga for o in (r.oversamples or [])} == \
            S.OVERSAMPLE_STATUSES

    def test_source_type_and_confidence_are_the_exemplar_headers_legend(self):
        assert S.SOURCE_TYPES == {"local-documentation", "wb-catalog",
                                  "derived-from-data", "not-found"}
        assert S.CONFIDENCE_LEVELS == {"high", "medium", "low"}
        assert S.DOCUMENTED_SOURCE_TYPES < S.SOURCE_TYPES

    def test_every_frozen_field_name_is_used_by_a_shipped_record(self):
        """A frozen field nobody uses is a word the freeze invented."""
        recs = _shipped()
        top = {k for r in recs for k in r} - {"country", "wave"}
        assert top == set(SamplingRecord._FIELDS) - {"country", "wave"}
        for name, allowed in (("stratification", S._STRATIFICATION_FIELDS),
                              ("selection", S._SELECTION_FIELDS),
                              ("panel", S._PANEL_FIELDS)):
            used = {k for r in recs for k in (r.get(name) or {})}
            assert used == set(allowed), (name, set(allowed) ^ used)
        used = {k for r in recs for o in (r.oversamples or []) for k in o}
        assert used == set(S._OVERSAMPLE_FIELDS)
        # GH #1135: the measurement block's fields are frozen the same way.
        used = {k for r in recs
                for k in ((r.get("panel") or {}).get("cluster_id_measurement")
                          or {})}
        assert used == set(S._MEASUREMENT_FIELDS), \
            set(S._MEASUREMENT_FIELDS) ^ used


# ---------------------------------------------------------------------------
# 4. config round trip
# ---------------------------------------------------------------------------

class TestConfigRoundTrip:

    def test_every_shipped_file_loads(self):
        """A malformed record RAISES (a config bug), so loading them all is
        the test that every one is well-formed."""
        import lsms_library as ll
        for name in ll.countries():
            sampling_records(name)

    def test_uganda_has_a_record_for_every_wave(self):
        import lsms_library as ll
        c = ll.Country("Uganda")
        assert set(c.sampling) == set(c.waves)

    def test_a_known_record_carries_its_verbatim_statement_and_exception(self):
        r = sampling_records("Uganda")["2005-06"]
        assert r.stages == 2 and "IDP" in r.stages_note    # recorded, not averaged
        assert r.selection["psu"] is None                   # absence is written
        assert "selected randomly" in r.selection["_status"]
        r = sampling_records("Uganda")["2013-14"]
        assert r.documentation_status == "carried-forward"

    def test_an_uncurated_country_yields_an_empty_record_not_an_error(self):
        assert sampling_records("NoSuchCountry") == {}

    def test_country_sampling_is_the_loader(self):
        import lsms_library as ll
        assert ll.Country("Uganda").sampling is sampling_records("Uganda")


# ---------------------------------------------------------------------------
# 5. the record rides on attrs
# ---------------------------------------------------------------------------

class TestRecordsRideOnAttrs:

    def test_attach_puts_the_record_on_attrs(self):
        df = _frame_with_record()
        blk = df.attrs[S.ATTRS_KEY]["Uganda"]["2009-10"]
        assert isinstance(blk, SamplingRecord)
        assert blk.design_tag == blk["design_tag"] == "two-stage-stratified-cluster"
        assert df.attrs[S.ATTRS_RESOLUTION_KEY]["Uganda"] == "exact"

    def test_an_absent_optional_field_reads_as_None_not_KeyError(self):
        r = rec()
        assert r.frame is None and "frame" not in r

    def test_the_record_compares_equal_to_the_plain_dict_it_round_trips_as(self):
        r = rec(**NESTED)
        import json
        assert r == json.loads(json.dumps(r))
        assert r == rec(**NESTED) and r is not rec(**NESTED)

    def test_records_are_immutable_all_the_way_down(self):
        """The loader is lru_cached: a nested mutation would poison it."""
        r = sampling_records("Uganda")["2009-10"]
        for call in (lambda: r.__setitem__("confidence", "low"),
                     lambda: r.update({"confidence": "low"}),
                     lambda: r.pop("confidence"),
                     lambda: r["stratification"].__setitem__("variable", "x"),
                     lambda: r["stratification"]["explicit_labels"].append("x"),
                     lambda: r["oversamples"][0].update({"status": "x"}),
                     lambda: r["stage_units"].sort()):
            with pytest.raises(TypeError, match="immutable"):
                call()
        assert sampling_records("Uganda")["2009-10"].stratification["variable"] \
            == "stratum"

    def test_records_with_nested_values_are_hashable(self):
        """Population's tuple-of-items hash raises on a nested record."""
        assert len({rec(**NESTED), rec(**NESTED)}) == 1

    def test_deepcopy_and_pickle_round_trip(self):
        r = rec(**NESTED)
        for clone in (copy.deepcopy(r), pickle.loads(pickle.dumps(r))):
            assert type(clone) is SamplingRecord and clone == r
            assert type(clone["stratification"]) is S._FrozenDict

    def test_to_parquet_still_works(self, tmp_path):
        df = _frame_with_record()
        path = tmp_path / "s.parquet"
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            df.to_parquet(path)
        back = pd.read_parquet(path)
        blk = back.attrs[S.ATTRS_KEY]["Uganda"]["2009-10"]
        assert blk == dict(df.attrs[S.ATTRS_KEY]["Uganda"]["2009-10"])

    def test_records_from_attrs_reads_records_AND_round_tripped_dicts(self):
        import json
        df = _frame_with_record()
        as_dicts = pd.DataFrame({"x": [1]})
        as_dicts.attrs = json.loads(json.dumps(df.attrs))
        for frame in (df, as_dicts):
            out = S.records_from_attrs(frame)
            assert {type(x) for x in out} == {SamplingRecord}
            assert {x.wave for x in out} == {"2009-10", "2010-11"}

    def test_an_unknown_field_is_rejected_by_the_constructor_too(self):
        with pytest.raises(TypeError, match="unexpected field"):
            SamplingRecord(country="A", wave="1", design_tg="typo")

    def test_country_sampling_and_attrs_return_the_SAME_type(self):
        from_property = sampling_records("Uganda")["2009-10"]
        from_attrs = _frame_with_record().attrs[S.ATTRS_KEY]["Uganda"]["2009-10"]
        assert type(from_property) is type(from_attrs) is SamplingRecord


class TestWaveResolution:

    def test_a_frame_with_no_t_axis_reports_every_wave(self):
        df = pd.DataFrame({"i": ["1"], "x": [1]}).set_index("i")
        S.attach(df, "Uganda")
        assert set(df.attrs[S.ATTRS_KEY]["Uganda"]) == set(sampling_records("Uganda"))
        assert df.attrs[S.ATTRS_RESOLUTION_KEY]["Uganda"] == "all-waves (no t axis)"

    def test_a_frame_reports_only_the_waves_it_holds(self):
        assert set(_frame_with_record().attrs[S.ATTRS_KEY]["Uganda"]) == \
            {"2009-10", "2010-11"}

    def test_an_unmatched_t_is_marked_unrecorded_not_silently_dropped(self):
        df = _frame_with_record(waves=("2009-10", "1999"))
        r = df.attrs[S.ATTRS_KEY]["Uganda"]["1999"]
        assert r.design_tag == S.UNRECORDED_TAG and not r.documented

    def test_an_uncurated_country_gets_nothing_attached(self):
        """Absence is silence, exactly like population's {}."""
        df = _frame_with_record(country="NoSuchCountry")
        assert S.ATTRS_KEY not in df.attrs

    def test_attach_never_raises(self):
        S.attach(object(), "Uganda")
        S.attach(pd.DataFrame(), "Uganda")

    # Red team r1 (GH #793): GhanaLSS leaves its first wave uncurated by design
    # (no preceding boundary), so judging "is this a wave label?" by the
    # records alone sent a one-wave 1987-88 frame down the all-waves fallback.
    GHANA_WAVES = ["1987-88", "1988-89", "1991-92", "1998-99", "2005-06",
                   "2012-13", "2016-17"]

    def _ghana_frame(self, t, known_waves):
        df = pd.DataFrame({"t": [t], "i": ["1"], "x": [1]}).set_index(["t", "i"])
        S.attach(df, "GhanaLSS", known_waves=known_waves)
        return df

    def test_an_uncurated_KNOWN_wave_resolves_exact_with_a_placeholder(self):
        df = self._ghana_frame("1987-88", self.GHANA_WAVES)
        recs = df.attrs[S.ATTRS_KEY]["GhanaLSS"]
        assert df.attrs[S.ATTRS_RESOLUTION_KEY]["GhanaLSS"] == "exact"
        assert sorted(recs) == ["1987-88"]
        assert recs["1987-88"].design_tag == S.UNRECORDED_TAG == "unrecorded"

    def test_a_t_that_is_no_known_wave_still_falls_back_loudly(self):
        df = self._ghana_frame("1999", self.GHANA_WAVES)
        assert df.attrs[S.ATTRS_RESOLUTION_KEY]["GhanaLSS"] == \
            "all-waves (no t value matched a wave label)"
        assert set(df.attrs[S.ATTRS_KEY]["GhanaLSS"]) == \
            set(sampling_records("GhanaLSS"))

    def test_without_known_waves_the_records_alone_decide(self):
        """The keyword is opt-in; ``None`` keeps the records-only reading."""
        df = self._ghana_frame("1987-88", None)
        assert df.attrs[S.ATTRS_RESOLUTION_KEY]["GhanaLSS"].startswith("all-waves")


# ---------------------------------------------------------------------------
# 6. Feature carries it, and nothing is changed
# ---------------------------------------------------------------------------

class _FakeCountry:
    """Returns a real 2-row frame carrying a real sampling record."""

    def __init__(self, name):
        self.name = name

    def household_roster(self, **kwargs):
        df = pd.DataFrame({"t": ["2009-10", "2010-11"], "i": ["1", "2"],
                           "Age": [30, 40]}).set_index(["t", "i"])
        S.attach(df, self.name)
        return df


class TestFeatureCarriesTheRecord:

    def test_the_sampling_carrier_is_registered_with_its_own_merge(self):
        F = sys.modules["lsms_library.feature"]
        carrier = [c for c in F._ATTRS_CARRIERS if c.name == "sampling"]
        assert len(carrier) == 1
        assert carrier[0].attach is S.merge_attrs
        assert carrier[0].keys == (S.ATTRS_KEY, S.ATTRS_RESOLUTION_KEY)

    def test_feature_reattaches_for_kept_countries_and_changes_no_data(
            self, monkeypatch):
        import lsms_library as ll
        from lsms_library.feature import Feature

        monkeypatch.setattr(ll, "Country", lambda name, **kw: _FakeCountry(name))
        f = Feature("household_roster")
        object.__setattr__(f, "_countries", ["Uganda", "Benin"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with_records = f(["Uganda", "Benin"], currency=None)
        assert set(with_records.attrs[S.ATTRS_KEY]) == {"Uganda"}   # Benin: none
        assert isinstance(
            with_records.attrs[S.ATTRS_KEY]["Uganda"]["2009-10"], SamplingRecord)

        monkeypatch.setattr(S, "sampling_records", lambda country: {})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            without = f(["Uganda", "Benin"], currency=None)
        assert S.ATTRS_KEY not in without.attrs
        pd.testing.assert_frame_equal(with_records, without)


# ---------------------------------------------------------------------------
# 7. cache neutrality
# ---------------------------------------------------------------------------

def test_the_sampling_module_is_not_folded_into_any_build_fingerprint():
    """The attach lives in ``_finalize_result`` (excluded, read-path); the
    ``.yml`` is not a build input.  Asserted structurally, like the audits."""
    R = sys.modules["lsms_library._build_registry"]
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = [p.split("=")[0] for p in parts if "lsms_library.sampling" in p]
    assert not leaked, leaked
    assert ".yml" not in sys.modules["lsms_library.country"]._BUILD_INPUT_SUFFIXES


# ---------------------------------------------------------------------------
# 8. delivered on a real table (needs the built cache)
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_the_record_arrives_on_a_built_uganda_table():
    import lsms_library as ll
    try:
        s = ll.Country("Uganda").sample(waves=["2009-10", "2010-11"])
    except Exception as e:                        # pragma: no cover
        pytest.skip(f"Uganda sample not buildable here: {e}")
    recs = s.attrs[S.ATTRS_KEY]["Uganda"]
    assert set(recs) == {"2009-10", "2010-11"}
    assert recs["2009-10"] == sampling_records("Uganda")["2009-10"]


@pytest.mark.requires_s3
def test_a_first_wave_frame_carries_only_its_own_placeholder():
    """Red team r1 (GH #793): before the fix this frame carried 1988-89 ..
    2016-17 under ``'all-waves (no t value matched a wave label)'``, though
    1987-88 IS a wave label.  ``Country._finalize_result`` now passes
    ``known_waves=self.waves``."""
    import lsms_library as ll
    try:
        s = ll.Country("GhanaLSS").sample(waves=["1987-88"])
    except Exception as e:                        # pragma: no cover
        pytest.skip(f"GhanaLSS sample not buildable here: {e}")
    assert s.attrs[S.ATTRS_RESOLUTION_KEY]["GhanaLSS"] == "exact"
    recs = s.attrs[S.ATTRS_KEY]["GhanaLSS"]
    assert sorted(recs) == ["1987-88"]
    assert recs["1987-88"].design_tag == "unrecorded"


# ===========================================================================
# GH #942 -- the `panel:` continuity declaration and the Country-level warning
# ===========================================================================

def panel(**kw):
    block = {"household_continuity": "panel",
             "cluster_id_continuity": "wave-scoped",
             "cluster_id_note": "renumbered.", "source_type": "derived-from-data",
             "confidence": "high", "locator": "measured"}
    block.update(kw)
    return {k: v for k, v in block.items() if v is not None}


class TestThePanelDeclaration:
    """Per boundary, two orthogonal fields, own provenance (DESIGN Q1/Q2)."""

    def test_a_panel_only_entry_is_a_record(self):
        r = SamplingRecord.from_config("GhanaLSS", "1988-89", {"panel": panel()})
        assert r.design_tag is None and r.cluster_id_continuity == "wave-scoped"
        assert r.household_continuity == "panel"

    def test_a_panel_only_entry_needs_its_own_provenance(self):
        with pytest.raises(ValueError, match="missing"):
            SamplingRecord.from_config(
                "A", "2", {"panel": {"role": "panel"}})

    @pytest.mark.parametrize("drop", ["household_continuity",
                                      "cluster_id_continuity"])
    def test_the_two_facts_come_together(self, drop):
        with pytest.raises(ValueError, match="orthogonal"):
            SamplingRecord.from_config("A", "2", {"panel": panel(**{drop: None})})

    @pytest.mark.parametrize("field,bad", [
        ("household_continuity", "refresh-by-design"),   # NOT a value (Q2)
        ("cluster_id_continuity", "rendered-differently"),  # deliberately absent
        ("cluster_id_continuity", "panel-stable"),
        ("source_type", "questionnaire"),
        ("confidence", "certain"),
    ])
    def test_the_panel_vocabularies_are_controlled(self, field, bad):
        with pytest.raises(ValueError, match="controlled vocabulary"):
            SamplingRecord.from_config("A", "2", {"panel": panel(**{field: bad})})

    @pytest.mark.parametrize("drop", ["source_type", "confidence"])
    def test_a_continuity_reading_carries_its_own_provenance(self, drop):
        """Even on a DESIGN record: the design triple describes the design
        statement, not this measurement."""
        with pytest.raises(ValueError, match="missing"):
            SamplingRecord.from_config(
                "A", "2", design(panel=panel(**{drop: None})))

    def test_a_continuity_reading_needs_a_locator_unless_not_found(self):
        with pytest.raises(ValueError, match="locator"):
            SamplingRecord.from_config("A", "2", {"panel": panel(locator=None)})
        SamplingRecord.from_config(
            "A", "2", {"panel": panel(locator=None, source_type="not-found",
                                      confidence="low")})

    @pytest.mark.parametrize("value", sorted(
        S.CLUSTER_ID_CONTINUITY - {"stable"}))
    def test_every_verdict_but_stable_needs_a_reason(self, value):
        with pytest.raises(ValueError, match="cluster_id_note"):
            SamplingRecord.from_config("A", "2", {"panel": panel(
                cluster_id_continuity=value, cluster_id_note=None)})
        SamplingRecord.from_config("A", "2", {"panel": panel(
            cluster_id_continuity="stable", cluster_id_note=None)})

    def test_the_warning_set_excludes_unobservable_on_purpose(self):
        """GLSS3 onward are different EAs with no linkage: grouping on v
        across them is CORRECT (DESIGN §4.2)."""
        assert S.INCOMPARABLE_CLUSTER_IDS == {"wave-scoped", "redefined", "mixed"}
        assert S.INCOMPARABLE_CLUSTER_IDS < S.CLUSTER_ID_CONTINUITY

    def test_no_shipped_first_wave_declares_a_boundary(self):
        """The record of wave W describes W vs its PREDECESSOR; the first
        wave has none."""
        import lsms_library as ll
        for name in ll.countries():
            recs = sampling_records(name)
            if not recs:
                continue
            first = ll.Country(name).waves[0]
            assert first not in recs or recs[first].cluster_id_continuity is None, name
            assert set(recs) <= set(ll.Country(name).waves), name


# -- the audit, on synthetic frames ----------------------------------------

WAVES = ["w1", "w2", "w3", "w4"]


@pytest.fixture
def declared(monkeypatch):
    """A fake country whose boundaries are w2: wave-scoped, w3: unobservable,
    w4: stable."""
    recs = {
        "w2": SamplingRecord.from_config("Fake", "w2", {"panel": panel(
            cluster_id_note="Renumbered at w2. More detail here.")}),
        "w3": SamplingRecord.from_config("Fake", "w3", {"panel": panel(
            household_continuity="independent",
            cluster_id_continuity="unobservable",
            cluster_id_note="No linkage.")}),
        "w4": SamplingRecord.from_config("Fake", "w4", {"panel": panel(
            cluster_id_continuity="stable")}),
    }
    monkeypatch.setattr(S, "sampling_records",
                        lambda c: recs if c == "Fake" else {})
    S._clear_sampling_reports()
    yield recs
    S._clear_sampling_reports()


def frame(waves, v_where="index"):
    rows = [(w, f"c{k}", f"h{k}") for w in waves for k in range(2)]
    df = pd.DataFrame(rows, columns=["t", "v", "i"]).assign(x=1.0)
    return df.set_index(["t", "v", "i"] if v_where == "index" else ["t", "i"])


def warns(fn):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = fn()
    return out, [w for w in caught
                 if issubclass(w.category, S.ClusterIdentityWarning)]


class TestTheAudit:

    def test_a_frame_crossing_a_wave_scoped_boundary_is_reported(self, declared):
        rep = S.audit_cluster_identity(frame(["w1", "w2"]), country="Fake",
                                       table="t", waves=WAVES)
        assert [(b["from"], b["to"]) for b in rep["boundaries"]] == [("w1", "w2")]
        assert rep["boundaries"][0]["in_frame"]

    def test_v_as_a_column_counts(self, declared):
        assert S.audit_cluster_identity(frame(["w1", "w2"], v_where="column"),
                                        country="Fake", table="t", waves=WAVES)

    @pytest.mark.parametrize("waves", [["w1"], ["w2", "w3"], ["w3", "w4"],
                                       ["w2", "w3", "w4"]])
    def test_silent_when_no_incomparable_boundary_is_spanned(self, declared,
                                                            waves):
        assert S.audit_cluster_identity(frame(waves), country="Fake",
                                        table="t", waves=WAVES) is None

    def test_a_skipped_wave_still_counts_and_says_so(self, declared):
        """`waves=['w1','w3']` crosses w2's renumbering though w2 is absent."""
        rep = S.audit_cluster_identity(frame(["w1", "w3"]), country="Fake",
                                       table="t", waves=WAVES)
        assert [b["to"] for b in rep["boundaries"]] == ["w2"]
        assert rep["boundaries"][0]["in_frame"] is False

    def test_silent_without_v_without_a_file_or_without_wave_labels(self, declared):
        no_v = frame(["w1", "w2"]).reset_index("v", drop=True)
        assert S.audit_cluster_identity(no_v, country="Fake", table="t",
                                        waves=WAVES) is None
        assert S.audit_cluster_identity(frame(["w1", "w2"]), country="Other",
                                        table="t", waves=WAVES) is None
        assert S.audit_cluster_identity(frame(["1999", "2000"]), country="Fake",
                                        table="t", waves=WAVES) is None


class TestTheWarning:

    def test_it_warns_names_the_boundary_and_returns_the_frame_unchanged(
            self, declared):
        df = frame(["w1", "w2"])
        before = df.copy()
        out, caught = warns(lambda: S.check_cluster_identity(
            df, country="Fake", table="household_roster", waves=WAVES))
        assert out is df
        pd.testing.assert_frame_equal(out, before)
        assert len(caught) == 1
        msg = str(caught[0].message)
        for token in ("Fake/household_roster", "w1 -> w2", "wave-scoped",
                      "Renumbered at w2.", "MEASURED, not documented",
                      "unchanged", "droplevel('v')", "LSMS_CLUSTER_STRICT"):
            assert token in msg, (token, msg)
        assert "More detail" not in msg          # first sentence only

    def test_the_ledger_files_it(self, declared):
        warns(lambda: S.check_cluster_identity(
            frame(["w1", "w2"]), country="Fake", table="sample", waves=WAVES))
        assert len(S.sampling_reports(country="Fake", table="sample")) == 1
        assert S.sampling_reports(country="Fake", table="other") == []

    def test_strict_raises_a_class_the_legacy_fallbacks_do_not_catch(
            self, declared, monkeypatch):
        monkeypatch.setenv("LSMS_CLUSTER_STRICT", "1")
        with pytest.raises(S.ClusterIdentityError, match="w1 -> w2"):
            S.check_cluster_identity(frame(["w1", "w2"]), country="Fake",
                                     table="t", waves=WAVES)
        # Country.__getattr__'s derived paths catch exactly these and fall back
        # to a legacy build; a strict refusal must not become a second attempt.
        assert not issubclass(S.ClusterIdentityError,
                              (RuntimeError, ValueError, KeyError))

    def test_a_broken_audit_warns_rather_than_breaking_the_read(
            self, declared, monkeypatch):
        def boom(*a, **k):
            raise ZeroDivisionError("x")
        monkeypatch.setattr(S, "audit_cluster_identity", boom)
        df = frame(["w1", "w2"])
        out, caught = warns(lambda: S.check_cluster_identity(
            df, country="Fake", table="t", waves=WAVES))
        assert out is df and "could not run" in str(caught[0].message)

    def test_nested_table_fetches_are_silent(self, declared):
        """The v-join fetches sample() for ALL waves inside the caller's
        table call; that frame is not what the caller asked for."""
        inner = S.outermost_call(lambda: S.check_cluster_identity(
            frame(["w1", "w2"]), country="Fake", table="sample", waves=WAVES))
        outer = S.outermost_call(lambda: (inner(), S.check_cluster_identity(
            frame(["w3", "w4"]), country="Fake", table="roster", waves=WAVES)))
        _, caught = warns(outer)
        assert caught == []
        assert S.sampling_reports(country="Fake") == []

    def test_once_per_outermost_call(self, declared):
        """food_expenditures() finalises food_acquired AND the derived table
        at depth 1 -- one report, not two."""
        def twice():
            for table in ("food_acquired", "food_expenditures"):
                S.check_cluster_identity(frame(["w1", "w2"]), country="Fake",
                                         table=table, waves=WAVES)
        _, caught = warns(S.outermost_call(twice))
        assert len(caught) == 1
        _, caught = warns(S.outermost_call(twice))      # a NEW call reports again
        assert len(caught) == 1

    def test_the_depth_unwinds_after_an_exception(self, declared):
        def bad():
            raise KeyError("x")
        with pytest.raises(KeyError):
            S.outermost_call(bad)()
        assert S._CALL_STATE.get() is None

    def test_outermost_call_keeps_the_methods_name_and_doc(self):
        def sample(waves=None):
            """Doc."""
        wrapped = S.outermost_call(sample)
        assert wrapped.__name__ == "sample" and wrapped.__doc__ == "Doc."


def test_the_warning_is_excluded_from_every_build_fingerprint():
    R = sys.modules["lsms_library._build_registry"]
    for name in ("check_cluster_identity", "audit_cluster_identity"):
        assert f"lsms_library.sampling.{name}" in R._EXCLUDED_CALLABLES
    assert {"_CLUSTER_LEDGER", "_CALL_STATE"} <= R._EXCLUDED_CONSTANTS
    assert ("lsms_library.country.Country._finalize_result"
            in R._EXCLUDED_CALLABLES)


# ===========================================================================
# GH #1135 -- the boundary's SIZE: a typed measurement record, and the first
# line of the warning (ruling @ligon 2026-10-23: print what is measured, say
# "not measured" otherwise, never print the conditional Nigeria W5 86..106 as
# settled)
# ===========================================================================

def measured(**kw):
    """A minimal valid ``cluster_id_measurement`` block."""
    block = {"linked_households": 100, "households_changed": 10,
             "clusters_renumbered": 1, "source_type": "derived-from-data",
             "confidence": "high", "locator": "measured"}
    block.update(kw)
    return {k: v for k, v in block.items() if v is not None}


def with_measurement(**kw):
    return {"panel": panel(cluster_id_measurement=measured(**kw))}


class TestTheMeasurementRecord:
    """Optional, typed, with its own provenance -- and the freeze still
    refuses anything else."""

    def test_a_measurement_block_is_accepted_and_frozen(self):
        r = SamplingRecord.from_config("A", "2", with_measurement())
        m = r["panel"]["cluster_id_measurement"]
        assert isinstance(m, S._FrozenDict)
        assert m["linked_households"] == 100
        with pytest.raises(TypeError, match="immutable"):
            m.__setitem__("linked_households", 1)

    def test_the_block_is_optional(self):
        r = SamplingRecord.from_config("A", "2", {"panel": panel()})
        assert "cluster_id_measurement" not in r["panel"]

    @pytest.mark.parametrize("kw,error", [
        ({"n_psu": 3}, r"unknown field\(s\) \['n_psu'\] in "
                       r"panel\.cluster_id_measurement"),
        ({"linked_households": True}, "non-negative integer"),
        ({"linked_households": 100.0}, "non-negative integer"),
        ({"linked_households": "100"}, "non-negative integer"),
        ({"households_changed": -1}, "non-negative integer"),
        ({"households_changed": 101}, "exceeds linked_households"),
        ({"households_changed_net_of_movers": 11, "mover_rule": "x"},
         "exceeds households_changed"),
        ({"households_changed_net_of_movers": 5}, "mover_rule comes with"),
        ({"mover_rule": "sentinel"}, "mover_rule comes with"),
        ({"linked_households": None}, "without its denominator"),
        ({"linked_households": None, "households_changed": None,
          "clusters_renumbered": None}, "carries no count"),
        ({"source_type": None}, "travels with its own provenance"),
        ({"confidence": None}, "travels with its own provenance"),
        ({"locator": None}, "travels with its own provenance"),
        ({"source_type": "not-found", "confidence": "low"}, "OMITTING"),
        ({"source_type": "questionnaire"}, "controlled vocabulary"),
        ({"confidence": "certain"}, "controlled vocabulary"),
    ])
    def test_a_malformed_block_is_refused(self, kw, error):
        """Each case matches the NEW error text: on a schema without the
        block every one of them fails as 'unknown field', which a loose
        match would wrongly accept."""
        with pytest.raises(ValueError, match=error):
            SamplingRecord.from_config("A", "2", with_measurement(**kw))

    def test_a_measurement_needs_a_declared_boundary(self):
        block = {"panel": {"role": "panel", "source_type": "derived-from-data",
                           "confidence": "high",
                           "cluster_id_measurement": measured()}}
        with pytest.raises(ValueError, match="measures a declared boundary"):
            SamplingRecord.from_config("A", "2", block)

    def test_a_net_count_may_equal_its_bounds(self):
        SamplingRecord.from_config("A", "2", with_measurement(
            households_changed=10, households_changed_net_of_movers=10,
            mover_rule="none moved"))
        SamplingRecord.from_config("A", "2", with_measurement(
            linked_households=0, households_changed=None,
            clusters_renumbered=None))

    def test_every_shipped_incomparable_boundary_is_measured(self):
        """The ruling's first line needs a size for every warned boundary;
        the curation covers all of today's (stable ones carry none)."""
        import lsms_library as ll
        for name in ll.countries():
            for w, r in sampling_records(name).items():
                if r.cluster_id_continuity in S.INCOMPARABLE_CLUSTER_IDS:
                    assert r["panel"].get("cluster_id_measurement"), (name, w)

    @pytest.mark.parametrize("country,wave,n,d,net,k", [
        # lifted from each boundary's cluster_id_note / counts-safe.json
        ("GhanaLSS", "1988-89", 712, 712, None, 79),
        ("Ethiopia", "2013-14", 3776, 3776, None, 333),
        ("Tanzania", "2012-13", 2904, 591, None, 82),
        ("Tanzania", "2014-15", 521, 108, None, None),
        ("Tanzania", "2019-20", 849, 849, None, None),
        ("Uganda", "2009-10", 2601, 191, None, None),
        ("Uganda", "2010-11", 2640, 433, None, None),
        ("Uganda", "2013-14", 1717, 1717, None, None),
        ("Uganda", "2018-19", 2973, 2973, None, None),
        ("Nigeria", "2018Q3", 1493, 79, 10, 1),
        ("Nigeria", "2023Q3", 5067, 343, None, 12),
        ("Burkina_Faso", "2018-19", 0, None, None, None),
        ("Niger", "2018-19", 0, None, None, None),
    ])
    def test_the_shipped_counts(self, country, wave, n, d, net, k):
        m = sampling_records(country)[wave]["panel"]["cluster_id_measurement"]
        assert m.get("linked_households") == n
        assert m.get("households_changed") == d
        assert m.get("households_changed_net_of_movers") == net
        assert m.get("clusters_renumbered") == k


def _shipped_message(country, waves_in_frame):
    """The warning a frame holding *waves_in_frame* gets from the SHIPPED
    records -- synthetic rows, real config, no data."""
    import lsms_library as ll
    waves = ll.Country(country).waves
    df = pd.DataFrame({"t": list(waves_in_frame), "v": "x", "i": "1",
                       "x": 1.0}).set_index(["t", "v", "i"])
    rep = S.audit_cluster_identity(df, country=country, table="sample",
                                   waves=waves)
    assert rep is not None
    return rep, S._format_cluster_report(rep)


class TestTheFirstLine:

    def test_nigeria_w3_w4_states_both_sizes(self):
        """The ruling's own example: a reader can judge 0.67%."""
        _, msg = _shipped_message("Nigeria", ["2016Q1", "2018Q3"])
        first = msg.splitlines()[0]
        assert "2016Q1 -> 2018Q3: 1 cluster id renumbered" in first, first
        assert ("10 of 1,493 linked households (0.67%) change `v` net of "
                "movers") in first, first

    def test_a_100_percent_boundary_states_its_size(self):
        _, msg = _shipped_message("GhanaLSS", ["1987-88", "1988-89"])
        first = msg.splitlines()[0]
        assert "1987-88 -> 1988-89: 79 cluster ids renumbered" in first, first
        assert "net-of-movers share: not measured" in first, first
        assert "712 of 712 linked households, 100%, change `v` counting " \
            "movers" in first, first

    def test_the_conditional_nigeria_w5_figure_is_never_printed_as_settled(
            self, monkeypatch):
        """86..106 is conditional on what W5's 'original location' means.
        The first line must SAY net-of-movers is not measured (a positive
        claim, so this fails on a formatter that prints no sizes at all),
        and no 86 / 106 may appear anywhere in the message, nor the
        sentinel-only 112 on the first line."""
        import lsms_library as ll
        waves = ll.Country("Nigeria").waves
        for pair in (["2019Q1", "2023Q3"], ["2016Q1", "2024Q1"]):
            _, msg = _shipped_message("Nigeria", pair)
            first = msg.splitlines()[0]
            assert ("2019Q1 -> 2023Q3: 12 cluster ids renumbered, "
                    "net-of-movers share: not measured") in first, first
            assert re.search(r"\b(86|106)\b", msg) is None, msg
            assert not re.search(r"\b112\b", first), first
            # the strict refusal carries the same text
            df = pd.DataFrame({"t": pair, "v": "x", "i": "1", "x": 1.0}
                              ).set_index(["t", "v", "i"])
            monkeypatch.setenv("LSMS_CLUSTER_STRICT", "1")
            with pytest.raises(S.ClusterIdentityError) as exc:
                S.check_cluster_identity(df, country="Nigeria",
                                         table="sample", waves=waves)
            monkeypatch.delenv("LSMS_CLUSTER_STRICT")
            refused = str(exc.value)
            assert "net-of-movers share: not measured" in refused
            assert re.search(r"\b(86|106)\b", refused) is None
        # ... and the record carries it only as a qualified note, never as
        # a count the formatter could print.
        m = sampling_records("Nigeria")["2023Q3"]["panel"][
            "cluster_id_measurement"]
        assert "households_changed_net_of_movers" not in m
        note = " ".join(m["note"].split())
        assert "86..106 holds ONLY IF" in note and "not established" in note

    def test_every_flagged_boundary_is_on_the_one_first_line(self):
        rep, msg = _shipped_message("Uganda", ["2005-06", "2019-20"])
        first = msg.splitlines()[0]
        for b in rep["boundaries"]:
            assert f"{b['from']} -> {b['to']}: " in first, (b["to"], first)
        assert first.count(" -> ") == len(rep["boundaries"]) == 4

    def test_no_household_link_is_said_not_divided(self):
        _, msg = _shipped_message("Burkina_Faso", ["2014", "2018-19"])
        first = msg.splitlines()[0]
        assert ("2014 -> 2018-19: cluster ids renumbered: not measured, no "
                "household is linked across it") in first, first

    def test_an_unmeasured_boundary_says_not_measured(self, declared):
        _, caught = warns(lambda: S.check_cluster_identity(
            frame(["w1", "w2"]), country="Fake", table="t", waves=WAVES))
        first = str(caught[0].message).splitlines()[0]
        assert "w1 -> w2: size not measured" in first, first

    def test_the_report_carries_the_measurement_whole(self):
        """`sampling_reports()` consumers must never parse the message."""
        rep, _ = _shipped_message("Nigeria", ["2016Q1", "2018Q3"])
        (b,) = rep["boundaries"]
        assert b["measurement"] == sampling_records("Nigeria")["2018Q3"][
            "panel"]["cluster_id_measurement"]
        assert b["measurement"]["households_changed_net_of_movers"] == 10

    def test_the_measurement_provenance_is_on_the_detail_line(self):
        _, msg = _shipped_message("Nigeria", ["2016Q1", "2018Q3"])
        detail = msg.splitlines()[1]
        assert "size: derived-from-data, confidence medium" in detail, detail

    @pytest.mark.parametrize("num,den,want", [
        (10, 1493, "0.67%"), (712, 712, "100%"), (0, 5, "0%"),
        (591, 2904, "20.35%"), (99999, 100000, ">99.99%"),
        (1, 100000, "<0.01%"),
    ])
    def test_a_partial_share_is_never_rounded_to_0_or_100(self, num, den,
                                                          want):
        assert S._share(num, den) == want


# -- the shipped declarations, and the reproduction --------------------------

class TestShippedDeclarations:

    @pytest.mark.parametrize("country,wave,value", [
        ("Niger", "2014-15", "stable"),              # ECVMA panel keeps GRAPPE
        ("Niger", "2018-19", "redefined"),           # ECVMA / EHCVM serials overlap
        ("Niger", "2021-22", "stable"),              # s00q07f1 == grappe
        ("GhanaLSS", "1988-89", "wave-scoped"),      # GLSS2 renumbering
        ("GhanaLSS", "1991-92", "unobservable"),     # no linkage: correct to group
        ("Ethiopia", "2013-14", "wave-scoped"),      # ea_id -> ea_id2
        ("Ethiopia", "2018-19", "unobservable"),     # Panel II
        ("Tanzania", "2012-13", "wave-scoped"),      # UPD4 round-3 renumbering
        ("Tanzania", "2019-20", "wave-scoped"),      # NPS-SDD layout
        ("Uganda", "2009-10", "mixed"),              # @lat,lon bins
        # was wave-scoped (two renderings of `comm`) until GH #1133 harmonised them
        ("Uganda", "2011-12", "stable"),
        ("Uganda", "2013-14", "redefined"),          # EA -> parish
        ("Nigeria", "2012Q3", "stable"),             # movers are singletons
        ("Burkina_Faso", "2018-19", "redefined"),    # EMC zd / EHCVM grappe serials
        ("Burkina_Faso", "2021-22", "stable"),       # grappe == grappe_EHCVM1
    ])
    def test_the_measured_boundaries(self, country, wave, value):
        assert sampling_records(country)[wave].cluster_id_continuity == value

    def test_tanzania_2020_21_is_a_household_refresh_not_a_cluster_fact(self):
        """The distinction a one-dimensional enum could not express (Q2)."""
        r = sampling_records("Tanzania")["2020-21"]
        assert r.household_continuity == "refresh"
        assert r.cluster_id_continuity == "unobservable"

    def test_niger_programme_switch_warns_and_its_panels_do_not(self):
        """Niger (GH #1247): ECVMA (2011-12, 2014-15) and EHCVM (2018-19,
        2021-22) are two programmes with no household linkage, but BOTH number
        their clusters from 1, so 267 ids recur and Region agrees on 27 --
        chance.  `unobservable` is silent because grouping across DISJOINT
        keyspaces is correct (GhanaLSS 1991-92); these overlap, so the
        boundary must warn.  Inside EHCVM the 79 new 2021-22 grappes are
        numbered 900-996, above 2018-19's 710 -- disjoint, which is why that
        boundary is `stable` and not `mixed`.  The 2014-15 note names the
        '20012' id collision so nobody re-measures it as a renumbering."""
        recs = sampling_records("Niger")
        r = recs["2018-19"]
        assert r.household_continuity == "independent"
        assert r.cluster_id_continuity in S.INCOMPARABLE_CLUSTER_IDS
        note = " ".join(r["panel"]["cluster_id_note"].split())
        assert "keyspaces OVERLAP" in note and "26.5 expected by chance" in note
        for wave in ("2014-15", "2021-22"):
            assert recs[wave].cluster_id_continuity not in S.INCOMPARABLE_CLUSTER_IDS
        assert "DISJOINT" in recs["2021-22"]["panel"]["cluster_id_note"]
        assert "'20012'" in recs["2014-15"]["panel"]["cluster_id_note"]

    def test_a_verified_linkage_is_never_declared_unobservable(self):
        """Uganda 2010-11 -> 2011-12: 2,510 households link.  The linkage IS
        verified, so `unobservable` ("no verified linkage") is wrong whatever
        the warning policy (dispatcher call on red team r1 H1, 2026-10-19; this
        test replaced one pinning the reverse).  It was `wave-scoped` while the
        survey's two renderings of `comm` were served un-harmonised (0 of 2,510
        kept `v`), and is `stable` since GH #1133 served the 7-character waves
        the 8-digit frame EA (2,510 of 2,510)."""
        r = sampling_records("Uganda")["2011-12"]
        assert r.cluster_id_continuity != "unobservable"
        assert r.cluster_id_continuity == "stable"
        assert r.cluster_id_continuity not in S.INCOMPARABLE_CLUSTER_IDS
        note = " ".join(r["panel"]["cluster_id_note"].split())
        assert "2,510 of 2,510" in note and "comm_frame_ea" in note
        assert "not a digit rule" in note

    def test_overlapping_serials_without_linkage_are_not_unobservable(self):
        """Burkina Faso 2014 -> 2018-19 (GH #1243): two independent draws, no
        household linkage, but BOTH number their clusters from 1, so the same
        `v` names an unrelated cluster (Region differs for 517 of 585 shared).
        `unobservable` is silent because grouping across DISJOINT keyspaces is
        correct (GhanaLSS 1991-92); here the keyspaces overlap, so it must
        warn.  The note also records that the 583 shared `i` are an id
        collision, so nobody re-measures this boundary as `stable`."""
        r = sampling_records("Burkina_Faso")["2018-19"]
        assert r.household_continuity == "independent"
        assert r.cluster_id_continuity in S.INCOMPARABLE_CLUSTER_IDS
        note = " ".join(r["panel"]["cluster_id_note"].split())
        assert "keyspaces OVERLAP" in note and "id collision" in note


@pytest.mark.requires_s3
class TestTheReproduction:
    """GH #942's own reproduction, a single-country two-wave call."""

    def _fresh(self, name="GhanaLSS"):
        import lsms_library as ll
        return ll.Country(name)          # fresh: no cached sample() for the v-join

    def test_the_issue_reproduction_now_warns_exactly_once(self):
        S._clear_sampling_reports()
        hc, caught = warns(lambda: self._fresh().household_characteristics(
            waves=["1987-88", "1988-89"]))
        assert len(caught) == 1, [str(w.message)[:200] for w in caught]
        assert "1987-88 -> 1988-89" in str(caught[0].message)
        idx = hc.index.to_frame(index=False)[["t", "v", "i"]].drop_duplicates()
        wide = idx.pivot_table(index="i", columns="t", values="v",
                               aggfunc="first").dropna()
        assert (wide["1987-88"] == wide["1988-89"]).sum() == 0   # still served

    def test_a_request_inside_one_regime_is_silent_despite_the_v_join(self):
        """The inner sample() spans ALL seven waves, 1988-89 included; the
        caller asked for two later ones and must hear nothing."""
        _, caught = warns(lambda: self._fresh().household_roster(
            waves=["2005-06", "2012-13"]))
        assert caught == []

    def test_niger_warns_across_the_ecvma_ehcvm_boundary_only(self):
        """GH #1247: 2014-15 (ECVMA `GRAPPE`) and 2018-19 (EHCVM `grappe`)
        reuse the integers 1-270 for unrelated clusters; each programme's
        panel keeps its own `v`.  One warning wherever that boundary is
        spanned, none inside either panel."""
        S._clear_sampling_reports()
        _, caught = warns(lambda: self._fresh("Niger").sample(
            waves=["2014-15", "2018-19"]))
        assert len(caught) == 1, [str(w.message)[:200] for w in caught]
        assert ("2014-15 -> 2018-19: cluster_id_continuity=redefined"
                in str(caught[0].message))
        _, caught = warns(lambda: self._fresh("Niger").sample())
        assert len(caught) == 1, [str(w.message)[:200] for w in caught]
        for pair in (["2011-12", "2014-15"], ["2018-19", "2021-22"]):
            _, caught = warns(lambda: self._fresh("Niger").sample(waves=pair))
            assert caught == [], (pair, [str(w.message)[:200] for w in caught])

    def test_the_served_data_are_identical_with_and_without_the_record(
            self, monkeypatch):
        c = sys.modules["lsms_library.country"]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with_rec = self._fresh().household_roster(waves=["1987-88", "1988-89"])
            monkeypatch.setattr(S, "sampling_records", lambda country: {})
            monkeypatch.setattr(c, "sampling_records", lambda country: {})
            without = self._fresh().household_roster(waves=["1987-88", "1988-89"])
        assert S.ATTRS_KEY in with_rec.attrs and S.ATTRS_KEY not in without.attrs
        pd.testing.assert_frame_equal(with_rec, without)

    def test_uganda_comm_rendering_boundary_is_silent_once_harmonised(self):
        """H1 (red team r1): `sample(waves=['2010-11', '2011-12'])` kept `v`
        for 0 of 2,510 linked households and was SILENT while 2011-12 was
        `unobservable`; it then warned once while `wave-scoped`.  GH #1133
        serves both waves the 8-digit frame EA, so every linked household keeps
        `v` and the call is silent.  A boundary that is still incomparable
        (2005-06 -> 2009-10, `mixed`: synthetic GPS bins) still warns."""
        S._clear_sampling_reports()
        s, caught = warns(lambda: self._fresh("Uganda").sample(
            waves=["2010-11", "2011-12"]))
        assert caught == [], [str(w.message)[:200] for w in caught]
        idx = s.reset_index()[["i", "t", "v"]].dropna()
        wide = idx.pivot_table(index="i", columns="t", values="v",
                               aggfunc="first").dropna()
        assert (wide["2010-11"] == wide["2011-12"]).sum() == len(wide) == 2510
        S._clear_sampling_reports()
        _, caught = warns(lambda: self._fresh("Uganda").sample(
            waves=["2005-06", "2009-10"]))
        assert len(caught) == 1, [str(w.message)[:200] for w in caught]
        assert ("2005-06 -> 2009-10: cluster_id_continuity=mixed"
                in str(caught[0].message))

    def test_burkina_warns_across_the_emc_ehcvm_boundary_only(self):
        """GH #1243: 2014 (EMC `zd`) and 2018-19 (EHCVM `grappe`) reuse the
        integers 1-585 for unrelated clusters; the EHCVM panel keeps its
        `grappe`.  One warning for the first pair, none for the second."""
        S._clear_sampling_reports()
        _, caught = warns(lambda: self._fresh("Burkina_Faso").sample(
            waves=["2014", "2018-19"]))
        assert len(caught) == 1, [str(w.message)[:200] for w in caught]
        assert ("2014 -> 2018-19: cluster_id_continuity=redefined"
                in str(caught[0].message))
        _, caught = warns(lambda: self._fresh("Burkina_Faso").sample(
            waves=["2018-19", "2021-22"]))
        assert caught == [], [str(w.message)[:200] for w in caught]

    def test_strict_mode_refuses_the_reproduction(self, monkeypatch):
        monkeypatch.setenv("LSMS_CLUSTER_STRICT", "1")
        with pytest.raises(S.ClusterIdentityError):
            self._fresh().household_roster(waves=["1987-88", "1988-89"])

    def test_a_real_nigeria_call_states_the_size_on_its_first_line(self):
        """GH #1135: the ruling's test case, on served data."""
        S._clear_sampling_reports()
        _, caught = warns(lambda: self._fresh("Nigeria").sample(
            waves=["2016Q1", "2018Q3"]))
        n = len(caught)
        assert n == 1, n
        first = str(caught[0].message).splitlines()[0]
        assert ("2016Q1 -> 2018Q3: 1 cluster id renumbered, 10 of 1,493 "
                "linked households (0.67%) change `v` net of movers") in first

    # Every curated linked / changed pair, against the served sample() --
    # the oracle for the lifted counts.  Not Burkina Faso / Niger: their N=0
    # comes from the absence of any linkage, which shared `i` cannot show
    # (583 colliding ids / 0 by construction).
    @pytest.mark.parametrize("country,earlier,wave", [
        ("GhanaLSS", "1987-88", "1988-89"),
        ("Ethiopia", "2011-12", "2013-14"),
        ("Tanzania", "2010-11", "2012-13"),
        ("Tanzania", "2012-13", "2014-15"),
        ("Tanzania", "2014-15", "2019-20"),
        ("Uganda", "2005-06", "2009-10"),
        ("Uganda", "2009-10", "2010-11"),
        ("Uganda", "2011-12", "2013-14"),
        ("Uganda", "2015-16", "2018-19"),
        ("Nigeria", "2016Q1", "2018Q3"),
        ("Nigeria", "2019Q1", "2023Q3"),
    ])
    def test_the_curated_size_matches_the_served_sample(self, country,
                                                        earlier, wave):
        m = sampling_records(country)[wave]["panel"]["cluster_id_measurement"]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            s = self._fresh(country).sample(waves=[earlier, wave])
        idx = s.reset_index()[["i", "t", "v"]].dropna()
        idx["t"] = idx["t"].astype(str)
        idx["v"] = idx["v"].astype(str)
        dup = int(idx.duplicated(["i", "t"]).sum())
        assert dup == 0, dup
        a = idx[idx["t"] == earlier].set_index("i")["v"]
        b = idx[idx["t"] == wave].set_index("i")["v"]
        both = a.index.intersection(b.index)
        n = int(len(both))
        d = int((a.loc[both] != b.loc[both]).sum())
        assert n == m["linked_households"], (n, m["linked_households"])
        assert d == m["households_changed"], (d, m["households_changed"])
