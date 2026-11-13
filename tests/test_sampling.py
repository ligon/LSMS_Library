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
        # GH #1134 added `not-stated`, whose first record is Albania 2008; the
        # both-directions rule over every shipped record is the test just below.
        assert {r.design_tag for r in uga} == S.DESIGN_TAGS - {"not-stated"}
        assert {r.documentation_status for r in uga} == S.DOCUMENTATION_STATUSES
        assert {o["status"] for r in uga for o in (r.oversamples or [])} == \
            S.OVERSAMPLE_STATUSES

    def test_every_allowed_design_tag_is_used_by_a_shipped_record(self):
        """GH #1134: a tag is added only with the first evidenced record."""
        tags = {r.design_tag for r in _shipped() if r.design_tag is not None}
        assert tags == S.DESIGN_TAGS

    @pytest.mark.parametrize("source_type", sorted(S.NOT_STATED_SOURCE_TYPES))
    def test_not_stated_is_valid_where_no_document_states_a_design(self, source_type):
        rec = SamplingRecord.from_config("X", "2000", design(
            design_tag="not-stated", source_type=source_type, confidence="low"))
        assert rec.design_tag == "not-stated" and not rec.documented

    @pytest.mark.parametrize(
        "source_type", sorted(S.SOURCE_TYPES - S.NOT_STATED_SOURCE_TYPES))
    def test_not_stated_is_refused_under_a_document(self, source_type):
        with pytest.raises(ValueError, match="says nothing states a design"):
            SamplingRecord.from_config("X", "2000", design(
                design_tag="not-stated", source_type=source_type))

    def test_not_stated_still_needs_its_triple(self):
        """Ruling: not option C.  The tag never travels without the other two."""
        block = design(design_tag="not-stated", source_type="not-found")
        block.pop("confidence")
        with pytest.raises(ValueError, match="travel together"):
            SamplingRecord.from_config("X", "2000", block)

    def test_the_first_not_stated_record_is_albania_2008(self):
        rec = sampling_records("Albania")["2008"]
        assert (rec.design_tag, rec.source_type) == ("not-stated", "not-found")
        assert rec.design_statement is None and not rec.documented
        assert "No content available" in rec.locator

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
        # GH #1135: the measurement block's fields are frozen the same way;
        # the only exemptions are named, with reasons, in
        # `_MEASUREMENT_FIELDS_UNUSED`.
        used = {k for r in recs
                for k in ((r.get("panel") or {}).get("cluster_id_measurement")
                          or {})}
        want = set(S._MEASUREMENT_FIELDS) - set(S._MEASUREMENT_FIELDS_UNUSED)
        assert used == want, want ^ used


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
        # Liberia, since Benin gained a sampling.yml with its 2021-22 wave (GH #1697).
        object.__setattr__(f, "_countries", ["Uganda", "Liberia"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with_records = f(["Uganda", "Liberia"], currency=None)
        assert set(with_records.attrs[S.ATTRS_KEY]) == {"Uganda"}   # Liberia: none
        assert isinstance(
            with_records.attrs[S.ATTRS_KEY]["Uganda"]["2009-10"], SamplingRecord)

        monkeypatch.setattr(S, "sampling_records", lambda country: {})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            without = f(["Uganda", "Liberia"], currency=None)
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
# line of the warning (rulings @ligon 2026-10-23: print what is measured, say
# "not measured" otherwise; after the red-team review, no net-of-movers count
# for Nigeria either -- the count depends on the mover marker)
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
        n = m["linked_households"]
        assert n == 100, n
        with pytest.raises(TypeError, match="immutable"):
            m.__setitem__("linked_households", 1)

    def test_the_block_is_optional(self):
        r = SamplingRecord.from_config("A", "2", {"panel": panel()})
        assert "cluster_id_measurement" not in r["panel"]

    @pytest.mark.parametrize("kw,error", [
        ({"n_psu": 3}, r"unknown field\(s\) \['n_psu'\] in "
                       r"panel\.cluster_id_measurement"),
        ({"linked_households": True}, "non-negative integer"),
        ({"households_changed": False}, "non-negative integer"),
        ({"linked_households": 100.0}, "non-negative integer"),
        ({"linked_households": "100"}, "non-negative integer"),
        ({"households_changed": -1}, "non-negative integer"),
        ({"households_changed": 101}, "exceeds linked_households"),
        ({"households_changed_net_of_movers": 11, "mover_rule": "x"},
         "exceeds households_changed"),
        ({"households_changed": None,
          "households_changed_net_of_movers": 101, "mover_rule": "x"},
         "exceeds linked_households"),
        # the red team's K examples (GH #1135 review)
        ({"households_changed": 0, "clusters_renumbered": 3},
         "clusters_renumbered=3 exceeds households_changed=0"),
        ({"linked_households": 0, "households_changed": None,
          "clusters_renumbered": 5},
         "clusters_renumbered=5 exceeds linked_households=0"),
        ({"linked_households": 5, "households_changed": 5,
          "clusters_renumbered": 50},
         "clusters_renumbered=50 exceeds households_changed=5"),
        ({"linked_households": 5, "households_changed": None,
          "clusters_renumbered": 50},
         "clusters_renumbered=50 exceeds linked_households=5"),
        ({"households_changed_net_of_movers": 5}, "mover_rule comes with"),
        ({"mover_rule": "sentinel"}, "mover_rule comes with"),
        ({"households_changed_net_of_movers": 5, "mover_rule": 7},
         "mover_rule must be a non-empty string"),
        ({"locator": 7}, "locator must be a non-empty string"),
        ({"note": ["a"]}, "note must be a non-empty string"),
        ({"note": "  "}, "note must be a non-empty string"),
        ({"linked_households": None}, "without its denominator"),
        ({"linked_households": None, "households_changed": None,
          "clusters_renumbered": None}, "carries no count"),
        ({"source_type": None}, "travels with its own provenance"),
        ({"confidence": None}, "travels with its own provenance"),
        ({"locator": None}, "travels with its own provenance"),
        ({"source_type": "not-found", "confidence": "low"}, "OMITTING"),
        ({"source_type": "questionnaire"}, "controlled vocabulary"),
        ({"confidence": "certain"}, "controlled vocabulary"),
        ({"not_applicable": ["n_psu"]}, "not_applicable must be"),
        ({"not_applicable": []}, "not_applicable must be"),
        ({"not_applicable": "clusters_renumbered"}, "not_applicable must be"),
        ({"clusters_renumbered": None,
          "not_applicable": ["clusters_renumbered", "clusters_renumbered"]},
         "not_applicable must be"),
        ({"not_applicable": ["clusters_renumbered"]},
         "both given and not_applicable"),
    ])
    def test_a_malformed_block_is_refused(self, kw, error):
        """Each case matches the NEW error text: on a schema without the
        block every one of them fails as 'unknown field', which a loose
        match would wrongly accept."""
        with pytest.raises(ValueError, match=error):
            SamplingRecord.from_config("A", "2", with_measurement(**kw))

    @pytest.mark.parametrize("continuity", ["stable", "unobservable"])
    def test_a_block_on_a_boundary_that_never_warns_is_refused(self,
                                                               continuity):
        """A size nobody reads is a number nobody checks (red team)."""
        block = {"panel": panel(
            household_continuity="independent",
            cluster_id_continuity=continuity, cluster_id_note="x.",
            cluster_id_measurement=measured())}
        with pytest.raises(ValueError, match="sizes a boundary the warning "
                                             "fires on"):
            SamplingRecord.from_config("A", "2", block)

    def test_a_measurement_needs_a_declared_boundary(self):
        block = {"panel": {"role": "panel", "source_type": "derived-from-data",
                           "confidence": "high",
                           "cluster_id_measurement": measured()}}
        with pytest.raises(ValueError, match="sizes a boundary the warning "
                                             "fires on"):
            SamplingRecord.from_config("A", "2", block)

    def test_coherent_edge_cases_are_accepted(self):
        SamplingRecord.from_config("A", "2", with_measurement(
            households_changed=10, households_changed_net_of_movers=10,
            mover_rule="none moved"))
        SamplingRecord.from_config("A", "2", with_measurement(
            linked_households=0, households_changed=None,
            clusters_renumbered=None, not_applicable=["clusters_renumbered"]))
        SamplingRecord.from_config("A", "2", with_measurement(
            households_changed=10, clusters_renumbered=10))

    def test_every_shipped_incomparable_boundary_is_measured(self):
        """The approved port curates these 13 dated boundaries exactly.
        Other warned boundaries may remain explicitly unmeasured; their
        warning and strict policy do not depend on inventing a count."""
        import lsms_library as ll
        expected = {
            ("Burkina_Faso", "2018-19"), ("Ethiopia", "2013-14"),
            ("GhanaLSS", "1988-89"), ("Niger", "2018-19"),
            ("Nigeria", "2018Q3"), ("Nigeria", "2023Q3"),
            ("Tanzania", "2012-13"), ("Tanzania", "2014-15"),
            ("Tanzania", "2019-20"), ("Uganda", "2009-10"),
            ("Uganda", "2010-11"), ("Uganda", "2013-14"),
            ("Uganda", "2018-19"),
        }
        records = {(name, w): r for name in ll.countries()
                   for w, r in sampling_records(name).items()
                   if (r.get("panel") or {}).get("cluster_id_measurement")}
        assert set(records) == expected
        assert all(r.cluster_id_continuity in S.INCOMPARABLE_CLUSTER_IDS
                   for r in records.values())

    def test_current_unmeasured_boundary_preserves_warning_span_and_strict(
            self, monkeypatch):
        """Real current metadata, synthetic rows; no Mali data measurement."""
        import lsms_library as ll
        waves = ll.Country("Mali").waves
        record = sampling_records("Mali")["2018-19"]
        assert record.cluster_id_continuity == "redefined"
        assert "cluster_id_measurement" not in record["panel"]
        monkeypatch.delenv("LSMS_CLUSTER_STRICT", raising=False)
        for requested, in_frame in ((["2017-18", "2018-19"], True),
                                    (["2017-18", "2021-22"], False)):
            S._clear_sampling_reports()
            df = frame(requested)
            before = df.copy(deep=True)
            report = S.audit_cluster_identity(
                df, country="Mali", table="sample", waves=waves)
            (boundary,) = report["boundaries"]
            assert (boundary["from"], boundary["to"]) == ("2017-18", "2018-19")
            assert boundary["cluster_id_continuity"] == "redefined"
            assert boundary["measurement"] is None
            assert boundary["in_frame"] is in_frame
            message = S._format_cluster_report(report)
            assert "size not measured" in message.splitlines()[0]
            assert "no household is linked" not in message
            assert ("between the waves you asked for" in message.splitlines()[0]) \
                is (not in_frame)
            out, caught = warns(lambda: S.check_cluster_identity(
                df, country="Mali", table="sample", waves=waves))
            assert out is df
            pd.testing.assert_frame_equal(df, before)
            assert len(caught) == 1
            assert caught[0].category is S.ClusterIdentityWarning
            assert str(caught[0].message) == message
            assert S.sampling_reports(country="Mali", table="sample") == [report]
            with monkeypatch.context() as strict:
                strict.setenv("LSMS_CLUSTER_STRICT", "1")
                with pytest.raises(S.ClusterIdentityError) as exc:
                    S.check_cluster_identity(
                        df, country="Mali", table="sample", waves=waves)
            assert str(exc.value) == message
            pd.testing.assert_frame_equal(df, before)
            assert S.sampling_reports(country="Mali", table="sample") == [report]
        S._clear_sampling_reports()

    def test_the_unused_fields_are_named_with_reasons(self):
        """The two exempted from the used-by-a-shipped-record test."""
        unused = S._MEASUREMENT_FIELDS_UNUSED
        assert set(unused) == {"households_changed_net_of_movers",
                               "mover_rule"}
        assert set(unused) < S._MEASUREMENT_FIELDS
        assert all(isinstance(v, str) and v for v in unused.values())

    @pytest.mark.parametrize("country,wave,n,d,k,na", [
        # Dated committed boundary measurements; Nigeria follows the current
        # GH #1264 crosswalk notes, not the superseded Oct23 cover oracle.
        ("GhanaLSS", "1988-89", 712, 712, 79, None),
        ("Ethiopia", "2013-14", 3776, 3776, 333, None),
        ("Tanzania", "2012-13", 2904, 591, 82, None),
        ("Tanzania", "2014-15", 521, 108, 35, None),
        ("Tanzania", "2019-20", 849, 849, None, ["clusters_renumbered"]),
        ("Uganda", "2009-10", 2601, 191, None, ["clusters_renumbered"]),
        ("Uganda", "2010-11", 2640, 433, None, ["clusters_renumbered"]),
        ("Uganda", "2013-14", 1717, 1717, None, ["clusters_renumbered"]),
        ("Uganda", "2018-19", 2973, 2973, None, ["clusters_renumbered"]),
        ("Nigeria", "2018Q3", 1454, 54, 1, None),
        ("Nigeria", "2023Q3", 4736, 267, None, None),
        ("Burkina_Faso", "2018-19", 0, None, None, ["clusters_renumbered"]),
        ("Niger", "2018-19", 0, None, None, ["clusters_renumbered"]),
    ])
    def test_the_shipped_counts(self, country, wave, n, d, k, na):
        m = sampling_records(country)[wave]["panel"]["cluster_id_measurement"]
        got = (m.get("linked_households"), m.get("households_changed"),
               m.get("clusters_renumbered"),
               None if m.get("not_applicable") is None
               else list(m["not_applicable"]),
               m.get("households_changed_net_of_movers"),
               m.get("mover_rule"))
        want = (n, d, k, na, None, None)
        assert got == want, (got, want)

    def test_nigeria_records_the_marker_dependence_not_a_net_count(self):
        """Current crosswalk counts do not settle a mover classifier.
        Preserve the Oct23 not-measured ruling without requiring its
        superseded cover counts or conditional bounds as current evidence."""
        recs = sampling_records("Nigeria")
        for wave in ("2018Q3", "2023Q3"):
            m = recs[wave]["panel"]["cluster_id_measurement"]
            assert "households_changed_net_of_movers" not in m, wave
            assert "mover_rule" not in m, wave
            note = m["note"]
            assert any(word in note.casefold() for word in
                       ("not measured", "unknown")), wave
            # Current provenance is the already-committed #1264 crosswalk
            # measurement (dated 2026-11-04), not the old cover oracle.
            assert "2026-11-04" in m["locator"], wave
        # Neither withdrawn nor conditional historical bounds are a current
        # net count; both remain excluded from warning text below.
        note = recs["2023Q3"]["panel"]["cluster_id_measurement"]["note"]
        assert "86..106" not in note


class TestTheLoaderRefusesNonCanonicalIntegers:
    """PyYAML 1.1 reads `012` as 10; a hand-copied count must not."""

    BODY = """Sampling:
  '2':
    panel:
      household_continuity: panel
      cluster_id_continuity: wave-scoped
      cluster_id_note: renumbered.
      source_type: derived-from-data
      confidence: high
      locator: measured
      cluster_id_measurement:
        linked_households: {n}
        source_type: derived-from-data
        confidence: high
        locator: measured
"""

    def _load(self, tmp_path, monkeypatch, n):
        f = tmp_path / "sampling.yml"
        f.write_text(self.BODY.format(n=n))
        monkeypatch.setattr(S, "_sampling_path", lambda country: f)
        S.sampling_records.cache_clear()
        try:
            return S.sampling_records("Fake1135")
        finally:
            S.sampling_records.cache_clear()

    # ("0o12" and "1e3" are strings to YAML 1.1: refused as non-integers.)
    @pytest.mark.parametrize("spelling", ["012", "0x10", "1_493", "1:30",
                                          "+5", "0b11"])
    def test_a_non_decimal_spelling_is_refused(self, tmp_path, monkeypatch,
                                               spelling):
        with pytest.raises(ValueError, match="not plain decimal"):
            self._load(tmp_path, monkeypatch, spelling)

    def test_plain_decimal_loads(self, tmp_path, monkeypatch):
        recs = self._load(tmp_path, monkeypatch, "1493")
        n = recs["2"]["panel"]["cluster_id_measurement"]["linked_households"]
        assert n == 1493, n


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


def _fragment_message(measurement, in_frame=True):
    """Line 1 for one synthetic boundary carrying *measurement*."""
    rep = {"country": "Fake", "table": "t", "waves": ["w1", "w2"],
           "boundaries": [{
               "from": "w1", "to": "w2", "cluster_id_continuity": "wave-scoped",
               "household_continuity": "panel",
               "source_type": "derived-from-data", "confidence": "high",
               "locator": "x", "note": "renumbered.", "in_frame": in_frame,
               "measurement": measurement}]}
    return S._format_cluster_report(rep).splitlines()[0]


class TestTheFirstLine:

    def test_nigeria_w3_w4_states_its_size_as_ruled(self):
        """Current #1264 note: 54 of 1,454 counting movers; the approved
        first-line contract still labels net-of-movers as not measured."""
        _, msg = _shipped_message("Nigeria", ["2016Q1", "2018Q3"])
        first = msg.splitlines()[0]
        want = ("2016Q1 -> 2018Q3: 1 cluster id renumbered; 54 of 1,454 linked "
                "households (3.71%) change `v` counting movers; net-of-movers "
                "share: not measured")
        ok = want in first
        assert ok, "line 1 lacks the ruled 2018Q3 size"
        ok = "0.67%" not in first and "10 of 1,493" not in first
        assert ok, "the withdrawn sentinel figure is on line 1"
        assert "79 of 1,493" not in first, "obsolete pre-#1264 size is on line 1"
        ok = "between the waves" not in first
        assert ok, "an in-frame boundary is qualified as between"

    def test_a_100_percent_boundary_states_its_size(self):
        _, msg = _shipped_message("GhanaLSS", ["1987-88", "1988-89"])
        first = msg.splitlines()[0]
        want = ("1987-88 -> 1988-89: 79 cluster ids renumbered; 712 of 712 "
                "linked households (100%) change `v` counting movers; "
                "net-of-movers share: not measured")
        ok = want in first
        assert ok, "line 1 lacks the GhanaLSS size"

    def test_no_conditional_or_sentinel_figure_reaches_the_message(
            self, monkeypatch):
        """Neither the withdrawn 86..106 nor the corrected 226..249 is
        settled, so neither may be printed -- on any line, strict or not --
        and the sentinel-only 112 stays off line 1.  The positive claim
        ('not measured') makes this fail on a formatter that prints no
        sizes at all."""
        import lsms_library as ll
        waves = ll.Country("Nigeria").waves
        bad = re.compile(r"\b(86|106|226|249|50\.\.56)\b")
        size = ("cluster ids renumbered: not measured; 267 of 4,736 linked "
                "households (5.64%) change `v` counting movers; net-of-movers "
                "share: not measured")
        for pair, qual in ((["2019Q1", "2023Q3"], ""),
                           (["2016Q1", "2024Q1"],
                            " (between the waves you asked for)")):
            _, msg = _shipped_message("Nigeria", pair)
            first = msg.splitlines()[0]
            want = f"2019Q1 -> 2023Q3{qual}: {size}"
            ok = want in first
            assert ok, f"line 1 lacks the 2023Q3 size for {pair}"
            hit = bad.search(msg)
            ok = hit is None
            assert ok, f"unsettled figure {hit and hit.group(0)}"
            ok = re.search(r"\b112\b", first) is None
            assert ok, "112 is on line 1"
            assert "11 cluster ids renumbered" not in first, "obsolete K is on line 1"
            assert "343 of 5,067" not in first, "obsolete pre-#1264 size is on line 1"
            df = pd.DataFrame({"t": pair, "v": "x", "i": "1", "x": 1.0}
                              ).set_index(["t", "v", "i"])
            monkeypatch.setenv("LSMS_CLUSTER_STRICT", "1")
            with pytest.raises(S.ClusterIdentityError) as exc:
                S.check_cluster_identity(df, country="Nigeria",
                                         table="sample", waves=waves)
            monkeypatch.delenv("LSMS_CLUSTER_STRICT")
            refused = str(exc.value)
            ok = want in refused.splitlines()[0]
            assert ok, "strict text differs"
            ok = bad.search(refused) is None
            assert ok, "unsettled figure (strict)"

    def test_every_flagged_boundary_is_on_the_one_first_line(self):
        rep, msg = _shipped_message("Uganda", ["2005-06", "2019-20"])
        first = msg.splitlines()[0]
        n = len(rep["boundaries"])
        for b in rep["boundaries"]:
            ok = f"{b['from']} -> {b['to']}" in first
            assert ok, b["to"]
        arrows, bars = first.count(" -> "), first.count(" | ")
        assert (arrows, bars, n) == (4, 3, 4), (arrows, bars, n)

    def test_not_applicable_is_not_not_measured(self):
        _, msg = _shipped_message("Uganda", ["2011-12", "2013-14"])
        first = msg.splitlines()[0]
        ok = ("2011-12 -> 2013-14: cluster ids renumbered: not applicable; "
              "1,717 of 1,717 linked households (100%) change `v` counting "
              "movers") in first
        assert ok, "line 1 lacks Uganda's not-applicable"

    def test_no_household_link_is_said_not_divided(self):
        _, msg = _shipped_message("Burkina_Faso", ["2014", "2018-19"])
        first = msg.splitlines()[0]
        ok = ("2014 -> 2018-19: cluster ids renumbered: not applicable; no "
              "household is linked across it") in first
        assert ok, "line 1 lacks the Burkina Faso size"

    def test_a_boundary_between_the_requested_waves_says_so_on_line_1(self):
        """SPAN over-warns (#1135 H2): Tanzania ['2008-09', '2014-15'] keeps
        `v` for every linked household, yet crosses two renumberings."""
        _, msg = _shipped_message("Tanzania", ["2008-09", "2014-15"])
        first = msg.splitlines()[0]
        for b in ("2010-11 -> 2012-13", "2012-13 -> 2014-15"):
            ok = f"{b} (between the waves you asked for): " in first
            assert ok, b

    @pytest.mark.parametrize("measurement,want,never", [
        # absent counts are "not measured", never 0 / "no household"
        ({"clusters_renumbered": 2}, "w1 -> w2: 2 cluster ids renumbered; "
         "linked households: not measured", "no household"),
        ({"linked_households": 50}, "cluster ids renumbered: not measured; "
         "households changing `v`: not measured; net-of-movers share: not "
         "measured", "0 of 50"),
        ({"linked_households": 100, "households_changed": 10,
          "households_changed_net_of_movers": 5, "mover_rule": "r"},
         "10 of 100 linked households (10%) change `v` counting movers; "
         "net-of-movers share: 5 of 100 (5%)",
         "net-of-movers share: not measured"),
        ({"linked_households": 0, "not_applicable": ["clusters_renumbered"]},
         "cluster ids renumbered: not applicable; no household is linked",
         "0 of 0"),
        (None, "w1 -> w2: size not measured", "renumbered"),
    ])
    def test_each_state_prints_as_itself(self, measurement, want, never):
        first = _fragment_message(measurement)
        ok = want in first
        assert ok, f"missing: {want[:40]}"
        ok = never not in first
        assert ok, f"present: {never}"

    def test_an_unmeasured_boundary_says_not_measured(self, declared):
        _, caught = warns(lambda: S.check_cluster_identity(
            frame(["w1", "w2"]), country="Fake", table="t", waves=WAVES))
        first = str(caught[0].message).splitlines()[0]
        ok = "w1 -> w2: size not measured" in first
        assert ok, "no 'size not measured'"

    def test_the_report_carries_the_measurement_whole(self):
        """`sampling_reports()` consumers must never parse the message."""
        rep, _ = _shipped_message("Nigeria", ["2016Q1", "2018Q3"])
        (b,) = rep["boundaries"]
        same = b["measurement"] == sampling_records("Nigeria")["2018Q3"][
            "panel"]["cluster_id_measurement"]
        assert same, "report measurement differs from the record"
        d = b["measurement"]["households_changed"]
        assert d == 54, d

    def test_the_measurement_provenance_is_on_the_detail_line(self):
        _, msg = _shipped_message("Nigeria", ["2016Q1", "2018Q3"])
        detail = msg.splitlines()[1]
        ok = "size: derived-from-data, confidence high" in detail
        assert ok, "detail line lacks the size provenance"

    @pytest.mark.parametrize("num,den,want", [
        (10, 1493, "0.67%"), (79, 1493, "5.29%"), (712, 712, "100%"),
        (0, 5, "0%"), (591, 2904, "20.35%"), (99999, 100000, ">99.99%"),
        (1, 100000, "<0.01%"),
    ])
    def test_a_partial_share_is_never_rounded_to_0_or_100(self, num, den,
                                                          want):
        got = S._share(num, den)
        assert got == want, (got, want)


# -- the shipped declarations, and the reproduction --------------------------

class TestShippedDeclarations:

    @pytest.mark.parametrize("country,wave,value", [
        ("Niger", "2014-15", "stable"),              # ECVMA panel keeps GRAPPE
        ("Niger", "2018-19", "redefined"),           # ECVMA / EHCVM serials overlap
        ("Niger", "2021-22", "stable"),              # s00q07f1 == grappe
        ("Togo", "2021-22", "stable"),               # one point per grappe, = 2018's fix
        ("Benin", "2021-22", "stable"),              # one point per grappe, = 2018-19's fix
        ("CotedIvoire", "2021-22", "stable"),        # one point per grappe, = 2018-19's fix
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
        ("Mali", "2017-18", "stable"),               # same EAs, identical fixes
        ("Mali", "2018-19", "redefined"),            # EAC-I / EHCVM serials overlap
        ("Mali", "2021-22", "stable"),               # same EAs, identical fixes
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


def _nigeria_linked_cluster_operands(a, b, earlier, wave, links_path):
    """Pair GH #1448's committed interviewed links (ledger 1687 §2–4)."""
    import json

    # Retain duplicate JSON keys so a repeated current endpoint cannot hide.
    links = json.loads(links_path.read_text(encoding="utf-8"),
                       object_pairs_hook=list)
    pairs = []
    for key, target in links:
        t, i = key.split(",")
        previous, previous_i = target.split(",")
        if t == wave:
            assert previous == earlier, (wave, previous, earlier)
            pairs.append((previous_i, i))
    assert pairs, (earlier, wave)
    previous_ids, current_ids = zip(*pairs)
    assert len(previous_ids) == len(set(previous_ids)), "duplicate previous endpoint"
    assert len(current_ids) == len(set(current_ids)), "duplicate current endpoint"
    assert a.index.is_unique and b.index.is_unique, "duplicate sample endpoint"
    # .loc refuses absent endpoints; null v must also fail, never shrink N.
    left, right = a.loc[list(previous_ids)], b.loc[list(current_ids)]
    assert left.notna().all() and right.notna().all(), "null linked v"
    left = left.set_axis(right.index.copy())
    pd.testing.assert_index_equal(left.index, right.index, exact=True)
    return left, right


def _uganda_historical_cluster_view(sample):
    """Undo only registered #1120 additions for the dated #1135 TEST operand.

    This never changes the public sample or invents an eligibility rule. The
    exact source-defined inverse reproduced all four historical N/D pairs on
    the closed current output (ledger 1687 Uganda §2–4). Ordinary EA, GPS-bin
    and mover labels remain; only the historical comparison view is selected.
    """
    from lsms_library.derivations import MULTI_SEP, parse_key

    early = {"2005-06", "2009-10", "2010-11", "2011-12"}
    late = {"2013-14", "2015-16", "2018-19", "2019-20"}
    unlocated = "Uganda::sample::unlocated-identity"
    agriculture = "Uganda::sample::agriculture-cover"
    scopes = {
        unlocated: early | late,
        agriculture: {"2015-16"},
        "Uganda::sample::ea-comm-link": early,
        "Uganda::sample::ea-own-comm": early,
        "Uganda::sample::ea-tracking-key": {"2018-19", "2019-20"},
        # GH #1536 (2026-11-12): an `ea` fill; it never touches `v`.
        "Uganda::sample::ea-same-household": {"2009-10"},
    }
    assert isinstance(sample, pd.DataFrame)
    assert list(sample.index.names) == ["i", "t"], "historical operand index"
    assert sample.index.is_unique, "duplicate current household/wave"
    assert {"v", "Derivation"} <= set(sample), "missing historical lineage"
    keep = []
    for wave, value, marker in zip(sample.index.get_level_values("t"),
                                   sample["v"], sample["Derivation"]):
        wave = str(wave)
        assert wave in early | late, "unsupported historical wave"
        if isinstance(marker, str):
            keys = [part.strip() for part in marker.split(MULTI_SEP)]
            assert all(keys), "empty derivation token"
            assert len(keys) == len(set(keys)), "duplicate derivation token"
        else:
            assert pd.api.types.is_scalar(marker) and pd.isna(marker), (
                "malformed derivation marker")
            keys = []
        for key in keys:
            parse_key(key)  # Existing registered-key syntax, not a new parser.
            assert key in scopes, "unknown historical derivation"
            assert wave in scopes[key], "mis-scoped historical derivation"
        is_unlocated = isinstance(value, str) and value.startswith("unlocated-")
        assert (unlocated in keys) == is_unlocated, "unlocated marker/v mismatch"
        keep.append(not (unlocated in keys or
                         (wave == "2015-16" and agriculture in keys)))
    return sample.loc[pd.Series(keep, index=sample.index, dtype=bool)].copy(deep=True)


def _ethiopia_w2_links(path, *, historical=False):
    """Read reviewed W2 links, retaining duplicate-key refusal (ledger §2–4)."""
    import hashlib
    import json

    if historical:
        # Full public fbc213417 blob, unchanged through pre-D1 cc26ce13.
        assert hashlib.sha256(path.read_bytes()).hexdigest() == (
            "52cc2e73e48b800e406953bafd76a9722579b39c3499667dc45a366e08fb8023"
        ), "historical Ethiopia fixture SHA256"

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            assert key not in result, "duplicate Ethiopia map key"
            result[key] = value
        return result

    maps = json.loads(path.read_text(encoding="utf-8"),
                      object_pairs_hook=unique_object)
    assert isinstance(maps, dict) and "2013-14" in maps, "missing W2 map"
    return maps["2013-14"]


def _ethiopia_historical_cluster_view(sample, earlier, wave, old, current):
    """Select the dated HH-link operand, preserving the full API frame (§3–4).

    Membership is the RANGE of the reviewed old W2 map, never a count fitted
    to current rows. Current PP-only links remain public; their extra edges
    must not change the historical N/D/K measurement.
    """
    from tests.test_gh1196_parquet_index_policy import _exact

    assert (earlier, wave) == ("2011-12", "2013-14"), "historical Ethiopia boundary"
    for links in (old, current):
        assert isinstance(links, dict) and links, "empty Ethiopia links"
        assert all(isinstance(k, str) and k.strip() and
                   isinstance(v, str) and v.strip() for k, v in links.items()), (
            "invalid Ethiopia link endpoint")
        assert len(links) == len(set(links.values())), "noninjective Ethiopia links"
    assert old.keys() <= current.keys(), "missing old Ethiopia link"
    assert all(current[k] == v for k, v in old.items()), "changed old Ethiopia link"
    assert isinstance(sample, pd.DataFrame)
    assert list(sample.index.names) == ["i", "t"], "historical operand index"
    assert sample.index.is_unique, "duplicate current household/wave"
    assert sample.columns.is_unique and "v" in sample, "missing or duplicate v"
    assert not sample.index.to_frame(index=False).isna().any().any(), "null sample key"
    assert set(sample.index.get_level_values("t")) == {earlier, wave}, "incorrect sample waves"
    before = copy.deepcopy(sample)
    view = sample.loc[sample.index.get_level_values("i").isin(old.values())].copy(deep=True)
    _exact(sample, before)  # Complete current records, weights, masks and typed attrs.
    return view


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
        (2005-06 -> 2009-10, `mixed`: synthetic GPS bins) still warns.  #1358
        (GH #1120 D2) added 30 linked households that 2011-12 knows only from
        AGSEC1, as `unlocated-` identities: not clusters, so not compared."""
        S._clear_sampling_reports()
        s, caught = warns(lambda: self._fresh("Uganda").sample(
            waves=["2010-11", "2011-12"]))
        assert caught == [], [str(w.message)[:200] for w in caught]
        idx = s.reset_index()[["i", "t", "v"]].dropna()
        wide = idx.pivot_table(index="i", columns="t", values="v",
                               aggfunc="first").dropna()
        assert not wide["2010-11"].str.startswith("unlocated-").any()
        located = wide[~wide["2011-12"].str.startswith("unlocated-")]
        assert len(wide) - len(located) == 30
        assert (located["2010-11"] == located["2011-12"]).sum() == len(located) == 2510
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
        want = ("2016Q1 -> 2018Q3: 1 cluster id renumbered; 54 of 1,454 linked "
                "households (3.71%) change `v` counting movers; net-of-movers "
                "share: not measured")
        ok = want in first
        assert ok, "served call lacks the ruled 2018Q3 size"

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
        if country == "Uganda":
            # The four curated records precede locating-cover/singleton additions.
            s = _uganda_historical_cluster_view(s)
        elif country == "Ethiopia":
            from pathlib import Path
            from lsms_library.paths import countries_root

            s = _ethiopia_historical_cluster_view(
                s, earlier, wave,
                _ethiopia_w2_links(Path(__file__).parent / "fixtures" /
                                   "ethiopia_updated_ids_fbc213417.json", historical=True),
                _ethiopia_w2_links(countries_root() / "Ethiopia" / "_" / "updated_ids.json"))
        idx = s.reset_index()[["i", "t", "v"]].dropna()
        idx["t"] = idx["t"].astype(str)
        idx["v"] = idx["v"].astype(str)
        dup = int(idx.duplicated(["i", "t"]).sum())
        assert dup == 0, dup
        a = idx[idx["t"] == earlier].set_index("i")["v"]
        b = idx[idx["t"] == wave].set_index("i")["v"]
        if country == "Nigeria":
            # GH #1448 counts interviewed links, not all shared served IDs.
            from lsms_library.paths import countries_root
            a, b = _nigeria_linked_cluster_operands(
                a, b, earlier, wave,
                countries_root() / "Nigeria" / "_" / "panel_ids.json")
            both = b.index
        else:
            both = a.index.intersection(b.index)
            a, b = a.loc[both], b.loc[both]
        n = int(len(both))
        d = int((a != b).sum())
        assert n == m["linked_households"], (n, m["linked_households"])
        assert d == m["households_changed"], (d, m["households_changed"])
        # clusters_renumbered, by the docstring's definition: earlier real
        # ids among changed households, all of whose linked households go to
        # one single different `v` that no other earlier id receives.
        if m.get("clusters_renumbered") is None:
            return
        syn = ("moved-", "@")
        changed_real = (a != b) & ~a.str.startswith(syn) & ~b.str.startswith(syn)
        senders = b.groupby(b).apply(lambda g: a.loc[g.index].nunique())
        k = 0
        for e in a[changed_real].unique():
            dest = set(b[a == e])
            if len(dest) == 1:
                f = next(iter(dest))
                k += int(f != e and senders[f] == 1)
        assert k == m["clusters_renumbered"], (k, m["clusters_renumbered"])


@pytest.mark.parametrize("earlier,wave", [
    ("2016Q1", "2018Q3"), ("2019Q1", "2023Q3"),
])
class TestNigeriaLinkedClusterOperands:
    """Invented endpoint controls; no Country/API or survey data (ledger §4–5)."""

    def _write_links(self, path, earlier, wave, pairs):
        import json

        path.parent.mkdir(parents=True, exist_ok=True)
        # A list of pairs deliberately permits duplicate JSON keys.
        path.write_text("{" + ",".join(
            json.dumps(f"{wave},{i}") + ":" + json.dumps(f"{earlier},{old}")
            for old, i in pairs) + "}", encoding="utf-8")
        return path

    def _exercise_original_assertions(self, tmp_path, monkeypatch, earlier,
                                      wave, a, b, pairs, measurement):
        """Exercise the unchanged N/D/K body with explicitly synthetic operands."""
        from types import SimpleNamespace
        import lsms_library.paths as paths

        root = tmp_path / "countries"
        self._write_links(root / "Nigeria" / "_" / "panel_ids.json",
                          earlier, wave, pairs)
        monkeypatch.setattr(paths, "countries_root", lambda: root)
        monkeypatch.setattr(sys.modules[__name__], "sampling_records", lambda _: {
            wave: {"panel": {"cluster_id_measurement": measurement}}})
        frame = pd.concat([
            a.rename_axis("i").reset_index().assign(t=earlier),
            b.rename_axis("i").reset_index().assign(t=wave),
        ]).assign(weight=2.5, untouched="payload").set_index(["i", "t"])
        frame.attrs = {"synthetic": {"unchanged": [1, 2]}}
        before = frame.copy(deep=True)
        before.attrs = copy.deepcopy(frame.attrs)
        check = TestTheReproduction()
        monkeypatch.setattr(check, "_fresh", lambda _: SimpleNamespace(
            sample=lambda waves: frame))
        check.test_the_curated_size_matches_the_served_sample("Nigeria", earlier, wave)
        pd.testing.assert_frame_equal(frame, before, check_exact=True)
        assert frame.attrs == before.attrs

    def test_shared_unlinked_and_same_n_membership_mutation(
            self, tmp_path, monkeypatch, earlier, wave):
        a = pd.Series(["A", "C", "E"], index=["h1", "h2", "extra"], name="v")
        b = pd.Series(["B", "C", "F"], index=a.index.copy(), name="v")
        m = dict(linked_households=2, households_changed=1, clusters_renumbered=1)
        self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
                                           a, b, [("h1", "h1"), ("h2", "h2")], m)
        # Same N, different members: the original D assertion must notice.
        with pytest.raises(AssertionError, match=r"\(2, 1\)"):
            self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
                a, b, [("h1", "h1"), ("extra", "extra")], m)
        changed = b.copy()
        changed.loc["h2"] = "D"
        with pytest.raises(AssertionError, match=r"\(2, 1\)"):
            self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
                a, changed, [("h1", "h1"), ("h2", "h2")], m)
        # Wrong N also reaches the original numerical assertion.
        with pytest.raises(AssertionError, match=r"\(2, 3\)"):
            self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
                a, b, [("h1", "h1"), ("h2", "h2")], dict(m, linked_households=3))

    def test_nonidentity_pairing(self, tmp_path, monkeypatch, earlier, wave):
        a = pd.Series(["Q", "X", "P"], index=["old-b", "shared", "old-a"], name="v")
        b = pd.Series(["P", "Y", "R"], index=["new-a", "shared", "new-b"], name="v")
        pairs = [("old-b", "new-b"), ("old-a", "new-a")]
        path = self._write_links(tmp_path / "links.json", earlier, wave, pairs)
        left, right = _nigeria_linked_cluster_operands(a, b, earlier, wave, path)
        pd.testing.assert_series_equal(left, pd.Series(
            ["Q", "P"], index=["new-b", "new-a"], name="v"))
        pd.testing.assert_series_equal(right, b.loc[["new-b", "new-a"]])
        self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
            a, b, pairs, dict(linked_households=2, households_changed=1,
                             clusters_renumbered=1))

    @pytest.mark.parametrize("bad", ["predecessor", "current_round"])
    def test_wrong_declared_round_refuses(self, tmp_path, earlier, wave, bad):
        a = pd.Series(["A"], index=["h"], name="v")
        path = self._write_links(tmp_path / "links.json",
            "wrong" if bad == "predecessor" else earlier,
            "wrong" if bad == "current_round" else wave, [("h", "h")])
        with pytest.raises(AssertionError):
            _nigeria_linked_cluster_operands(a, a, earlier, wave, path)
        self._write_links(path, earlier, wave, [("h", "h")])
        left, right = _nigeria_linked_cluster_operands(a, a, earlier, wave, path)
        pd.testing.assert_series_equal(left, right)

    @pytest.mark.parametrize("side", ["previous", "current"])
    def test_duplicate_endpoint_refuses(self, tmp_path, earlier, wave, side):
        a = pd.Series(["A", "B"], index=["h1", "h2"], name="v")
        pairs = [("h1", "h1"),
                 ("h1", "h2") if side == "previous" else ("h2", "h1")]
        path = self._write_links(tmp_path / "links.json", earlier, wave, pairs)
        with pytest.raises(AssertionError, match=f"duplicate {side} endpoint"):
            _nigeria_linked_cluster_operands(a, a, earlier, wave, path)
        self._write_links(path, earlier, wave, [("h1", "h1"), ("h2", "h2")])
        left, right = _nigeria_linked_cluster_operands(a, a, earlier, wave, path)
        pd.testing.assert_series_equal(left, right)

    @pytest.mark.parametrize("side", ["previous", "current"])
    @pytest.mark.parametrize("bad", ["absent", "null"])
    def test_absent_or_null_v_refuses(self, tmp_path, earlier, wave, side, bad):
        a = pd.Series(["A", "B"], index=["h1", "h2"], name="v", dtype="string")
        b = a.copy()
        path = self._write_links(tmp_path / "links.json", earlier, wave,
                                 [("h1", "h1"), ("h2", "h2")])
        broken = a.drop("h2") if bad == "absent" else a.mask(a.index == "h2")
        left, right = (broken, b) if side == "previous" else (a, broken)
        with pytest.raises(KeyError if bad == "absent" else AssertionError):
            _nigeria_linked_cluster_operands(left, right, earlier, wave, path)
        left, right = _nigeria_linked_cluster_operands(a, b, earlier, wave, path)
        pd.testing.assert_series_equal(left, right)

    def test_typed_index_name_and_input_immutability(self, tmp_path, earlier, wave):
        a = pd.Series(["A", "B"], index=pd.Index(
            ["old2", "old1"], dtype="string", name="previous-household"),
            name="v", dtype="string")
        b = pd.Series(["B", "C"], index=pd.Index(
            ["new1", "new2"], dtype="string", name="current-household"),
            name="v", dtype="string")
        a.attrs, b.attrs = {"side": ["left"]}, {"side": ["right"]}
        before_a, before_b = copy.deepcopy(a), copy.deepcopy(b)
        path = self._write_links(tmp_path / "links.json", earlier, wave,
                                 [("old2", "new2"), ("old1", "new1")])
        left, right = _nigeria_linked_cluster_operands(a, b, earlier, wave, path)
        pd.testing.assert_index_equal(left.index, b.loc[["new2", "new1"]].index,
                                      exact=True)
        pd.testing.assert_index_equal(left.index, right.index, exact=True)
        pd.testing.assert_series_equal(left, before_a.set_axis(right.index))
        pd.testing.assert_series_equal(a, before_a, check_exact=True)
        pd.testing.assert_series_equal(b, before_b, check_exact=True)
        assert a.attrs == before_a.attrs and b.attrs == before_b.attrs

    @pytest.mark.parametrize("destination,k", [
        (["B", "B", "D"], 2),       # two clean one-to-one changes
        (["B", "X", "D"], 1),       # A splits: only C -> D qualifies
        (["B", "B", "B"], 0),       # A and C merge
        (["moved-h1", "moved-h2", "D"], 1),  # synthetic endpoints excluded
    ])
    def test_original_k_split_merge_assertion(self, tmp_path, monkeypatch,
                                              earlier, wave, destination, k):
        a = pd.Series(["A", "A", "C"], index=["h1", "h2", "h3"], name="v")
        b = pd.Series(destination, index=a.index.copy(), name="v")
        pairs = [(i, i) for i in a.index]
        m = dict(linked_households=3, households_changed=3, clusters_renumbered=k)
        self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
                                           a, b, pairs, m)
        with pytest.raises(AssertionError, match=rf"\({k}, {k + 1}\)"):
            self._exercise_original_assertions(tmp_path, monkeypatch, earlier, wave,
                                               a, b, pairs, dict(m, clusters_renumbered=k + 1))


class TestUgandaHistoricalClusterView:
    """Invented source-lineage controls; no sample/API/provider calls (§4–5)."""

    U = "Uganda::sample::unlocated-identity"
    A = "Uganda::sample::agriculture-cover"
    E = "Uganda::sample::ea-tracking-key"

    def _frame(self):
        ids = ["h1", "h2", "ag", "blank", "h1", "h2", "ag", "blank", "late"]
        waves = ["2015-16"] * 4 + ["2018-19"] * 5
        index = pd.MultiIndex.from_arrays([
            pd.array(ids, dtype="string[pyarrow]"),
            pd.array(waves, dtype="string[pyarrow]"),
        ], names=["i", "t"])
        # The original unused domains must survive too, not just observed keys.
        index = index.set_levels([
            level.insert(len(level), "unused-i" if n == 0 else "unused-t")
            for n, level in enumerate(index.levels)])
        frame = pd.DataFrame({
            "v": pd.array(["A", "P", "Q", "unlocated-blank", "B", "P", "T",
                           "R", "unlocated-late"], dtype="string[pyarrow]"),
            "Derivation": pd.array([None, None, self.A, self.U, self.E, None,
                                    None, None, self.U], dtype="string[pyarrow]"),
            "weight": pd.array([1., None, 5., 0., 2., 3., 4., 5., 6.], dtype="Float64"),
            "panel_weight": pd.array([0., 1., None, 3., 4., 5., 6., 7., 8.], dtype="Float64"),
            "untouched": pd.array(["None", "nan", None, "<NA>"] * 2 + ["last"],
                                  dtype="string[pyarrow]"),
        }, index=index)
        frame.attrs = {"id_converted": True, "nested": {
            "typed": [1, 1.0, True], "unknown": None, "literal": "nan"}}
        return frame

    def _one(self, wave, value, marker):
        return pd.DataFrame({"v": [value], "Derivation": [marker],
                             "weight": pd.array([1.], dtype="Float64")},
                            index=pd.MultiIndex.from_tuples(
                                [("h", wave)], names=["i", "t"]))

    def _original_assertions(self, frame, monkeypatch, n=2, d=1):
        from types import SimpleNamespace
        from tests.test_gh1196_parquet_index_policy import _exact

        before = copy.deepcopy(frame)
        monkeypatch.setattr(sys.modules[__name__], "sampling_records", lambda _: {
            "2018-19": {"panel": {"cluster_id_measurement": {
                "linked_households": n, "households_changed": d,
                "not_applicable": ["clusters_renumbered"],
            }}}})
        check = TestTheReproduction()
        monkeypatch.setattr(check, "_fresh", lambda _: SimpleNamespace(
            sample=lambda waves: frame))
        try:
            check.test_the_curated_size_matches_the_served_sample(
                "Uganda", "2015-16", "2018-19")
        finally:
            _exact(frame, before)

    def test_source_view_retains_exact_expected_members_and_full_current_frame(self):
        from tests.test_gh1196_parquet_index_policy import _exact

        frame = self._frame()
        before = copy.deepcopy(frame)
        expected = frame.iloc[[0, 1, 4, 5, 6, 7]].copy(deep=True)
        got = _uganda_historical_cluster_view(frame)
        _exact(got, expected)
        _exact(frame, before)
        # Later observed counterparts survive: this is a per-wave source view,
        # not a global blacklist of household IDs or null/zero weights.
        assert ("ag", "2018-19") in got.index
        assert ("blank", "2018-19") in got.index
        assert pd.isna(got.loc[("h2", "2015-16"), "weight"])

    def test_original_exact_assertions_and_same_total_wrong_members(self, monkeypatch):
        frame = self._frame()
        self._original_assertions(frame, monkeypatch)
        # Same six view rows and same N, but swap a historical member for the
        # AG-only member: the independent unchanged D assertion must fail.
        wrong = copy.deepcopy(frame)
        wrong.loc[("h2", "2015-16"), "Derivation"] = self.A
        wrong.loc[("ag", "2015-16"), "Derivation"] = pd.NA
        assert len(_uganda_historical_cluster_view(wrong)) == 6
        with pytest.raises(AssertionError, match=r"\(2, 1\)"):
            self._original_assertions(wrong, monkeypatch)

    def test_original_N_assertion_is_not_bypassed(self, monkeypatch):
        with pytest.raises(AssertionError, match=r"\(2, 3\)"):
            self._original_assertions(self._frame(), monkeypatch, n=3)

    @pytest.mark.parametrize("value", ["@1.25,2.5", "moved-h", "000123"])
    def test_bins_movers_and_ordinary_labels_remain(self, value):
        from tests.test_gh1196_parquet_index_policy import _exact

        frame = self._one("2018-19", value, None)
        _exact(_uganda_historical_cluster_view(frame), frame)

    @pytest.mark.parametrize("wave,key", [
        ("2005-06", "Uganda::sample::ea-comm-link"),
        ("2011-12", "Uganda::sample::ea-own-comm"),
        ("2018-19", "Uganda::sample::ea-tracking-key"),
        ("2019-20", "Uganda::sample::ea-tracking-key"),
    ])
    def test_separate_EA_annotations_are_kept(self, wave, key):
        from tests.test_gh1196_parquet_index_policy import _exact

        frame = self._one(wave, "v", key)
        _exact(_uganda_historical_cluster_view(frame), frame)

    def test_composed_registered_keys_still_disclose_missing_original_v(self):
        from lsms_library.derivations import MULTI_SEP
        from tests.test_gh1196_parquet_index_policy import _exact

        frame = self._one("2019-20", "unlocated-h", self.U + MULTI_SEP + self.E)
        before = copy.deepcopy(frame)
        _exact(_uganda_historical_cluster_view(frame), frame.iloc[:0])
        _exact(frame, before)

    @pytest.mark.parametrize("marker", [
        "", "bad", "Uganda::sample::unknown", "Other::sample::unlocated-identity",
        "Uganda::cluster_features::unlocated-identity", 17, False, ["bad"],
        {"key": "bad"},
    ])
    def test_wrong_or_malformed_marker_refuses(self, marker):
        frame = self._one("2018-19", "v", None)
        frame["Derivation"] = frame["Derivation"].astype(object)
        frame.iat[0, frame.columns.get_loc("Derivation")] = marker
        with pytest.raises((AssertionError, ValueError)):
            _uganda_historical_cluster_view(frame)

    @pytest.mark.parametrize("value,marker", [
        ("unlocated-h", None), ("unlocated-h", "Uganda::sample::ea-tracking-key"),
        ("v", "Uganda::sample::unlocated-identity"),
        ("@1,2", "Uganda::sample::unlocated-identity"),
        ("moved-h", "Uganda::sample::unlocated-identity"),
        (None, "Uganda::sample::unlocated-identity"),
    ])
    def test_missing_or_inconsistent_unlocated_marker_refuses(self, value, marker):
        with pytest.raises(AssertionError, match="unlocated marker/v mismatch"):
            _uganda_historical_cluster_view(self._one("2018-19", value, marker))

    @pytest.mark.parametrize("wave,key", [
        ("2018-19", "Uganda::sample::agriculture-cover"),
        ("2015-16", "Uganda::sample::ea-comm-link"),
        ("2013-14", "Uganda::sample::ea-own-comm"),
        ("2015-16", "Uganda::sample::ea-tracking-key"),
    ])
    def test_known_but_mis_scoped_keys_refuse(self, wave, key):
        with pytest.raises(AssertionError, match="mis-scoped"):
            _uganda_historical_cluster_view(self._one(wave, "v", key))

    @pytest.mark.parametrize("key,value", [
        ("Uganda::sample::unlocated-identity", "unlocated-h"),
        ("Uganda::sample::agriculture-cover", "v"),
    ])
    def test_duplicate_tokens_are_not_silently_deduplicated(self, key, value):
        from lsms_library.derivations import MULTI_SEP

        frame = self._one("2015-16", value, key + MULTI_SEP + " " + key)
        with pytest.raises(AssertionError, match="duplicate derivation token"):
            _uganda_historical_cluster_view(frame)

    @pytest.mark.parametrize("column", ["v", "Derivation"])
    def test_missing_operand_field_refuses(self, column):
        with pytest.raises(AssertionError, match="missing historical lineage"):
            _uganda_historical_cluster_view(self._frame().drop(columns=column))

    def test_duplicate_current_key_refuses(self):
        frame = self._frame()
        with pytest.raises(AssertionError, match="duplicate current household/wave"):
            _uganda_historical_cluster_view(pd.concat([frame, frame.iloc[[0]]]))

    def test_scope_contract_matches_existing_registered_metadata(self):
        from lsms_library.derivations import derivation_records

        records = derivation_records("Uganda")  # Existing UTF-8 metadata reader.
        assert records[self.U].waves == "all"
        assert records[self.A].waves == ["2015-16"]
        assert set(records[self.E].waves) == {"2018-19", "2019-20"}
        for name in ["ea-comm-link", "ea-own-comm"]:
            assert set(records[f"Uganda::sample::{name}"].waves) == {
                "2005-06", "2009-10", "2010-11", "2011-12"}


# ===========================================================================
# GH #1577 -- `served_strata`: what the SERVED `strata` column holds
# ===========================================================================
#
# Ruling (@ligon, 2026-11-12): "Registry + sampling.yml tag".  Constructed
# strata stay in the derivations registry; a per-wave tag records the
# native-code, native-label and absent cases, widened under GH #1134's rule
# (one value at a time, each with its first evidenced record).

def strata(**kw):
    """A minimal valid ``served_strata`` block (a native label)."""
    block = {"holds": "native-label", "variable": "strate",
             "source_type": "derived-from-data", "confidence": "high",
             "locator": "measured"}
    block.update(kw)
    return {k: v for k, v in block.items() if v is not None}


CONSTRUCTED = dict(holds="constructed", variable=None,
                   derivation="A::sample::region-milieu-stratum")


class TestServedStrataIsItsOwnRecord:
    """Like ``panel``, the block vouches for itself, so a FIRST wave -- which
    has no boundary and so no ``panel`` -- can say what its strata hold."""

    def test_a_served_strata_only_entry_is_a_record(self):
        r = SamplingRecord.from_config("A", "1", {"served_strata": strata()})
        assert r.design_tag is None and r.cluster_id_continuity is None
        assert r.strata_holds == "native-label"
        assert type(r["served_strata"]) is S._FrozenDict

    def test_beside_a_panel_block_no_design_triple_is_needed(self):
        r = SamplingRecord.from_config(
            "A", "2", {"panel": panel(), "served_strata": strata()})
        assert r.design_tag is None and r.strata_holds == "native-label"
        assert r.cluster_id_continuity == "wave-scoped"

    def test_the_panel_beside_it_still_needs_its_own_provenance(self):
        """``served_strata`` vouches for itself, not for the panel block."""
        with pytest.raises(ValueError, match="missing"):
            SamplingRecord.from_config(
                "A", "2", {"panel": {"role": "panel"},
                           "served_strata": strata()})

    def test_a_design_field_beside_it_still_needs_the_triple(self):
        with pytest.raises(ValueError, match="travel together"):
            SamplingRecord.from_config(
                "A", "1", {"stages": 2, "served_strata": strata()})
        r = SamplingRecord.from_config("A", "1", design(served_strata=strata()))
        assert r.design_tag and r.strata_holds == "native-label"

    @pytest.mark.parametrize("block", [
        {}, {"panel": None}, {"served_strata": None},
        {"panel": None, "served_strata": None},
    ])
    def test_an_empty_entry_is_still_refused(self, block):
        with pytest.raises(ValueError, match="empty sampling record"):
            SamplingRecord.from_config("A", "1", block)

    def test_unrecorded_is_None_never_absent(self):
        assert SamplingRecord.unrecorded("A", "1").strata_holds is None
        assert rec().strata_holds is None


class TestServedStrataRefusals:

    @pytest.mark.parametrize("bad", [
        "code", "label", "native", "region", "region-x-milieu",
        "decoded", "derived", "not-stated", S.UNRECORDED_TAG,
    ])
    def test_an_unknown_holds_value_is_refused(self, bad):
        with pytest.raises(ValueError, match="controlled vocabulary"):
            SamplingRecord.from_config(
                "A", "1", {"served_strata": strata(holds=bad)})

    @pytest.mark.parametrize("field,bad", [
        ("source_type", "questionnaire"), ("confidence", "certain")])
    def test_its_provenance_is_controlled(self, field, bad):
        with pytest.raises(ValueError, match="controlled vocabulary"):
            SamplingRecord.from_config(
                "A", "1", {"served_strata": strata(**{field: bad})})

    @pytest.mark.parametrize("drop", ["holds", "source_type", "confidence",
                                      "locator"])
    def test_the_tag_never_travels_without_its_provenance(self, drop):
        with pytest.raises(ValueError, match="missing"):
            SamplingRecord.from_config(
                "A", "1", {"served_strata": strata(**{drop: None})})

    def test_the_design_triple_does_not_vouch_for_it(self):
        """A design record's provenance describes the design statement, not
        the served column."""
        with pytest.raises(ValueError, match="missing"):
            SamplingRecord.from_config("A", "1", design(
                served_strata=strata(source_type=None, confidence=None)))

    def test_not_found_is_refused(self):
        with pytest.raises(ValueError, match="holds: absent"):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                holds="absent", variable=None, source_type="not-found",
                confidence="low")})

    @pytest.mark.parametrize("block", [
        strata(count=25),                                   # a derived count
        strata(labels=["Agadez pastorale"]),
        strata(design_tag="two-stage-stratified-cluster"),
    ])
    def test_an_unknown_field_is_refused(self, block):
        with pytest.raises(ValueError, match="unknown"):
            SamplingRecord.from_config("A", "1", {"served_strata": block})

    @pytest.mark.parametrize("field,bad", [
        ("holds", 1), ("variable", ""), ("variable", 7), ("note", " "),
        ("locator", ""), ("derivation", ["A::sample::x"]),
    ])
    def test_values_are_non_empty_strings(self, field, bad):
        with pytest.raises(ValueError, match="non-empty string"):
            SamplingRecord.from_config(
                "A", "1", {"served_strata": strata(**{field: bad})})

    def test_a_non_mapping_is_refused(self):
        with pytest.raises(ValueError, match="must be a mapping"):
            SamplingRecord.from_config("A", "1",
                                       {"served_strata": "native-code"})

    @pytest.mark.parametrize("holds", sorted(S.NATIVE_STRATA))
    def test_a_native_value_names_its_variable(self, holds):
        with pytest.raises(ValueError, match="name it in `variable`"):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                holds=holds, variable=None)})

    @pytest.mark.parametrize("holds", sorted(S.NATIVE_STRATA))
    def test_a_native_value_has_no_derivation(self, holds):
        with pytest.raises(ValueError, match="no derivation"):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                holds=holds, derivation="A::sample::x")})

    def test_constructed_is_accepted_with_its_registry_key(self):
        r = SamplingRecord.from_config(
            "A", "1", {"served_strata": strata(**CONSTRUCTED)})
        assert r.served_strata["derivation"] == "A::sample::region-milieu-stratum"

    def test_constructed_needs_its_registry_key(self):
        with pytest.raises(ValueError, match="derivations registry"):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                **{**CONSTRUCTED, "derivation": None})})

    def test_constructed_restates_nothing_the_registry_holds(self):
        with pytest.raises(ValueError, match="may not name a `variable`"):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                **{**CONSTRUCTED, "variable": "s00q01"})})

    @pytest.mark.parametrize("key,match", [
        ("region-milieu-stratum", "three"),             # not a key at all
        ("A::sample::", "empty name"),
        ("A::sample::a+b", "may not contain"),
        ("B::sample::region-milieu-stratum", "must be a key of A's"),
        ("::sample::region-milieu-stratum", "must be a key of A's"),
        ("A::food_acquired::x", "must be a key of A's"),
        ("A::::x", "must be a key of A's"),
    ])
    def test_constructed_key_must_be_this_countrys_sample_key(self, key, match):
        with pytest.raises(ValueError, match=match):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                **{**CONSTRUCTED, "derivation": key})})

    @pytest.mark.parametrize("key", [
        " A::sample::region-milieu-stratum ",     # padded (red team F2)
        "A::sample::region-milieu-stratum ",
        "A:: sample ::region-milieu-stratum",       # re-spaced slot
        "A::sample:: region-milieu-stratum",
    ])
    def test_constructed_key_must_be_written_canonically(self, key):
        """`parse_key` strips its slots, so a padded key used to pass the
        country/table check while matching no registry key and no stamp."""
        with pytest.raises(ValueError, match="not written as a registry key"):
            SamplingRecord.from_config("A", "1", {"served_strata": strata(
                **{**CONSTRUCTED, "derivation": key})})

    def test_the_holds_refusal_names_its_own_origin(self):
        """`holds` is not the Uganda exemplar's vocabulary (red team F8)."""
        with pytest.raises(ValueError) as exc:
            SamplingRecord.from_config(
                "A", "1", {"served_strata": strata(holds="label")})
        assert "GH #1577" in str(exc.value)
        assert "Uganda exemplar" not in str(exc.value)

    @pytest.mark.parametrize("extra", [{"variable": "strate"},
                                       {"derivation": "A::sample::x"}])
    def test_absent_names_nothing(self, extra):
        block = strata(holds="absent", variable=None)
        block.update(extra)
        with pytest.raises(ValueError, match="serves nothing"):
            SamplingRecord.from_config("A", "1", {"served_strata": block})


class TestServedStrataIsFrozen:
    """GH #1134's rule, applied to the new tag: every value is used by a
    shipped record, and the first record of each is the one sampling.py
    names."""

    def test_the_vocabulary_is_exactly_the_four_values(self):
        assert S.STRATA_HOLDS == {"native-code", "native-label",
                                  "constructed", "absent"}
        assert S.NATIVE_STRATA < S.STRATA_HOLDS

    def test_every_allowed_value_is_used_by_a_shipped_record(self):
        used = {r.strata_holds for r in _shipped()} - {None}
        assert used == S.STRATA_HOLDS

    def test_every_served_strata_field_is_used_by_a_shipped_record(self):
        used = {k for r in _shipped() for k in (r.get("served_strata") or {})}
        assert used == set(S._SERVED_STRATA_FIELDS), \
            set(S._SERVED_STRATA_FIELDS) ^ used

    @pytest.mark.parametrize("country,wave,holds", [
        ("Mali", "2017-18", "native-code"),
        ("Niger", "2011-12", "native-label"),
        ("Mali", "2018-19", "constructed"),
        ("Mali", "2014-15", "absent"),
    ])
    def test_the_first_record_of_each_value(self, country, wave, holds):
        r = sampling_records(country)[wave]
        assert r.strata_holds == holds
        blk = r.served_strata
        assert blk["source_type"] and blk["confidence"] and blk["locator"]

    def test_the_named_first_records_are_cited_in_sampling_py(self):
        import inspect
        src = inspect.getsource(S)
        for cite in ("Mali 2017-18", "Niger 2011-12", "Mali 2018-19",
                     "Mali 2014-15"):
            assert cite in src, cite

    def test_a_first_wave_tag_declares_no_boundary(self):
        for country, wave in (("Mali", "2014-15"), ("Niger", "2011-12")):
            r = sampling_records(country)[wave]
            assert r.get("panel") is None and r.strata_holds is not None


# -- the registry, both directions ----------------------------------------

#: Countries whose registered strata construction is NOT yet tagged in their
#: own ``sampling.yml``, each with its reason.  The 2026-11-13 batch kept
#: these two files for other streams; their records are proposed in the
#: #1577 report for integration.  ``test_a_pending_country_is_still_pending``
#: fails once one is tagged, so the entry cannot outlive its reason.
SERVED_STRATA_PENDING: dict[str, str] = {
    "Benin": "sampling.yml edited by another 2026-11-13 stream; record proposed "
             "in the #1577 report",
    "Guinea-Bissau": "sampling.yml created by another 2026-11-13 stream; record "
                     "proposed in the #1577 report",
}


def _registered_strata_constructions():
    """``{(country, wave): key}`` for every registered ``sample`` derivation
    that writes ``strata``, in every country.  Filtered on the COLUMN, not on
    ``::sample::`` -- Tanzania, Uganda and Nigeria register sample keys for
    other columns."""
    import lsms_library as ll
    from lsms_library.derivations import derivation_records
    out = {}
    for name in ll.countries():
        for key, r in derivation_records(name).items():
            if r.table != "sample" or "strata" not in (r.columns or []):
                continue
            waves = (list(ll.Country(name).waves) if r.waves in (None, "all")
                     else list(r.waves))
            for w in waves:
                assert (name, w) not in out, (name, w, key, out.get((name, w)))
                out[(name, w)] = key
    return out


class TestServedStrataAgreesWithTheRegistry:

    def test_every_constructed_tag_resolves_to_a_registered_entry(self):
        from lsms_library.derivations import derivation_records
        import lsms_library as ll
        for name in ll.countries():
            for wave, r in sampling_records(name).items():
                if r.strata_holds != "constructed":
                    continue
                key = r.served_strata["derivation"]
                entry = derivation_records(name).get(key)
                assert entry is not None, (name, wave, key)
                assert entry.table == "sample", (name, wave, key)
                assert "strata" in (entry.columns or []), (name, wave, key)
                assert entry.waves in (None, "all") or wave in entry.waves, \
                    (name, wave, key, entry.waves)

    def test_every_registered_strata_construction_is_tagged(self):
        """The converse: a registered construction of `strata` is visible from
        `Country(c).sampling` too, under the same key."""
        missing = []
        for (name, wave), key in sorted(_registered_strata_constructions().items()):
            if name in SERVED_STRATA_PENDING:
                continue
            r = sampling_records(name).get(wave)
            got = (r.served_strata or {}) if r is not None else {}
            if (got.get("holds"), got.get("derivation")) != ("constructed", key):
                missing.append((name, wave, key, got.get("holds")))
        assert not missing, missing

    @pytest.mark.parametrize("country", sorted(SERVED_STRATA_PENDING))
    def test_a_pending_country_is_still_pending(self, country):
        cells = [w for (c, w) in _registered_strata_constructions() if c == country]
        tagged = [w for w in cells
                  if (sampling_records(country).get(w) is not None
                      and sampling_records(country)[w].strata_holds is not None)]
        # NOTHING tagged (red team F5): a half-tagged pending country -- one
        # wave pasted, another registered later -- must fail here, not pass
        # silently until someone notices.
        assert cells and not tagged, (
            f"{country} is (partly) tagged: {tagged} of {cells}.  Tag every "
            "registered wave and remove it from SERVED_STRATA_PENDING.")

    def test_a_native_or_absent_tag_has_no_registered_construction(self):
        """A registered construction of a wave's strata contradicts a tag that
        says the value is recorded, or that nothing is served."""
        registered = _registered_strata_constructions()
        import lsms_library as ll
        for name in ll.countries():
            for wave, r in sampling_records(name).items():
                if r.strata_holds in S.NATIVE_STRATA | {"absent"}:
                    assert (name, wave) not in registered, (name, wave)


# -- static: a native record is what the wave's config serves (red team F4) --
#
# Read from config and source text only (no data, no import of a country
# module): the record's `variable` must be the variable and file the wave's
# data_info.yml maps to `strata`, declared bare (no `mapping:` / `mappings:`),
# and nothing in the wave may relabel it -- the effective `strata` formatter is
# absent or `format_id`, and no `sample` / sub-df hook touches `strata`.  A
# spelling fold by the country's `strata` categorical table is allowed, as the
# `native-label` definition says.

_VARIABLE = re.compile(r"^(\S+) \((.+)\)$")


def _strata_declarations(block):
    """``[(sub-df name or None, file, value)]`` for each `strata` myvar."""
    out = []
    if "dfs" in block:
        for name in block["dfs"]:
            sub = block.get(name) or {}
            if "strata" in (sub.get("myvars") or {}):
                out.append((name, sub.get("file"), sub["myvars"]["strata"]))
    elif "strata" in (block.get("myvars") or {}):
        out.append((None, block.get("file"), block["myvars"]["strata"]))
    return out


def _module_names(paths):
    """``{name: (path, node, names-of-that-module)}``, later paths overriding
    earlier ones (as ``Wave.formatting_functions`` does)."""
    import ast
    out = {}
    for p in paths:
        if not p.exists():
            continue
        tree = ast.parse(p.read_text(encoding="utf-8"))
        local = {}
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                local[node.name] = node
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        local[t.id] = node.value
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for a in node.names:
                    local[a.asname or a.name.split(".")[0]] = node
        for name, node in local.items():
            out[name] = (p, node, local)
    return out


def _resolve(entry, depth=0):
    """Follow ``alias = other_function`` inside one module."""
    import ast
    p, node, local = entry
    if isinstance(node, ast.Name) and depth < 5 and node.id in local:
        return _resolve((p, local[node.id], local), depth + 1)
    return entry


def _body_without_docstring(fn):
    import ast
    body = list(fn.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(
            getattr(body[0], "value", None), ast.Constant) and isinstance(
            body[0].value.value, str):
        body = body[1:]
    return body


def _touches_strata(fn):
    import ast
    for stmt in _body_without_docstring(fn):
        for n in ast.walk(stmt):
            if isinstance(n, ast.Constant) and n.value == "strata":
                return True
            if isinstance(n, ast.Attribute) and n.attr == "strata":
                return True
    return False


def _is_format_id_passthrough(fn):
    import ast
    body = _body_without_docstring(fn)
    args = [a.arg for a in fn.args.args]
    if len(args) != 1 or len(body) != 1 or not isinstance(body[0], ast.Return):
        return False
    call = body[0].value
    return (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
            and call.func.id == "format_id" and len(call.args) == 1
            and not call.keywords and isinstance(call.args[0], ast.Name)
            and call.args[0].id == args[0])


def _native_config_problems(country, wave, variable=None):
    """Why the wave's `strata` is not a bare, un-relabelled recorded variable
    (and, given `variable`, not the one the record names).  [] if none."""
    import ast
    import lsms_library as ll
    from pathlib import PurePosixPath
    from lsms_library.yaml_utils import load_yaml
    c = ll.Country(country)
    w = c[wave]
    with open(w.file_path / "_" / "data_info.yml", encoding="utf-8") as fh:
        block = (load_yaml(fh) or {}).get("sample") or {}
    problems = []
    decls = _strata_declarations(block)
    if len(decls) != 1:
        return [f"{len(decls)} `strata` declarations in the sample block"]
    sub, file, value = decls[0]
    if not isinstance(value, str):
        problems.append(f"`strata` is not declared bare: {value!r}")
    if not isinstance(file, str):
        problems.append(f"`strata` comes from more than one file: {file!r}")
    if variable is not None:
        m = _VARIABLE.match(variable)
        if not m:
            problems.append(f"variable {variable!r} is not 'name (file)'")
        else:
            name, rec_file = m.group(1), PurePosixPath(m.group(2)).parts
            if value != name:
                problems.append(f"config serves {value!r}, record names {name!r}")
            if not isinstance(file, str) or \
                    PurePosixPath(file).parts[-len(rec_file):] != rec_file:
                problems.append(f"config reads {file!r}, record names {m.group(2)!r}")
    names = _module_names([
        c.file_path / "_" / f"{country.lower()}.py", c.file_path / "_" / "mapping.py",
        w.file_path / "_" / f"{w.wave_folder}.py", w.file_path / "_" / "mapping.py"])
    if "strata" in names:
        p, node, _ = _resolve(names["strata"])
        if not (isinstance(node, ast.FunctionDef) and _is_format_id_passthrough(node)):
            problems.append(f"`strata` formatter in {p.name} is not format_id")
    for hook in ["sample"] + ([sub] if sub else []):
        if hook not in names:
            continue
        p, node, _ = _resolve(names[hook])
        if not isinstance(node, ast.FunctionDef):
            problems.append(f"`{hook}` hook in {p.name} cannot be read")
        elif _touches_strata(node):
            problems.append(f"`{hook}` hook in {p.name} writes or reads `strata`")
    return problems


def _native_records():
    try:
        return sorted((r.country, r.wave) for r in _shipped()
                      if r.strata_holds in S.NATIVE_STRATA)
    except Exception:                                  # pragma: no cover
        return []


class TestANativeRecordIsWhatTheConfigServes:

    @pytest.mark.parametrize("country,wave", _native_records())
    def test_the_variable_is_the_configs_and_nothing_relabels_it(self, country,
                                                                wave):
        rec = sampling_records(country)[wave]
        assert _native_config_problems(
            country, wave, rec.served_strata["variable"]) == []

    def test_there_are_native_records_to_check(self):
        assert len(_native_records()) >= 2

    @pytest.mark.parametrize("country,wave,why", [
        # The #1573 decode: its `sample` hook relabels the code.
        ("Burkina_Faso", "2014", "hook"),
        # A district mapping on the declaration itself.
        ("Malawi", "2010-11", "not declared bare"),
    ])
    def test_the_check_bites_on_a_relabelled_wave(self, country, wave, why):
        problems = _native_config_problems(country, wave)
        assert any(why in p for p in problems), problems

    def test_the_check_bites_on_a_wrong_variable_or_file(self):
        assert _native_config_problems("Mali", "2017-18",
                                       "strate (EACIPOIDS.dta)")
        assert _native_config_problems("Mali", "2017-18",
                                       "poids_leger (EACI17_ECHANTILLON.dta)")


# -- delivered: the tag agrees with the served column ----------------------

def _tagged():
    """Evaluated at collection.  A malformed shipped file must not turn this
    module into a collection error; ``test_every_shipped_file_loads`` is the
    test that fails for it."""
    try:
        return sorted((r.country, r.wave) for r in _shipped()
                      if r.strata_holds is not None)
    except Exception:                                  # pragma: no cover
        return []


@pytest.mark.requires_s3
@pytest.mark.parametrize("country,wave", _tagged())
def test_the_tag_agrees_with_the_served_strata(country, wave):
    """absent: NA on every row; constructed: the pointed key on every row that
    has a stratum, and on no row that has none; native-code: digit strings;
    native-label: no value is a bare code."""
    import lsms_library as ll
    from lsms_library.derivations import MULTI_SEP
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        s = ll.Country(country).sample(waves=[wave])
    assert len(s), (country, wave)
    blk = sampling_records(country)[wave].served_strata
    if blk["holds"] == "absent":
        # Whether a one-wave frame carries the column depends on cache state,
        # identically on base (red team F1): Mali 2014-15 has no `strata` on a
        # cold one-wave call and an all-NA one after an all-wave call.  Both
        # shapes serve no stratum, so both pass.
        assert "strata" not in s or s["strata"].isna().all(), \
            s["strata"].dropna().unique()[:5]
        return
    values = s["strata"]
    present = values.notna()
    assert present.any(), (country, wave)
    as_text = values[present].astype(str).str.strip()
    is_code = as_text.str.fullmatch(r"[0-9]+(\.0)?")
    if blk["holds"] == "native-code":
        assert is_code.all(), as_text[~is_code].unique()[:5]
    else:
        assert not is_code.any(), as_text[is_code].unique()[:5]
    if blk["holds"] == "constructed":
        keys = s.get("Derivation")
        assert keys is not None, "no Derivation column"
        carried = keys.fillna("").astype(str).str.split(MULTI_SEP).apply(
            lambda ks: blk["derivation"] in [k.strip() for k in ks])
        assert carried[present].all(), int((~carried[present]).sum())
        assert not carried[~present].any(), int(carried[~present].sum())
