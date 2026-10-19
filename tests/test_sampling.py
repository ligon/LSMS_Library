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


# -- the shipped declarations, and the reproduction --------------------------

class TestShippedDeclarations:

    @pytest.mark.parametrize("country,wave,value", [
        ("GhanaLSS", "1988-89", "wave-scoped"),      # GLSS2 renumbering
        ("GhanaLSS", "1991-92", "unobservable"),     # no linkage: correct to group
        ("Ethiopia", "2013-14", "wave-scoped"),      # ea_id -> ea_id2
        ("Ethiopia", "2018-19", "unobservable"),     # Panel II
        ("Tanzania", "2012-13", "wave-scoped"),      # UPD4 round-3 renumbering
        ("Tanzania", "2019-20", "wave-scoped"),      # NPS-SDD layout
        ("Uganda", "2009-10", "mixed"),              # @lat,lon bins
        ("Uganda", "2011-12", "unobservable"),       # OUR rendering defect: held
        ("Uganda", "2013-14", "redefined"),          # EA -> parish
        ("Nigeria", "2012Q3", "stable"),             # movers are singletons
    ])
    def test_the_measured_boundaries(self, country, wave, value):
        assert sampling_records(country)[wave].cluster_id_continuity == value

    def test_tanzania_2020_21_is_a_household_refresh_not_a_cluster_fact(self):
        """The distinction a one-dimensional enum could not express (Q2)."""
        r = sampling_records("Tanzania")["2020-21"]
        assert r.household_continuity == "refresh"
        assert r.cluster_id_continuity == "unobservable"

    def test_a_library_defect_is_never_declared_a_survey_renumbering(self):
        """Uganda 2010-11 -> 2011-12 measures 0% but is our rendering, so it
        must not be `wave-scoped` (DESIGN.org §3.3, Q5)."""
        r = sampling_records("Uganda")["2011-12"]
        assert r.cluster_id_continuity not in S.INCOMPARABLE_CLUSTER_IDS
        assert "rendering" in r["panel"]["cluster_id_note"]


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

    def test_strict_mode_refuses_the_reproduction(self, monkeypatch):
        monkeypatch.setenv("LSMS_CLUSTER_STRICT", "1")
        with pytest.raises(S.ClusterIdentityError):
            self._fresh().household_roster(waves=["1987-88", "1988-89"])
