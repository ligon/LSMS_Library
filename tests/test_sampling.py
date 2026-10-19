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
