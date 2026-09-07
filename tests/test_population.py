"""The population record: config -> `df.attrs` -> a warning, and never a filter.

GH #603 / #601.  Three things are being pinned here, and they are pinned
because each one has a specific way of going wrong:

1. **The triple travels together.**  ``universe_tag`` is an editorial reading --
   the source document says so in capitals -- and a reading quoted without
   ``source_type`` and ``confidence`` has been laundered into a fact.  The
   loader refuses such a record.

2. **``attrs`` survives assembly.**  ``merge()`` and ``set_index()`` drop
   ``DataFrame.attrs``; that has already cost this library a real bug (the
   ``id_converted`` flag, Burkina Faso 2021-22, 392 duplicate tuples, commit
   ``4db41a27``).  A record that evaporates on the way to the caller is worth
   nothing, so the survival is tested through the operations that drop it.

3. **Nothing is fenced.**  #603 proposed excluding ``specialized`` frames from
   ``Feature()`` by default and @ligon declined.  The test that the warning
   fires is paired with a test that the row count did not change.
"""

import warnings

import pandas as pd
import pytest

from lsms_library import population as P
from lsms_library.population import (
    PopulationHeterogeneityWarning,
    PopulationRecord,
    comparability_class,
    pool_report,
    population_records,
)


def rec(country="A", wave="1", tag="national-all-households",
        source_type="local-documentation", confidence="high", **kw):
    return PopulationRecord(country=country, wave=wave, universe_tag=tag,
                            source_type=source_type, confidence=confidence, **kw)


# ---------------------------------------------------------------------------
# 1. the triple travels together
# ---------------------------------------------------------------------------

class TestTheTagNeverTravelsAlone:
    """An editorial reading without its provenance is a fact it never earned."""

    @pytest.mark.parametrize("omit", ["universe_tag", "source_type", "confidence"])
    def test_a_record_missing_any_of_the_three_is_rejected(self, omit):
        block = {"universe_tag": "specialized", "source_type": "wb-catalog",
                 "confidence": "high"}
        block.pop(omit)
        with pytest.raises(ValueError, match="travel together"):
            PopulationRecord.from_config("Liberia", "2018-19", block)

    @pytest.mark.parametrize("field,bad", [
        ("universe_tag", "national"),        # the WARN-time class, not a tag
        ("universe_tag", "general"),         # #603's rejected vocabulary
        ("universe_tag", P.UNRECORDED_TAG),  # a library sentinel, not config
        ("source_type", "questionnaire"),
        ("confidence", "certain"),
    ])
    def test_every_field_is_validated_not_just_the_tag(self, field, bad):
        block = {"universe_tag": "specialized", "source_type": "wb-catalog",
                 "confidence": "high"}
        block[field] = bad
        with pytest.raises(ValueError, match="controlled vocabulary"):
            PopulationRecord.from_config("Liberia", "2018-19", block)

    def test_confidence_low_is_visible_not_silently_trusted(self):
        """`low` means "sample-design text; do not launder into a universe"."""
        niger = population_records("Niger")["2021-22"]
        assert niger.confidence == "low"
        assert "confidence" in niger.to_dict()
        report = pool_report([rec("Niger", "2021-22", "national-claimed",
                                  confidence="low"),
                              rec("Liberia", "2018-19", "specialized")], "t")
        assert "confidence=low" in report
        assert "Niger 2021-22" in report


# ---------------------------------------------------------------------------
# 2. every tag round-trips from config
# ---------------------------------------------------------------------------

class TestConfigRoundTrip:

    def test_every_tag_in_the_controlled_vocabulary_is_present_in_config(self):
        """All ten values appear in the shipped `population.yml` files.

        A tag that no wave carries would mean the promotion lost a row.
        """
        import lsms_library as ll
        seen = set()
        for name in ll.countries():
            for r in population_records(name).values():
                seen.add(r.universe_tag)
        assert seen == set(P.UNIVERSE_TAGS), P.UNIVERSE_TAGS - seen

    def test_every_wave_of_every_configured_country_has_a_record(self):
        import lsms_library as ll
        missing = []
        for name in ll.countries():
            c = ll.Country(name)
            recs = c.population
            missing += [f"{name}/{w}" for w in c.waves if w not in recs]
        assert not missing, missing

    def test_a_known_record_carries_its_verbatim_exclusions(self):
        """The exclusions are the point: they are what makes two "nationally
        representative" surveys represent different populations."""
        liberia = population_records("Liberia")["2018-19"]
        assert liberia.universe_tag == "specialized"
        assert "Montserrado" in liberia.exclusions
        assert "2.5" in liberia.population_statement

    def test_inert_rows_are_recorded_in_config_but_never_returned(self):
        """Albania 1996 was swept; it has no `_/` config, so it is not a wave."""
        assert "1996" not in population_records("Albania")
        import yaml
        from lsms_library.paths import countries_root
        with open(countries_root() / "Albania" / "_" / "population.yml") as fh:
            raw = yaml.safe_load(fh)
        assert raw["Population"]["1996"]["status"] == "inert"

    def test_a_pp_ph_round_inherits_its_wave_directory_record(self):
        """Nigeria's five wave dirs are ten API waves; the doc keyed by dir."""
        nga = population_records("Nigeria")
        assert nga["2010Q3"].documented_as == "2010-11"
        assert nga["2011Q1"].documented_as == "2010-11"
        assert nga["2010Q3"].universe_tag == nga["2011Q1"].universe_tag
        assert nga["2018Q3"].universe_tag == "region-excluded"   # rural Borno

    def test_an_unswept_country_yields_an_empty_record_not_an_error(self):
        assert population_records("NoSuchCountry") == {}


# ---------------------------------------------------------------------------
# 3. the warn-time collapse
# ---------------------------------------------------------------------------

class TestComparabilityCollapse:
    """@ligon: national-all-households and national-claimed are both national.

    The collapse is a *comparability* judgement applied at warn time.  It is
    NOT a re-tagging: 28 waves are `national-claimed`, and how well documented
    a universe is stays a real fact about those surveys.
    """

    def test_the_two_national_tags_collapse(self):
        assert comparability_class("national-all-households") == "national"
        assert comparability_class("national-claimed") == "national"

    def test_no_other_tag_collapses(self):
        for tag in P.UNIVERSE_TAGS - {"national-all-households", "national-claimed"}:
            assert comparability_class(tag) == tag

    def test_the_collapse_does_not_leak_into_the_data_model(self):
        """`national` is a warn-time class and must never be a stored tag."""
        assert "national" not in P.UNIVERSE_TAGS
        import lsms_library as ll
        tags = {r.universe_tag
                for name in ll.countries()
                for r in population_records(name).values()}
        assert "national" not in tags
        assert {"national-all-households", "national-claimed"} <= tags

    def test_a_homogeneous_national_pool_does_not_warn(self):
        """The pinned no-fire case: the two national tags together are silent."""
        assert pool_report([rec("Uganda", "2005-06", "national-claimed"),
                            rec("Benin", "2018-19", "national-all-households")],
                           "household_roster") is None

    def test_a_single_tag_pool_does_not_warn(self):
        assert pool_report([rec("A", "1"), rec("B", "1")], "t") is None
        assert pool_report([], "t") is None


class TestWhatCountsAsMateriallyDifferent:

    def test_region_excluded_warns_against_national(self):
        report = pool_report([rec("Benin", "2018-19", "national-all-households"),
                              rec("Ethiopia", "2021-22", "region-excluded")], "t")
        assert report is not None
        assert "region-excluded" in report

    def test_specialized_warns_but_is_not_excluded(self):
        report = pool_report([rec("Benin", "2018-19", "national-all-households"),
                              rec("Liberia", "2018-19", "specialized")], "t")
        assert "specialized" in report
        assert "Nothing was dropped" in report

    def test_an_unknown_universe_is_reported_as_unknown_not_as_different(self):
        report = pool_report([rec("Tanzania", "2019-20", "national-all-households"),
                              rec("Tanzania", "2020-21", "not-stated")], "t")
        assert report is not None
        assert "absence of information" in report
        assert "materially different" not in report.split("Nothing was")[0]

    def test_a_pool_of_only_unknowns_says_nothing(self):
        """There is no comparability claim to make about two unknowns."""
        assert pool_report([rec("A", "1", "not-stated"),
                            rec("B", "1", "not-stated")], "t") is None

    def test_a_genuinely_mixed_pool_names_every_class(self):
        report = pool_report([rec("Benin", "2018-19", "national-all-households"),
                              rec("Ethiopia", "2011-12", "rural-and-small-town"),
                              rec("China", "1995-97", "subnational-area"),
                              rec("Liberia", "2018-19", "specialized")], "t")
        for cls in ("national", "rural-and-small-town", "subnational-area",
                    "specialized"):
            assert cls in report


# ---------------------------------------------------------------------------
# 4. attrs survival
# ---------------------------------------------------------------------------

def _frame_with_record():
    df = pd.DataFrame({"t": ["2018-19", "2018-19"], "i": ["1", "2"],
                       "Age": [30, 40]})
    P.attach(df, "Liberia")
    return df


class TestAttrsSurvival:
    """The hazard `CLAUDE.md` documents -- re-measured, and restated as a RULE.

    `CLAUDE.md` said "merge() and set_index() both drop DataFrame.attrs": true
    of pandas 2.x, and the origin of the `id_converted` bug.  On the pinned
    pandas 3.0.2 the per-method framing is wrong in BOTH directions -- `merge`
    with matching `attrs` preserves them, and `set_index` always does.  One
    sentence covers every case:

        `attrs` survive an operation only when every input AGREES.  Any
        disagreement -- INCLUDING one side having none -- yields {}.

    A single-input operation therefore cannot lose them (nothing to disagree
    with), and cross-country assembly always does (one record per country, by
    design).

    These tests pin all seven cells, deliberately including the *preserving*
    ones.  A test that only covered the drops would pass against a future
    pandas that dropped unconditionally -- and we would lose the signal that
    `Feature`'s explicit re-attach is what is keeping the record alive.
    """

    OTHER = {"population": {"SomewhereElse": {"1999": {}}}}

    def test_attach_puts_the_record_on_attrs(self):
        df = _frame_with_record()
        assert df.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]["universe_tag"] == \
            "specialized"
        assert df.attrs[P.ATTRS_RESOLUTION_KEY]["Liberia"] == "exact"

    # ---- the rule: single input, nothing to disagree with -> PRESERVES ----

    @pytest.mark.parametrize("op", [
        lambda d: d.set_index(["t", "i"]),
        lambda d: d.set_index(["t", "i"]).reset_index(),
        lambda d: d.rename(columns={"Age": "age"}),
        lambda d: d.astype({"Age": "Int64"}),
        lambda d: d.dropna(how="all"),
        lambda d: d.groupby(["t", "i"]).first(),
        lambda d: d.copy(),
    ])
    def test_single_input_operations_preserve(self, op):
        assert P.ATTRS_KEY in op(_frame_with_record()).attrs

    # ---- the rule: inputs AGREE -> PRESERVES (this is what I got wrong) ----

    def test_merge_preserves_when_both_sides_carry_the_SAME_attrs(self):
        """`merge` is not inherently lossy.  Stating it as "merge drops" sends
        the reader to the wrong mitigation -- and to the wrong diagnosis of
        code that is already fine.  See the measured table in CLAUDE.md.
        """
        left = _frame_with_record()
        right = pd.DataFrame({"i": ["1", "2"], "x": [1, 2]})
        right.attrs = dict(left.attrs)
        assert P.ATTRS_KEY in left.merge(right, on="i").attrs

    def test_concat_preserves_when_inputs_carry_the_SAME_attrs(self):
        a = _frame_with_record()
        b = _frame_with_record()
        assert P.ATTRS_KEY in pd.concat([a, b]).attrs

    # ---- the rule: any disagreement -> {} ----

    def test_merge_drops_when_the_other_side_has_NO_attrs(self):
        """The shape of any merge against a frame that never passed through
        `_finalize_result` -- a raw `get_dataframe` result, a lookup table.

        This docstring used to say "NOT `_join_v_from_sample`: sample() goes
        through _finalize_result too, so both sides now carry the same record
        and that merge hits the *preserving* branch."  RE-MEASURED 2026-09-06
        on Liberia, Albania and Ethiopia, on the unmodified pre-#603-attrs code
        as well as after: **that is false.**  `attach_population` is called at
        `country.py:3034` and `_join_v_from_sample` at `country.py:2891` -- 143
        lines EARLIER in the same `_finalize_result` call -- so at merge time
        the left frame has no population record yet while `sample()`, which
        completed its own `_finalize_result`, has one.  Every observation was
        `left_has_pop=False, right_has_pop=True, attrs_equal=False,
        out_attrs_empty=True`.  It is a *disagreeing* merge, i.e. this very
        case with the sides swapped.  What keeps `id_converted` alive there is
        the explicit `result.attrs = dict(df.attrs)`, and what puts the
        population record on the result is `attach_population` running after.
        See `TestVJoinIsADisagreeingMerge` below.
        """
        df = _frame_with_record()
        other = pd.DataFrame({"i": ["1", "2"], "x": [1, 2]})
        assert not other.attrs
        assert P.ATTRS_KEY not in df.merge(other, on="i").attrs

    def test_merge_drops_when_the_two_sides_DISAGREE(self):
        df = _frame_with_record()
        other = pd.DataFrame({"i": ["1", "2"], "x": [1, 2]})
        other.attrs = dict(self.OTHER)
        assert P.ATTRS_KEY not in df.merge(other, on="i").attrs

    def test_concat_drops_when_one_input_has_NO_attrs(self):
        a = _frame_with_record()
        b = pd.DataFrame({"t": ["2018-19"], "i": ["3"], "Age": [50]})
        assert P.ATTRS_KEY not in pd.concat([a, b]).attrs

    def test_concat_drops_when_inputs_DISAGREE(self):
        """The load-bearing one: this IS what cross-country assembly does, and
        it is the case that makes Feature's re-attach necessary rather than
        decorative."""
        a, b = _frame_with_record(), _frame_with_record()
        P.attach(b, "Benin")
        b.attrs[P.ATTRS_KEY] = {"Benin": b.attrs[P.ATTRS_KEY].get("Benin", {})}
        assert a.attrs[P.ATTRS_KEY] != b.attrs[P.ATTRS_KEY]
        assert P.ATTRS_KEY not in pd.concat([a, b]).attrs

    def test_the_record_can_be_carried_across_an_op_that_drops_it(self):
        df = _frame_with_record()
        keep = dict(df.attrs)
        out = df.merge(pd.DataFrame({"i": ["1", "2"], "x": [1, 2]}), on="i")
        out.attrs = dict(keep)
        assert out.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]["universe_tag"] == \
            "specialized"

    def test_the_other_valid_mitigation_is_making_the_inputs_agree(self):
        """Per the rule there are TWO fixes, not one.  Pinning both so the
        docs cannot drift back to "copy attrs" as the only option."""
        left = _frame_with_record()
        right = pd.DataFrame({"i": ["1", "2"], "x": [1, 2]})
        right.attrs = dict(left.attrs)          # make them agree, don't copy after
        assert left.merge(right, on="i").attrs[P.ATTRS_KEY]["Liberia"]

    def test_merge_attrs_unions_countries_and_waves(self):
        a = pd.DataFrame({"x": [1]})
        P.attach(pd.DataFrame({"t": ["2018-19"], "x": [1]}), "Liberia")
        src1 = {P.ATTRS_KEY: {"Liberia": {"2018-19": {"universe_tag": "specialized"}}},
                P.ATTRS_RESOLUTION_KEY: {"Liberia": "exact"}}
        src2 = {P.ATTRS_KEY: {"Uganda": {"2005-06": {"universe_tag": "national-claimed"}}},
                P.ATTRS_RESOLUTION_KEY: {"Uganda": "exact"}}
        P.merge_attrs(a, [src1, src2])
        assert set(a.attrs[P.ATTRS_KEY]) == {"Liberia", "Uganda"}
        assert set(a.attrs[P.ATTRS_RESOLUTION_KEY]) == {"Liberia", "Uganda"}

    def test_records_from_attrs_round_trips(self):
        df = _frame_with_record()
        back = P.records_from_attrs(df)
        assert len(back) == 1
        assert back[0].country == "Liberia" and back[0].wave == "2018-19"
        assert back[0].universe_tag == "specialized"

    def test_attrs_values_are_json_serializable(self):
        """A consumer must be able to serialize a frame's metadata.

        Since #603-attrs the values are `PopulationRecord`s rather than plain
        dicts, and this test is the reason that had to be a `dict` SUBCLASS:
        `pandas.io.parquet` serialises `df.attrs` with a bare `json.dumps`, so
        anything else in `attrs` turns a user's `df.to_parquet()` into a
        `TypeError`.  See `TestRecordsRideOnAttrs::test_to_parquet_still_works`.
        """
        import json
        df = _frame_with_record()
        json.dumps(df.attrs[P.ATTRS_KEY])




class TestVJoinIsADisagreeingMerge:
    """`_join_v_from_sample`'s merge does NOT preserve `attrs`, and this pins why.

    The docs (AGENTS.md, docs/guide/population.md, and the sibling test above)
    used to name it as the live example of the *preserving* branch.  Measured on
    Liberia, Albania and Ethiopia -- and on the pre-change code, so this is a
    documentation error and not a regression -- it is the opposite: the left
    frame has no population record at merge time because `attach_population`
    runs later in the same `_finalize_result`.  Reproduced here in pure pandas
    so the pin costs no microdata.
    """

    def test_the_side_that_has_the_record_is_the_RIGHT_one(self):
        left = pd.DataFrame({"t": ["2018-19"], "i": ["1"], "Age": [30]})
        left.attrs["id_converted"] = True           # what the left DOES carry
        v_lookup = pd.DataFrame({"t": ["2018-19"], "i": ["1"], "v": ["c1"]})
        P.attach(v_lookup, "Liberia")               # what sample() carries
        assert P.ATTRS_KEY not in left.attrs
        assert P.ATTRS_KEY in v_lookup.attrs
        out = left.merge(v_lookup, on=["t", "i"], how="left")
        assert not out.attrs, "a disagreeing merge must yield {}"

    def test_both_mitigations_are_what_actually_keeps_it_alive(self):
        """The explicit copy carries `id_converted`; `attach` re-adds the record."""
        left = pd.DataFrame({"t": ["2018-19"], "i": ["1"], "Age": [30]})
        left.attrs["id_converted"] = True
        v_lookup = pd.DataFrame({"t": ["2018-19"], "i": ["1"], "v": ["c1"]})
        P.attach(v_lookup, "Liberia")
        out = left.merge(v_lookup, on=["t", "i"], how="left")
        out.attrs = dict(left.attrs)                # country.py does exactly this
        assert out.attrs["id_converted"] is True
        P.attach(out, "Liberia")                    # ... and then this
        assert out.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"].universe_tag == "specialized"


class TestRecordsRideOnAttrs:
    """`attrs` carries the record itself -- and every property that depends on.

    @ligon asked for `attrs['population'][c][w].universe_tag` to work.  Making
    the record a `dict` subclass rather than putting a frozen dataclass in
    `attrs` is not a stylistic choice: pandas serialises `attrs` with
    `json.dumps` on the parquet write path, and pandas propagates `attrs` by
    comparing them for EQUALITY.  Both are pinned below, because both would
    fail silently or remotely from the change that caused them.
    """

    def test_both_spellings_work_on_the_object_in_attrs(self):
        block = _frame_with_record().attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]
        assert isinstance(block, PopulationRecord)
        assert block.universe_tag == "specialized" == block["universe_tag"]
        assert block.source_type and block.confidence

    def test_an_absent_optional_field_reads_as_None_not_KeyError(self):
        """The complaint that started this: the dict had no `documented_as`."""
        block = _frame_with_record().attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]
        assert block.documented_as is None
        assert "documented_as" not in block          # mapping content unchanged
        with pytest.raises(KeyError):
            block["documented_as"]

    def test_the_record_compares_equal_to_the_plain_dict_it_replaced(self):
        """The load-bearing property.

        pandas propagates `attrs` only when every input compares equal, so a
        record that compared by identity would silently start dropping `attrs`
        from merges that preserve them today.  Equality with the *plain dict*
        additionally means a frame read back from a pre-existing parquet still
        agrees with a freshly built one.
        """
        r = rec()
        assert r == dict(r) and dict(r) == r
        assert r == rec() and r is not rec()

    def test_merge_preserves_across_a_record_and_an_equivalent_plain_dict(self):
        new = _frame_with_record()
        old = pd.DataFrame({"i": ["1", "2"], "x": [1, 2]})
        old.attrs = {k: ({c: {w: dict(r) for w, r in ws.items()}
                          for c, ws in v.items()} if k == P.ATTRS_KEY else v)
                     for k, v in new.attrs.items()}
        assert type(old.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]) is dict
        assert P.ATTRS_KEY in new.merge(old, on="i").attrs

    def test_records_are_immutable_because_the_loader_is_cached(self):
        r = population_records("Liberia")["2018-19"]
        for call in (lambda: r.__setitem__("confidence", "low"),
                     lambda: r.update({"confidence": "low"}),
                     lambda: r.pop("confidence"),
                     lambda: r.clear()):
            with pytest.raises(TypeError, match="immutable"):
                call()
        assert population_records("Liberia")["2018-19"].confidence == r.confidence

    def test_records_are_hashable(self):
        assert len({rec(), rec()}) == 1

    def test_deepcopy_and_pickle_round_trip(self):
        """pandas deepcopies `attrs` on EVERY propagation, so this is hot."""
        import copy
        import pickle
        r = rec(notes="x")
        for clone in (copy.deepcopy(r), pickle.loads(pickle.dumps(r))):
            assert type(clone) is PopulationRecord and clone == r

    def test_to_parquet_still_works(self, tmp_path):
        """The regression a dataclass in `attrs` would have caused, pinned.

        pandas 3.0.2 `io/parquet.py` does `json.dumps(df.attrs)`; pyarrow's
        `pandas_compat` does the same and warns.  A non-JSON value in `attrs`
        raises `TypeError` on any user's `df.to_parquet()`.
        """
        df = _frame_with_record()
        path = tmp_path / "p.parquet"
        with warnings.catch_warnings():
            warnings.simplefilter("error")   # pyarrow's attrs-drop warning is fatal
            df.to_parquet(path)
        back = pd.read_parquet(path)
        blk = back.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]
        assert blk == dict(df.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"])

    def test_records_from_attrs_reads_records_AND_round_tripped_dicts(self):
        """A parquet round trip loses the class -- `json.loads` returns dicts."""
        df = _frame_with_record()
        as_dicts = pd.DataFrame({"x": [1]})
        as_dicts.attrs = {P.ATTRS_KEY: {"Liberia": {
            "2018-19": dict(df.attrs[P.ATTRS_KEY]["Liberia"]["2018-19"])}}}
        for frame in (df, as_dicts):
            out = P.records_from_attrs(frame)
            assert [type(x) for x in out] == [PopulationRecord]
            assert out[0].universe_tag == "specialized"

    def test_an_unknown_field_is_rejected_rather_than_silently_kept(self):
        with pytest.raises(TypeError, match="unexpected field"):
            PopulationRecord(country="A", wave="1", universe_tag="specialized",
                             source_type="wb-catalog", confidence="high",
                             univese_tag="typo")

    def test_country_population_and_attrs_return_the_SAME_type(self):
        """The asymmetry #603 complained about, pinned shut."""
        from_property = population_records("Liberia")["2018-19"]
        from_attrs = _frame_with_record().attrs[P.ATTRS_KEY]["Liberia"]["2018-19"]
        assert type(from_property) is type(from_attrs) is PopulationRecord

class TestWaveResolution:

    def test_a_frame_with_no_t_axis_reports_every_wave(self):
        df = pd.DataFrame({"i": ["1"], "x": [1]})
        P.attach(df, "Ethiopia")
        assert set(df.attrs[P.ATTRS_KEY]["Ethiopia"]) == \
            set(population_records("Ethiopia"))
        assert df.attrs[P.ATTRS_RESOLUTION_KEY]["Ethiopia"] == "all-waves (no t axis)"

    def test_a_frame_reports_only_the_waves_it_holds(self):
        df = pd.DataFrame({"t": ["2011-12"], "x": [1]})
        P.attach(df, "Ethiopia")
        assert set(df.attrs[P.ATTRS_KEY]["Ethiopia"]) == {"2011-12"}

    def test_an_unmatched_t_is_marked_unrecorded_not_silently_dropped(self):
        """`unrecorded` != `not-stated`: nobody looked, vs looked and found none."""
        df = pd.DataFrame({"t": ["2011-12", "3011-12"], "x": [1, 2]})
        P.attach(df, "Ethiopia")
        got = df.attrs[P.ATTRS_KEY]["Ethiopia"]
        assert got["3011-12"]["universe_tag"] == P.UNRECORDED_TAG
        assert got["3011-12"]["universe_tag"] != "not-stated"

    def test_attach_never_raises(self):
        class Exploding(pd.DataFrame):
            @property
            def index(self):
                raise RuntimeError("boom")
        P.attach(Exploding(), "Ethiopia")     # must not propagate


# ---------------------------------------------------------------------------
# 5. Feature(): warn, never fence
# ---------------------------------------------------------------------------

class TestFeatureDoesNotFence:
    """The adjudication itself.  A warning is the maximum intervention."""

    def test_the_returned_data_is_value_identical_with_and_without_records(
            self, monkeypatch):
        """The strongest form of "metadata only".

        Build the same assembly twice -- once with the real records, once with
        the config emptied -- and require the frames to be equal.  If this ever
        fails, the population record has started changing data.
        """
        import lsms_library as ll
        from lsms_library.feature import Feature

        monkeypatch.setattr(ll, "Country", lambda name, **kw: _FakeCountry(name))
        f = Feature("household_roster")
        object.__setattr__(f, "_countries", ["Liberia", "Benin"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with_records = f(["Liberia", "Benin"], currency=None)

        monkeypatch.setattr(P, "population_records", lambda country: {})
        without = f(["Liberia", "Benin"], currency=None)

        pd.testing.assert_frame_equal(with_records, without)
        # Only the metadata differs: real tags vs the `unrecorded` sentinel.
        assert {r.universe_tag for r in P.records_from_attrs(with_records)} == \
            {"specialized", "national-all-households"}
        assert {r.universe_tag for r in P.records_from_attrs(without)} == \
            {P.UNRECORDED_TAG}

    def test_feature_attaches_and_warns_without_dropping_a_row(self, monkeypatch):
        """A two-country assembly with incomparable universes keeps both."""
        import lsms_library as ll
        from lsms_library.feature import Feature

        def fake_country(name, **kw):
            return _FakeCountry(name)

        monkeypatch.setattr(ll, "Country", fake_country)
        f = Feature("household_roster")
        object.__setattr__(f, "_countries", ["Liberia", "Benin"])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            out = f(["Liberia", "Benin"], currency=None)
        assert len(out) == 4                     # 2 rows from each -- none fenced
        assert set(out.index.get_level_values("country")) == {"Liberia", "Benin"}
        assert set(out.attrs[P.ATTRS_KEY]) == {"Liberia", "Benin"}
        msgs = [str(w.message) for w in caught
                if issubclass(w.category, PopulationHeterogeneityWarning)]
        assert len(msgs) == 1, msgs
        assert "specialized" in msgs[0] and "Nothing was dropped" in msgs[0]

    def test_feature_is_silent_on_a_homogeneous_pool(self, monkeypatch):
        import lsms_library as ll
        from lsms_library.feature import Feature

        monkeypatch.setattr(ll, "Country", lambda name, **kw: _FakeCountry(name))
        f = Feature("household_roster")
        object.__setattr__(f, "_countries", ["Benin", "Togo"])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            out = f(["Benin", "Togo"], currency=None)
        assert len(out) == 4
        assert not [w for w in caught
                    if issubclass(w.category, PopulationHeterogeneityWarning)]


class _FakeCountry:
    """Minimal stand-in: returns a real 2-row frame carrying a real record."""

    _WAVE = {"Liberia": "2018-19", "Benin": "2018-19", "Togo": "2018"}

    def __init__(self, name):
        self.name = name

    def household_roster(self, **kwargs):
        t = self._WAVE[self.name]
        df = pd.DataFrame({"t": [t, t], "i": ["1", "2"], "Age": [30, 40]}
                          ).set_index(["t", "i"])
        P.attach(df, self.name)
        return df
