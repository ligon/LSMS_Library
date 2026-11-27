"""GH #1102: exact read-time roster regressions using invented L2 operands."""
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import Country, Feature, country as core, local_tools


NA = pd.NA
UNKNOWN = (NA, NA, NA)
OTHER_RELATIVE = (NA, NA, "consanguineal")
UNRELATED = (0, 0, "unrelated")
HEAD = (0, 0, "consanguineal")
FIELDS = ("Sex", "Age", "Relationship", "Generation", "Distance", "Affinity")
DTYPES = ("string", "Float64", "string", "Int64", "Int64", "string")


@pytest.mark.parametrize(
    "name,baseline,control,bare,named",
    [
        ("Malawi", "2004-05", "2010-11", "Other", [
            ("h1", "2", "Other non-relative", UNRELATED),
            ("h2", "1", "Other relative", OTHER_RELATIVE),
        ]),
        ("Uganda", "2005-06", "2009-10", "other", [
            ("h1", "2", "Non-relative", UNRELATED),
            ("h2", "1", "Other relatives", OTHER_RELATIVE),
            ("h3", "1", "step child", (-1, 0, "step")),
            ("h3", "2", "Sister/Brother of head or spouse", (0, 1, NA)),
        ]),
    ],
    ids=["Malawi-IHS2", "Uganda-UNHS"],
)
def test_scoped_other_through_country_and_feature(monkeypatch, tmp_path, name,
                                                  baseline, control, bare, named):
    # The same household and person keys recur in the two waves deliberately.
    # Neither the bare spelling nor household ID can stand in for row wave.
    rows = [(baseline, "h1", "1", bare, UNKNOWN),
            *((baseline, *row) for row in named),
            (baseline, "h2", "2", "Head", HEAD),
            (baseline, "h4", "1", NA, UNKNOWN),
            (control, "h1", "1", bare, UNRELATED),
            (control, "h2", "2", "Head", HEAD)]
    index = pd.MultiIndex.from_tuples([(t, i, pid) for t, i, pid, _, _ in rows],
                                      names=["t", "i", "pid"])
    raw = pd.DataFrame({"Sex": "male", "Age": 23,
                        "Relationship": [label for _, _, _, label, _ in rows]}, index=index)
    sample_v = pd.DataFrame(
        [(t, i, f"cluster-{t}-{i}") for t, i in dict.fromkeys((r[0], r[1]) for r in rows)],
        columns=["t", "i", "v"],
    ).set_index(["t", "i"])

    def expected(waves, feature=False):
        subset = [row for row in rows if row[0] in waves]
        keys = [(i, t, f"cluster-{t}-{i}", pid) for t, i, pid, _, _ in subset]
        names = ["i", "t", "v", "pid"]
        if feature:
            keys = [(name, t, v, i, pid) for i, t, v, pid in keys]
            names = ["country", "t", "v", "i", "pid"]
        idx = pd.MultiIndex.from_arrays(
            [pd.array(values, dtype="string" if level == "v" else "str")
             for level, values in zip(names, zip(*keys))], names=names,
        )
        return pd.DataFrame(
            {field: pd.array(values, dtype=dtype) for field, dtype, values in
             zip(FIELDS, DTYPES, zip(*[("M", 23, label, *kin) for _, _, _, label, kin in subset]))},
            index=idx,
        )

    # Keep the production call, L2 read gate, coverage selection, v join and
    # finalizer. Stand in for the hash/footer/decoder and the sample companion.
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    for var in ("LSMS_NO_CACHE", "LSMS_BUILD_BACKEND", "LSMS_CLUSTER_STRICT"):
        monkeypatch.delenv(var, raising=False)
    core.data_root.cache_clear()
    path = core.data_root(name) / "var/household_roster.parquet"
    exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda self: self == path or exists(self))
    init = Country.__init__
    decodes = []

    def init_with_companions(self, country, *args, **kwargs):
        assert country == name
        init(self, country, *args, **kwargs)
        self._panel_ids_attempted = True  # No re-keying in these four waves.
        self._sample_v_cache = sample_v

    def decode(cache_path, **kwargs):
        assert cache_path == path
        decodes.append(cache_path)
        return raw.copy(), None, None

    def no_source(*args, **kwargs):
        pytest.fail("The invented L2 hit must not enter a source or build path")

    monkeypatch.setattr(Country, "__init__", init_with_companions)
    monkeypatch.setattr(Country, "_table_cache_hash", lambda *a: "invented-hash")
    monkeypatch.setattr(core, "cache_freshness", lambda *a: "fresh")
    monkeypatch.setattr(core, "read_parquet_cache_waves", lambda p: [baseline, control])
    monkeypatch.setattr(core, "_read_managed_parquet_cache", decode)
    for attr in ("read_parquet_build_drops", "read_parquet_grain_audit",
                 "read_parquet_datetime_audit"):
        monkeypatch.setattr(core, attr, lambda p: None)
    for attr in ("get_dataframe", "to_parquet", "_log_issue"):
        monkeypatch.setattr(core, attr, no_source)
    monkeypatch.setattr(core.Wave, "grab_data", no_source)
    monkeypatch.setattr(local_tools, "get_dataframe", no_source)
    monkeypatch.setattr(local_tools, "to_parquet", no_source)
    monkeypatch.setattr(type(local_tools.DVCFS), "fs", property(no_source))

    def check(call, want):
        n = len(decodes)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            got = call()
        assert decodes[n:] == [path]
        assert not [w for w in caught if "Unknown relationship labels" in str(w.message)]
        pd.testing.assert_frame_equal(got, want, check_exact=True, check_index_type=True)

    country = Country(name)
    for waves in ([baseline], [control], [baseline, control]):
        check(lambda: country.household_roster(waves=waves), expected(waves))
    check(lambda: Feature("household_roster")([name], waves=[baseline]),
          expected([baseline], feature=True))

    kin_fields = list(FIELDS[2:])
    want = expected([baseline])[kin_fields].copy()
    key = ("h1", baseline, f"cluster-{baseline}-h1", "1")
    for field, value in zip(FIELDS[3:], UNRELATED):
        want.at[key, field] = value
    other_method = country._finalize_result(
        raw.loc[[baseline], :].copy(), {}, "individual_education",
    )
    pd.testing.assert_frame_equal(other_method[kin_fields], want, check_exact=True)


def test_scoped_match_does_not_hide_a_true_unknown():
    label = "Invented #1102 unmapped label"
    idx = pd.MultiIndex.from_tuples([("2004-05", p) for p in ("1", "2", "3", "4")],
                                    names=["t", "pid"])
    raw = pd.DataFrame({"Relationship": ["Other", label, NA, "other"]}, index=idx)
    with pytest.warns(UserWarning) as caught:
        got = core._expand_kinship(raw, roster_country="Malawi")
    assert [str(w.message) for w in caught] == [
        f"Unknown relationship labels (add to kinship.yml): {[label]!r}"
    ]
    expected = raw.astype("string").assign(
        Generation=pd.array([NA, NA, NA, 0], dtype="Int64"),
        Distance=pd.array([NA, NA, NA, 0], dtype="Int64"),
        Affinity=pd.array([NA, NA, NA, "unrelated"], dtype="string"),
    )
    pd.testing.assert_frame_equal(got, expected, check_exact=True)


@pytest.mark.parametrize("roster_country,with_wave", [("Uganda", True), ("Malawi", False)])
def test_bare_other_keeps_fallback_without_matching_provenance(roster_country, with_wave):
    idx = pd.Index(["2004-05"], name="t" if with_wave else "pid")
    raw = pd.DataFrame({"Relationship": ["Other"]}, index=idx)
    want = raw.astype("string").assign(
        Generation=pd.array([0], dtype="Int64"),
        Distance=pd.array([0], dtype="Int64"),
        Affinity=pd.array(["unrelated"], dtype="string"),
    )
    pd.testing.assert_frame_equal(core._expand_kinship(raw, roster_country=roster_country),
                                  want, check_exact=True)
