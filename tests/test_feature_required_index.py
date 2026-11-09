"""GH1588: schema-invalid providers cannot vote away healthy household rows.

These exercise the actual Feature loop/concat/carriers with invented providers,
never a real Country getter. Required/optional declarations come from the schema.
"""
from copy import deepcopy
from itertools import permutations
import warnings

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import lsms_library as ll
from lsms_library.errors import LabelUnavailableError, UnitsUnavailableError
from lsms_library.feature import (Feature, _canonical_index_levels,
                                  _index_info_section, _required_index_levels)


def frame(table, name, *, missing=(), source=True, market=False, currency=None):
    # Native ordering differs deliberately from the declaration. Values are
    # distinctive across waves, households, sources and countries.
    levels = ["i", "t", "v"]
    if table.startswith("food_"):
        levels += ["j"]
        if table != "food_expenditures":
            levels += ["u"]
        if source:
            levels += ["s"]
    if table == "household_roster":
        levels += ["pid"]
    if market:
        levels.remove("v")
        levels += ["m"]
    if currency == "index":
        levels += ["currency"]
    levels = [x for x in levels if x not in missing]
    values = {"i": ["h2", "h1", "h1"], "t": ["w2", "w1", "w1"],
              "v": ["c2", "c1", "c1"], "j": ["rice", "beans", "beans"],
              "u": ["Kg", "Kg", "Kg"], "s": ["purchased", "produced", "inkind"],
              "pid": ["1", "2", "3"], "m": ["North", "South", None],
              "currency": ["XXX", "XXX", "XXX"]}
    if not source and table.startswith("food_"):
        # A no-source frame still has a unique household/item key.
        values["i"] = ["h2", "h1", "h3"]
    idx = pd.MultiIndex.from_arrays([values[x] for x in levels], names=levels)
    out = pd.DataFrame({("Expenditure" if table == "food_expenditures" else "value"): pd.array([17.25, None, 8.5], dtype="Float64"),
                        "answer": pd.array([1, 2, None], dtype="Int64"),
                        "Derivation": pd.array([f"{name}::fixture::one", None,
                                                f"{name}::fixture::two"], dtype="string")},
                       index=idx)
    out.attrs = {"recall": {name: {"w1": {"rows": 3, "recall_days": 7}}},
                 "derivations": {name: {f"{name}::fixture::one": {"rows": 1}}},
                 "unit_kg": {name: {"quintal": 100.0}},
                 "id_converted": True}
    return out


@pytest.fixture
def assemble(monkeypatch):
    calls = []

    def run(table, providers, order=None, **kwargs):
        class Provider:
            def __init__(self, name, trust_cache=False):
                self.name = name

            def load(self, currency=None, market=None, labels=None, units=None, numeraire=None):
                run.call_kwargs.append({"currency": currency, "market": market, "numeraire": numeraire})
                calls.append(self.name)
                value = providers[self.name]
                if isinstance(value, Exception):
                    raise value
                # Return the original: Feature must not mutate provider data.
                return value

        setattr(Provider, table, Provider.load)
        monkeypatch.setattr(ll, "Country", Provider)
        feature = Feature(table)
        feature._countries = list(providers)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = feature(order, **kwargs)
        return result, [str(w.message) for w in caught]

    run.calls = calls
    run.call_kwargs = []
    return run


def assert_kept(out, providers, names):
    assert set(out.index.get_level_values("country")) == set(names)
    for name in names:
        expected = providers[name]
        actual = out.xs(name, level="country")
        actual = actual.reorder_levels(expected.index.names)
        assert_frame_equal(actual, expected, check_exact=True)
        for col in expected:
            assert actual[col].array.equals(expected[col].array)
    for key in ("recall", "derivations", "unit_kg"):
        assert out.attrs[key] == {name: providers[name].attrs[key][name] for name in names}


TABLES = ["household_characteristics", "food_expenditures"]


def test_required_optins_do_not_change_canonical_or_other_table_contracts():
    assert _index_info_section()["required_index_levels"] == {
        "household_characteristics": ["t", "v", "i"],
        "plot_cultivation": ["t", "i", "cultivation_obs"],
        "farm_cost_operands": ["t", "i", "cost_obs"],
        "food_expenditures": ["t", "v", "i", "j"],
    }
    assert _required_index_levels("household_characteristics") == ["t", "v", "i"]
    assert _required_index_levels("food_expenditures") == ["t", "v", "i", "j"]
    assert _canonical_index_levels("household_characteristics") == ["t", "v", "i", "m"]
    assert _canonical_index_levels("food_expenditures") == []
    for table in ("food_acquired", "food_prices", "food_quantities", "interview_date",
                  "household_roster", "people_last7days"):
        assert _required_index_levels(table) == []


@pytest.mark.parametrize("table", TABLES)
@pytest.mark.parametrize("bad_name", ["A", "Z"])
def test_missing_v_cannot_win_in_either_order(assemble, table, bad_name):
    good_name = "Z" if bad_name == "A" else "A"
    providers = {bad_name: frame(table, bad_name, missing=["v"]),
                 good_name: frame(table, good_name)}
    snapshots = deepcopy(providers)
    for order in permutations(providers):
        out, messages = assemble(table, providers, list(order), currency=None)
        assert_kept(out, providers, [good_name])
        record = out.attrs["index_unavailable"][bad_name]
        assert record["missing"] == ["v"]
        assert record["index"] == list(providers[bad_name].index.names)
        assert record["rows"] == 3
        assert sum("missing required index" in m for m in messages) == 1
        assert not any("divergent index" in m for m in messages)
    for name, before in snapshots.items():
        assert_frame_equal(providers[name], before, check_exact=True)
        assert providers[name].attrs == before.attrs


@pytest.mark.parametrize("table,missing", [
    ("household_characteristics", ["t"]),
    ("household_characteristics", ["i"]),
    ("food_expenditures", ["j"]),
    ("food_expenditures", ["t", "v", "i"]),
])
def test_each_required_level_is_checked_not_only_v(assemble, table, missing):
    providers = {"bad": frame(table, "bad", missing=missing),
                 "good": frame(table, "good")}
    for order in permutations(providers):
        out, _ = assemble(table, providers, list(order), currency=None)
        assert_kept(out, providers, ["good"])
        assert set(out.attrs["index_unavailable"]["bad"]["missing"]) == set(missing)


def test_invalid_majority_and_multiple_missing_axes_do_not_vote(assemble):
    table = "household_characteristics"
    providers = {"bad1": frame(table, "bad1", missing=["v"]),
                 "bad2": frame(table, "bad2", missing=["v"]),
                 "bad3": frame(table, "bad3", missing=["i"]),
                 "good1": frame(table, "good1"), "good2": frame(table, "good2")}
    # Invalid countries also outnumber the healthy ones by ROWS.
    providers["bad1"] = pd.concat([providers["bad1"]] * 5)
    for order in (list(providers), list(reversed(providers))):
        out, messages = assemble(table, providers, order, currency=None)
        assert_kept(out, providers, ["good1", "good2"])
        assert set(out.attrs["index_unavailable"]) == {"bad1", "bad2", "bad3"}
        assert out.attrs["index_unavailable"]["bad1"]["rows"] == 15
        assert sum("missing required index" in m for m in messages) == 1


@pytest.mark.parametrize("table", TABLES)
def test_single_and_all_invalid_are_shaped_degrades(assemble, table):
    providers = {"bad1": frame(table, "bad1", missing=["v"]),
                 "bad2": frame(table, "bad2", missing=["i"])}
    for subset in (["bad1"], ["bad2"], ["bad1", "bad2"], ["bad2", "bad1"]):
        out, messages = assemble(table, providers, subset, currency=None)
        assert out.shape == (0, 0)
        assert list(out.index.names) == ["country", *_canonical_index_levels(table)]
        assert out.groupby("country").size().empty
        assert set(out.attrs) == {"index_unavailable"}
        assert set(out.attrs["index_unavailable"]) == set(subset)
        assert any("kept 0" in m for m in messages)


@pytest.mark.parametrize("table", ["household_characteristics", "food_expenditures"])
def test_empty_default_strict_subsets_and_no_call_for_unrequested_provider(assemble, table):
    providers = {"bad": frame(table, "bad", missing=["v"]),
                 "good": frame(table, "good"), "empty": frame(table, "empty").iloc[:0]}
    out, messages = assemble(table, providers, [], currency=None)
    assert out.shape == (0, 0) and not out.attrs
    assert not messages and assemble.calls == []
    out, _ = assemble(table, providers, ["good", "empty"], currency=None)
    assert_kept(out, providers, ["good"])
    assert "index_unavailable" not in out.attrs
    assert assemble.calls == ["good", "empty"]
    out, _ = assemble(table, providers, currency=None)  # default all
    assert_kept(out, providers, ["good"])
    assert set(out.attrs["index_unavailable"]) == {"bad"}


@pytest.mark.parametrize("source", [False, True])
def test_optional_source_is_not_required_or_fabricated(assemble, source):
    table = "food_expenditures"
    providers = {name: frame(table, name, source=source) for name in ("a", "b")}
    out, messages = assemble(table, providers, currency=None)
    assert_kept(out, providers, list(providers))
    assert ("s" in out.index.names) is source
    assert "index_unavailable" not in out.attrs
    assert not messages


def test_mixed_optional_sources_keep_existing_named_shape_degrade(assemble):
    table = "food_expenditures"
    providers = {"legacy": frame(table, "legacy", source=False),
                 "sourced": frame(table, "sourced", source=True)}
    for order in permutations(providers):
        out, messages = assemble(table, providers, list(order), currency=None)
        assert_kept(out, providers, ["legacy"])
        assert "index_unavailable" not in out.attrs
        assert any("divergent index shape" in m and "sourced" in m for m in messages)
        assert "s" not in out.index.names


def test_unregistered_feature_retains_its_modal_fallback(assemble):
    providers = {"a": frame("sample", "a"), "b": frame("sample", "b"),
                 "c": frame("sample", "c", missing=["v"])}
    for order in permutations(providers):
        out, messages = assemble("sample", providers, list(order), currency=None)
        assert_kept(out, providers, ["a", "b"])
        assert "index_unavailable" not in out.attrs
        assert any("modal index shape" in m for m in messages)


def test_null_v_values_are_kept_and_not_repaired(assemble):
    table = "household_characteristics"
    providers = {name: frame(table, name) for name in ("a", "b")}
    df = providers["a"].reset_index()
    df["v"] = pd.NA
    providers["a"] = df.set_index(providers["a"].index.names)
    out, messages = assemble(table, providers, currency=None)
    # Concatenation shares sibling level dictionaries, so xs(a)'s all-null
    # object v becomes string when b has recorded string v. Exact1fb Feature
    # shows the same representation; compare the complete assembly, not a
    # raw provider's unused dictionary (null-domain-proof.json in the report).
    assert_frame_equal(out, pd.concat(providers, names=["country"]), check_exact=True)
    for col in out:
        assert out[col].array.equals(pd.concat(providers, names=["country"])[col].array)
    assert len(out) == sum(map(len, providers.values()))
    assert out.xs("a").index.get_level_values("v").isna().all()
    assert not messages and "index_unavailable" not in out.attrs


def test_all_empty_providers_have_no_false_schema_exclusions(assemble):
    table = "food_expenditures"
    providers = {"empty": frame(table, "empty", missing=["v"]).iloc[:0]}
    out, messages = assemble(table, providers, currency=None)
    assert out.shape == (0, 0) and not out.attrs
    assert list(out.index.names) == ["country", *_canonical_index_levels(table)]
    assert messages == ["No data for food_expenditures in empty"]


@pytest.mark.parametrize("market", [None, "Region"])
def test_all_invalid_keeps_baseline_empty_shape_and_requested_requirements(assemble, market):
    table = "food_expenditures"
    providers = {"bad": frame(table, "bad", missing=["i"],
                               market=market is not None, currency="index")}
    out, _ = assemble(table, providers, market=market)
    expected = ["country"]  # unregistered canonical shape stays unchanged
    assert list(out.index.names) == expected
    assert out.shape == (0, 0) and out.groupby("country").size().empty
    assert out.attrs["index_unavailable"]["bad"]["missing"] == ["i"]
    required = ["t", "v", "i", "j", "currency"]
    if market:
        required.remove("v")
        required.append("m")
    assert out.attrs["index_unavailable"]["bad"]["required"] == required


def test_requested_currency_level_is_required(assemble):
    table = "food_expenditures"
    providers = {"good": frame(table, "good", currency="index"),
                 "bad": frame(table, "bad")}
    out, _ = assemble(table, providers)
    assert_kept(out, providers, ["good"])
    assert out.attrs["index_unavailable"]["bad"]["missing"] == ["currency"]


@pytest.mark.parametrize("table", ["household_characteristics", "food_expenditures"])
@pytest.mark.parametrize("currency", [None, "index"])
def test_market_replaces_required_v_with_required_m(assemble, table, currency):
    providers = {"good": frame(table, "good", market=True,
                               currency=currency if table.startswith("food_") else None),
                 "bad": frame(table, "bad", market=True, missing=["m"],
                              currency=currency if table.startswith("food_") else None)}
    for order in permutations(providers):
        out, _ = assemble(table, providers, list(order), market="Region", currency=currency)
        assert_kept(out, providers, ["good"])
        assert out.attrs["index_unavailable"]["bad"]["missing"] == ["m"]
        assert out.index.get_level_values("m").isna().sum() == 1


def test_required_v_as_column_does_not_license_promotion(assemble):
    providers = {"bad": frame("household_characteristics", "bad", missing=["v"]),
                 "good": frame("household_characteristics", "good")}
    providers["bad"]["v"] = ["c2", "c1", "c1"]
    out, _ = assemble("household_characteristics", providers, currency=None)
    assert_kept(out, providers, ["good"])
    assert out.attrs["index_unavailable"]["bad"]["missing"] == ["v"]


def test_index_failure_coexists_with_existing_label_unit_and_build_degrades(assemble):
    providers = {"bad": frame("food_expenditures", "bad", missing=["v"]),
                 "label": LabelUnavailableError("no curated column"),
                 "unit": UnitsUnavailableError(units="unitprice", missing=["Price"]),
                 "failed": RuntimeError("invented provider failure"),
                 "good": frame("food_expenditures", "good")}
    out, messages = assemble("food_expenditures", providers, currency=None, labels="Aggregate", units="unitprice")
    assert_kept(out, providers, ["good"])
    assert out.attrs["labels_unavailable"] == ["label"]
    assert out.attrs["units_unavailable"] == ["unit"]
    assert set(out.attrs["index_unavailable"]) == {"bad"}
    assert any("RuntimeError: invented provider failure" in m for m in messages)


def test_declared_alias_and_sentinels_still_align(assemble):
    canonical = _canonical_index_levels("crop_production")
    levels = ["t", "v", "i", "plot", "crop"]
    providers = {}
    for name in ("a", "b"):
        df = pd.DataFrame({"Quantity": [2.0, 3.0], "u": ["Kg", None]},
                          index=pd.MultiIndex.from_tuples(
                              [("w1", "c1", "h1", "p1", "beans"),
                               ("w1", "c1", "h1", "p2", "rice")], names=levels))
        providers[name] = df
    before = deepcopy(providers)
    out, _ = assemble("crop_production", providers, currency=None)
    assert len(out) == 4 and out.index.is_unique
    assert list(out.index.names) == ["country", *canonical]
    assert set(out.index.get_level_values("u")) == {"Kg", "Unknown"}
    assert out["Quantity"].tolist() == [2.0, 3.0, 2.0, 3.0]
    assert out.attrs["canonical_alignment"]["promoted"] == {"a": {"u": 1}, "b": {"u": 1}}
    for name in providers:
        assert_frame_equal(providers[name], before[name])


def test_untargeted_single_visit_optin_retains_baseline_fabrication(assemble):
    providers = {"good": pd.DataFrame({"Int_t": ["2020-01-01"]},
                 index=pd.MultiIndex.from_tuples([("w1", "c1", "h1")], names=["t", "v", "i"]))}
    providers["bad"] = providers["good"].droplevel("v")
    out, _ = assemble("interview_date", providers, currency=None)
    assert set(out.index.get_level_values("country")) == {"good", "bad"}
    assert out.xs("bad").index.get_level_values("v").isna().all()
    assert out.index.get_level_values("visit").isna().all()
    assert "index_unavailable" not in out.attrs


@pytest.mark.parametrize('sources,multipliers,keep_source', [
    ([False, False, True], [1, 1, 1], False),
    ([False, True, True], [1, 1, 1], True),
    ([False, True], [1, 1], False),
    ([False, True], [1, 2], True),
    ([False, True], [2, 1], False),
    ([False, False], [1, 1], False),
    ([True, True], [1, 1], True),
])
def test_valid_optional_source_cohorts_preserve_baseline_modal_winner(
        assemble, sources, multipliers, keep_source):
    # No canonical registration: optional s must not gain a new preference.
    names = ['provider' + str(n) for n in range(len(sources))]
    providers = {n: pd.concat([frame('food_expenditures', n, source=s)] * k)
                 for n, s, k in zip(names, sources, multipliers)}
    snapshots = deepcopy(providers)
    expected = [n for n, s in zip(names, sources) if s is keep_source]
    for order in permutations(names):
        out, messages = assemble('food_expenditures', providers, list(order), currency=None)
        assert_kept(out, providers, expected)
        assert ('s' in out.index.names) is keep_source
        assert 'index_unavailable' not in out.attrs
        assert not any('missing required index' in m for m in messages)
        assert any('divergent index shape' in m for m in messages) is (len(set(sources)) > 1)
    for n in names:
        assert_frame_equal(providers[n], snapshots[n], check_exact=True)
        assert providers[n].attrs == snapshots[n].attrs


@pytest.mark.parametrize('has_m', [[False, False, True], [False, True, True],
                                   [False, True], [False, False], [True, True]])
def test_valid_optional_market_detail_keeps_existing_canonical_preference(assemble, has_m):
    providers = {}
    for n, present in enumerate(has_m):
        name = 'provider' + str(n)
        df = frame('household_characteristics', name)
        if present:
            df = df.assign(m=['North', 'South', None]).set_index('m', append=True)
        providers[name] = df
    expected = [name for name, present in zip(providers, has_m)
                if present is any(has_m)]
    for order in permutations(providers):
        out, messages = assemble('household_characteristics', providers, list(order), currency=None)
        assert_kept(out, providers, expected)
        assert 'index_unavailable' not in out.attrs
        assert not any('missing required index' in m for m in messages)


@pytest.mark.parametrize('table,missing', [('people_last7days', ['pid']),
                                          ('food_prices', ['s']),
                                          ('food_quantities', ['s']),
                                          ('household_roster', ['v'])])
def test_untargeted_reduced_provider_contracts_are_not_reinterpreted(assemble, table, missing):
    providers = {'only': frame(table, 'only', missing=missing)}
    out, messages = assemble(table, providers, currency=None)
    assert_kept(out, providers, ['only'])
    assert 'index_unavailable' not in out.attrs
    assert not any('missing required index' in m for m in messages)


@pytest.mark.parametrize('dtype', ['object', 'string'])
def test_present_null_v_and_literal_source_identity_survive(assemble, dtype):
    df = frame('food_expenditures', 'good').reset_index()
    df['v'] = pd.array([pd.NA, 'c1', 'c1'], dtype=dtype)
    providers = {'good': df.set_index(['i', 't', 'v', 'j', 's']),
                 'bad': frame('food_expenditures', 'bad', missing=['v'])}
    out, _ = assemble('food_expenditures', providers, currency=None)
    assert_kept(out, providers, ['good'])
    assert out.index.get_level_values('s').tolist() == ['purchased', 'produced', 'inkind']
    assert out.index.get_level_values('v').isna().sum() == 1


@pytest.mark.parametrize('currency', [None, 'column'])
def test_currency_axis_not_required_when_request_does_not_create_it(assemble, currency):
    providers = {'good': frame('food_expenditures', 'good')}
    out, _ = assemble('food_expenditures', providers, currency=currency)
    assert_kept(out, providers, ['good'])
    assert 'currency' not in out.index.names and 'index_unavailable' not in out.attrs


def test_empty_request_does_not_make_optional_axes_mandatory(assemble):
    out, messages = assemble('food_expenditures', {'bad': frame('food_expenditures', 'bad', missing=['v'])}, [], currency='index', market='Region')
    assert out.empty and list(out.index.names) == ['country']
    assert not out.attrs and not messages and not assemble.calls


def test_numeraire_requires_existing_currency_axis_and_is_forwarded(assemble, monkeypatch):
    import lsms_library.conversion as conversion
    monkeypatch.setattr(conversion, 'conversion_targets', lambda: ['invented_basis'])
    providers = {'good': frame('food_expenditures', 'good', currency='index'),
                 'bad': frame('food_expenditures', 'bad')}
    out, _ = assemble('food_expenditures', providers, currency='column', numeraire='invented_basis')
    assert_kept(out, providers, ['good'])
    assert out.attrs['index_unavailable']['bad']['missing'] == ['currency']
    assert all(k['numeraire'] == 'invented_basis' and k['currency'] is None
               for k in assemble.call_kwargs)


def test_required_guard_does_not_reduce_extra_source_detail_or_screen_money(assemble):
    df = frame('food_expenditures', 'good')
    df = df.assign(native_record=['first', 'second', 'third']).set_index('native_record', append=True)
    df['Expenditure'] = pd.array([-3.25, pd.NA, 8.5], dtype='Float64')
    providers = {'good': df, 'bad': df.droplevel('v')}
    snapshots = deepcopy(providers)
    out, _ = assemble('food_expenditures', providers, currency=None)
    assert_kept(out, providers, ['good'])
    assert out.index.get_level_values('native_record').tolist() == ['first', 'second', 'third']
    assert out.Expenditure.array.equals(df.Expenditure.array)
    for name in providers:
        assert_frame_equal(providers[name], snapshots[name], check_exact=True)
