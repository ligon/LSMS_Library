"""The derived-values mechanism (``lsms_library.derivations``).

A served number that is a CONSTRUCTION from raw answers, not a survey answer,
must be (1) retrievable raw by its original names, (2) labelled on the row in
a way that survives concat / cache / ``Feature()``, (3) a named function with
its assumptions and their evidence in a registry, and (4) one construction
per parquet.  ``SkunkWorks/derived_values.org``.  This module pins the
mechanism; ``tests/test_ghanalss_12b.py`` pins the first instance.

Two tiers, following ``test_recall_record.py``:

* **Config-only** (everything but ``TestDelivered``) -- the loader, the key
  grammar, the attrs summary, the record's JSON / attrs behaviour, and the
  drift test in both directions.  No cache, no microdata.
* **Data-gated** (``TestDelivered``) -- that the summary actually arrives on
  a built table; skipped when the cache is cold.
"""
from __future__ import annotations

import copy
import pickle
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.derivations import (
    BASES, TABLE_COLUMNS, REQUIRED_FIELDS, DerivationRecord, attach, derivation_records,
    derivations_table, derive_function_names, framework_records, merge_attrs,
    parse_key, records_for, resolve_callable,
)
from lsms_library.paths import countries_root

#: Countries carrying a `_/derivations.yml`.
COUNTRIES = sorted(
    p.parent.parent.name
    for p in Path(countries_root()).glob("*/_/derivations.yml"))

GOOD = dict(
    rule="Expenditure = A * B",
    function="lsms_library.transformations:food_kg_factors",
    inputs="lsms_library.transformations:food_kg_factors",
    raw_variables=["X.A", "X.B"],
    assumptions=[dict(text="the window is a fortnight", basis="questionnaire",
                      evidence="manual p.70"),
                 dict(text="a random fortnight of the year", basis="modelling-choice")],
    waves=["1987-88"], columns=["Expenditure"], rows={"s": "produced"},
    since="2026-09-11",
)


def _rec(key="C::food_acquired::x", country="C", **over):
    block = {**GOOD, **over}
    for k, v in list(over.items()):
        if v is None:
            block.pop(k, None)
    return DerivationRecord.from_config(key, block, country)


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------

class TestKeys:
    def test_three_slots(self):
        assert parse_key("GhanaLSS::food_acquired::12b-fortnight") == (
            "GhanaLSS", "food_acquired", "12b-fortnight")

    def test_empty_slots_mean_all(self):
        assert parse_key("::household_roster::age-dob") == ("", "household_roster", "age-dob")
        assert parse_key("Uganda::::a5aq8-div100") == ("Uganda", "", "a5aq8-div100")

    @pytest.mark.parametrize("bad", ["a::b", "a::b::c::d", "a::b::", "a::b::c+d", 7])
    def test_malformed_keys_are_refused(self, bad):
        with pytest.raises(ValueError):
            parse_key(bad)

    def test_key_country_must_match_the_file(self):
        with pytest.raises(ValueError, match="does not match the file"):
            DerivationRecord.from_config("Uganda::t::x", GOOD, "GhanaLSS")
        with pytest.raises(ValueError, match="does not match the file"):
            DerivationRecord.from_config("GhanaLSS::t::x", GOOD, None)   # framework file
        assert DerivationRecord.from_config("::t::x", GOOD, None).country is None


# ---------------------------------------------------------------------------
# the loader's refusals
# ---------------------------------------------------------------------------

class TestLoaderRefuses:
    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_each_required_field(self, field):
        with pytest.raises(ValueError, match="missing"):
            _rec(**{field: None})

    def test_an_assumption_without_a_basis(self):
        with pytest.raises(ValueError, match="no basis"):
            _rec(assumptions=[dict(text="it is a fortnight")])

    def test_a_basis_outside_the_vocabulary(self):
        with pytest.raises(ValueError, match="not one of"):
            _rec(assumptions=[dict(text="x", basis="vibes")])

    def test_the_vocabulary_is_this_modules_own(self):
        """`modelling-choice` is here and NOT in recall's ladder; do not merge them."""
        from lsms_library.recall import BASIS_LADDER
        assert "modelling-choice" in BASES
        assert "modelling-choice" not in BASIS_LADDER
        assert "not-recorded" not in BASES

    @pytest.mark.parametrize("field", ["function", "inputs"])
    def test_callables_are_dotted(self, field):
        with pytest.raises(ValueError, match="module:function"):
            _rec(**{field: "no_colon_here"})

    def test_unknown_fields_are_refused(self):
        with pytest.raises(ValueError, match="unknown field"):
            _rec(options={"window": 14})

    def test_a_good_record_normalises(self):
        r = _rec()
        assert r.key == "C::food_acquired::x"
        assert r.country == "C" and r.table == "food_acquired" and r.name == "x"
        assert r.bases == ("questionnaire", "modelling-choice")
        assert r.covers_wave("1987-88") and not r.covers_wave("1988-89")
        assert r.validated_against is None            # absent optional -> None


# ---------------------------------------------------------------------------
# the record rides on attrs
# ---------------------------------------------------------------------------

class TestRecordOnAttrs:
    def test_immutable_and_hashable(self):
        r = _rec()
        with pytest.raises(TypeError):
            r["rule"] = "other"
        assert hash(r) == hash(DerivationRecord(dict(r)))

    def test_deepcopy_and_pickle(self):
        r = _rec()
        assert copy.deepcopy(r) == r and pickle.loads(pickle.dumps(r)) == r

    def test_json_round_trip_through_to_parquet(self, tmp_path):
        """pandas serialises attrs with a bare json.dumps; lists must stay lists."""
        r = _rec()
        df = pd.DataFrame({"x": [1.0]})
        df.attrs["derivation"] = r
        df.to_parquet(tmp_path / "x.parquet")
        back = pd.read_parquet(tmp_path / "x.parquet")
        assert back.attrs["derivation"] == r
        assert back.attrs["derivation"]["assumptions"] == r.assumptions

    def test_merge_preserves_attrs_when_both_sides_carry_the_same_record(self):
        """The population precedent: attrs survive only when every input agrees."""
        r = _rec()
        idx = pd.Index(["a", "b"], name="i")
        left = pd.DataFrame({"x": [1, 2]}, index=idx)
        right = pd.DataFrame({"y": [3, 4]}, index=idx)
        left.attrs["d"] = r
        right.attrs["d"] = DerivationRecord(dict(r))     # equal by value, not identity
        merged = left.merge(right, left_index=True, right_index=True)
        assert merged.attrs.get("d") == r
        right.attrs["d"] = _rec(rule="different")
        assert not left.merge(right, left_index=True, right_index=True).attrs


# ---------------------------------------------------------------------------
# the attrs summary
# ---------------------------------------------------------------------------

def _frame(values):
    idx = pd.MultiIndex.from_tuples([("w", str(i)) for i in range(len(values))],
                                    names=["t", "i"])
    return pd.DataFrame({"Expenditure": range(len(values)), "Derivation": values},
                        index=idx)


class TestAttach:
    def test_counts_per_key_and_splits_joined_keys(self):
        df = _frame(["a::t::x", None, "a::t::x+a::t::y", pd.NA, "a::t::y"])
        attach(df, "a", "t")
        got = df.attrs["derivations"]["a"]
        assert got["a::t::x"] == {"rows": 2, "columns": [], "in": "t"}
        assert got["a::t::y"] == {"rows": 2, "columns": [], "in": "t"}

    def test_nothing_to_say_attaches_nothing(self):
        df = _frame([None, None])
        attach(df, "a", "t")
        assert "derivations" not in df.attrs
        df2 = pd.DataFrame({"Expenditure": [1.0]})
        attach(df2, "a", "t")
        assert "derivations" not in df2.attrs

    def test_attach_is_never_fatal(self):
        class Broken:
            attrs = {}
            columns = property(lambda self: (_ for _ in ()).throw(RuntimeError("x")))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            attach(Broken(), "a", "t")           # must not raise

    def test_merge_attrs_unions_countries(self):
        a, b = _frame(["a::t::x"]), _frame(["b::t::z"])
        attach(a, "a", "t"); attach(b, "b", "t")
        merged = merge_attrs([a, b, _frame([None])])
        assert set(merged) == {"a", "b"}
        assert merged["a"]["a::t::x"]["rows"] == 1


# ---------------------------------------------------------------------------
# the registry on disk, and the drift test in both directions
# ---------------------------------------------------------------------------

def _all_records():
    for c in COUNTRIES:
        for key, r in derivation_records(c).items():
            yield c, key, r
    for key, r in framework_records().items():
        yield None, key, r


class TestRegistry:
    def test_the_framework_file_loads(self):
        assert isinstance(framework_records(), dict)
        assert all(r.country is None for r in framework_records().values())

    def test_every_country_file_loads_with_its_own_country(self):
        for c in COUNTRIES:
            for key, r in derivation_records(c).items():
                assert r.country == c, key

    def test_every_registered_callable_resolves(self):
        """Direction 1 of the drift test: the YAML names real functions."""
        bad = {}
        for c, key, r in _all_records():
            for field in ("function", "inputs"):
                try:
                    assert callable(resolve_callable(getattr(r, field)))
                except Exception as exc:                       # noqa: BLE001
                    bad[f"{key}.{field}"] = f"{type(exc).__name__}: {exc}"
        assert not bad, bad

    def test_every_derive_function_in_a_country_module_is_registered(self):
        """Direction 2: a `derive_*` in a country module without a registry
        entry is an unregistered derivation -- the defect the mechanism exists
        to make visible.  Read with `ast`, never imported."""
        registered = {r.function for _, _, r in _all_records()}
        unregistered = []
        for path in sorted(Path(countries_root()).glob("*/_/*.py")):
            country = path.parent.parent.name
            for fn in derive_function_names(path):
                spec = f"lsms_library.countries.{country}._.{path.stem}:{fn}"
                if spec not in registered:
                    unregistered.append(spec)
        assert not unregistered, (
            f"derive_* functions with no entry in their country's "
            f"_/derivations.yml: {unregistered}")

    def test_corpus_table_has_one_row_per_key(self):
        tbl = derivations_table()
        assert tbl.index.is_unique
        assert list(tbl.columns) == TABLE_COLUMNS[1:]
        n = sum(1 for _ in _all_records())
        assert len(tbl) == n

    def test_records_for_filters_by_table(self):
        for c in COUNTRIES:
            for key, r in records_for(c, "food_acquired").items():
                assert r.table in (None, "food_acquired"), key

    def test_the_column_is_declared_optional_in_the_canonical_schema(self):
        from lsms_library.feature import _load_global_columns
        cols = _load_global_columns()
        decl = cols["food_acquired"]["Derivation"]
        assert decl["type"] == "str" and decl.get("optional") is True
        assert not decl.get("required")

    def test_nothing_in_a_data_scheme_names_a_derivation(self):
        """`_SCHEME_NON_COLUMN_KEYS` trap: a `derivation:` key in data_scheme.yml
        would be read as a required column.  The registry lives in its own file."""
        from lsms_library.yaml_utils import load_yaml
        offenders = []
        for path in Path(countries_root()).glob("*/_/data_scheme.yml"):
            scheme = (load_yaml(path) or {}).get("Data Scheme") or {}
            for table, entry in scheme.items():
                if isinstance(entry, dict) and any(
                        str(k).lower().startswith("derivation") for k in entry):
                    offenders.append(f"{path.parent.parent.name}/{table}")
        assert not offenders, offenders


# ---------------------------------------------------------------------------
# data-gated
# ---------------------------------------------------------------------------

class TestDelivered:
    def test_summary_arrives_on_a_built_table(self):
        if "GhanaLSS" not in COUNTRIES:
            pytest.skip("GhanaLSS carries no derivations.yml")
        try:
            import lsms_library as ll
            fa = ll.Country("GhanaLSS").food_acquired()
        except Exception as e:                    # pragma: no cover - no microdata
            pytest.skip(f"GhanaLSS food_acquired not buildable here: {e}")
        summ = fa.attrs["derivations"]["GhanaLSS"]
        key = "GhanaLSS::food_acquired::12b-fortnight"
        assert summ[key]["in"] == "food_acquired"
        assert summ[key]["rows"] == int(fa["Derivation"].notna().sum()) > 0
        assert summ[key]["columns"] == ["Expenditure", "Quantity"]
        assert str(fa["Derivation"].dtype) == "string"      # the canonical `str` cast
