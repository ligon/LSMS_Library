"""GH #823: Niger 2018-19 community_prices from s05_co_ner2018.dta.

Synthetic s05 / s07b frames through the REAL helpers the wave script calls
(``niger._s05_code_maps``, ``niger._s05_community_prices``,
``niger._finish_community_prices``) and the REAL registered derivation
(``Niger/2018-19/_/mapping.py:derive_s05_one_item_per_price``), so the rules
@ligon decided (D1-D7, 2026-10-22) are pinned without data:

* unit-size codes ``releveID = 10 * household unit + size``: size 0 serves the
  household unit label, a sized code ``<unit> (<Size>)``, and the size comes
  from the DIGIT (Louche 1252 = Moyen, 1253 = Grand) whatever the s05 label says;
* items join BY CODE to the household label, narrower products included, and
  keep the s05 label as ``Description``;
* s05q03 / s05q04 -> obs 1 / 2; a duplicated key's second record -> obs 3 / 4;
  a byte-identical row and a second record's repeated slot are dropped;
* ``Quantity = 1`` with the registry key on every row.

The data-gated class checks the real build and that ``derivation_inputs``
joins the served rows.
"""
from __future__ import annotations

import importlib
import sys
import warnings

import pandas as pd
import pytest

from lsms_library.paths import countries_root

KEY = "Niger::community_prices::one-item-per-price"
INDEX = ["t", "v", "j", "u", "variety", "visit", "obs"]

HH_CATEGORIES = {
    # raw household labels, double-encoded the way s07b_me_ner2018 ships them
    "s07bq01": {7: "Mil", 41: "Poisson sÃ©chÃ©", 67: "PastÃ¨que, Melon",
                130: "ThÃ©"},
    "s07bq03b": {100: "Kg", 108: "BoÃ®te de tomate", 125: "Louche",
                 143: "Tas", 512: "Tiya"},
    "s07bq03c": {0: "Taille unique", 1: "Petit", 2: "Moyen", 3: "Grand",
                 4: "Quart", 5: "Demi", 6: "Entier"},
}
# harmonize_food keys on the RAW label (as food_acquired's YAML mapping does);
# the wave's mojibake decode runs after it.
ITEM_MAP = {"Poisson sÃ©chÃ©": "Poisson séché"}
UNIT_MAP = {"Kg": "Kg", "Tiya": "Tiya"}


@pytest.fixture(scope="module")
def niger():
    sys.path.insert(0, str(countries_root() / "Niger" / "_"))
    try:
        import niger as mod
    finally:
        sys.path.pop(0)
    return mod


@pytest.fixture(scope="module")
def wave_mapping():
    """2018-19/_/mapping.py, resolved by path exactly as the registry does."""
    derivations = importlib.import_module("lsms_library.derivations")
    rec = derivations.derivation_records("Niger")[KEY]
    return (derivations.resolve_callable(rec.function),
            derivations.resolve_callable(rec.inputs), rec)


@pytest.fixture(scope="module")
def decode(wave_mapping):
    fn = wave_mapping[0]
    return sys.modules[fn.__module__]._decode_mojibake


class TestUnitLabels:
    @pytest.mark.parametrize("code, expected", [
        (1000, "Kg"),                    # size 0: the bare household unit
        (5120, "Tiya"),
        (1080, "Boîte de tomate"),       # size 0, household label decoded
        (1431, "Tas (Petit)"),           # sized: Uganda's "Unit (Size)" form
        (5124, "Tiya (Quart)"),
        (5126, "Tiya (Entier)"),
        (1252, "Louche (Moyen)"),        # s05 labels 1252 "grand": digit wins
        (1253, "Louche (Grand)"),        # s05 labels 1253 "moyen": digit wins
    ])
    def test_releve_code_is_household_unit_plus_size(self, niger, decode, code, expected):
        got = niger._s05_unit_label(code, HH_CATEGORIES["s07bq03b"],
                                    HH_CATEGORIES["s07bq03c"], UNIT_MAP, decode)
        assert got == expected

    def test_a_sized_unit_is_never_the_bare_unit(self, niger, decode):
        bare = niger._s05_unit_label(1430, HH_CATEGORIES["s07bq03b"],
                                     HH_CATEGORIES["s07bq03c"], UNIT_MAP, decode)
        sized = {niger._s05_unit_label(c, HH_CATEGORIES["s07bq03b"],
                                       HH_CATEGORIES["s07bq03c"], UNIT_MAP, decode)
                 for c in (1431, 1432, 1433)}
        assert bare == "Tas" and bare not in sized and len(sized) == 3

    def test_an_unknown_unit_code_raises(self, niger, decode):
        with pytest.raises(KeyError):
            niger._s05_unit_label(9990, HH_CATEGORIES["s07bq03b"],
                                  HH_CATEGORIES["s07bq03c"], UNIT_MAP, decode)


class TestItemsByCode:
    def test_code_bridge_takes_the_household_label(self, niger, decode):
        j, u = niger._s05_code_maps([7, 41, 67, 130], [1000, 1431], HH_CATEGORIES,
                                    ITEM_MAP, UNIT_MAP, decode)
        # harmonize_food hit, then decode; and the three narrower s05 products
        # take the household's broader label (D3).
        assert j == {7: "Mil", 41: "Poisson séché", 67: "Pastèque, Melon",
                     130: "Thé"}
        assert u == {1000: "Kg", 1431: "Tas (Petit)"}

    def test_two_codes_on_one_served_label_raise(self, niger, decode):
        cats = dict(HH_CATEGORIES, s07bq01={1: "Mil", 2: "Mil"})
        with pytest.raises(ValueError, match="share a served label"):
            niger._s05_code_maps([1, 2], [1000], cats, ITEM_MAP, UNIT_MAP, decode)


def _s05(rows):
    """(codes, labels) frames in the s05_co_ner2018 layout."""
    cols = ["grappe", "vague", "Existe", "s05q01", "s05q02", "s05q03", "s05q04"]
    codes = pd.DataFrame(rows, columns=cols)
    labels = codes.copy()
    labels["s05q01"] = codes["s05q01"].map(
        {7: "Mil", 41: "poisson séché", 130: "Thé vert"})
    labels["Existe"] = "Oui"
    return codes, labels


ROWS = [
    # grappe vague Existe prod unit   q03    q04
    (1,      2,    1,     7,   1000,  200.0, 200.0),   # equal readings: both served
    (1,      2,    1,     130, 1431,  100.0,  85.0),   # the 0.85 pattern: as recorded
    (2,      1,    1,     41,  1431,  400.0, 340.0),   # first record of a key
    (2,      1,    1,     41,  1252,  150.0, 150.0),   # Louche 1252
    (2,      1,    1,     41,  1431,  400.0, 400.0),   # second record: slot 1 repeats
    (2,      1,    1,     41,  1252,  150.0, 150.0),   # byte-identical copy
]


@pytest.fixture(scope="module")
def built(niger, decode, wave_mapping):
    codes, labels = _s05(ROWS)
    j, u = niger._s05_code_maps(codes["s05q01"].unique(), codes["s05q02"].unique(),
                                HH_CATEGORIES, ITEM_MAP, UNIT_MAP, decode)
    long = niger._s05_community_prices(codes, labels, j, u)
    finished = niger._finish_community_prices(long, "2018-19")
    return long, wave_mapping[0](finished)


class TestReadings:
    def test_obs_numbering(self, built):
        long, _ = built
        got = {(r.v, r.u, r.obs): r.Price for r in long.itertuples()}
        assert got == {
            ("1", "Kg", 1): 200.0, ("1", "Kg", 2): 200.0,
            ("1", "Tas (Petit)", 1): 100.0, ("1", "Tas (Petit)", 2): 85.0,
            ("2", "Tas (Petit)", 1): 400.0, ("2", "Tas (Petit)", 2): 340.0,
            # second record: slot 1 (400) repeats the first record's slot 1
            # and is dropped; slot 2 (400 vs 340) is new -> obs 4.
            ("2", "Tas (Petit)", 4): 400.0,
            # the byte-identical copy adds nothing.
            ("2", "Louche (Moyen)", 1): 150.0, ("2", "Louche (Moyen)", 2): 150.0,
        }

    def test_levels_and_description(self, built):
        long, _ = built
        assert set(long["variety"]) == {"unknown_variety"}
        assert long.set_index("v")["visit"].groupby(level=0).first().to_dict() == {"1": 2, "2": 1}
        desc = long.set_index("j")["Description"].groupby(level=0).first().to_dict()
        assert desc == {"Mil": "Mil", "Poisson séché": "poisson séché", "Thé": "Thé vert"}

    def test_existe_other_than_one_is_not_served(self, niger, decode):
        codes, labels = _s05([(3, 1, 2, 7, 1000, 10.0, 10.0)])
        long = niger._s05_community_prices(codes, labels, {7: "Mil"}, {1000: "Kg"})
        assert long.empty


class TestDerivation:
    def test_quantity_one_and_key_on_every_row(self, built):
        _, served = built
        assert list(served.index.names) == ["t", "v", "i", "j", "u", "variety",
                                            "visit", "obs"]
        assert served.index.is_unique and len(served) == 9
        assert (served["Quantity"] == 1.0).all()
        assert str(served["Quantity"].dtype) == "Float64"
        assert (served["Derivation"] == KEY).all()

    def test_registry_entry(self, wave_mapping):
        _, _, rec = wave_mapping
        assert rec.waves == ["2018-19"] and rec.columns == ["Quantity"]
        assert {a["basis"] for a in rec.assumptions} >= {"questionnaire"}
        assert rec.function.endswith("2018-19._.mapping:derive_s05_one_item_per_price")
        assert rec.inputs.endswith("2018-19._.mapping:inputs_community_prices")


@pytest.mark.requires_s3
class TestBuilt:
    """The real build (cache-gated): rows, levels, and the inputs join."""

    @pytest.fixture(scope="class")
    def served(self):
        import lsms_library as ll
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = ll.Country("Niger").community_prices(waves=["2018-19"])
        except Exception as e:  # pragma: no cover - data not available
            pytest.skip(f"Niger 2018-19 community_prices not buildable here: {e}")
        return df

    def test_rows_and_levels(self, served):
        assert list(served.index.names) == INDEX and served.index.is_unique
        assert len(served) == 84444
        obs = served.index.get_level_values("obs").value_counts().to_dict()
        assert obs == {1: 42087, 2: 42087, 3: 134, 4: 136}
        assert served.index.get_level_values("v").nunique() == 504
        assert (served["Quantity"] == 1.0).all()
        assert (served["Derivation"] == KEY).all()

    def test_no_second_record_repeats_its_first_records_slot(self, served):
        x = served.reset_index()
        x["slot"] = (x["obs"] - 1) % 2 + 1
        content = ["t", "v", "j", "u", "variety", "visit", "slot", "Price",
                   "Description"]
        first = pd.MultiIndex.from_frame(x.loc[x["obs"] <= 2, content])
        later = pd.MultiIndex.from_frame(x.loc[x["obs"] > 2, content])
        assert len(later) == 270 and int(later.isin(first).sum()) == 0

    def test_inputs_join_the_served_rows(self, served):
        import lsms_library as ll
        raw = ll.Country("Niger").derivation_inputs(KEY, wave="2018-19")
        assert {"s05q03", "s05q04", "s05q01", "s05q02"} <= set(raw.columns)
        keys = ["t", "v", "j", "u", "visit"]
        s = pd.MultiIndex.from_frame(served.reset_index()[keys].drop_duplicates())
        r = pd.MultiIndex.from_frame(raw.reset_index()[keys].drop_duplicates())
        assert s.isin(r).all() and r.isin(s).all()
