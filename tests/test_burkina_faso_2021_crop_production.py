"""Burkina Faso 2021-22 ``crop_production`` (GH #1436 second residual, #824, #1437).

The builder is ``Burkina_Faso/2021-22/_/crop_production.py``: the panel s16c harvest
lines plus the s16d household-crop sales, on Mali 2021-22's option-(d) rule (#843 /
#824), with the harvest from the s16d dispositions where the file lacks it (ruling
@ligon 2026-11-13, "Extend Mali's rule") and the line as ``production_obs`` (ruling
2026-11-13, "Add production_obs").  Facts of this wave, each with a test here:

* the two files label ONE crop list with two value-label sets, so a sale is matched to
  its lines by CODE (by label, 332 of 1,548 sales would find no line);
* the harvest block is blank on every finished row (#1437): a recorded quantity is a
  partial harvest (harvest-to-date), and a finished crop's harvest is the sum of its
  dispositions where they share one unit;
* (household, field, parcel, crop) repeats on two pairs of distinct lines, which
  ``production_obs`` keeps apart, so ``LSMS_GRAIN_STRICT=1`` passes;
* codes 55, 58 and 64 take the label the roster text gives them.

Pure tests need no data.  The oracles read s16c and s16d directly through
``get_dataframe`` and import nothing from the wave's builder.
"""
from __future__ import annotations

import importlib.util
import pathlib
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root


def _tests_conftest():
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
WAVE = "2021-22"
ROOT = countries_root() / COUNTRY
WAVE_DIR = ROOT / WAVE / "_"
KEY_SALE = "Burkina_Faso::crop_production::hh-crop-sale-single-row"
KEY_TO_DATE = "Burkina_Faso::crop_production::harvest-to-date"
KEY_DISP = "Burkina_Faso::crop_production::harvest-from-dispositions"
KEY_TEXT = "Burkina_Faso::crop_production::crop-code-from-roster-text"
KEYS = [KEY_SALE, KEY_TO_DATE, KEY_DISP, KEY_TEXT]
OBS_NA = "unresolved_observation"
DISPOSITIONS = ("s16dq02", "s16dq03", "s16dq05", "s16dq13")


@pytest.fixture(scope="module")
def mapping():
    spec = importlib.util.spec_from_file_location("bfa_2021_mapping", WAVE_DIR / "mapping.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ----------------------------------------------------------------------- config

def test_the_builder_reads_both_files_by_literal_path():
    text = (WAVE_DIR / "crop_production.py").read_text(encoding="utf-8")
    # Literal paths: the cache hash folds a script's literal data refs (scan_script_data_refs).
    assert "'../Data/s16c_me_bfa2021.dta'" in text
    assert "'../Data/s16d_me_bfa2021.dta'" in text


def test_unruled_kilograms_are_not_served():
    """Survey kg and sold kg wait for a ruling naming this country (drafted residual)."""
    text = (WAVE_DIR / "crop_production.py").read_text(encoding="utf-8")
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    body = code.split('"""', 2)[-1]
    for name in ("Quantity_kg", "Quantity_sold_kg"):
        assert name not in body, name


def test_the_declared_index_carries_production_obs():
    import yaml
    with open(ROOT / "_" / "data_scheme.yml", encoding="utf-8") as fh:
        scheme = yaml.safe_load(fh)
    index = scheme["Data Scheme"]["crop_production"]["index"] if "Data Scheme" in scheme \
        else scheme["crop_production"]["index"]
    assert index == "(t, i, plot_id, crop, u, condition, production_obs)"


def test_every_key_is_registered():
    from lsms_library.derivations import records_for
    keys = set(records_for(COUNTRY, "crop_production"))
    assert set(KEYS) <= keys


# ----------------------------------------------------------------------- pure

def test_code_labels_reads_a_bare_code_as_unlabelled(mapping):
    labelled = pd.Series(["Mil", "64.0", "Mil", pd.NA])
    codes = pd.Series([1.0, 64.0, 1.0, None])
    assert mapping._code_labels(labelled, codes, "x") == {1: "Mil", 64: None}


def test_code_labels_refuses_two_labels_for_one_code(mapping):
    with pytest.raises(ValueError, match="two labels"):
        mapping._code_labels(pd.Series(["Mil", "Sorgho"]), pd.Series([1, 1]), "x")


def test_crop_label_rule(mapping):
    harvest = {3: "Riz", 55: "Autre (à préciser)", 58: None, 64: None, 65: None, 99: None}
    records = {3: "Riz Paddy", 55: None, 58: None, 64: None, 65: "Autre (à préciser)", 99: None,
               77: "Vu ailleurs"}
    table = mapping._crop_label_table(harvest, records, {"Riz": "Riz"})
    # A spelling difference on the SAME code keeps the harvest file's label.
    assert table[3] == "Riz"
    # The roster text labels 55, 58 and 64.
    assert (table[55], table[58], table[64]) == ("Anacarde", "Banane douce", "Soja")
    # A code the harvest file leaves bare takes s16d's label.
    assert table[65] == "Autre (à préciser)"
    assert table[77] == "Vu ailleurs"
    # Neither file labels it: the source-qualified identity.
    assert table[99] == "Unknown [Burkina_Faso 2021-22 s16cq04=99]"
    # A label the other file gives to a different code identifies neither.
    assert mapping.derive_crop_code_label(5, {5: "Mil"}, {6: "Mil"}) == \
        "Unknown [Burkina_Faso 2021-22 s16cq04=5]"


def test_one_unit_list_for_every_field(mapping):
    harvest = {1: "Kilogramme", 3: "Plat Yoruba", 8: None}
    sold = {1: "Kilogramme", 3: "Yorouba", 8: None, 13: None}
    table = mapping._unit_code_table([harvest, sold])
    assert table[3] == "Plat Yoruba", "the harvest field's spelling, for every field"
    assert table[13] == "Unknown [Burkina_Faso 2021-22 UML=13]"
    assert table[8] == "Unknown [Burkina_Faso 2021-22 UML=8]"


def test_household_id_is_the_country_rule(mapping):
    frame = pd.DataFrame({"grappe": [2, 2], "menage": [1.0, 126.0]})
    assert mapping._crop_household_ids(frame).tolist() == ["2001", "2126"]


# ----------------------------------------------------------------------- data

@pytest.fixture(scope="module")
def raw():
    data = ROOT / WAVE / "Data"
    c = get_dataframe(str(data / "s16c_me_bfa2021.dta"), convert_categoricals=False)
    d = get_dataframe(str(data / "s16d_me_bfa2021.dta"), convert_categoricals=False)
    c = c[c["s16cq04"].notna()].copy()
    d = d[~d.duplicated()].copy()
    d = d[d["s16dq01"].notna()]
    for f in (c, d):
        f["hh"] = (f["grappe"].astype("Int64").astype(str) + "/"
                   + f["menage"].astype("Int64").astype(str))
    return c, d


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country(COUNTRY).crop_production(waves=[WAVE])


def _sales(d):
    q = pd.to_numeric(d["s16dq05a"], errors="coerce")
    v = pd.to_numeric(d["s16dq06"], errors="coerce")
    return d[(q > 0) | (v > 0)]


def _keyed(served, key):
    flat = served.reset_index()
    return flat[flat["Derivation"].astype("string").str.split("+").map(
        lambda ks: isinstance(ks, list) and key in ks)]


def _lines_per_pair(c, d):
    lines = c.groupby(["hh", "s16cq04"]).size()
    return lines.reindex(pd.MultiIndex.from_arrays([d["hh"], d["s16dq01"]])).fillna(0)


@requires_s3
def test_value_sold_is_the_raw_s16d_sum(served):
    d = get_dataframe(str(ROOT / WAVE / "Data" / "s16d_me_bfa2021.dta"),
                      convert_categoricals=False)
    assert served["Value_sold"].sum() == pytest.approx(
        pd.to_numeric(d["s16dq06"], errors="coerce").sum(), rel=1e-12)


@requires_s3
def test_every_sale_is_on_exactly_one_row_by_crop_code(served, raw):
    c, d = raw
    sales = _sales(d)
    n = _lines_per_pair(c, sales)
    assert int((n == 0).sum()) == 0
    flat = served.reset_index()
    carried = flat[flat["Quantity_sold"].notna() | flat["Value_sold"].notna()]
    assert len(carried) == len(sales)
    farm = carried[carried["plot_id"] == "Farm-level"]
    assert len(farm) == int((n >= 2).sum())
    assert len(_keyed(served, KEY_SALE)) == int((n == 1).sum())
    assert not farm["Derivation"].fillna("").str.contains(KEY_SALE, regex=False).any()
    assert (farm["production_obs"] == OBS_NA).all()


@requires_s3
def test_the_sale_state_is_served_on_every_sale(served, raw):
    """GH #1528 for 2021-22: s16dq05d, folded as 2018-19 folds it, answer kept literally."""
    _, d = raw
    sales = _sales(d)
    flat = served.reset_index()
    carried = flat[flat["Quantity_sold"].notna() | flat["Value_sold"].notna()]
    answered = int(pd.to_numeric(sales["s16dq05d"], errors="coerce").notna().sum())
    assert int(carried["condition_sold_native"].notna().sum()) == answered
    assert int(carried["Condition_sold"].notna().sum()) == answered
    others = flat[~(flat["Quantity_sold"].notna() | flat["Value_sold"].notna())]
    assert others["Condition_sold"].isna().all() and others["condition_sold_native"].isna().all()


@requires_s3
def test_recorded_harvest_is_partial_and_keyed(served, raw):
    c, _ = raw
    q = pd.to_numeric(c["s16cq16a"], errors="coerce")
    finished = pd.to_numeric(c["s16cq11"], errors="coerce")
    # GH #1437: 0 finished rows record a quantity in the held release.
    assert int((q.notna() & (finished == 1)).sum()) == 0
    keyed = _keyed(served, KEY_TO_DATE)
    assert len(keyed) == int(q.notna().sum())
    assert keyed["Quantity"].sum() == pytest.approx(q.sum())


@requires_s3
def test_harvest_from_dispositions_follows_mali_rule(served, raw):
    """A finished crop with no recorded quantity and one disposition unit (by code) gets
    the sum of its dispositions, on its single line or on its one Farm-level row."""
    c, d = raw
    lines = c.assign(fin=pd.to_numeric(c["s16cq11"], errors="coerce").eq(1),
                     rec=pd.to_numeric(c["s16cq16a"], errors="coerce").notna())
    pairs = lines.groupby(["hh", "s16cq04"]).agg(n=("fin", "size"), fin=("fin", "all"),
                                                rec=("rec", "any"))
    long = pd.concat([d[["hh", "s16dq01"]].assign(
        q=pd.to_numeric(d[f"{v}a"], errors="coerce"),
        u=pd.to_numeric(d[f"{v}b"], errors="coerce")) for v in DISPOSITIONS])
    long = long[long["q"] > 0]
    agg = long.groupby(["hh", "s16dq01"]).agg(total=("q", "sum"), units=("u", "nunique"),
                                             unitless=("u", lambda s: s.isna().any()))
    agg.index = agg.index.set_names(["hh", "s16cq04"])
    j = pairs.join(agg, how="inner")
    want = j[j["fin"] & ~j["rec"] & j["units"].eq(1) & ~j["unitless"]]
    keyed = _keyed(served, KEY_DISP)
    assert len(keyed) == len(want)
    assert int((keyed["plot_id"] == "Farm-level").sum()) == int(want["n"].gt(1).sum())
    assert keyed["Quantity"].sum() == pytest.approx(want["total"].sum())
    assert keyed["Quantity"].notna().all()
    # The other derived and recorded quantities are disjoint.
    assert not keyed["Derivation"].str.contains(KEY_TO_DATE, regex=False).any()


@requires_s3
def test_production_obs_keeps_the_repeated_lines_apart(served, raw):
    c, _ = raw
    assert served.index.is_unique
    flat = served.reset_index()
    plots = flat[flat["plot_id"] != "Farm-level"]
    assert plots["production_obs"].str.startswith("row:dvc:").all()
    assert plots["production_obs"].is_unique
    key = ["hh", "s16cq02", "s16cq03", "s16cq04"]
    repeated = int(c.duplicated(key, keep=False).sum())
    assert repeated == 4
    # Every s16c crop line is served, the repeated ones included.
    assert len(plots) == len(c)
    # No keyed row shares its plot-crop key with another line.
    no_obs = served.index.droplevel("production_obs")
    dup = no_obs.duplicated(keep=False)
    assert int(dup.sum()) == repeated
    assert served.loc[dup, "Derivation"].isna().all()


@requires_s3
def test_grain_strict_passes_on_crop_production(monkeypatch):
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY).crop_production()
    assert df.index.is_unique and len(df) > 0


@requires_s3
def test_units_share_one_vocabulary(served):
    flat = served.reset_index()
    sold = set(flat["Unit_sold"].dropna())
    harvest = set(flat["u"].dropna())
    assert "Kilogramme" not in sold | harvest, "the read path serves 'Kg'"
    assert "Yorouba" not in sold, "code 3 is spelled once across the fields"
    assert {"Kg", "Plat Yoruba"} <= sold


@requires_s3
def test_crop_labels_from_the_roster_text(served, raw):
    c, _ = raw
    flat = served.reset_index()
    assert not flat["crop"].str.startswith("Unknown [").any()
    keyed = _keyed(served, KEY_TEXT)
    assert set(keyed["crop"]) == {"Anacarde", "Banane douce", "Soja"}
    plots = keyed[keyed["plot_id"] != "Farm-level"]
    codes = pd.to_numeric(c["s16cq04"]).isin([55, 58, 64])
    assert len(plots) == int(codes.sum())


@requires_s3
def test_no_household_is_off_sample(served):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        s = ll.Country(COUNTRY).sample(waves=[WAVE])
    assert set(served.index.get_level_values("i")) <= set(s.index.get_level_values("i"))


@requires_s3
@pytest.mark.parametrize("key", KEYS)
def test_inputs_join_to_the_keyed_served_rows(served, key):
    """`derivation_inputs` returns the raw answers of each keyed row, at the served line
    grain (t, i, plot_id, crop) after the id walk (COMMON-RULES).  One input row per keyed
    row, except the text key, whose inputs also hold the s16d record a Farm-level row of a
    many-line crop MAY carry."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        got = ll.Country(COUNTRY).derivation_inputs(key, wave=WAVE)
    levels = ["t", "i", "plot_id", "crop"]
    assert list(got.index.names) == levels
    assert got.index.is_unique
    keyed = _keyed(served, key)
    want = set(zip(*(keyed[n].astype(str) for n in levels)))
    have = set(zip(*(got.index.get_level_values(n).astype(str) for n in levels)))
    assert want <= have
    if key != KEY_TEXT:
        assert len(got) == len(keyed) and have == want
    if key == KEY_SALE:
        assert got["s16dq06"].astype("float64").sum() == pytest.approx(keyed["Value_sold"].sum())


@requires_s3
def test_2018_19_is_untouched_by_the_new_wave():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY).crop_production(waves=["2018-19"])
    assert len(df) == 15_587
    assert set(df.index.get_level_values("production_obs")) == {OBS_NA}
    assert "Derivation" not in df.columns or df["Derivation"].isna().all()
