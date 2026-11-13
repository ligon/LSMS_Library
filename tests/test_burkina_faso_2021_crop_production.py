"""Burkina Faso 2021-22 ``crop_production`` (GH #1436 second residual, #824, #1437).

The builder is ``Burkina_Faso/2021-22/_/crop_production.py``: the panel s16c harvest
lines plus the s16d household-crop sales, on Mali 2021-22's option-(d) rule (#843 /
#824): a sale goes on the crop's single line, else on one Farm-level row.  Two facts of
this wave shape it, and each has a test here:

* the two files label ONE crop list with two value-label sets, so a sale is matched to
  its lines by CODE (by label, 332 of 1,548 sales would find no line);
* the harvest block is blank on every finished row (#1437), so the recorded quantities
  are the 179 unfinished ones, keyed harvest-to-date; nothing is derived for the rest.

Pure tests need no data.  The oracle reads s16c and s16d directly through
``get_dataframe`` and imports nothing from the wave's builder.
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


def test_unruled_columns_are_not_served():
    """Survey kilograms and the sale's state wait for a ruling naming this country."""
    text = (WAVE_DIR / "crop_production.py").read_text(encoding="utf-8")
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    body = code.split('"""', 2)[-1]
    for name in ("Quantity_kg", "Quantity_sold_kg", "Condition_sold", "harvest_totals",
                 "derive_hh_crop_harvest_total"):
        assert name not in body, name


def test_both_keys_are_registered():
    from lsms_library.derivations import records_for
    keys = set(records_for(COUNTRY, "crop_production"))
    assert {KEY_SALE, KEY_TO_DATE} <= keys


# ----------------------------------------------------------------------- pure

def test_code_labels_reads_a_bare_code_as_unlabelled(mapping):
    labelled = pd.Series(["Mil", "64.0", "Mil", pd.NA])
    codes = pd.Series([1.0, 64.0, 1.0, None])
    assert mapping._code_labels(labelled, codes, "x") == {1: "Mil", 64: None}


def test_code_labels_refuses_two_labels_for_one_code(mapping):
    with pytest.raises(ValueError, match="two labels"):
        mapping._code_labels(pd.Series(["Mil", "Sorgho"]), pd.Series([1, 1]), "x")


def test_a_label_the_other_file_gives_another_code_identifies_neither(mapping):
    harvest = {3: "Riz", 55: "Autre (à préciser)", 64: None, 65: None}
    records = {3: "Riz Paddy", 55: None, 64: None, 65: "Autre (à préciser)"}
    table = mapping._crop_label_table(harvest, records, {"Riz": "Riz"})
    # A spelling difference on the SAME code keeps the harvest file's label.
    assert table[3] == "Riz"
    # 55's label is s16d's label for 65: it identifies neither, so 55 is qualified.
    assert table[55] == "Unknown [Burkina_Faso 2021-22 s16cq04=55]"
    assert table[64] == "Unknown [Burkina_Faso 2021-22 s16cq04=64]"
    assert table[65] == "Unknown [Burkina_Faso 2021-22 s16cq04=65]"


def test_unit_labels_qualify_a_bare_code_and_keep_na(mapping):
    got = mapping._unit_labels(pd.Series(["Kilogramme", "8.0", pd.NA]),
                               pd.Series([1.0, 8.0, None]), "s16cq16b", {"Kg": "Kg"})
    assert got.tolist()[:2] == ["Kilogramme", "Unknown [Burkina_Faso 2021-22 s16cq16b=8]"]
    assert pd.isna(got.iloc[2])


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
    lines = c.groupby(["hh", "s16cq04"]).size()
    n = lines.reindex(pd.MultiIndex.from_arrays([sales["hh"], sales["s16dq01"]])).fillna(0)
    assert int((n == 0).sum()) == 0
    flat = served.reset_index()
    carried = flat[flat["Quantity_sold"].notna() | flat["Value_sold"].notna()]
    assert len(carried) == len(sales)
    farm = carried[carried["plot_id"] == "Farm-level"]
    assert len(farm) == int((n >= 2).sum())
    keyed = carried["Derivation"].astype("string").str.contains(KEY_SALE, regex=False).fillna(False)
    assert int(keyed.sum()) == int((n == 1).sum())
    assert not farm["Derivation"].astype("string").str.contains(KEY_SALE, regex=False).fillna(False).any()
    assert (farm["u"] == "Unknown").all() and farm["Quantity"].isna().all()


@requires_s3
def test_recorded_harvest_is_the_unfinished_rows_and_keyed(served, raw):
    c, _ = raw
    q = pd.to_numeric(c["s16cq16a"], errors="coerce")
    finished = pd.to_numeric(c["s16cq11"], errors="coerce")
    # GH #1437: 0 finished rows record a quantity in the held release.
    assert int((q.notna() & (finished == 1)).sum()) == 0
    flat = served.reset_index()
    has_q = flat["Quantity"].notna()
    assert int(has_q.sum()) == int(q.notna().sum())
    assert flat.loc[has_q, "Quantity"].sum() == pytest.approx(q.sum())
    keyed = flat["Derivation"].astype("string").str.contains(KEY_TO_DATE, regex=False).fillna(False)
    assert (keyed == has_q).all()


@requires_s3
def test_served_index_is_unique_and_condition_is_in_vocabulary(served):
    assert served.index.is_unique
    cond = served.index.get_level_values("condition")
    assert not pd.isna(cond).any()
    assert set(cond) <= {"shelled", "unshelled", "shell_not_applicable", "unknown_condition"}


@requires_s3
def test_no_household_is_off_sample(served):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        s = ll.Country(COUNTRY).sample(waves=[WAVE])
    assert set(served.index.get_level_values("i")) <= set(s.index.get_level_values("i"))


def _keyed(served, key):
    flat = served.reset_index()
    return flat[flat["Derivation"].astype("string").str.contains(key, regex=False).fillna(False)]


@requires_s3
@pytest.mark.parametrize("key", [KEY_SALE, KEY_TO_DATE])
def test_inputs_join_one_to_one_to_the_keyed_served_rows(served, raw, key):
    """`derivation_inputs` returns the raw answers of each keyed row, at the served line
    grain (t, i, plot_id, crop) after the id walk: one input row per keyed served row, and
    no other (COMMON-RULES)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        got = ll.Country(COUNTRY).derivation_inputs(key, wave=WAVE)
    levels = ["t", "i", "plot_id", "crop"]
    assert list(got.index.names) == levels
    assert got.index.is_unique
    keyed = _keyed(served, key)
    want = set(zip(*(keyed[n].astype(str) for n in levels)))
    have = set(zip(*(got.index.get_level_values(n).astype(str) for n in levels)))
    assert len(got) == len(keyed)
    assert have == want
    c, d = raw
    if key == KEY_SALE:
        lines = c.groupby(["hh", "s16cq04"]).size()
        sales = _sales(d)
        n = lines.reindex(pd.MultiIndex.from_arrays([sales["hh"], sales["s16dq01"]])).fillna(0)
        assert len(got) == int((n == 1).sum())
        # The inputs carry the sale as recorded: the same money as the keyed rows.
        assert got["s16dq06"].astype("float64").sum() == pytest.approx(keyed["Value_sold"].sum())
    else:
        assert len(got) == int(pd.to_numeric(c["s16cq16a"], errors="coerce").notna().sum())


@requires_s3
def test_2018_19_is_untouched_by_the_new_wave():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY).crop_production(waves=["2018-19"])
    assert len(df) == 15_587
    assert "Derivation" not in df.columns or df["Derivation"].isna().all()
