"""Niger CS07 rice: ``j`` follows the VARIETY code for imported rice -- GH #1090.

CS07 product ``cs07q01 = "Riz"`` maps through ``harmonize_food`` to
``Autre Riz local`` for both its varieties, so imported rice was served as
local.  Harmonisation choice, ruled by @ligon 2026-11-03:

* 7031 "Riz thailandais vendu en vrac au kilo" -> ``Riz importé non parfumé``
  (the household EHCVM imported non-scented rice label);
* 7032 "Riz local vendu en vrac au kilo" keeps ``Autre Riz local``.

Measured, cold private roots: 683 rows (2011-12 461, 2014-15 222) move from
``Autre Riz local`` to ``Riz importé non parfumé``; 130,487 rows before and
after, and the frame is otherwise identical (``variety``, ``Description``,
prices, quantities, units, 2018-19).  See ``niger._CS07_VARIETY_J``.
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

COUNTRY = "Niger"
IMPORTED = "Riz importé non parfumé"
LOCAL = "Autre Riz local"
#: Served rows per (variety, wave) after GH #1090.
SERVED = {("7031", "2011-12"): 461, ("7031", "2014-15"): 222,
          ("7032", "2011-12"): 187, ("7032", "2014-15"): 166}


def _aws_creds_available() -> bool:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_shared_conftest", Path(__file__).with_name("conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


needs_data = pytest.mark.skipif(
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


@pytest.fixture(scope="module")
def niger_module():
    sys.path.insert(0, str(countries_root() / COUNTRY / "_"))
    try:
        import niger
    finally:
        sys.path.pop(0)
    return niger


def test_override_names_the_household_labels(niger_module):
    """The override serves a label the household food module already serves,
    so the (j, u) join to food_acquired holds; 7032 is not overridden."""
    table = df_from_orgfile(
        str(countries_root() / COUNTRY / "_" / "categorical_mapping.org"),
        name="harmonize_food")
    preferred = set(table["Preferred Label"].astype(str).str.strip())
    assert niger_module._CS07_VARIETY_J == {"7031": IMPORTED}
    assert IMPORTED in preferred and LOCAL in preferred


def test_variety_code_overrides_the_product_label(niger_module):
    """Synthetic 2014-15 grid: two rice varieties under one product label."""
    nan = float("nan")
    codes = pd.DataFrame(
        [(10, 1, 703, 7031, 1, 400.0, 1.0, nan, nan, nan, nan),
         (10, 1, 703, 7032, 1, 350.0, 1.0, nan, nan, nan, nan),
         (10, 1, 701, 7011, 1, 200.0, 1.0, nan, nan, nan, nan)],
        columns=["grappe", "numquest", "cs07q01", "cs07q02", "cs07q03",
                 "cs07q04", "cs07q05", "cs07q06", "cs07q07", "cs07q08", "cs07q09"])
    labels = codes.copy()
    labels["cs07q01"] = ["Riz", "Riz", "Mil"]
    labels["cs07q02"] = ["Riz thailandais vendu en vrac au kilo",
                         "Riz local vendu en vrac au kilo", "mil vendu au kilo"]
    labels["cs07q03"] = "Kg"
    triples = [("cs07q04", "cs07q05", "cs07q03"), ("cs07q06", "cs07q07", "cs07q03"),
               ("cs07q08", "cs07q09", "cs07q03")]
    out = niger_module._community_price_triples(
        labels, {"Riz": LOCAL, "Mil": "Mil"}, {"Kg": "Kg"}, triples, codes=codes)
    got = dict(zip(out["variety"], out["j"]))
    assert got == {"7031": IMPORTED, "7032": LOCAL, "7011": "Mil"}
    # The variety label as recorded is untouched.
    assert out.loc[out["variety"] == "7031", "Description"].iloc[0] == (
        "Riz thailandais vendu en vrac au kilo")


@needs_data
def test_served_rice_varieties():
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = ll.Country(COUNTRY).community_prices().reset_index()
    x = r[r["variety"].isin(["7031", "7032"])]
    assert x.groupby(["variety", "t"]).size().to_dict() == SERVED
    assert set(x.loc[x["variety"] == "7031", "j"]) == {IMPORTED}
    assert set(x.loc[x["variety"] == "7032", "j"]) == {LOCAL}
