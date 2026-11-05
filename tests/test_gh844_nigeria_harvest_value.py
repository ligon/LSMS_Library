"""Nigeria ``crop_production.Value_harvested`` -- the household's own Naira
valuation of the harvest (GH #844).

Every GHS-Panel post-harvest crop module asks, on the same plot-crop row as
the harvest quantity, what that harvest is worth:

* W1/W2 ``sa3q18``: "If you had sold all [CROP] harvested since the last
  visit, what would be the total value?"
* W3 ``sa3iq6a``; W4 ``sa3iq6a`` / ``sa3iiiq14``; W5 ``sa3iq10`` /
  ``sa3iiiq24``: "What is the estimated value of the [CROP] harvested ...?"

Until GH #844 no table carried it.  It is a hypothetical valuation, never a
transaction, so it is a column DISTINCT from ``Value_sold``.  Pinned here:

1. the wiring (data-free): ``nigeria.crop_production_for_wave`` carries the
   named column positionally, as float64, and delivers all-NaN float64 when
   a frame names none (the concat-dtype hazard ``KgFactor`` guards);
2. the config (data-free): every frame dict in the country script names its
   wave's valuation column, and the scheme declares it REQUIRED;
3. the identification (data-gated): each named column's Stata variable label
   is the valuation question, not a neighbour;
4. the built table (data-gated, warm cache only): per wave, every raw
   valuation is served, or on a row with no crop label.
"""
from __future__ import annotations

import ast
import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

NIGERIA = countries_root() / "Nigeria"
TABLE = "crop_production"
COL = "Value_harvested"

#: Per wave dir: the source file of each frame (relative to the wave dir),
#: its valuation column, and a fragment its Stata variable label must carry.
VALUATION_COLUMNS = {
    "2010-11": [("Data/Post Harvest Wave 1/Agriculture/secta3_harvestw1.dta",
                 "sa3q18", "if all had been sold")],
    "2012-13": [("Data/Post Harvest Wave 2/Agriculture/secta3_harvestw2.dta",
                 "sa3q18", "value if you had sold all")],
    "2015-16": [("Data/secta3i_harvestw3.dta", "sa3iq6a", "estimated value")],
    "2018-19": [("Data/secta3i_harvestw4.dta", "sa3iq6a", "estimated value"),
                ("Data/secta3iii_harvestw4.dta", "sa3iiiq14", "estimated value")],
    "2023-24": [("Data/Post Harvest Wave 5/Agriculture/secta3i_harvestw5.dta",
                 "sa3iq10", "estimated value"),
                ("Data/Post Harvest Wave 5/Agriculture/secta3iii_harvestw5.dta",
                 "sa3iiiq24", "estimated value")],
}
PH_T = {"2010-11": "2011Q1", "2012-13": "2013Q1", "2015-16": "2016Q1",
        "2018-19": "2019Q1", "2023-24": "2024Q1"}

#: The crop-code column of each wave's harvest files (W1 names it sa3q2).
CROP_COL = {"2010-11": "sa3q2", "2012-13": "cropcode", "2015-16": "cropcode",
            "2018-19": "cropcode", "2023-24": "cropcode"}


def _load_nigeria_module():
    spec = importlib.util.spec_from_file_location("nigeria", NIGERIA / "_" / "nigeria.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("nigeria", mod)
    spec.loader.exec_module(mod)
    return mod


def _frame_dicts_by_wave():
    """``{wave_dir: [{keyword: literal}]}`` for each frame ``dict(...)`` (one
    carrying ``qty=``) in the country script, read with ``ast`` -- the script
    is never imported, it runs data reads."""
    source = (NIGERIA / "_" / "crop_production.py").read_text()
    tree = ast.parse(source)
    lines = source.splitlines()
    out = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "dict"):
            continue
        kws = {kw.arg: kw.value for kw in node.keywords}
        if "qty" not in kws:
            continue
        wave = None
        for ln in range(node.lineno - 1, -1, -1):
            wave = next((w for w in VALUATION_COLUMNS if f"../{w}/" in lines[ln]), None)
            if wave:
                break
        assert wave is not None, f"frame dict at line {node.lineno} has no wave path above it"
        lit = {k: v.value for k, v in kws.items() if isinstance(v, ast.Constant)}
        out.setdefault(wave, []).append(lit)
    return out


def _frame(values, *, name_it=True):
    n = len(values)
    df = pd.DataFrame({
        "hhid": [f"h{k}" for k in range(n)],
        "plotid": ["1"] * n,
        "cropcode": [1010] * n,
        "q": [10.0] * n,
        "unit": [1] * n,
        "val": values,
    })
    dec = df.copy()
    dec["unit"] = "1. KILOGRAMS"
    fr = dict(df=df, dec=dec, hhid="hhid", plot="plotid", crop="cropcode",
              qty="q", unit="unit", perennial=False)
    if name_it:
        fr["value_harvested"] = "val"
    return fr


# --------------------------------------------------------------------------
# 1. wiring (data-free)
# --------------------------------------------------------------------------

class TestWiring:
    def test_valuation_is_carried_positionally_as_float(self):
        ng = _load_nigeria_module()
        out = ng.crop_production_for_wave(
            "2016Q1", [_frame([2500.0, np.nan, 0.0, 1.95e9])], {1010: "Maize"})
        assert out[COL].dtype == np.float64, out[COL].dtype
        got = out.reset_index().sort_values("i")[COL].tolist()
        # Stored as recorded: NaN stays NaN, a zero stays zero, an outlier is
        # neither clipped nor stripped (Site Q screens Quantity, not this).
        assert got[0] == 2500.0 and np.isnan(got[1]) and got[2] == 0.0 and got[3] == 1.95e9

    def test_frame_without_valuation_delivers_all_nan_float(self):
        ng = _load_nigeria_module()
        out = ng.crop_production_for_wave(
            "2016Q1", [_frame([1.0, 2.0], name_it=False)], {1010: "Maize"})
        assert out[COL].isna().all()
        assert out[COL].dtype == np.float64, out[COL].dtype

    def test_valuation_never_feeds_the_sale_columns(self):
        ng = _load_nigeria_module()
        out = ng.crop_production_for_wave(
            "2019Q1", [_frame([5000.0, 7000.0])], {1010: "Maize"})
        assert out["Value_sold"].isna().all()
        assert out["Quantity_sold"].isna().all()


# --------------------------------------------------------------------------
# 2. config (data-free)
# --------------------------------------------------------------------------

class TestConfig:
    def test_every_frame_names_its_waves_valuation_column(self):
        frames = _frame_dicts_by_wave()
        assert set(frames) == set(VALUATION_COLUMNS)
        for wave, specs in VALUATION_COLUMNS.items():
            named = sorted(f.get("value_harvested") for f in frames[wave])
            assert named == sorted(col for _, col, _ in specs), (wave, named)

    def test_scheme_declares_it_required_float(self):
        scheme = load_yaml(NIGERIA / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert scheme[COL] == "float", scheme[COL]
        assert COL not in scheme["index"]


# --------------------------------------------------------------------------
# 3. identification (data-gated)
# --------------------------------------------------------------------------

@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(VALUATION_COLUMNS))
def test_named_column_is_the_valuation_question(wave):
    pyreadstat = pytest.importorskip("pyreadstat")
    from lsms_library.data_access import get_data_file
    for rel, col, fragment in VALUATION_COLUMNS[wave]:
        path = get_data_file(str(NIGERIA / wave / rel))
        assert path is not None, f"raw Nigeria harvest file unavailable: {wave}/{rel}"
        _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
        label = dict(zip(meta.column_names, meta.column_labels)).get(col)
        assert label is not None, f"{col} missing from {rel}"
        assert fragment in label.lower(), (wave, col, label)


# --------------------------------------------------------------------------
# 4. the built table (data-gated; warm cache only, never cold-builds)
# --------------------------------------------------------------------------

def _raw_accounting(ng, wave):
    """``(raw, no_label)`` valuation counts for one wave, from the raw files
    alone: every non-null valuation, and those on a row with no resolved crop
    label or household (``crop_production_for_wave``'s filter)."""
    from lsms_library.local_tools import get_dataframe
    labels = ng._crop_labels()
    parts = []
    for rel, col, _ in VALUATION_COLUMNS[wave]:
        d = get_dataframe(str(NIGERIA / wave / rel), convert_categoricals=False)
        parts.append(pd.DataFrame({
            "i": d["hhid"].apply(ng.format_id).values,
            "plot_id": d["plotid"].apply(ng.format_id).values,
            "crop": pd.to_numeric(d[CROP_COL[wave]], errors="coerce")
                      .astype("Int64").map(labels).astype("string").values,
            "val": pd.to_numeric(d[col], errors="coerce").values,
        }))
    raw = pd.concat(parts, ignore_index=True)
    valued = raw["val"].notna()
    labelled = raw["i"].notna() & raw["crop"].notna()
    return int(valued.sum()), int((valued & ~labelled).sum())


@pytest.mark.requires_s3
def test_api_frame_accounts_for_every_raw_valuation(ng_in_country_dir):
    """Per wave: served + no-crop-label == raw non-null.

    Re-pinned by GH #1433: the keep-first dedup term is gone, because every
    labelled line is served."""
    from lsms_library.local_tools import data_root, read_parquet_cache_waves
    path = data_root() / "Nigeria" / "var" / f"{TABLE}.parquet"
    try:
        cached = pd.read_parquet(path)
    except FileNotFoundError:
        pytest.skip("no warm Nigeria crop_production parquet (absent, or mid-rebuild)")
    if COL not in cached.columns:
        pytest.skip("warm Nigeria crop_production parquet predates GH #844 (re-warm needed)")
    if "production_obs" not in cached.index.names:
        pytest.skip("warm Nigeria crop_production parquet predates GH #1433 (re-warm needed)")
    coverage = read_parquet_cache_waves(path)
    if coverage is not None and set(PH_T.values()) - set(coverage):
        pytest.skip(f"partial warm cache: lacks {sorted(set(PH_T.values()) - set(coverage))}")
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country("Nigeria", assume_cache_fresh=True).crop_production()
    assert COL in df.columns and COL not in df.index.names
    assert str(df[COL].dtype) in ("float64", "Float64"), df[COL].dtype
    served = df[COL].notna().groupby(np.asarray(df.index.get_level_values("t"))).sum()
    for wave, t in PH_T.items():
        raw, no_label = _raw_accounting(ng_in_country_dir, wave)
        assert served.get(t, 0) > 0, (t, "valuation absent from a wave that asks it")
        assert served.get(t, 0) + no_label == raw, (t, served.get(t, 0), no_label, raw)


@pytest.fixture
def ng_in_country_dir(monkeypatch):
    """The Nigeria module, with cwd where its country scripts run (its crop
    label lookup resolves categorical tables relative to cwd)."""
    monkeypatch.chdir(NIGERIA / "_")
    return _load_nigeria_module()
