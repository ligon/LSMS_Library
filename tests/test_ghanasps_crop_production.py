"""GhanaSPS ``crop_production`` -- the S4AV2 season label error, pinned (refs #729, #140).

2009-10's crop-harvest module is TWO wide files: S4AV1 (last MAJOR season)
and S4AV2 (last MINOR season), five harvest slots per plot each.  Every
quantity / value label in S4AV2 says "harvested in the last major season"
and is WRONG -- the questionnaire heads the block "LAST MINOR SEASON: CROP
(HARVESTS) 1" and asks "A122. What is the quantity harvested in the last
minor season?".  A build that trusts the .dta labels stamps both blocks
major and double-counts the season, with every framework guard green.  The
data tests here pin the two things that catch it: the wave delivers TWO
seasons, and the rows that come from S4AV2 carry ``season == 'minor'`` and
are the SMALLER set.

The one Python piece the YAML waves need is the ``ghanasps.crop_production``
hook; 2009-10 is a wave script.  Both contain a bounded REDUCER -- duplicate
(t, i, plot_id, j, u, season) lines (two harvest events of one product in one
unit) are summed -- kept honest by pinning the per-wave group counts, so a
NEW duplicate turns this file red instead of being summed away.

Data-dependent tests are marked ``requires_s3`` (the conftest's documented
spelling) and are skipped in the data-free CI job.  They do NOT swallow
exceptions.  The grain condition is asserted directly (zero
``GrainCollapseWarning`` naming ``crop_production``); ``LSMS_READ_STRICT=1``
is not relied on because on this branch it is fatal inside ``GhanaSPS/sample``
(documented in CONTENTS.org), so the read-strict condition for THIS table is
asserted via ``null_read_reports``.
"""
import warnings

import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import all_dfs_from_orgfile, get_dataframe
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

WAVES = ("2009-10", "2013-14", "2017-18")
CROP_MAPPING = ["harmonize_crop", "Alternate Spelling", "Preferred Label"]
UNIT_MAPPING = ["harvest_units", "Alternate Spelling", "Preferred Label"]
# Duplicate (t, i, plot_id, j, u, season) groups the script / hook SUM, per
# wave.  A change here is a change in the SOURCE and must be looked at.
EXPECTED_DEDUP_GROUPS = {"2009-10": 33, "2013-14": 2, "2017-18": 0}
# Delivered rows per wave (post-finalize).  Pins the melt, the no-harvest
# drops and the reducer together.
EXPECTED_ROWS = {"2009-10": 11795, "2013-14": 6886, "2017-18": 6428}
# 2009-10 rows per season -- the discriminating check for the label error:
# minor comes from S4AV2 and is the smaller set.
EXPECTED_W1_SEASON_ROWS = {"major": 9292, "minor": 2503}
# Positive 2009-10 unit codes the codebook does not define, delivered as the
# code string (accepted residuals).
W1_RESIDUAL_UNIT_CODES = {"1", "46", "50", "51", "53", "57", "58", "61", "80", "94"}


# --------------------------------------------------------------------------
# Config: the wiring itself (data-free)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gsps_root():
    root = countries_root() / "GhanaSPS"
    if not root.is_dir():
        pytest.skip("GhanaSPS config tree unavailable")
    return root


@pytest.fixture(scope="module")
def scheme_entry(gsps_root):
    with open(gsps_root / "_" / "data_scheme.yml") as f:
        return load_yaml(f)["Data Scheme"]["crop_production"]


@pytest.fixture(scope="module")
def wave_specs(gsps_root):
    out = {}
    for w in WAVES:
        with open(gsps_root / w / "_" / "data_info.yml") as f:
            out[w] = (yaml.safe_load(f) or {}).get("crop_production")
    return out


@pytest.fixture(scope="module")
def tables(gsps_root):
    return all_dfs_from_orgfile(gsps_root / "_" / "categorical_mapping.org")


def test_index_is_plot_crop_unit_season_without_v_or_condition(scheme_entry):
    assert scheme_entry["index"].replace(" ", "") == "(t,i,plot_id,j,u,season)"
    assert scheme_entry.get("materialize") == "make"


def test_sales_columns_are_optional_because_two_waves_never_have_them(scheme_entry):
    """Site B fires on a required column 100% null in a wave's t slice:
    Quantity_sold is never asked in 2009-10, and 2013-14's sales file has no
    plotid, so both sales columns must be `optional`."""
    for col in ("Quantity_sold", "Value_sold"):
        entry = scheme_entry[col]
        assert isinstance(entry, dict) and entry.get("optional") is True, col
    assert scheme_entry["Quantity"] == "float"
    assert scheme_entry["harvest_month"] == "str"
    assert "condition" not in scheme_entry["index"]
    assert "intercropped" not in scheme_entry


def test_2009_10_is_a_wave_script_not_yaml(gsps_root, wave_specs):
    """The two-season matrix has to be melted; YAML cannot."""
    assert (gsps_root / "2009-10" / "_" / "crop_production.py").exists()
    assert wave_specs["2009-10"] is None
    src = (gsps_root / "2009-10" / "_" / "crop_production.py").read_text()
    assert "S4AV1.dta" in src and "S4AV2.dta" in src
    assert "'major'" in src and "'minor'" in src


def test_yaml_waves_map_j_and_u_at_extraction(wave_specs):
    w2 = wave_specs["2013-14"]
    assert w2["file"] == "04n_harvestquestions.dta"
    assert w2["idxvars"]["j"] == ["cropname", {"mappings": CROP_MAPPING}]
    assert w2["idxvars"]["u"] == ["harvestunit", {"mappings": UNIT_MAPPING}]
    assert "v" not in w2["idxvars"]
    w3 = wave_specs["2017-18"]
    assert w3["dfs"] == ["df_harvest", "df_sales"]
    assert w3["df_harvest"]["file"] == "04n_harvestquestions.dta"
    assert w3["df_sales"]["file"] == "04o_cropsalesstoresquestions.dta", (
        "04o_cropsalesstoresSERVICES is agricultural services, not sales")
    assert w3["merge_on"] == ["i", "plot_id", "j"] and w3["merge_how"] == "left"
    assert w3["df_harvest"]["idxvars"]["j"] == ["cropname", {"mappings": CROP_MAPPING}]


def test_2013_14_has_no_sales_columns_by_design(wave_specs):
    """2013-14's 04o file has no plotid -> no plot-crop sales."""
    mv = wave_specs["2013-14"]["myvars"]
    assert not any(k.startswith("_sq_") for k in mv)
    assert "Quantity_sold" not in mv and "Value_sold" not in mv


@pytest.mark.parametrize("table", ["harmonize_crop", "harvest_units"])
def test_mapping_tables_have_each_spelling_exactly_once(tables, table):
    """`set_index().to_dict()` keeps the LAST row on a duplicate key."""
    t = tables[table]
    dup = t["Alternate Spelling"].duplicated(keep=False)
    assert not dup.any(), t.loc[dup]


def test_harvest_units_targets_are_the_food_acquired_vocabulary(gsps_root, tables):
    """crop_production.u must be the same vocabulary food_acquired.u delivers:
    the harmonizedunit Preferred Labels in _/units.org."""
    import sys
    sys.path.insert(0, str(gsps_root / "_"))
    import ghanasps
    vocab = set(ghanasps.harvest_unit_vocabulary())
    targets = set(tables["harvest_units"]["Preferred Label"].dropna().astype(str))
    assert targets <= vocab, targets - vocab


def test_harmonize_crop_reuses_food_labels_and_maps_the_w3_other(tables):
    lookup = tables["harmonize_crop"].set_index("Alternate Spelling")["Preferred Label"].to_dict()
    assert lookup["Groundnut/Peanut"] == lookup["groundnut/peanut"] == lookup["Groundnut/ Pea nut"] == "Groundnuts"
    assert lookup["Guinea corn/Sorghum"] == lookup["Guinea"] == "Sorghum"
    assert lookup["Oil Palm"] == "Palm Nuts"
    assert lookup["Other"] == "Other Crop"
    assert lookup["pepe"] == lookup["Pepper"]


# --------------------------------------------------------------------------
# Data: the content checks no framework guard makes
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gsps_build():
    """``Country('GhanaSPS').crop_production()`` plus every warning it emitted."""
    from lsms_library.null_read_audit import null_read_reports

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country("GhanaSPS").crop_production()
    assert df is not None and not df.empty, "GhanaSPS crop_production built empty"
    grain = [str(w.message) for w in caught
             if w.category.__name__ == "GrainCollapseWarning"
             and "crop_production" in str(w.message)]
    nullread = null_read_reports(country="GhanaSPS", table="crop_production")
    flat = df.reset_index()
    flat["t"] = flat["t"].astype(str)
    return flat, grain, nullread


@pytest.mark.requires_s3
def test_index_unique_no_grain_warning_no_null_read(gsps_build):
    df, grain, nullread = gsps_build
    assert not df.duplicated(subset=["t", "i", "plot_id", "j", "u", "season"]).any()
    assert df["j"].notna().all() and df["u"].notna().all() and df["season"].notna().all()
    assert not grain, grain
    assert not nullread, nullread


@pytest.mark.requires_s3
def test_w1_delivers_two_seasons_and_minor_is_the_smaller_set(gsps_build):
    """THE label-error pin.  S4AV2's labels say major; the questionnaire says
    minor; the wave must show both seasons with minor the smaller."""
    df, _, _ = gsps_build
    w1 = df[df["t"] == "2009-10"]
    per = w1.groupby("season").size().to_dict()
    assert set(per) == {"major", "minor"}, per
    assert per["minor"] < per["major"], per
    assert per == EXPECTED_W1_SEASON_ROWS, per


@pytest.mark.requires_s3
def test_s4av2_rows_carry_season_minor():
    """Recompute, from S4AV2.dta itself, the set of (household, plot) with a
    minor-season crop-1 quantity, and check every one of them is delivered
    under season == 'minor' -- i.e. the season came from the FILE, not from
    S4AV2's (wrong) variable labels."""
    import sys
    root = countries_root() / "GhanaSPS"
    sys.path.insert(0, str(root / "_"))
    import ghanasps
    v2 = get_dataframe(str(root / "2009-10/Data/S4AV2.dta"), convert_categoricals=False)
    has_minor = v2[v2["s4v_a122i"].notna() & v2["s4av2_plotno"].notna()]
    assert len(has_minor) == 1412, len(has_minor)
    # the script drops a slot whose crop id is missing (5 here) or undefined
    # (code 47, 1 here) -- no crop identity, no row; documented and counted
    has_minor = has_minor[has_minor["s4v_a121i"].isin(list(ghanasps._W1_CROP_CODES))]
    assert len(has_minor) == 1406, len(has_minor)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country("GhanaSPS").crop_production().reset_index()
    w1 = df[df["t"].astype(str) == "2009-10"]
    minor_plots = set(zip(w1.loc[w1["season"] == "minor", "i"].astype(str),
                          w1.loc[w1["season"] == "minor", "plot_id"].astype(str)))
    src_plots = set(zip(has_minor["hhno"].astype(int).astype(str),
                        has_minor["s4av2_plotno"].astype(int).astype(str)))
    assert src_plots <= minor_plots, f"{len(src_plots - minor_plots)} S4AV2 plots not delivered as minor"
    v1 = get_dataframe(str(root / "2009-10/Data/S4AV1.dta"), convert_categoricals=False)
    assert int(v1["s4v_a81i"].notna().sum()) == 4825


@pytest.mark.requires_s3
def test_single_recall_waves_carry_the_constant_season(gsps_build):
    df, _, _ = gsps_build
    for wave in ("2013-14", "2017-18"):
        assert set(df.loc[df["t"] == wave, "season"]) == {"annual"}, wave


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_row_counts_pin_the_melt_and_the_bounded_reducer(gsps_build, wave):
    df, _, _ = gsps_build
    assert len(df[df["t"] == wave]) == EXPECTED_ROWS[wave], (wave, len(df[df["t"] == wave]))


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_j_and_u_within_declared_vocabularies(gsps_build, tables, gsps_root, wave):
    """An unmapped label passes through unchanged, so a plausible top-10
    proves nothing -- the subset assertion is the test."""
    import sys
    sys.path.insert(0, str(gsps_root / "_"))
    import ghanasps
    df, _, _ = gsps_build
    w = df[df["t"] == wave]
    crop_vocab = set(tables["harmonize_crop"]["Preferred Label"].dropna().astype(str))
    got_j = set(w["j"].astype(str))
    if wave == "2009-10":
        # part-qualified products exist only where the part is recorded:
        # 'Cocoyam Leaves' or 'Crop (part)' with Crop a vocabulary label
        qualified = {j for j in got_j - crop_vocab
                     if j == "Cocoyam Leaves" or (j.endswith(")") and j.rsplit(" (", 1)[0] in crop_vocab)}
        assert "Cocoyam Leaves" in qualified
        got_j -= qualified
    else:
        assert "Cocoyam Leaves" not in got_j
    assert got_j <= crop_vocab, f"{wave}: {got_j - crop_vocab}"
    # 'Kg': the global categorical_mapping/u.org folds every kg variant onto
    # 'Kg' at API time for any table with a u level (parquet: 'Kilogram')
    unit_vocab = set(ghanasps.harvest_unit_vocabulary()) | {"Unknown", "Kg"}
    if wave == "2009-10":
        unit_vocab |= W1_RESIDUAL_UNIT_CODES
    got_u = set(w["u"].astype(str))
    assert got_u <= unit_vocab, f"{wave}: {got_u - unit_vocab}"


@pytest.mark.requires_s3
def test_sales_asymmetry_is_as_documented(gsps_build):
    df, _, _ = gsps_build
    w1, w2, w3 = (df[df["t"] == w] for w in WAVES)
    assert w1["Quantity_sold"].isna().all(), "2009-10 never asks a sold quantity"
    assert w1["Value_sold"].notna().mean() > 0.5, "2009-10 A83/A124 revenue"
    assert w2["Quantity_sold"].isna().all() and w2["Value_sold"].isna().all(), "2013-14: no plotid in 04o"
    assert 0.4 < w3["Value_sold"].notna().mean() < 0.7
    assert 0.35 < w3["Quantity_sold"].notna().mean() < 0.6
    # Quantity_sold only where the sale unit is the harvest unit -> never on Unknown/Other rows
    assert w3.loc[w3["u"].isin(["Unknown", "Other"]), "Quantity_sold"].isna().all()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_every_harvest_plot_is_in_plot_features(gsps_build, wave):
    """Same plot_id derivation as plot_features -> the join is total."""
    df, _, _ = gsps_build
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pf = ll.Country("GhanaSPS").plot_features().reset_index()
    pf["t"] = pf["t"].astype(str)
    keys = set(zip(df.loc[df["t"] == wave, "i"].astype(str), df.loc[df["t"] == wave, "plot_id"].astype(str)))
    pf_keys = set(zip(pf.loc[pf["t"] == wave, "i"].astype(str), pf.loc[pf["t"] == wave, "plot_id"].astype(str)))
    assert keys <= pf_keys, f"{wave}: {len(keys - pf_keys)} harvest plots not in plot_features"
    assert "v" in df.columns and df.loc[df["t"] == wave, "v"].notna().all()


@pytest.mark.requires_s3
def test_harvest_month_tokens(gsps_build):
    df, _, _ = gsps_build
    hm = df["harvest_month"].dropna().astype(str)
    toks = hm.str.split().explode()
    assert toks.str.fullmatch(r"(0[1-9]|1[0-2])").all()
    # 2009-10 is a plot-level season-end PROXY: one token per row
    assert df.loc[df["t"] == "2009-10", "harvest_month"].dropna().astype(str).str.split().map(len).eq(1).all()
    assert df.loc[df["t"] == "2017-18", "harvest_month"].dropna().astype(str).str.split().map(len).max() == 12


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_dedup_groups_are_exactly_the_known_ones(gsps_root, wave):
    """Recompute from the SOURCE files the duplicate (plot, product, unit,
    season) groups the script / hook sum, and pin their number; a NEW
    duplicate is a change in the source that must be looked at."""
    import sys
    import pandas as pd
    sys.path.insert(0, str(gsps_root / "_"))
    import ghanasps
    label = ghanasps.crop_label_map()
    if wave == "2009-10":
        units = ghanasps.w1_unit_labels()
        rows = []
        for fn, plot, slots, season in (
            ("S4AV1.dta", "s4av1_plotno", [("s4v_a80i", "s4v_a80ii", "s4v_a81i", "s4v_a81ii", "s4v_a83i", "s4v_a83ii"),
                                            ("s4v_a88i", "s4v_a88ii", "s4v_a89i", "s4v_a89ii", "s4v_a91i", "s4v_a91ii"),
                                            ("s4v_a96i", "s4v_a96ii", "s4v_a97i", "s4v_a97ii", "s4v_a99i", "s4v_a99ii"),
                                            ("s4v_a104i", "s4v_a104ii", "s4v_a105i", "s4v_a105ii", "s4v_a107i", "s4v_a107ii"),
                                            ("s4v_a112i", "s4v_a112ii", "s4v_a113i", "s4v_a113ii", "s4v_a115i", "s4v_a115ii")], "major"),
            ("S4AV2.dta", "s4av2_plotno", [("s4v_a121i", "s4v_a121ii", "s4v_a122i", "s4v_a122ii", "s4v_a124i", "s4v_a124ii"),
                                            ("s4v_a129i", "s4v_a129ii", "s4v_a130i", "s4v_a130ii", "s4v_a132i", "s4v_a132ii"),
                                            ("s4v_a137i", "s4v_a137ii", "s4v_a138i", "s4v_a138ii", "s4v_a140i", "s4v_a140ii"),
                                            ("s4v_a145i", "s4v_a145ii", "s4v_a146i", "s4v_a146ii", "s4v_a148i", "s4v_a148ii"),
                                            ("s4v_a153i", "s4v_a153ii", "s4v_a154i", "s4v_a154ii", "s4v_a156i", "s4v_a156ii")], "minor")):
            d = get_dataframe(str(gsps_root / "2009-10/Data" / fn), convert_categoricals=False)
            for cid, part, qty, unit, ced, pes in slots:
                p = pd.DataFrame({"i": d["hhno"], "plot": d[plot], "crop": d[cid], "part": d[part],
                                  "qty": d[qty], "unit": d[unit], "ced": d[ced], "pes": d[pes]})
                p["season"] = season
                rows.append(p)
        r = pd.concat(rows)
        r = r[r["crop"].isin(list(ghanasps._W1_CROP_CODES)) & r["plot"].notna()
              & r[["qty", "unit", "ced", "pes"]].notna().any(axis=1)]
        crop = r["crop"].astype(int).map(ghanasps._W1_CROP_CODES).map(lambda s: label.get(s, s))
        part = r["part"].map(ghanasps._W1_PART_CODES)
        # the script's unit rule: decoded label; undefined positive code -> the
        # code string; missing / 0 / -1 -> Unknown
        u = r["unit"].map(units)
        residual = u.isna() & r["unit"].notna() & (r["unit"] > 0)
        u = u.where(~residual, r["unit"].where(residual).map(lambda x: str(int(x)) if pd.notna(x) else x))
        r = r.assign(j=[ghanasps.product_label(c, p) for c, p in zip(crop, part)],
                     u=u.where(u.notna(), "Unknown"))
        key = ["i", "plot", "j", "u", "season"]
    else:
        unit_tbl = all_dfs_from_orgfile(gsps_root / "_" / "categorical_mapping.org")["harvest_units"]
        unit_map = unit_tbl.set_index("Alternate Spelling")["Preferred Label"].to_dict()
        d = get_dataframe(str(gsps_root / wave / "Data/04n_harvestquestions.dta"))
        d = d[(d["cropname"].astype(str).str.strip() != "")
              & ~(d["harvestquantity"].isna() & d["harvestunit"].isna())]
        u = d["harvestunit"].astype(object).map(lambda x: unit_map.get(x, x))
        u = ghanasps.fold_other_unit(u.where(u.notna(), "Unknown"), d["harvestunitother"])
        r = d.assign(i=d["FPrimary"], plot=d["plotid"],
                     j=d["cropname"].map(lambda s: label.get(s, s)),
                     u=u, season="annual")
        key = ["i", "plot", "j", "u", "season"]
    dup = r.duplicated(subset=key, keep=False)
    groups = r[dup].groupby(key, dropna=False).size()
    assert len(groups) == EXPECTED_DEDUP_GROUPS[wave], (
        f"{wave}: {len(groups)} duplicate groups, expected {EXPECTED_DEDUP_GROUPS[wave]}:\n{groups}")
