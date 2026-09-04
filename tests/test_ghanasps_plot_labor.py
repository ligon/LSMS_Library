"""GhanaSPS ``plot_labor`` -- the two silent mislabels, pinned (refs #729, #140).

Two things in this table would be WRONG WITH EVERY FRAMEWORK GUARD GREEN, and
both are pinned here.

1. **2009-10's stage assignment.**  Section 4 IX asks one nine-cell labour
   battery four times per season, in eight files (S4AIX1..8), and the variable
   label TEXT is identical across the four stage files.  What is NOT identical
   is the question number each label is prefixed with -- S4AIX1 carries
   A290..A298, S4AIX2 A299..A307, S4AIX3 A308..A316, S4AIX4 A317..A325, and
   A327..A362 for the minor-season four -- and the questionnaire's own section
   headers name the stage of each A-range.  A wrong file -> stage map would
   leave every row present, the index unique and every sanity check passing.
   ``test_w1_stage_files_carry_their_questionnaire_a_numbers`` recomputes the
   A-numbers from the .dta labels themselves.

2. **2017-18's ``stageid`` is an ORDINAL, not a stage code.**  It looks exactly
   like 2013-14's ``stagenum``, which IS the stage (a full 7 x 4,693 grid).
   ``stageid == 1`` is "Clearing and land preparation" on 2,285 rows,
   "Planting" on 1,130, "Harvesting" on 42.  Keying wave 3 on it would
   mislabel about 60 per cent of its rows, silently.

Also pinned: the brief's own STEP 3 requirement that all eight 2009-10
(season, stage) cells are populated with minor << major; that the later
waves' ``*days`` columns are per-worker durations so PersonDays multiplies by
the worker count; and that there is no ``Wage`` column, because Uganda's and
Nigeria's ``Wage`` is cash paid and this is a rate.

Data-dependent tests are marked ``requires_s3`` (the conftest's documented
spelling) and are skipped in the data-free CI job.  They do NOT swallow
exceptions.  ``LSMS_READ_STRICT=1`` is not relied on -- on this branch it is
fatal inside ``GhanaSPS/sample`` (CONTENTS.org) -- so the read-strict
condition for THIS table is asserted via ``null_read_reports``.
"""
import warnings

import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

WAVES = ("2009-10", "2013-14", "2017-18")

#: file number -> (stage, first A-number, last A-number), from the eight
#: questionnaire section headers quoted in _/categorical_mapping.org.
W1_BLOCKS = {
    1: ("land_preparation", 290, 298), 5: ("land_preparation", 327, 335),
    2: ("field_management", 299, 307), 6: ("field_management", 336, 344),
    3: ("harvesting",       308, 316), 7: ("harvesting",       345, 353),
    4: ("post_harvest",     317, 325), 8: ("post_harvest",     354, 362),
}

LATER_STAGES = {"clearing_and_land_preparation", "ploughing", "planting",
                "chemical_application", "weeding", "harvesting", "post_harvest"}
W1_STAGES = {"land_preparation", "field_management", "harvesting", "post_harvest"}

EXPECTED_ROWS = {"2009-10": 30864, "2013-14": 27874, "2017-18": 31149}
#: The STEP 3 check: all eight 2009-10 (season, stage) cells populated,
#: minor << major.
EXPECTED_W1_CELLS = {
    ("major", "land_preparation"): 6866, ("minor", "land_preparation"): 1511,
    ("major", "field_management"): 6761, ("minor", "field_management"): 2133,
    ("major", "harvesting"):       5949, ("minor", "harvesting"):       1709,
    ("major", "post_harvest"):     4652, ("minor", "post_harvest"):     1283,
}


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
        return load_yaml(f)["Data Scheme"]["plot_labor"]


@pytest.fixture(scope="module")
def tables(gsps_root):
    return all_dfs_from_orgfile(gsps_root / "_" / "categorical_mapping.org")


def test_index_keeps_stage_and_season_and_omits_v(scheme_entry):
    assert scheme_entry["index"].replace(" ", "") == "(t,i,plot_id,season,stage,source)"
    assert scheme_entry.get("materialize") == "make"


def test_there_is_no_wage_column_because_this_is_a_rate(scheme_entry):
    """Uganda's and Nigeria's ``plot_labor.Wage`` is CASH PAID; GhanaSPS asks
    "How much on average did you pay each man ...?" plus "per day or per
    acre?".  Different quantities must not share a column name."""
    assert "Wage" not in scheme_entry
    for col in ("WageRateMen", "WageRateWomen", "WageRateChildren"):
        assert scheme_entry[col]["monetary"] is True, col


def test_wave_only_columns_are_optional_or_site_b_fires(scheme_entry):
    """Site B warns on a REQUIRED declared column 100% null in any wave's t
    slice.  2009-10 asks no payment at all (so all four wage columns are
    empty there) and only 2009-10 asks hours."""
    for col in ("Hours", "WageRateMen", "WageRateWomen",
                "WageRateChildren", "WageUnit"):
        assert scheme_entry[col].get("optional") is True, col
    assert scheme_entry["PersonDays"] == "float"


def test_all_three_waves_are_scripts(gsps_root):
    """The `source` axis is a melt across column groups; YAML cannot do it."""
    for w in WAVES:
        assert (gsps_root / w / "_" / "plot_labor.py").exists(), w
        with open(gsps_root / w / "_" / "data_info.yml") as f:
            assert (yaml.safe_load(f) or {}).get("plot_labor") is None, w


def test_w3_script_does_not_key_on_stageid(gsps_root):
    """The trap: wave 3's `stageid` is the ordinal position of the stage in
    the plot, not a stage code."""
    src = (gsps_root / "2017-18" / "_" / "plot_labor.py").read_text()
    assert "'stagename'" in src
    assert "later_wave_labor(stage_df, 'stageid'" not in src
    w2 = (gsps_root / "2013-14" / "_" / "plot_labor.py").read_text()
    assert "'stage'" in w2 and "later_wave_labor(stage_df, 'stagenum'" not in w2


@pytest.mark.parametrize("table,expected", [
    ("harmonize_stage", W1_STAGES | LATER_STAGES),
    ("harmonize_labor_source",
     {"self", "family", "communal", "hired", "other", "casual", "permanent"}),
    ("WageUnit", {"Day", "Week", "Month", "Plot", "Acres", "Poles", "Ropes", "Other"}),
])
def test_vocabularies(tables, table, expected):
    t = tables[table]
    assert set(t["Preferred Label"].astype(str).str.strip()) == expected


def test_wage_unit_reuses_the_area_unit_spellings(tables):
    """So a per-area rate converts with plot_features().Area."""
    area = set(tables["AreaUnit"]["Preferred Label"].astype(str).str.strip())
    wage = set(tables["WageUnit"]["Preferred Label"].astype(str).str.strip())
    assert {"Acres", "Poles", "Ropes", "Plot", "Other"} <= area
    assert {"Acres", "Poles", "Ropes", "Plot"} <= wage & area


def test_stage_table_is_idempotent(tables):
    """Every Preferred Label is its own Alternate Spelling, so the API-time
    auto-dispatch (WageUnit) and a second script pass are no-ops."""
    for name in ("harmonize_stage", "harmonize_labor_source", "WageUnit"):
        t = tables[name]
        alt = set(t["Alternate Spelling"].astype(str).str.strip())
        pref = set(t["Preferred Label"].astype(str).str.strip())
        assert pref <= alt, (name, sorted(pref - alt))


# --------------------------------------------------------------------------
# Data: the content checks no framework guard makes
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gsps_labor():
    """``Country('GhanaSPS').plot_labor()`` plus every warning it emitted."""
    from lsms_library.null_read_audit import null_read_reports

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country("GhanaSPS").plot_labor()
    assert df is not None and not df.empty, "GhanaSPS plot_labor built empty"
    grain = [str(w.message) for w in caught
             if w.category.__name__ == "GrainCollapseWarning"
             and "plot_labor" in str(w.message)]
    nullread = null_read_reports(country="GhanaSPS", table="plot_labor")
    flat = df.reset_index()
    flat["t"] = flat["t"].astype(str)
    return flat, grain, nullread


@pytest.mark.requires_s3
def test_index_unique_no_grain_warning_no_null_read(gsps_labor):
    df, grain, nullread = gsps_labor
    keys = ["t", "i", "plot_id", "season", "stage", "source"]
    assert not df.duplicated(subset=keys).any()
    for k in keys:
        assert df[k].notna().all(), k
    assert not grain, grain
    assert not nullread, nullread


@pytest.mark.requires_s3
def test_v_is_joined_from_sample(gsps_labor):
    """plot_labor is in neither exclusion list of lsms_library/data_info.yml
    (absent from index_info AND from `Join v from sample > skip_extra`), so
    _join_v_from_sample() attaches v."""
    df, _, _ = gsps_labor
    assert "v" in df.columns
    assert df["v"].notna().all()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_rows_per_wave(gsps_labor, wave):
    df, _, _ = gsps_labor
    assert len(df[df["t"] == wave]) == EXPECTED_ROWS[wave]


@pytest.mark.requires_s3
def test_w1_delivers_all_eight_season_stage_cells_minor_smaller(gsps_labor):
    """THE brief's STEP 3 check 4.  A missing cell means a file was not read;
    a minor cell larger than its major twin means the seasons were swapped."""
    df, _, _ = gsps_labor
    w1 = df[df["t"] == "2009-10"]
    per = w1.groupby(["season", "stage"]).size().to_dict()
    assert set(per) == set(EXPECTED_W1_CELLS), sorted(per)
    assert per == EXPECTED_W1_CELLS, per
    for stage in W1_STAGES:
        assert per[("minor", stage)] < per[("major", stage)], stage


@pytest.mark.requires_s3
def test_stage_and_source_vocabularies_are_per_instrument(gsps_labor):
    """2009-10's four coarse stages and three sources; the later waves' seven
    and five.  Neither is folded onto the other -- the containment lives in
    harmonize_stage / harmonize_labor_source."""
    df, _, _ = gsps_labor
    w1 = df[df["t"] == "2009-10"]
    assert set(w1["stage"]) == W1_STAGES
    assert set(w1["source"]) == {"casual", "permanent", "family"}
    assert set(w1["season"]) == {"major", "minor"}
    for wave in ("2013-14", "2017-18"):
        d = df[df["t"] == wave]
        assert set(d["stage"]) == LATER_STAGES, wave
        assert set(d["source"]) == {"self", "family", "communal", "hired", "other"}, wave
        assert set(d["season"]) == {"last"}, wave


@pytest.mark.requires_s3
def test_w1_stage_files_carry_their_questionnaire_a_numbers():
    """Recompute, from each S4AIX file's own variable labels, the A-numbers of
    its labour cells, and check they are the range the questionnaire heads
    with the stage this build assigns.  This is what makes the 2009-10 stage
    assignment checkable at all: the label TEXT is identical across the four
    stage files, the question NUMBER is not."""
    import io
    from pathlib import Path

    import pandas as pd

    from lsms_library.local_tools import _ensure_dvc_pulled, data_root

    root = countries_root() / "GhanaSPS" / "2009-10" / "Data"
    for fileno, (stage, lo, hi) in W1_BLOCKS.items():
        src = root / f"S4AIX{fileno}.dta"
        sidecar = src.parent / (src.name + ".dvc")
        if not sidecar.exists():
            pytest.skip(f"{src.name} sidecar unavailable")
        md5 = yaml.safe_load(sidecar.read_text())["outs"][0]["md5"]
        _ensure_dvc_pulled(str(src))
        blob = Path(data_root()) / "dvc-cache" / md5[:2] / md5[2:]
        with pd.io.stata.StataReader(io.BytesIO(blob.read_bytes())) as rdr:
            labels = rdr.variable_labels()
        seen = set()
        for var, lab in labels.items():
            if not var.startswith("s4aix_"):
                continue
            n = var[len("s4aix_"):].rstrip("i")
            if n.isdigit() and lo <= int(n) <= hi:
                seen.add(int(n))
                assert lab.startswith(f"A{n}."), (fileno, var, lab)
        assert seen == set(range(lo, hi + 1)), (
            f"S4AIX{fileno} carries {sorted(seen)}, not the A{lo}-A{hi} range "
            f"the questionnaire heads '{stage}'")


@pytest.mark.requires_s3
def test_w3_stageid_is_an_ordinal_not_a_stage_code():
    """The wave-3 trap, recomputed from the source: `stageid` values are NOT
    in 1:1 correspondence with stage names (unlike 2013-14's `stagenum`)."""
    from lsms_library.local_tools import get_dataframe

    root = countries_root() / "GhanaSPS"
    q3 = get_dataframe(str(root / "2017-18" / "Data" / "04m_aglabourquestions.dta"),
                       convert_categoricals=False)
    per_id = q3[q3["stagename"].astype(str).str.strip() != ""].groupby("stageid")[
        "stagename"].nunique()
    assert (per_id > 1).any(), (
        "stageid now maps 1:1 to a stage name; it has always been the ordinal "
        "position of the stage in the plot -- re-check before trusting it")
    q2 = get_dataframe(str(root / "2013-14" / "Data" / "04m_aglabourquestions.dta"),
                       convert_categoricals=False)
    assert q2.groupby("stagenum")["stage"].nunique().max() == 1, (
        "2013-14's stagenum IS the stage; if this changes the two scripts "
        "must be re-read together")


@pytest.mark.requires_s3
def test_person_days_multiply_by_the_worker_count():
    """The later waves' `*days` columns are PER-WORKER durations ("how many
    days ON AVERAGE did EACH OF ...").  Recompute one wave's hired
    person-days both ways and check the delivered total matches the product,
    not the bare days column."""
    from lsms_library.local_tools import get_dataframe

    df, *_ = _labor_frame()
    root = countries_root() / "GhanaSPS"
    q = get_dataframe(str(root / "2013-14" / "Data" / "04m_aglabourquestions.dta"),
                      convert_categoricals=False)
    product = float((q["hiredwomen"] * q["hiredwomendays"]).sum(min_count=1)
                    + (q["hiredmen"] * q["hiredmendays"]).sum(min_count=1))
    bare = float(q["hiredwomendays"].sum() + q["hiredmendays"].sum())
    delivered = float(df[(df["t"] == "2013-14") & (df["source"] == "hired")]["PersonDays"].sum())
    assert abs(delivered - product) < 1.0, (delivered, product)
    assert delivered > 2 * bare, (delivered, bare)


@pytest.mark.requires_s3
def test_wage_rates_only_on_hired_rows_and_only_where_asked(gsps_labor):
    df, _, _ = gsps_labor
    rate_cols = ["WageRateMen", "WageRateWomen", "WageRateChildren", "WageUnit"]
    hired_like = {"hired", "casual", "permanent"}
    assert not df[~df["source"].isin(hired_like)][rate_cols].notna().any().any()
    # 2009-10 asks NO payment question at all (Section 4 IX is A289-A362,
    # days / hours / workers only) -> every wage cell empty in that wave.
    w1 = df[df["t"] == "2009-10"]
    assert not w1[rate_cols].notna().any().any()
    # 2017-18 drops the child rate that 2013-14's M193 asks.
    assert df[df["t"] == "2017-18"]["WageRateChildren"].notna().sum() == 0
    assert df[df["t"] == "2013-14"]["WageRateChildren"].notna().sum() > 0
    # A per-day cedi rate is single- to low-double-digit GHS.
    for wave, lo, hi in (("2013-14", 3, 30), ("2017-18", 5, 60)):
        d = df[(df["t"] == wave) & (df["WageUnit"] == "Day")]
        med = d["WageRateMen"].median()
        assert lo <= med <= hi, (wave, med)


@pytest.mark.requires_s3
def test_hours_is_2009_10_only(gsps_labor):
    df, _, _ = gsps_labor
    assert df[df["t"] == "2009-10"]["Hours"].notna().mean() > 0.99
    for wave in ("2013-14", "2017-18"):
        assert df[df["t"] == wave]["Hours"].notna().sum() == 0, wave


@pytest.mark.requires_s3
def test_currency_attaches_ghs():
    """The three rate columns are declared `monetary: true` in the GhanaSPS
    data_scheme.yml, which currency._monetary_columns() unions in."""
    from lsms_library.currency import _monetary_columns

    cols = _monetary_columns("plot_labor", "GhanaSPS")
    assert {"WageRateMen", "WageRateWomen", "WageRateChildren"} <= set(cols)
    df = ll.Country("GhanaSPS").plot_labor(currency="index")
    assert "currency" in df.index.names
    assert set(df.index.get_level_values("currency")) == {"GHS"}


@pytest.mark.requires_s3
def test_every_labour_plot_is_a_plot_features_plot(gsps_labor):
    df, _, _ = gsps_labor
    pf = ll.Country("GhanaSPS").plot_features().reset_index()
    pf["t"] = pf["t"].astype(str)
    keys = set(map(tuple, pf[["t", "i", "plot_id"]].to_numpy()))
    lab = df[["t", "i", "plot_id"]].drop_duplicates()
    missing = [tuple(r) for r in lab.to_numpy() if tuple(r) not in keys]
    assert not missing, missing[:10]


def _labor_frame():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country("GhanaSPS").plot_labor()
    flat = df.reset_index()
    flat["t"] = flat["t"].astype(str)
    return flat, caught
