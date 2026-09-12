"""GhanaSPS ``livestock`` -- HerdValue, not ValuePerAnimal, pinned (refs #729, #736, #140).

All three GhanaSPS instruments ask for the herd TOTAL -- "What is the
current value of these animals if you sold *all of them*" -- so the value
column is the canonical ``HerdValue`` (additive over ``animal``), never
``ValuePerAnimal`` (a per-head price that Uganda / Nigeria / Malawi record
and which is NOT additive).  Mapping the GhanaSPS number onto
``ValuePerAnimal`` would ship a herd total in a unit-price column: the
right shape, every framework guard green, and every downstream sum wrong.
The config tests here pin that declaration data-free.

The one Python piece is a bounded REDUCER: ``ghanasps.livestock`` sums the
duplicate ``(t, i, animal)`` lines the sources contain (2009-10: 20
households with 2-3 ``Other Farm Animals`` lines; 2017-18: one household
with ``Chickens/roosters`` in the main file and ``Chickens`` in ``_osp``).
That is legal only because every declared column is additive at this
grain, and it is kept honest by pinning the per-wave counts below -- a NEW
duplicate must turn this file red rather than be summed away.

Data-dependent tests are marked ``requires_s3`` (the conftest's documented
spelling; nothing is imported from it) and are skipped in the data-free CI
job.  They do NOT swallow exceptions.  The grain condition is asserted
directly (zero ``GrainCollapseWarning`` naming ``livestock``) rather than
by running under ``LSMS_GRAIN_STRICT``; ``LSMS_READ_STRICT=1`` is not
relied on either, because on this branch it is fatal inside
``GhanaSPS/sample`` (``Rural`` / ``weight`` / ``panel_weight`` 100% null in
2013-14 / 2017-18, a documented property of that table) which
``_join_v_from_sample`` re-enters for every household table -- so the
read-strict condition for THIS table is asserted via ``null_read_reports``.
"""
import warnings

import pytest
import yaml
from importlib.resources import files

import lsms_library as ll
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

WAVES = ("2009-10", "2013-14", "2017-18")
MAPPING = ["harmonize_species", "Alternate Spelling", "Preferred Label"]
# Wave-2 free-text spellings that are genuinely unmappable and are delivered
# as their own labels rather than guessed at (categorical_mapping.org).
PASSTHROUGH = {"50", "cut", "dake"}
# Duplicate (t, i, animal) groups the hook sums, per wave -- see the module
# docstring.  A change here is a change in the SOURCE, and must be looked at.
EXPECTED_DEDUP_GROUPS = {"2009-10": 20, "2013-14": 0, "2017-18": 1}
EXPECTED_ROWS = {"2009-10": 4248, "2013-14": 3963, "2017-18": 4284}


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
    # `load_yaml`, not `yaml.safe_load`: data_scheme.yml carries `!make` tags.
    with open(gsps_root / "_" / "data_scheme.yml") as f:
        return load_yaml(f)["Data Scheme"]["livestock"]


@pytest.fixture(scope="module")
def wave_specs(gsps_root):
    out = {}
    for w in WAVES:
        with open(gsps_root / w / "_" / "data_info.yml") as f:
            out[w] = yaml.safe_load(f)["livestock"]
    return out


@pytest.fixture(scope="module")
def species_table(gsps_root):
    return all_dfs_from_orgfile(gsps_root / "_" / "categorical_mapping.org")["harmonize_species"]


def _canonical_livestock_columns():
    with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)["Columns"]["livestock"]


def test_value_column_is_HerdValue_not_ValuePerAnimal(scheme_entry):
    """The additivity pin.  A herd total may never wear a unit-price name."""
    assert "HerdValue" in scheme_entry
    assert "ValuePerAnimal" not in scheme_entry
    assert "SalesValue" not in scheme_entry
    assert "Value" not in scheme_entry, "bare `Value` is forbidden in livestock (PR #736)"


def test_declared_columns_are_canonical_and_HerdValue_is_monetary(scheme_entry):
    canon = _canonical_livestock_columns()
    declared = {c for c in scheme_entry if c != "index"}
    assert declared == {"HeadCount", "HeadSold", "HerdValue"}
    assert declared <= set(canon), declared - set(canon)
    assert canon["HerdValue"].get("monetary") is True
    assert "HeadAcquired" not in scheme_entry, "never asked in any GhanaSPS wave"


def test_index_is_canonical_grain_without_v(scheme_entry):
    assert scheme_entry["index"].replace(" ", "") == "(t,i,animal)"


def test_HeadSold_is_optional_because_only_2017_18_asks_it(scheme_entry, wave_specs):
    hs = scheme_entry["HeadSold"]
    assert isinstance(hs, dict) and hs.get("optional") is True, (
        "HeadSold is asked in 2017-18 only; without `optional` the Site B "
        "null-read guard fires on the two waves that never ask it")
    assert "HeadSold" in wave_specs["2017-18"]["myvars"]
    assert "HeadSold" not in wave_specs["2009-10"]["myvars"]
    assert "HeadSold" not in wave_specs["2013-14"]["myvars"]


def test_w1_reads_both_cedis_and_pesewas(wave_specs):
    """Reading s3ai_3i alone truncates 25 rows silently (FINDINGS idiosyncrasy 8)."""
    mv = wave_specs["2009-10"]["myvars"]
    assert mv["_cedis"] == "s3ai_3i"
    assert mv["_pesewas"] == "s3ai_3ii"
    assert "HerdValue" not in mv, "W1 HerdValue is written by the hook, not extracted"
    assert wave_specs["2009-10"]["file"] == "S3AI.dta"


def test_w2_w3_read_currentvalue_as_HerdValue(wave_specs):
    for w in ("2013-14", "2017-18"):
        assert wave_specs[w]["myvars"]["HerdValue"] == "currentvalue"
        assert wave_specs[w]["myvars"]["HeadCount"] == "quantity"


def test_w3_concatenates_the_osp_file(wave_specs):
    """`_osp` = "other -- specify": the free-text tail of the wave-3 roster."""
    assert wave_specs["2017-18"]["file"] == [
        "03ai_animalquestions.dta", "03ai_animalquestions_osp.dta"]
    assert "dfs" not in wave_specs["2017-18"], "row-concat, not a merge"


@pytest.mark.parametrize("wave", WAVES)
def test_animal_is_mapped_at_extraction_through_harmonize_species(wave_specs, wave):
    animal = wave_specs[wave]["idxvars"]["animal"]
    assert isinstance(animal, list) and animal[-1] == {"mappings": MAPPING}
    assert "v" not in wave_specs[wave]["idxvars"]


def test_species_table_has_each_spelling_exactly_once(species_table):
    """`set_index().to_dict()` keeps the LAST row on a duplicate key, so a
    spelling shared by two waves must have exactly one row."""
    dup = species_table["Alternate Spelling"].duplicated(keep=False)
    assert not dup.any(), species_table.loc[dup]


def test_species_table_targets_reuse_precedent_labels(species_table):
    """Precedent label wins: Uganda's species-level plurals, Nigeria's and
    Malawi's `Guinea Fowl` / `Duck` / `Turkey` / `Fish` / `Dove/Pigeon` /
    `Other Livestock`.  Pins the targets so a re-spelling is deliberate."""
    targets = set(species_table["Preferred Label"].dropna())
    precedent = {"Cattle", "Sheep", "Goats", "Pigs", "Rabbits", "Chicken",
                 "Donkeys", "Other Poultry", "Guinea Fowl", "Duck", "Turkey",
                 "Fish", "Dove/Pigeon", "Other Livestock"}
    assert precedent <= targets, precedent - targets
    # The three source spellings of the value-bearing catch-alls resolve.
    lookup = species_table.set_index("Alternate Spelling")["Preferred Label"].to_dict()
    assert lookup["Other Farm Animals"] == "Other Livestock"
    assert lookup["Drought Animal"] == "Draught Animals"
    assert lookup["Draught animal (donkey, horse, bullock)"] == "Draught Animals"
    assert lookup["Chicken/Rosters"] == lookup["chickens"] == lookup["Chickens/roosters"] == "Chicken"
    assert "50" not in lookup and "cut" not in lookup and "dake" not in lookup


# --------------------------------------------------------------------------
# Data: the content checks no framework guard makes
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gsps_build():
    """``Country('GhanaSPS').livestock()`` plus every warning it emitted."""
    from lsms_library.null_read_audit import null_read_reports

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country("GhanaSPS").livestock()
    assert df is not None and not df.empty, "GhanaSPS livestock built empty"
    grain = [str(w.message) for w in caught
             if w.category.__name__ == "GrainCollapseWarning"
             and "livestock" in str(w.message)]
    nullread = null_read_reports(country="GhanaSPS", table="livestock")
    flat = df.reset_index()
    flat["t"] = flat["t"].astype(str)
    return flat, grain, nullread


@pytest.mark.requires_s3
def test_index_unique_no_grain_warning_no_null_read(gsps_build):
    df, grain, nullread = gsps_build
    assert not df.duplicated(subset=["t", "i", "animal"]).any()
    assert df["animal"].notna().all(), "a null animal survived the hook"
    assert not grain, grain
    assert not nullread, nullread


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_row_counts_pin_the_bounded_reducer(gsps_build, wave):
    """Delivered rows per wave.  Any change here means the SOURCE or the
    HOOK changed (a new duplicate, a keep-rule edge case, a re-issued .dta)
    and must be looked at; the duplicate groups themselves are pinned
    directly by ``test_dedup_groups_are_exactly_the_known_ones`` below."""
    df, _, _ = gsps_build
    w = df[df["t"] == wave]
    assert len(w) == EXPECTED_ROWS[wave], (wave, len(w))


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_animal_values_within_declared_vocabulary(gsps_build, species_table, wave):
    """The subset assertion -- an unmapped label passes through unchanged, so
    a plausible distribution proves nothing."""
    df, _, _ = gsps_build
    vocab = set(species_table["Preferred Label"].dropna()) | PASSTHROUGH
    got = set(df.loc[df["t"] == wave, "animal"].dropna())
    assert got <= vocab, f"{wave}: {got - vocab}"
    if wave != "2013-14":
        assert not (got & PASSTHROUGH), "pass-throughs are a wave-2 fact"


@pytest.mark.requires_s3
def test_no_zero_head_filler_survives(gsps_build):
    """Wave 2's grid has 10,884 zero-head rows; none carries a value, and
    none is delivered.  Every delivered row has >= 1 positive measure."""
    df, _, _ = gsps_build
    assert not (df["HeadCount"] == 0).any()
    positive = ((df["HeadCount"].fillna(0) > 0) | (df["HeadSold"].fillna(0) > 0)
                | (df["HerdValue"].fillna(0) > 0))
    assert positive.all()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_herd_value_per_head_orders_cattle_goats_chicken(gsps_build, wave):
    """Scale sanity per wave.  A pesewas-as-cedis mistake in wave 1 would be
    a x100 on the cattle figure; a per-head column mislabelled as a herd
    total would invert nothing here, which is why the wording -- not this
    test -- settles the column name."""
    df, _, _ = gsps_build
    w = df[(df["t"] == wave) & (df["HeadCount"] > 0)]
    per = w["HerdValue"] / w["HeadCount"]
    cattle = per[w["animal"] == "Cattle"].median()
    goats = per[w["animal"].isin(["Goats", "Sheep"])].median()
    chicken = per[w["animal"] == "Chicken"].median()
    assert cattle > goats > chicken, (wave, cattle, goats, chicken)
    assert 100 < cattle < 5000, f"{wave}: cattle {cattle} GHS/head is off-scale"


@pytest.mark.requires_s3
def test_HeadSold_is_2017_18_only(gsps_build):
    df, _, _ = gsps_build
    assert df.loc[df["t"] != "2017-18", "HeadSold"].isna().all()
    w3 = df[df["t"] == "2017-18"]
    assert w3["HeadSold"].notna().mean() > 0.99
    assert (w3["HeadSold"] > 0).sum() > 1000


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_dedup_groups_are_exactly_the_known_ones(species_table, wave):
    """Recompute, from the SOURCE files, the duplicate (i, animal) groups the
    hook sums -- after the null-animal drop and the keep-rule, exactly as
    the hook sees them -- and pin their number: 20 / 0 / 1.  A NEW
    duplicate is a change in the source that must be looked at, not summed
    away; this is what keeps the reducer bounded."""
    import pandas as pd
    from lsms_library.local_tools import get_dataframe

    root = countries_root() / "GhanaSPS"
    label = species_table.set_index("Alternate Spelling")["Preferred Label"].to_dict()
    if wave == "2009-10":
        r = get_dataframe(str(root / "2009-10/Data/S3AI.dta"))
        cedis = pd.to_numeric(r["s3ai_3i"], errors="coerce")
        pes = pd.to_numeric(r["s3ai_3ii"], errors="coerce")
        frame = pd.DataFrame({
            "i": r["hhno"].astype(str), "animal_raw": r["animal_id"].astype(object),
            "HeadCount": pd.to_numeric(r["s3ai_1"], errors="coerce"),
            "HeadSold": float("nan"),
            "HerdValue": (cedis.fillna(0) + pes.fillna(0) / 100).where(cedis.notna() | (pes.fillna(0) > 0)),
        })
    else:
        parts = [get_dataframe(str(root / wave / "Data/03ai_animalquestions.dta"))]
        if wave == "2017-18":
            parts.append(get_dataframe(str(root / wave / "Data/03ai_animalquestions_osp.dta")))
        r = pd.concat(parts, axis=0, sort=False)
        frame = pd.DataFrame({
            "i": r["FPrimary"].astype(str), "animal_raw": r["animal"].astype(object),
            "HeadCount": pd.to_numeric(r["quantity"], errors="coerce"),
            "HeadSold": (pd.to_numeric(r["quantitysold"], errors="coerce")
                         if "quantitysold" in r.columns else float("nan")),
            "HerdValue": pd.to_numeric(r["currentvalue"], errors="coerce"),
        })
    frame["animal"] = frame["animal_raw"].map(lambda x: label.get(x, x))
    frame = frame[frame["animal"].notna()]
    holds = (frame[["HeadCount", "HeadSold", "HerdValue"]].fillna(0) > 0).any(axis=1)
    frame = frame[holds]
    dup = frame.duplicated(subset=["i", "animal"], keep=False)
    groups = frame[dup].groupby(["i", "animal"]).size()
    assert len(groups) == EXPECTED_DEDUP_GROUPS[wave], (
        f"{wave}: {len(groups)} duplicate (i, animal) groups, expected "
        f"{EXPECTED_DEDUP_GROUPS[wave]} -- a new duplicate must be looked at:\n{groups}")
    if wave == "2009-10":
        assert set(groups.index.get_level_values("animal")) == {"Other Livestock"}
    if wave == "2017-18":
        assert list(groups.index) == [("106183008", "Chicken")]


@pytest.mark.requires_s3
def test_every_livestock_household_is_in_sample(gsps_build):
    """`v` is not attached to livestock by canonical design (index_info omits
    it; skip_extra) -- the equivalent check is that the join is available."""
    df, _, _ = gsps_build
    assert "v" not in df.columns
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        s = ll.Country("GhanaSPS").sample().reset_index()
    for wave in WAVES:
        hh = set(df.loc[df["t"] == wave, "i"].astype(str))
        sid = set(s.loc[s["t"].astype(str) == wave, "i"].astype(str))
        assert hh <= sid, f"{wave}: {len(hh - sid)} livestock households not in sample()"
