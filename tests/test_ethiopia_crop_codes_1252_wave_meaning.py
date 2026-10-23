"""Ethiopia crop codes whose MEANING changes between waves (GH #1252).

``harmonize_crop`` (``Ethiopia/_/categorical_mapping.org``) mapped each ESS
crop code to one label for every wave.  A sweep of the Stata value labels of
every crop-code variable in every crop module (§3/§4/§5/§7 post-planting,
§9/§9a/§10/§11/§12 post-harvest, the WB Crop_CF tables), every wave, finds
three codes that change meaning:

    code   W1 2011-12                       W2 2013-14     from W3 2015-16
       9   ROMAN                            ROMAN          Mung Bean/ MASHO
      19   CACTUS                           CACTUS         RED KIDENY BEANS
      86   FALLOW LAND (PH) / TEMPORARY GR  TEMPORARY GR   TEMPORARY GR

so W1/W2 cactus was served ``j = 'Red Kidney Beans'`` (and ``perennial`` was
set from the WB's W1-built list for code 19 in every wave, so W3-W5 kidney
beans were served perennial).  ``harmonize_crop_wave`` now overrides only
those (wave, code) cells, read through
``ethiopia._eth_crop_label_map_for_wave``, and every per-wave decode uses it.

Tiers: the synthetic tests need no data; ``requires_s3`` tests read the raw
``.dta`` blobs or build the tables.
"""
from __future__ import annotations

import importlib.util
import re
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

ROOT = countries_root() / "Ethiopia"
WAVES = ("2011-12", "2013-14", "2015-16", "2018-19", "2021-22")


@pytest.fixture(scope="module")
def eth():
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_1252_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# Synthetic / config
# ---------------------------------------------------------------------------

EXPECTED = {  # wave -> (code 9, code 19, code 86)
    "2011-12": ("Roman", "Cactus", "Fallow Land"),
    "2013-14": ("Roman", "Cactus", "Temporary Grass"),
    "2015-16": ("Mung bean", "Red Kidney Beans", "Temporary Grass"),
    "2018-19": ("Mung bean", "Red Kidney Beans", "Temporary Grass"),
    "2021-22": ("Mung bean", "Red Kidney Beans", "Temporary Grass"),
}


@pytest.mark.parametrize("wave", WAVES)
def test_each_wave_decodes_the_drifting_codes_by_its_own_meaning(eth, wave):
    m = eth._eth_crop_label_map_for_wave(wave)
    assert (m[9], m[19], m[86]) == EXPECTED[wave]


@pytest.mark.parametrize("wave", WAVES)
def test_only_the_drifting_codes_differ_from_the_code_keyed_table(eth, wave):
    """The override changes ONLY the drifting codes' labels (and the code-keyed
    default is the meaning from 2015-16 on, so W3-W5 equal it exactly)."""
    base = eth._eth_crop_label_map()
    m = eth._eth_crop_label_map_for_wave(wave)
    assert set(m) == set(base)
    differ = {c for c in m if m[c] != base[c]}
    assert differ <= {9, 19, 86}
    if wave >= "2015-16":
        assert differ == set()


def test_new_labels_collide_with_no_other_code(eth):
    """'Cactus', 'Roman' and 'Temporary Grass' are each one code's label in any
    wave, and none is a food_acquired label for something else."""
    for wave in WAVES:
        m = eth._eth_crop_label_map_for_wave(wave)
        for lab in ("Cactus", "Roman", "Temporary Grass"):
            assert len([c for c, v in m.items() if v == lab]) <= 1, (wave, lab)


COLMAP = dict(
    hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
    field_id="field_id", crop_code="crop_code", quantity="qty",
)


def _cp(eth, wave):
    h = pd.DataFrame([("HH1", "H1", 1, 1, 19, 3.0), ("HH1", "H1", 1, 2, 9, 2.0),
                      ("HH1", "H1", 1, 3, 72, 4.0)],
                     columns=["household_id", "holder_id", "parcel_id",
                              "field_id", "crop_code", "qty"])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return eth.crop_production_for_wave(wave, h, None, None, COLMAP)


@pytest.mark.parametrize("wave", WAVES)
def test_crop_production_rows_and_perennial_follow_the_wave(eth, wave):
    """Code 19 is perennial only as CACTUS (W1/W2) -- the meaning the WB
    ETH_ESS1.do PERENNIAL/FRUIT list was written for; coffee (72) is
    perennial in every wave."""
    df = _cp(eth, wave)
    j = set(df.index.get_level_values("j"))
    assert {EXPECTED[wave][0], EXPECTED[wave][1], "Coffee"} == j
    per = dict(zip(df.index.get_level_values("j"), df["perennial"]))
    assert per[EXPECTED[wave][1]] == (wave in ("2011-12", "2013-14"))
    assert bool(per["Coffee"])


# ---------------------------------------------------------------------------
# Raw oracle: each §9 file's OWN value label decides the served label
# ---------------------------------------------------------------------------

#: the survey's labels (after its "NN." prefix) -> our Preferred Labels
VOCAB = {"CACTUS": "Cactus", "ROMAN": "Roman", "RED KIDENY BEANS": "Red Kidney Beans",
         "REDKIDENY BEANS": "Red Kidney Beans", "MUNG BEAN/ MASHO": "Mung bean",
         "MUNG BEAN OR MASHO": "Mung bean", "FALLOW LAND": "Fallow Land",
         "TEMPORARY GR": "Temporary Grass"}
S9 = {"2011-12": ("sect9_ph_w1.dta", "crop_code", "ph_s9q12_a", None),
      "2013-14": ("sect9_ph_w2.dta", "crop_code", "ph_s9q04_a", "ph_s9q04_b"),
      "2015-16": ("sect9_ph_w3.dta", "crop_code", "ph_s9q04_a", "ph_s9q04_b"),
      "2018-19": ("sect9_ph_w4.dta", "s9q00b", "s9q05a", "s9q05b"),
      "2021-22": ("sect9_ph_w5.dta", "s9q00b", "s9q05a", "s9q05b")}


def _norm(s):
    s = re.sub(r"^(\s*\d+\s*\.\s*)+", "", str(s)).strip().upper()
    return re.sub(r"\s+", " ", s)


def _file_labels(wave):
    """(plot_id, expected label) for every §9 row with code 9 / 19 / 86 that has
    a quantity or a unit (the rows the build serves), labelled by the FILE's own
    Stata value label -- not by our table."""
    import pyreadstat
    from lsms_library.data_access import get_data_file
    from lsms_library.local_tools import format_id
    f, cc, qv, uv = S9[wave]
    df, meta = pyreadstat.read_dta(str(get_data_file(f"Ethiopia/{wave}/Data/{f}")),
                                   apply_value_formats=False)
    labels = meta.value_labels[meta.variable_to_label[cc]]
    code = pd.to_numeric(df[cc], errors="coerce")
    kept = pd.to_numeric(df[qv], errors="coerce").notna()
    if uv:
        kept |= pd.to_numeric(df[uv], errors="coerce").notna()
    d = df[code.isin([9, 19, 86]) & kept]
    plot = (d["holder_id"].apply(format_id).astype(str) + "_"
            + d["parcel_id"].apply(format_id).astype(str) + "_"
            + d["field_id"].apply(format_id).astype(str))
    exp = [VOCAB[_norm(labels[c])] for c in pd.to_numeric(d[cc]).astype(int)]
    return pd.DataFrame({"plot_id": plot.values, "expected": exp})


@pytest.fixture(scope="module")
def cp():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").crop_production()


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_raw_oracle_served_label_is_the_files_own(cp, wave):
    """Every served plot holding a code 9 / 19 / 86 §9 row carries the label
    that §9 file's own Stata value label gives (measured: 7 / 46 / 788 / 290
    / 297 plots, all agree; before GH #1252 W1 1 of 7 and W2 0 of 46)."""
    E = _file_labels(wave)
    s = cp[cp.index.get_level_values("t") == wave].reset_index()[["plot_id", "j"]]
    served = s.groupby("plot_id")["j"].apply(set)
    E = E[E["plot_id"].isin(served.index)]
    bad = [(p, e) for p, e in zip(E["plot_id"], E["expected"]) if e not in served[p]]
    assert not bad, (len(bad), len(E))
    assert len(E) > 0


@pytest.mark.requires_s3
def test_served_relabelled_rows_and_perennial(cp):
    """Measured cold 2026-10-23: W1 serves 5 Cactus / 1 Roman rows, W2 35 / 11
    (the issue's 45 cactus rows are raw §9 rows; 5 W2 ones have neither unit
    nor quantity and are not served).  No W1/W2 row is 'Red Kidney Beans' or
    'Mung bean'.  ``perennial``: True on every Cactus row, False on every Red
    Kidney Beans row (692 / 245 / 291 in W3 / W4 / W5 were True)."""
    t = np.asarray(cp.index.get_level_values("t"), dtype=object)
    j = np.asarray(cp.index.get_level_values("j"), dtype=object)
    n = lambda w, lab: int(((t == w) & (j == lab)).sum())  # noqa: E731
    assert (n("2011-12", "Cactus"), n("2011-12", "Roman")) == (5, 1)
    assert (n("2013-14", "Cactus"), n("2013-14", "Roman")) == (35, 11)
    for w in ("2011-12", "2013-14"):
        assert n(w, "Red Kidney Beans") == 0 and n(w, "Mung bean") == 0
    per = cp["perennial"]
    assert per[j == "Cactus"].eq(True).all()
    assert per[j == "Red Kidney Beans"].eq(False).all()
    assert {w: n(w, "Red Kidney Beans") for w in ("2015-16", "2018-19", "2021-22")} \
        == {"2015-16": 692, "2018-19": 245, "2021-22": 291}


@pytest.mark.requires_s3
def test_shipped_kg_factors_decode_by_the_same_wave_map(eth):
    """Crop_CF_Wave2 labels 19 CACTUS and 9 ROMAN: its factors must land on the
    W2 'Cactus' / 'Roman' rows they belong to, not on kidney beans / mung
    beans (measured: 15 Cactus and 1 Roman (crop, unit) keys in 2013-14)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cf = eth.crop_conversion_factors()
    t = np.asarray(cf.index.get_level_values("t"), dtype=object)
    j = np.asarray(cf.index.get_level_values("j"), dtype=object)
    assert int(((t == "2013-14") & (j == "Cactus")).sum()) == 15
    assert int(((t == "2013-14") & (j == "Roman")).sum()) == 1
    assert int(((t == "2013-14") & np.isin(j, ["Red Kidney Beans", "Mung bean"])).sum()) == 0


@pytest.mark.requires_s3
def test_sect12_derivation_inputs_decode_by_the_wave():
    """``inputs_sect12_sale`` keys its frame by the same wave decode, so a W2
    cactus sale's raw lines are found from its served row."""
    import lsms_library as ll
    raw = ll.Country("Ethiopia").derivation_inputs(
        "Ethiopia::crop_production::sect12-sale-period", wave="2013-14")
    j = raw.index.get_level_values("j")
    code = pd.to_numeric(raw["crop_code"], errors="coerce")
    assert set(j[code == 19].dropna()) == {"Cactus"}
    assert set(j[code == 9].dropna()) <= {"Roman"}


@pytest.mark.requires_s3
def test_plot_inputs_seed_crops_decode_by_the_wave():
    """``plot_inputs`` decodes seed crops through the same map: measured W1 13
    Cactus / 1 Roman rows, W2 39 / 15; 4 W4 rows move 'Fallow Land' ->
    'Temporary Grass'."""
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pi = Country("Ethiopia").plot_inputs()
    t = np.asarray(pi.index.get_level_values("t"), dtype=object)
    j = np.asarray(pi.index.get_level_values("j"), dtype=object)
    n = lambda w, lab: int(((t == w) & (j == lab)).sum())  # noqa: E731
    assert (n("2011-12", "Cactus"), n("2011-12", "Roman")) == (13, 1)
    assert (n("2013-14", "Cactus"), n("2013-14", "Roman")) == (39, 15)
    assert n("2018-19", "Temporary Grass") == 4 and n("2018-19", "Fallow Land") == 0
