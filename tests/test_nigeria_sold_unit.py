"""Nigeria ``crop_production.Unit_sold`` -- the sale's OWN unit code (GH #824).

The GHS-Panel post-harvest A3 module asks q6 "How much did you harvest?"
with a QUANTITY and a PROD. UNIT CODE, and asks q11 "How much did you sell
to or through this [BUYER]?" with *its own* QUANTITY and PROD. UNIT CODE
(``sa3q11b``).  ``_/crop_production.py`` passed the harvest unit and the
sold quantity but never the sold unit, so ``Value_sold / Quantity_sold`` was
filed under the HARVEST unit by assumption on every row --
``Nigeria/_/CONTENTS.org`` measured the assumption to fail on 3.1% (2011Q1)
/ 4.7% (2013Q1) of positive-sale rows.  ``Unit_sold`` is now a REPORTED
column on W1/W2, canonicalised through the same size-agnostic
``nigeria._canon_unit`` as ``u``.

Four things are pinned, and they fail differently:

1. **The wiring** (data-free): ``nigeria.crop_production_for_wave`` puts the
   canonicalised sold unit on the row, and delivers an all-NA ``string``
   column when the frame names none (W3-W5) -- the dtype pin is the same
   concat hazard ``KgFactor`` guards against.
2. **Which waves name it** (data-free, config): the country script is read
   with ``ast`` -- never imported, it runs data reads -- and only the W1/W2
   frame dicts may carry ``unit_sold``.  W3-W5 record sales in ``secta3ii``
   at hh-crop grain with no plot linkage (GH #843), so a sold unit there
   would be attached to a plot the survey never tied it to.
3. **The identification** (data-gated): the column is a UNIT column by the
   survey's own VALUE-LABEL vocabulary -- ``sa3q11b``'s code->label dict is
   IDENTICAL to the harvest unit's in both waves -- not by its variable
   label (W1's reads "(11a)...(unit)", a Stata varlabel slip).  This is the
   check @ligon's condition-as-unit trap warning on #824 demands, and it is
   stronger than Uganda's unit-only-share test because here the sets
   coincide exactly.
4. **The agreement rates** (data-gated): the raw-code figures CONTENTS.org
   records (96.9% / 95.3%) reproduce on the raw files, and the served,
   size-collapsed labels agree MORE often (97.7% / 96.3%) -- the difference
   is sales from a big sack of a harvest kept in a medium one, invisible to
   ``_canon_unit``.

Plus a GH #867 guard, inverted by ruling (@ligon, 2026-10-14): Nigeria's
``Other Inorganic Fertilizer`` (W1-W3 instrument code "4. other (specify)",
relabelled inorganic by ``categorical_mapping.org``) and Mali's ``Other
inorganic`` must NOT resolve to a nitrogen share here.  An earlier commit
on this branch aliased them to the 0.20 class share as a "spelling fix";
measured, that moved Mali's served kg N by +13.5% (9,640 rows, +122,434 kg)
and Nigeria's by +4,402 kg, undisclosed.  The vocabulary belongs to #867,
with its own disclosure and a maintainer signature -- so this test fails
the moment an alias reaches those labels by accident.

Schema facts are READ from the country's ``data_scheme.yml`` and from
``lsms_library/data_info.yml``, never hardcoded (CLAUDE.md, "Canonical
Schema").
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
UNIT_SOLD = "Unit_sold"

#: The sold-unit column, per wave dir, and the harvest-unit column it must
#: share a value-label set with.  W3-W5: none, by design (GH #843).
SOLD_UNIT_COLUMNS = {
    "2010-11": dict(
        file="Data/Post Harvest Wave 1/Agriculture/secta3_harvestw1.dta",
        unit="sa3q6b", unit_sold="sa3q11b", qty_sold="sa3q11a",
        value_sold="sa3q12", t="2011Q1"),
    "2012-13": dict(
        file="Data/Post Harvest Wave 2/Agriculture/secta3_harvestw2.dta",
        unit="sa3q6a2", unit_sold="sa3q11b", qty_sold="sa3q11a",
        value_sold="sa3q12", t="2013Q1"),
}
WAVES_WITHOUT_SOLD_UNIT = {"2015-16", "2018-19", "2023-24"}

#: Raw-code agreement recorded in Nigeria/_/CONTENTS.org (measured
#: 2026-09-08, re-measured 2026-09-24): positive-sale rows that record both
#: units, and how many carry the SAME raw unit code.
EXPECTED_RAW_AGREEMENT = {
    "2011Q1": dict(both=3548, same=3438),   # 96.9%
    "2013Q1": dict(both=3478, same=3316),   # 95.3%
}
#: The same rows after ``_canon_unit`` (size-agnostic) -- what a consumer of
#: the served ``Unit_sold != u`` flag actually sees.
EXPECTED_CANON_AGREEMENT = {
    "2011Q1": dict(both=3548, same=3466),   # 97.7%
    "2013Q1": dict(both=3478, same=3349),   # 96.3%
}


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _load_nigeria_module():
    """Import ``Nigeria/_/nigeria.py`` by path (it is not a package)."""
    spec = importlib.util.spec_from_file_location("nigeria", NIGERIA / "_" / "nigeria.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("nigeria", mod)
    spec.loader.exec_module(mod)
    return mod


def _script_source():
    return (NIGERIA / "_" / "crop_production.py").read_text()


def _frame_dicts_by_wave(source):
    """``{wave_dir: [set(keys of each frame dict)]}`` read from the country
    script with ``ast``.  A frame dict is a ``dict(...)`` call carrying a
    ``qty=`` keyword; its wave is the ``../<wave>/`` path literal nearest
    above it in the source."""
    tree = ast.parse(source)
    lines = source.splitlines()
    out = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "dict"):
            continue
        keys = {kw.arg for kw in node.keywords}
        if "qty" not in keys:
            continue
        wave = None
        for ln in range(node.lineno - 1, -1, -1):
            for w in list(SOLD_UNIT_COLUMNS) + sorted(WAVES_WITHOUT_SOLD_UNIT):
                if f"../{w}/" in lines[ln]:
                    wave = w
                    break
            if wave:
                break
        assert wave is not None, f"frame dict at line {node.lineno} has no wave path above it"
        out.setdefault(wave, []).append(keys)
    return out


def _raw(path):
    from lsms_library.local_tools import get_dataframe
    try:
        raw = get_dataframe(str(path), convert_categoricals=False)
        dec = get_dataframe(str(path), convert_categoricals=True)
    except Exception as exc:  # pragma: no cover - data-gated
        pytest.skip(f"raw Nigeria harvest file unavailable: {exc}")
    return raw, dec


def _synthetic_frame(dec_unit, dec_unit_sold, *, name_unit_sold):
    """One-frame input to ``crop_production_for_wave`` with decoded labels."""
    n = len(dec_unit)
    df = pd.DataFrame({
        "hhid": [f"h{k}" for k in range(n)],
        "plotid": ["1"] * n,
        "cropcode": [1010] * n,
        "sa3q6a": [10.0] * n,
        "sa3q6b": list(range(n)),
        "sa3q11a": [5.0] * n,
        "sa3q11b": list(range(n)),
        "sa3q12": [1000.0] * n,
    })
    dec = df.copy()
    dec["sa3q6b"] = dec_unit
    dec["sa3q11b"] = dec_unit_sold
    fr = dict(df=df, dec=dec, hhid="hhid", plot="plotid", crop="cropcode",
              qty="sa3q6a", unit="sa3q6b",
              qty_sold="sa3q11a", value_sold="sa3q12", sold_on="plot",
              perennial=False)
    if name_unit_sold:
        fr["unit_sold"] = "sa3q11b"
    return fr


# --------------------------------------------------------------------------
# 1. the wiring (data-free)
# --------------------------------------------------------------------------

class TestWiring:
    def test_sold_unit_is_canonicalised_like_the_harvest_unit(self):
        ng = _load_nigeria_module()
        harvest = ["130. SACK/BAG: BIG/LARGE (100 KG)", "1. KILOGRAMS", "3. LITRES", "TIYA"]
        sold = ["131. SACK/BAG: MEDIUM (50 KG)", "1. KILOGRAMS", "MUDU", None]
        fr = _synthetic_frame(harvest, sold, name_unit_sold=True)
        out = ng.crop_production_for_wave("2011Q1", [fr], {1010: "Maize"})
        assert UNIT_SOLD in out.columns
        assert out[UNIT_SOLD].dtype == pd.StringDtype() or str(out[UNIT_SOLD].dtype).startswith("string")
        got = out.reset_index().sort_values("i")[[ "u", UNIT_SOLD]]
        # big vs medium sack agree after the size-agnostic collapse; Kg==Kg;
        # a litre harvest sold by the mudu disagrees; a blank sold unit is NA.
        assert list(got["u"]) == ["Sack/Bag", "Kg", "l", "Tiya"]
        assert list(got[UNIT_SOLD].astype(object).where(got[UNIT_SOLD].notna(), None)) == \
            ["Sack/Bag", "Kg", "Mudu", None]
        # Unit_sold goes through the SAME canonicaliser as u.
        assert got[UNIT_SOLD].iloc[0] == ng._canon_unit(sold[0])

    def test_frame_without_sold_unit_delivers_all_na_string_column(self):
        ng = _load_nigeria_module()
        fr = _synthetic_frame(["1. KILOGRAMS"] * 3, ["1. KILOGRAMS"] * 3, name_unit_sold=False)
        out = ng.crop_production_for_wave("2016Q1", [fr], {1010: "Maize"})
        assert UNIT_SOLD in out.columns
        assert out[UNIT_SOLD].isna().all()
        assert str(out[UNIT_SOLD].dtype).startswith("string"), out[UNIT_SOLD].dtype

    def test_frame_without_plot_grain_sale_delivers_all_na_string_column(self):
        """W3-W5 shape: no ``sold_on`` at all."""
        ng = _load_nigeria_module()
        fr = _synthetic_frame(["1. KILOGRAMS"] * 2, ["1. KILOGRAMS"] * 2, name_unit_sold=True)
        fr.pop("sold_on")
        out = ng.crop_production_for_wave("2019Q1", [fr], {1010: "Maize"})
        assert out[UNIT_SOLD].isna().all()
        assert str(out[UNIT_SOLD].dtype).startswith("string")
        # and the sale quantity is not attached either -- a sold unit
        # without a plot-grain sale would be a unit for nothing.
        assert out["Quantity_sold"].isna().all()

    def test_column_order_keeps_every_pre_existing_column(self):
        ng = _load_nigeria_module()
        fr = _synthetic_frame(["1. KILOGRAMS"], ["1. KILOGRAMS"], name_unit_sold=True)
        out = ng.crop_production_for_wave("2011Q1", [fr], {1010: "Maize"})
        scheme = load_yaml(NIGERIA / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        declared = [k for k in scheme if k not in ("index", "materialize", "backend", "aggregation")]
        assert set(declared) <= set(out.columns), set(declared) - set(out.columns)


# --------------------------------------------------------------------------
# 2. which waves name it (data-free, config)
# --------------------------------------------------------------------------

class TestConfig:
    def test_only_w1_w2_frames_name_a_sold_unit(self):
        frames = _frame_dicts_by_wave(_script_source())
        for wave, cfg in SOLD_UNIT_COLUMNS.items():
            assert wave in frames, f"no frame dict found for {wave}"
            for keys in frames[wave]:
                assert "unit_sold" in keys, f"{wave} frame does not pass unit_sold"
                assert "sold_on" in keys
        for wave in WAVES_WITHOUT_SOLD_UNIT:
            assert wave in frames, f"no frame dict found for {wave}"
            for keys in frames[wave]:
                assert "unit_sold" not in keys, (
                    f"{wave} names a sold unit, but its sale sits in secta3ii "
                    "at hh-crop grain with no plot linkage (GH #843)")
                assert "sold_on" not in keys

    def test_w1_w2_name_sa3q11b_literally(self):
        src = _script_source()
        assert src.count("unit_sold='sa3q11b'") == 2

    def test_scheme_declares_unit_sold_optional(self):
        scheme = load_yaml(NIGERIA / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert UNIT_SOLD in scheme
        assert scheme[UNIT_SOLD] == {"type": "str", "optional": True}, scheme[UNIT_SOLD]
        assert UNIT_SOLD not in scheme["index"], "Unit_sold is a column, never an index level"

    def test_canonical_schema_declares_unit_sold_optional_for_crop_production(self):
        from importlib.resources import files
        canon = load_yaml(files("lsms_library") / "data_info.yml")
        col = canon["Columns"][TABLE][UNIT_SOLD]
        assert col["type"] == "str" and col["optional"] is True


# --------------------------------------------------------------------------
# 3 + 4. identification and agreement (data-gated)
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wave", sorted(SOLD_UNIT_COLUMNS))
class TestRawFiles:
    def test_sold_unit_shares_the_harvest_unit_value_label_set(self, wave):
        """The sold unit is a UNIT column by the survey's own value-label
        vocabulary -- the code->label dict is identical to the harvest
        unit's -- and not merely a column that merges cleanly."""
        pyreadstat = pytest.importorskip("pyreadstat")
        from lsms_library.data_access import get_data_file
        cfg = SOLD_UNIT_COLUMNS[wave]
        try:
            path = get_data_file(str(NIGERIA / wave / cfg["file"]))
        except Exception as exc:  # pragma: no cover - data-gated
            pytest.skip(f"raw file unavailable: {exc}")
        _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
        set_h = meta.variable_to_label.get(cfg["unit"])
        set_s = meta.variable_to_label.get(cfg["unit_sold"])
        assert set_h and set_s, (set_h, set_s)
        labels_h = meta.value_labels[set_h]
        labels_s = meta.value_labels[set_s]
        assert labels_s == labels_h, "sold-unit value labels differ from the harvest unit's"
        assert len(labels_h) >= 30

    def test_raw_and_canonical_agreement_rates(self, wave):
        ng = _load_nigeria_module()
        cfg = SOLD_UNIT_COLUMNS[wave]
        raw, dec = _raw(NIGERIA / wave / cfg["file"])
        q = pd.to_numeric(raw[cfg["qty_sold"]], errors="coerce")
        v = pd.to_numeric(raw[cfg["value_sold"]], errors="coerce")
        pos = (q > 0) & (v > 0)
        both = pos & raw[cfg["unit"]].notna() & raw[cfg["unit_sold"]].notna()
        same_raw = both & (raw[cfg["unit"]] == raw[cfg["unit_sold"]])
        hc = dec[cfg["unit"]].map(ng._canon_unit).astype("string")
        sc = dec[cfg["unit_sold"]].map(ng._canon_unit).astype("string")
        same_can = both & (hc == sc).fillna(False)
        t = cfg["t"]
        assert int(both.sum()) == EXPECTED_RAW_AGREEMENT[t]["both"]
        assert int(same_raw.sum()) == EXPECTED_RAW_AGREEMENT[t]["same"]
        assert int(same_can.sum()) == EXPECTED_CANON_AGREEMENT[t]["same"]
        # The size-agnostic collapse can only ADD agreement, never remove it.
        assert int(same_can.sum()) >= int(same_raw.sum())
        # Every recorded sold unit canonicalises (no silent NA on a label).
        recorded = dec[cfg["unit_sold"]].notna() & ~dec[cfg["unit_sold"]].astype(str).str.fullmatch(r"[\d.]+")
        assert sc[recorded].notna().all(), sorted(dec.loc[recorded & sc.isna(), cfg["unit_sold"]].astype(str).unique())


# --------------------------------------------------------------------------
# the built table (data-gated; skips where the cache is cold or pre-fix)
# --------------------------------------------------------------------------

class TestBuiltTable:
    def test_unit_sold_populated_only_in_w1_w2(self):
        from lsms_library.local_tools import data_root
        path = data_root() / "Nigeria" / "var" / f"{TABLE}.parquet"
        if not path.exists():
            pytest.skip("no warm Nigeria crop_production parquet")
        df = pd.read_parquet(path)
        if UNIT_SOLD not in df.columns:
            pytest.skip("warm Nigeria crop_production parquet predates Unit_sold (re-warm needed)")
        t = df.index.get_level_values("t") if "t" in df.index.names else df["t"]
        nonnull = df[UNIT_SOLD].notna().groupby(np.asarray(t)).sum()
        assert nonnull.get("2011Q1", 0) > 3000
        assert nonnull.get("2013Q1", 0) > 3000
        for w in ("2016Q1", "2019Q1", "2024Q1"):
            assert nonnull.get(w, 0) == 0, (w, nonnull.get(w))
        assert str(df[UNIT_SOLD].dtype).startswith("string"), df[UNIT_SOLD].dtype

    def test_api_frame_serves_unit_sold_through_finalize(self):
        """Same counts on the SERVED frame -- exercises `_finalize_result`
        (categorical mapping, canonical spellings, dtype coercion), which a
        raw `read_parquet` of the cache does not.  Reads the warm parquet
        only (`assume_cache_fresh=True`); never cold-builds in CI."""
        from lsms_library.local_tools import data_root
        path = data_root() / "Nigeria" / "var" / f"{TABLE}.parquet"
        if not path.exists():
            pytest.skip("no warm Nigeria crop_production parquet")
        if UNIT_SOLD not in pd.read_parquet(path, columns=None).columns:
            pytest.skip("warm Nigeria crop_production parquet predates Unit_sold (re-warm needed)")
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country("Nigeria", assume_cache_fresh=True).crop_production()
        assert UNIT_SOLD in df.columns
        assert str(df[UNIT_SOLD].dtype).startswith("string"), df[UNIT_SOLD].dtype
        assert UNIT_SOLD not in df.index.names
        t = df.index.get_level_values("t")
        nonnull = df[UNIT_SOLD].notna().groupby(np.asarray(t)).sum()
        assert nonnull.get("2011Q1", 0) > 3000
        assert nonnull.get("2013Q1", 0) > 3000
        for w in ("2016Q1", "2019Q1", "2024Q1"):
            assert nonnull.get(w, 0) == 0, (w, nonnull.get(w))
        # The only sold label absent from the harvest vocabulary is Olodo
        # (CONTENTS.org): the read-path passes do not move u and Unit_sold apart.
        # (`u` is a column on the Country frame; Feature() moves it to the index.)
        u = df["u"] if "u" in df.columns else df.index.get_level_values("u").to_series()
        extra = set(df[UNIT_SOLD].dropna().unique()) - set(u.dropna().unique())
        assert extra <= {"Olodo"}, extra


# --------------------------------------------------------------------------
# GH #867 guard: 'other inorganic' labels carry NO nitrogen share (by ruling)
# --------------------------------------------------------------------------

#: Labels that name an "other" fertilizer the instrument does not quantify.
#: Nigeria W1-W3 `s11dq3` is {1 npk, 2 urea, 3 composite manure, 4 other
#: (specify)}; `categorical_mapping.org` relabels code 4 "Other Inorganic
#: Fertilizer".  Mali's EACI slot is `Other inorganic` (categorical_mapping
#: `harmonize_input`).  A share for any of them is a maintainer decision on
#: GH #867, not a spelling alias (@ligon, 2026-10-14).
UNSHARED_OTHER_LABELS = ["Other Inorganic Fertilizer", "Other inorganic", "other (specify)"]


class TestOtherInorganicSpelling:
    @pytest.mark.parametrize("label", UNSHARED_OTHER_LABELS)
    def test_label_does_not_resolve_to_a_nitrogen_share(self, label):
        from lsms_library import transformations as T
        keys = [k for k in T._normalise_input_label(label) if k in T._NITROGEN_CONTENT]
        assert not keys, (
            f"{label!r} resolves to {keys} in _NITROGEN_CONTENT; a nitrogen share "
            "for an 'other' fertilizer slot is a GH #867 decision, not an alias "
            "(ruling @ligon 2026-10-14: Mali +13.5% kg N from the last one)")

    def test_nitrogen_kg_names_the_row_unmatched_and_warns(self):
        from lsms_library import transformations as T
        df = pd.DataFrame({
            "t": ["2011Q1"] * 2, "i": ["1", "1"], "plot_id": ["p1", "p1"],
            "input": ["Other Inorganic Fertilizer", "Urea"], "j": ["n/a", "n/a"],
            "Quantity": [10.0, 10.0], "u": ["Kg", "Kg"],
        }).set_index(["t", "i", "plot_id", "input", "j"])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            out = T.nitrogen_kg(df)
        tally = out.attrs["nitrogen_input_match"]
        assert "Other Inorganic Fertilizer" not in tally["matched_labels"]
        assert tally["unmatched_rows"] == 1
        assert "Other Inorganic Fertilizer" in tally["unmatched_labels"]
        # Only the urea row contributes; the 'other' row contributes nothing
        # and is NAMED rather than silently absorbed.
        assert float(out["Nitrogen_kg"].iloc[0]) == pytest.approx(10 * T._NITROGEN_CONTENT["urea"])
        cov = [x for x in w if issubclass(x.category, T.NutrientCoverageWarning)]
        assert cov and "Other Inorganic Fertilizer" in str(cov[0].message)
