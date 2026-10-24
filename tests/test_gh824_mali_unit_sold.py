"""Mali ``crop_production`` carries the SOLD unit -- GH #824 (Mali half).

``u`` is the HARVEST unit.  Every EAC-I sale block records the sold quantity
in its OWN unit variable, which the build used to drop:

==========  ==========================  ============================
wave        field crops                 trees
==========  ==========================  ============================
2014-15     ``s3aq23b`` (3A.23)         ``s3bq13b`` (3B.13)
2017-18     ``s7gq21b`` (s7g, hh x crop) ``s11fq14b`` (11F.14)
==========  ==========================  ============================

so ``Value_sold / Quantity_sold`` was a price per an unnamed unit.  Contract
(canonical ``data_info.yml``, ``Unit_sold`` note; precedents
``tests/test_niger_sold_unit.py``, ``tests/test_uganda_sold_unit.py``): a
per-row COLUMN, decoded from the sold variable's OWN value labels,
canonicalised at BUILD time through the same ``u`` table as the harvest unit,
NA-bearing (NA on every row whose sale question was answered 'Non'), and a
disagreement with ``u`` is a FLAG.  The two 2017-18 label sets are not
code-compatible -- code 10 is 'Autre' on the harvest set and 'Tonne' on the
sold set -- so the measured disagreement counts are by LABEL.

Three families: ``TestDeclared`` (config / source, no microdata),
``TestFinalize`` (``mali.crop_production_finalize`` on a synthetic frame) and
``TestServed`` (builds the real table; needs S3).  Measured 2026-10-23, cold
build; see ``Mali/_/CONTENTS.org``, "crop_production: the sold unit, and tree
rows per harvest month (GH #824, #1267)".
"""
from __future__ import annotations

import importlib.util
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, get_dataframe, id_walk
from lsms_library.paths import countries_root

COUNTRY = "Mali"
TABLE = "crop_production"

#: Priced (Quantity_sold > 0 and Value_sold > 0) rows of the SERVED table, by
#: wave and row kind: (priced, Unit_sold non-null, knows both units, differ).
#: Units compared by canonical LABEL.  The raw-file audit (GH #824 comment,
#: 2026-10-23) counted 572 / 191 knows-both before the build's collapse and
#: crop-label drop; the disagreement counts reproduce exactly.  2014-15 tree
#: was (198, 196, 190, 42) at e2836e5d2: under the PR #1272 rulings the sold
#: quantity (rounds 1-3) and value (round 3) were 12-month totals, NA on the
#: 6 priced rows whose month count is 0 (3) or missing (3), and round 3
#: masked the 999999 value code, which 2 more priced rows carry (both sold
#: in Kg, beside a different harvest unit: differ 42 -> 40) -> (190, 188,
#: 184, 40) at development 57d255585.  The revised ruling serves the sale as
#: recorded, so 5 of the 6 return (none is a disagreement; the sixth records
#: the 999998 value code) and the 2 999999 rows stay out.  The FIELD row lost
#: the 2 priced rows whose value is the 999999 code (569 -> 567; the PR #1272
#: follow-up masks it on field values too).
PRICED = {
    ("2014-15", "field"): (567, 566, 565, 95),
    ("2014-15", "tree"): (195, 193, 187, 40),
    ("2017-18", "field"): (451, 451, 449, 87),
    ("2017-18", "tree"): (127, 127, 127, 18),
}
#: 2017-18 priced rows whose harvest unit is the HARVEST set's code 10
#: ('Autre') and whose sale unit is the SOLD set's code 10 ('Tonne'): the
#: label artefact the audit found (harvest 'Autre' is tonne-sized on most
#: rows, by the survey's own kg factor).  Left distinct -- relabelling the
#: harvest 'Autre' is a decision.
AUTRE_TONNE_2017 = 30
#: 2014-15 served field rows whose raw s3aq24 is the 999999 code (and whose
#: sale was not answered 'Non'): served 999999 at development 57d255585,
#: NA now (the PR #1272 follow-up, 2026-10-23).
FIELD_VALUE_CODE_2014 = 12
#: 2017-18 field rows served Quantity_sold == 0 / Value_sold == 0 (ruling on
#: PR #1272: 0 only on an explicit s7gq20 == 'Non').  8,806 'Non' rows each,
#: plus one 'Oui' sale line that records a quantity of 0 (its value is not
#: recorded).  At e2836e5d2 the plain `sum` served 10,460 / 10,468: 1,653 /
#: 1,662 unrecorded sales as 0 (624 unanswered s7gq20, the rest 'Oui').
ZERO_SOLD_2017 = {"Quantity_sold": 8807, "Value_sold": 8806}
#: s7g 'Oui' rows with no quantity and no unit: every one is a Leger-
#: questionnaire household (eaci17_s00p2.typequest), whose s7g asks 7G.20 but
#: not 7G.21 / 7G.22 (the questionnaire hatches those columns).
OUI_WITHOUT_QUANTITY_2017 = 1607


def _root() -> Path:
    return Path(countries_root()) / COUNTRY


@pytest.fixture
def mali(monkeypatch):
    """``Mali/_/mali.py`` from THIS checkout, cwd at its dir (its table readers
    resolve ``categorical_mapping.org`` relative to the cwd)."""
    repo = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(repo), ll.__file__
    source = repo / "lsms_library/countries/Mali/_/mali.py"
    spec = importlib.util.spec_from_file_location("mali_824_actual_source", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(source.parent)
    return module


class _Quiet(pd.DataFrame):
    """A served frame whose repr is its shape only.  A failing test's
    ARGUMENTS -- fixture values -- are printed in pytest's long traceback, and
    served Mali frames carry household ids (public repository; red team
    2026-10-23).  Slicing returns a plain DataFrame (pandas' ``_constructor``),
    which is a local and is not printed."""

    def __repr__(self):
        return f"<{len(self)} rows x {self.shape[1]} columns, not printed>"

    __str__ = __repr__
    _repr_html_ = None


def _quiet(df: pd.DataFrame) -> _Quiet:
    q = _Quiet(df)
    q.attrs = dict(df.attrs)
    return q


def _n(mask) -> int:
    """True cells of a boolean mask, NA counted False -- a count, never a row.
    TestServed asserts on counts held in locals: pytest's assertion rewrite
    prints the operands of a failing assert, and served frames carry
    household ids (public repository)."""
    return int(pd.Series(mask).fillna(False).astype(bool).sum())


def _scheme_entry():
    class _Loader(yaml.SafeLoader):
        pass
    _Loader.add_constructor("!make", lambda loader, node: {"__make__": True})
    with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
        return yaml.load(f, Loader=_Loader)["Data Scheme"][TABLE]


def _u_table():
    t = df_from_orgfile(str(_root() / "_" / "categorical_mapping.org"), name="u")
    return dict(zip(t["Code"].astype(str).str.strip(), t["Preferred Label"]))


# ---------------------------------------------------------------------------
# config / source -- no microdata
# ---------------------------------------------------------------------------

class TestDeclared:

    def test_data_scheme_declares_unit_sold_optional_str(self):
        decl = _scheme_entry()["Unit_sold"]
        assert decl == {"type": "str", "optional": True}

    @pytest.mark.parametrize("wave, var", [("2014-15", "s3aq23b"), ("2017-18", "s7gq21b")])
    def test_wave_script_reads_the_field_crop_sold_unit(self, wave, var):
        src = (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert var in src and "'Unit_sold'" in src

    def test_2017_18_sale_is_keyed_on_the_sold_unit(self):
        """The Niger 2021-22 pattern: one sale per (i, crop, Unit_sold), and a
        (household, crop) sold in two units attaches nothing."""
        src = (_root() / "2017-18" / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "groupby(['i', 'crop', 'Unit_sold']" in src
        assert "units_per == 1" in src

    def test_2014_15_field_value_masks_its_code(self):
        """999999, the six-digit field's 'non declare' code, is masked on the
        field value s3aq24 as on the tree value s3bq14 (mali._mask_value_code)."""
        src = (_root() / "2014-15" / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "_mask_value_code(s3a['s3aq24'])" in src

    def test_2017_18_sale_sums_with_min_count_vectorised(self):
        """Ruling on PR #1272: an unrecorded sale stays NA -- sum(min_count=1),
        vectorised (no per-group callable: pandas deep-copies attrs per group,
        GH #871) -- and 0 is filled only on an explicit 'Non'."""
        src = (_root() / "2017-18" / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "[['Quantity_sold', 'Value_sold']].sum(min_count=1)" in src
        assert "'Quantity_sold': 'sum'" not in src
        assert ".agg(" not in src            # no per-group aggregation callable
        assert "eq('Non')" in src

    def test_u_table_maps_every_sold_unit_label(self):
        """Every label the four sold-unit variables carry, except the survey's
        'Manquant', has a row in the u table -- an unmapped label would be
        served NA silently.  'Chargement de camion' (11F) was the gap."""
        labels = {
            # s3aq23b / s3bq13b / s7gq21b / s11fq14b value labels (EAC-I .dta)
            "Kg", "Charretée", "Grenier", "Sac", "Bassine", "Panier", "Calebasse",
            "Moude", "Gerbe", "Autre", "Kilogramme", "Chargement camion", "Casier",
            "Tonne", "Chargement de camion",
        }
        table = _u_table()
        assert labels - set(table) == set()
        assert table["Chargement de camion"] == "Chargement camion"
        assert table["Kilogramme"] == "Kg" and table["Tonne"] == "Tonne"
        assert "Manquant" not in table


# ---------------------------------------------------------------------------
# the shared tail, on a synthetic frame
# ---------------------------------------------------------------------------

def _row(**kw):
    r = dict(t="2014-15", i="h1", plot_id="1_1", crop="Mil", u="Sac",
             Quantity=10.0, Quantity_sold=2.0, Unit_sold="Kg", Value_sold=500.0,
             planting_month=pd.NA, harvest_month=pd.NA, intercropped=pd.NA,
             perennial=False)
    r.update(kw)
    return r


class TestFinalize:

    def test_sold_unit_is_canonicalised_through_the_u_table(self, mali):
        got = mali.crop_production_finalize(pd.DataFrame([
            _row(Unit_sold="Kilogramme"), _row(plot_id="1_2", Unit_sold="Charretée"),
            _row(plot_id="1_3", Unit_sold="Chargement de camion")]))
        assert got["Unit_sold"].tolist() == ["Kg", "Charretée", "Chargement camion"]
        assert str(got["Unit_sold"].dtype) == "string"

    def test_manquant_and_missing_are_na_never_a_sentinel(self, mali):
        got = mali.crop_production_finalize(pd.DataFrame([
            _row(Unit_sold="Manquant"), _row(plot_id="1_2", Unit_sold=pd.NA)]))
        assert got["Unit_sold"].isna().all()
        assert not got["Unit_sold"].isin(["Unknown", "Manquant"]).any()

    def test_a_group_sold_in_two_units_keeps_no_sold_quantity(self, mali):
        got = mali.crop_production_finalize(pd.DataFrame([
            _row(Unit_sold="Sac", Quantity_sold=2.0),
            _row(Unit_sold="Kg", Quantity_sold=30.0)]))
        assert len(got) == 1
        assert pd.isna(got["Quantity_sold"].iloc[0])
        assert pd.isna(got["Unit_sold"].iloc[0])
        assert got["Value_sold"].iloc[0] == 1000.0        # the value is still the sum

    def test_a_group_sold_in_one_unit_sums_and_keeps_it(self, mali):
        got = mali.crop_production_finalize(pd.DataFrame([
            _row(Unit_sold="Sac", Quantity_sold=2.0),
            _row(Unit_sold="Sac", Quantity_sold=3.0),
            _row(Unit_sold=pd.NA, Quantity_sold=0.0)]))
        assert got["Quantity_sold"].iloc[0] == 5.0
        assert got["Unit_sold"].iloc[0] == "Sac"

    @pytest.mark.parametrize("wave, var", [("2014-15", "s3bq13b"), ("2017-18", "s11fq14b")])
    def test_tree_rows_take_their_own_sold_unit(self, mali, wave, var):
        raw = {"s3bq09": [2], "s3bq10a": [5.0], "s3bq10b": ["Sac"], "s3bq13a": [1.0],
               "s3bq13b": ["Kilogramme"], "s3bq14": [900.0],
               "s11fq10": ["2 mois"], "s11fq11a": [5.0], "s11fq11b": ["Sac"],
               "s11fq14a": [1.0], "s11fq14b": ["Kilogramme"], "s11fq15": [900.0]}
        got = mali.tree_crop_rows(pd.DataFrame(raw), wave)
        assert got["Unit_sold"].tolist() == ["Kilogramme"]
        assert got["u"].tolist() == ["Sac"]

    def test_a_frame_without_the_column_gets_an_all_na_one(self, mali):
        rows = pd.DataFrame([_row()]).drop(columns=["Unit_sold"])
        got = mali.crop_production_finalize(rows)
        assert "Unit_sold" in got.columns and got["Unit_sold"].isna().all()


# ---------------------------------------------------------------------------
# the served table
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
class TestServed:

    @pytest.fixture(scope="class")
    def built(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country(COUNTRY).crop_production()
        return _quiet(df)

    @pytest.fixture(scope="class")
    def flat(self, built):
        f = built.reset_index()
        f["kind"] = np.where(f["perennial"].fillna(False), "tree", "field")
        return _quiet(f)

    def test_unit_sold_is_a_string_column_never_a_sentinel(self, built):
        dtype = str(built["Unit_sold"].dtype)
        assert dtype == "string", dtype
        n_sentinel = _n(built["Unit_sold"].isin(["Unknown", "Manquant", "unit"]))
        assert n_sentinel == 0, f"{n_sentinel} rows carry a sentinel sold unit"
        unknown = sorted(set(built["Unit_sold"].dropna().astype(str)) - set(_u_table().values()))
        assert not unknown, f"sold-unit labels outside the u table: {unknown}"

    @pytest.mark.parametrize("wave, kind", sorted(PRICED))
    def test_priced_rows_and_label_disagreements_reproduce(self, flat, wave, kind):
        g = flat[(flat.t == wave) & (flat.kind == kind)
                 & (flat.Quantity_sold > 0) & (flat.Value_sold > 0)]
        knows = g.Unit_sold.notna() & g.u.notna()
        got = (len(g), _n(g.Unit_sold.notna()), _n(knows), _n(knows & (g.Unit_sold != g.u)))
        assert got == PRICED[(wave, kind)], f"{got} != {PRICED[(wave, kind)]}"

    def test_no_row_serves_a_value_code(self, flat):
        """At development 57d255585 FIELD_VALUE_CODE_2014 field rows of 2014-15
        served 999999 as a sale value; 999999 / 999998 are the six-digit
        field's 'non declare' / 'ne sait pas' codes."""
        n = _n(flat.Value_sold.isin([999998, 999999]))
        assert n == 0, f"{n} rows serve a value code"

    def test_the_field_value_code_rows_serve_no_value(self, flat, mali):
        """Raw oracle: every served 2014-15 field row whose raw s3aq24 is 999999
        (and whose sale was not answered 'Non') serves Value_sold NA."""
        s = get_dataframe(str(_root() / "2014-15" / "Data" / "EACIS3A_p2.dta"))
        f = pd.to_numeric(s["s3aq24"], errors="coerce")
        code = f.isin([999998, 999999]).fillna(False) & ~s["s3aq22"].astype("string").str.strip().eq("Non").fillna(False)
        f0 = s[code.to_numpy(dtype=bool)]
        hh = f0.apply(lambda r: mali.i(pd.Series([r["grappe"], r["menage"]])), axis=1)
        pf = f0["s3aq01"].astype("Int64").astype("string") + "_" + f0["s3aq02"].astype("Int64").astype("string")
        keys = pd.DataFrame({"t": "2014-15", "i": hh.values, "plot_id": pf.values,
                             "crop": mali._crop_labels(f0["s3aq03b"]).values}).dropna(subset=["crop"])
        keys = id_walk(keys.set_index(["t", "i"]), ll.Country(COUNTRY).updated_ids).reset_index()
        j = flat[flat.kind == "field"].merge(keys.drop_duplicates(), on=["t", "i", "plot_id", "crop"], how="inner")
        got = (len(j), _n(j.Value_sold.notna()))
        assert got == (FIELD_VALUE_CODE_2014, 0), f"(served code rows, with a value) {got}"

    def test_2017_18_code_10_is_compared_by_label(self, flat):
        g = flat[(flat.t == "2017-18") & (flat.Quantity_sold > 0) & (flat.Value_sold > 0)]
        n = _n((g.u == "Autre") & (g.Unit_sold == "Tonne"))
        assert n == AUTRE_TONNE_2017, f"{n} != {AUTRE_TONNE_2017}"

    def test_no_unit_on_a_filled_in_zero(self, flat):
        """A 'Non' answer is served as Quantity_sold = Value_sold = 0; such a row
        has no sold unit.  (At e2836e5d2 there was one exception: a 2017-18
        'Oui' line recording a quantity of 0 and a unit but no value, which the
        plain `sum` served with Value_sold = 0.  Its value is NA now.)"""
        z = flat[(flat.Quantity_sold == 0) & (flat.Value_sold == 0)]
        n = _n(z.Unit_sold.notna())
        assert n == 0, f"{n} filled-in zero sales carry a sold unit"

    @pytest.mark.parametrize("col", sorted(ZERO_SOLD_2017))
    def test_2017_18_unrecorded_sale_is_not_served_as_zero(self, flat, col):
        g = flat[(flat.t == "2017-18") & (flat.kind == "field")]
        n = _n(g[col] == 0)
        assert n == ZERO_SOLD_2017[col], f"{col}: {n} zeros != {ZERO_SOLD_2017[col]}"

    def test_2017_18_sale_matches_the_raw_answers_on_every_field_row(self, flat, mali):
        """Raw oracle for the ruling: 0 where s7gq20 == 'Non', else s7gq21a /
        s7gq22 AS RECORDED (NA stays NA), single-plot rule, joined on the served
        (t, i, plot_id, crop)."""
        def hh(d, key):
            return d.apply(lambda r: mali.i(pd.Series([r["grappe"], r[key]])), axis=1)

        def plot(d, a, b):
            f = d[a].astype("Int64").astype("string")
            p = d[b].astype("Int64").astype("string")
            return (f + "_" + p).where(f.notna() & p.notna(), pd.NA)

        data = _root() / "2017-18" / "Data"
        f = get_dataframe(str(data / "eaci17_s7fp2.dta"))
        fh = pd.DataFrame({"i": hh(f, "exploitation"), "plot_id": plot(f, "s7fq01", "s7fq02"),
                           "raw": f["s7fq03"].astype("string").values,
                           "crop": mali._crop_labels(f["s7fq03"]).values})
        npl = (fh.dropna(subset=["plot_id"]).groupby(["i", "raw"])["plot_id"]
               .nunique().rename("np").reset_index())
        g7 = get_dataframe(str(data / "eaci17_s7gp2.dta"))
        no = g7["s7gq20"].astype("string").str.strip().eq("Non").fillna(False).to_numpy(bool)
        sale = pd.DataFrame({
            "i": hh(g7, "exploitation"), "raw": g7["s7gq01"].astype("string").values,
            "eq": np.where(no, 0.0, pd.to_numeric(g7["s7gq21a"], errors="coerce")),
            "ev": np.where(no, 0.0, pd.to_numeric(g7["s7gq22"], errors="coerce"))})
        x = fh.merge(sale.merge(npl, on=["i", "raw"], how="left"), on=["i", "raw"], how="left")
        x.loc[x["np"].fillna(0) > 1, ["eq", "ev"]] = np.nan
        x["t"] = "2017-18"
        x = x.dropna(subset=["crop"])[["t", "i", "plot_id", "crop", "eq", "ev"]]
        x = id_walk(x.set_index(["t", "i"]), ll.Country(COUNTRY).updated_ids).reset_index()
        field = flat[(flat.t == "2017-18") & (flat.kind == "field")]
        j = field.merge(x, on=["t", "i", "plot_id", "crop"], how="inner")
        n_join, n_field = len(j), len(field)
        assert n_join == n_field, f"{n_join} of {n_field} field rows joined the raw sale"
        bad = {}
        for col, e in (("Quantity_sold", "eq"), ("Value_sold", "ev")):
            a, b = j[col].astype("float64"), j[e].astype("float64")
            bad[col] = _n(~((a == b) | (a.isna() & b.isna())).fillna(False))
        assert not any(bad.values()), f"field rows differing from the raw sale, by column: {bad}"

    def test_oui_without_quantity_is_the_leger_questionnaire(self):
        """The audit's surprise, settled: every 2017-18 s7g 'Oui' row with no
        quantity and no unit belongs to a Leger-questionnaire household, and no
        Leger row records 7G.21 or 7G.22 at all.  So those sales are unrecorded
        by design -- NA, not 0."""
        data = _root() / "2017-18" / "Data"
        g7 = get_dataframe(str(data / "eaci17_s7gp2.dta"))
        cover = get_dataframe(str(data / "eaci17_s00p2.dta"))[["grappe", "exploitation", "typequest"]]
        x = g7.merge(cover, on=["grappe", "exploitation"], how="left", validate="m:1")
        tq = x["typequest"].astype("string")
        oui = x["s7gq20"].astype("string").eq("Oui").fillna(False)
        none = x["s7gq21a"].isna() & x["s7gq21b"].isna()
        n = _n(oui & none)
        assert n == OUI_WITHOUT_QUANTITY_2017, f"{n} != {OUI_WITHOUT_QUANTITY_2017}"
        kinds = sorted(set(tq[oui & none].dropna().astype(str)))     # questionnaire labels only
        assert kinds == ["Questionnaire Leger"], kinds
        leger = tq.eq("Questionnaire Leger").fillna(False)
        n = int(x.loc[leger, ["s7gq21a", "s7gq21b", "s7gq22"]].notna().to_numpy().sum())
        assert n == 0, f"{n} Leger cells record 7G.21 / 7G.22"

    def test_unit_sold_matches_the_raw_sold_unit_on_every_field_row(self, flat, mali):
        """Raw oracle: canonical s3aq23b / s7gq21b, NA on 'Non', with the same
        single-plot rule for 2017-18, joined on the served (t, i, plot_id, crop).
        Units are canonicalised from the org table directly, not through
        ``mali._unit_labels``; ``mali.i`` / ``mali._crop_labels`` only build
        the join keys."""
        table = _u_table()

        def canon(x):
            return x.astype("string").str.strip().map(
                lambda v: table.get(v, pd.NA) if pd.notna(v) else pd.NA).astype("string")

        def hh(d, key):
            return d.apply(lambda r: mali.i(pd.Series([r["grappe"], r[key]])), axis=1)

        def plot(d, a, b):
            f = d[a].astype("Int64").astype("string")
            p = d[b].astype("Int64").astype("string")
            return (f + "_" + p).where(f.notna() & p.notna(), pd.NA)

        def raw(wave, fn):
            return get_dataframe(str(_root() / wave / "Data" / fn))

        s = raw("2014-15", "EACIS3A_p2.dta")
        e = pd.DataFrame({"t": "2014-15", "i": hh(s, "menage"),
                          "plot_id": plot(s, "s3aq01", "s3aq02"),
                          "crop": mali._crop_labels(s["s3aq03b"]).values,
                          "exp": canon(s["s3aq23b"]).values})
        e.loc[s["s3aq22"].astype("string").eq("Non").values, "exp"] = pd.NA
        g = e.dropna(subset=["crop"]).groupby(["t", "i", "plot_id", "crop"], dropna=False)["exp"]
        e14 = pd.DataFrame({"n": g.nunique(), "exp": g.first()}).reset_index()
        e14["exp"] = e14["exp"].where(e14["n"] == 1, pd.NA)

        f = raw("2017-18", "eaci17_s7fp2.dta")
        fh = pd.DataFrame({"i": hh(f, "exploitation"), "plot_id": plot(f, "s7fq01", "s7fq02"),
                           "raw": f["s7fq03"].astype("string").values,
                           "crop": mali._crop_labels(f["s7fq03"]).values})
        npl = (fh.dropna(subset=["plot_id"]).groupby(["i", "raw"])["plot_id"]
               .nunique().rename("np").reset_index())
        g7 = raw("2017-18", "eaci17_s7gp2.dta")
        sale = pd.DataFrame({"i": hh(g7, "exploitation"), "raw": g7["s7gq01"].astype("string").values,
                             "exp": canon(g7["s7gq21b"]).values})
        sale.loc[g7["s7gq20"].astype("string").eq("Non").values, "exp"] = pd.NA
        x = fh.merge(sale.merge(npl, on=["i", "raw"], how="left"), on=["i", "raw"], how="left")
        x.loc[x["np"].fillna(0) > 1, "exp"] = pd.NA
        x["t"] = "2017-18"
        e17 = x.dropna(subset=["crop"])[["t", "i", "plot_id", "crop", "exp"]]

        upd = ll.Country(COUNTRY).updated_ids
        exp = pd.concat([e14[["t", "i", "plot_id", "crop", "exp"]], e17])
        exp = id_walk(exp.set_index(["t", "i"]), upd).reset_index()
        field = flat[flat.kind == "field"]
        j = field.merge(exp, on=["t", "i", "plot_id", "crop"], how="inner")
        n_join, n_field = len(j), len(field)
        assert n_join == n_field, f"{n_join} of {n_field} field rows joined the raw sold unit"
        n_bad = _n(~((j.Unit_sold == j.exp) | (j.Unit_sold.isna() & j.exp.isna())).fillna(False))
        assert n_bad == 0, f"{n_bad} field rows whose Unit_sold is not the raw sold unit"
