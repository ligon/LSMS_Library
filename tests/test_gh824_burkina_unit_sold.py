"""Burkina Faso 2018-19 ``crop_production`` carries the SOLD unit -- GH #824.

``u`` is the HARVEST unit (``s16cq12b``).  The EHCVM s16c module records the
unit of the sold quantity ``s16cq16a`` in a column of its own, ``s16cq16b``,
on the same plot-crop line and on the same seven labels.  The builder, a
pre-#824 copy of Niger 2018-19, did not read it, so ``Quantity_sold`` was
served beside the harvest ``u`` with nothing marking that the sale unit can
differ -- and on 546 of the 5,347 priced rows it does.

The fix mirrors Niger 2018-19 (``Niger/2018-19/_/crop_production.py`` and
``Niger/_/niger.py:_finish_crop_production``, GH #824 commit ``04c1efbf9``):
a per-row COLUMN, not an index level; canonicalised at BUILD time through the
same ``u`` table as the harvest unit (read-time mapping only touches a column
named ``u``, the GH #1188 point); NA-bearing, never sentinel-filled; declared
``optional: true``.  A disagreement with ``u`` is a data-quality FLAG
(canonical ``data_info.yml``, ``Unit_sold`` note), so the agreement share is
PINNED below rather than asserted high.  The sale is on the harvest row, so no
row is added, removed or re-keyed.

Three families, as in ``tests/test_niger_sold_unit.py``:

* ``TestDeclared`` needs NO microdata: schema and the wave script's source.
* ``TestFinishTail`` runs the script's ``_finish_crop_production`` on a
  synthetic frame (extracted with ``ast``, as
  ``tests/test_burkina_faso_seed_crop_labels.py`` does; no data build).
* ``TestServed`` builds the real table and checks it against the raw file;
  skipped when Burkina Faso is unavailable.
"""
from __future__ import annotations

import ast
import os
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

COUNTRY = "Burkina_Faso"
TABLE = "crop_production"
WAVE = "2018-19"
UNIT_SOLD = "Unit_sold"

#: Measured on a cold build of the branch (2026-10-23), equal to the raw
#: s16c_me_bfa2018.dta counts.  "Priced" = Quantity_sold > 0 and Value_sold > 0.
PRICED = 5347
UNIT_SOLD_NON_NULL = 5356          # 9 more than PRICED: Quantity_sold > 0, no positive value
PRICED_AGREE = 4801
PRICED_DIFFER = 546
SERVED_ROWS = 15587                # unchanged by this fix
UNIT_LABELS = {"Autres (à préciser)", "Kg", "Plat yorouba", "Sac gros",
               "Sac moyen", "Tine", "Unité"}


def _root() -> Path:
    return countries_root() / COUNTRY


def _script() -> str:
    return (_root() / WAVE / "_" / f"{TABLE}.py").read_text(encoding="utf-8")


def _aws_creds_available() -> bool:
    """Delegate to the shared resolver in ``tests/conftest.py`` (loaded by
    path, as ``tests/test_niger_sold_unit.py`` does)."""
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


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the ``!make`` tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


def _finish_tail():
    """The production ``_finish_crop_production``, executed in isolation."""
    tree = ast.parse(_script())
    fn, = [node for node in tree.body
           if isinstance(node, ast.FunctionDef) and node.name == "_finish_crop_production"]
    env = {"pd": pd}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "crop_production.py", "exec"), env)
    return env["_finish_crop_production"]


def _u_table() -> dict:
    df = df_from_orgfile(_root() / "_" / "categorical_mapping.org", name="u")
    return dict(zip(df["Original Label"].astype(str).str.strip(),
                    df["Preferred Label"].astype(str).str.strip()))


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestDeclared:

    def test_data_scheme_declares_unit_sold_optional(self):
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            scheme = yaml.load(f, Loader=_SchemeLoader)
        cols = scheme["Data Scheme"][TABLE]
        assert isinstance(cols.get(UNIT_SOLD), dict), cols.get(UNIT_SOLD)
        assert cols[UNIT_SOLD].get("type") == "str"
        assert cols[UNIT_SOLD].get("optional") is True, "a row with no sale has no sold unit"
        assert UNIT_SOLD not in cols["index"], "Unit_sold is a per-row column, never an index level"

    def test_wave_script_reads_the_sold_unit_through_the_u_table(self):
        src = _script()
        assert "'Unit_sold':     _unit_labels(src['s16cq16b'], unit_map)" in src, (
            "Unit_sold must be s16cq16b canonicalised at BUILD time through the "
            "same u table as the harvest unit")
        assert "'u':             _unit_labels(src['s16cq12b'], unit_map)" in src

    def test_u_table_is_identity_on_the_harvest_and_sold_vocabulary_where_it_maps(self):
        """Both units go through ONE table, so a label cannot be spelled two
        ways; the three labels it does key map to themselves."""
        table = _u_table()
        for raw in ("Kg", "Tine", "Unité"):
            assert table.get(raw) == raw, (raw, table.get(raw))


# ---------------------------------------------------------------------------
# fixture-level: the script's tail on a synthetic frame
# ---------------------------------------------------------------------------

class TestFinishTail:

    def _frame(self, unit_sold):
        return pd.DataFrame({
            "i": ["1", "1", "2"], "plot_id": ["1_1", "1_2", "1_1"],
            "crop": ["Mil", "Mil", "Riz"], "u": ["Sac gros", "Kg", "Kg"],
            "Quantity": [10.0, 3.0, 5.0], "Quantity_sold": [4.0, None, None],
            "Value_sold": [800.0, None, None],
            **({} if unit_sold is None else {UNIT_SOLD: unit_sold}),
        })

    def test_unit_sold_is_a_string_column_and_na_stays_na(self):
        out = _finish_tail()(self._frame(["Plat yorouba", None, "Unknown"]), "2099-00")
        assert UNIT_SOLD in out.columns and UNIT_SOLD not in out.index.names
        assert out[UNIT_SOLD].dtype == "string"
        assert out[UNIT_SOLD].tolist()[0] == "Plat yorouba", "the sold unit is not replaced by u"
        assert out[UNIT_SOLD].isna().tolist() == [False, True, True], "never sentinel-filled"
        assert len(out) == 3
        assert list(out.columns).index(UNIT_SOLD) == list(out.columns).index("Value_sold") + 1

    def test_missing_column_yields_all_na(self):
        out = _finish_tail()(self._frame(None), "2099-00")
        assert UNIT_SOLD in out.columns and out[UNIT_SOLD].isna().all()


# ---------------------------------------------------------------------------
# raw file + built table
# ---------------------------------------------------------------------------

@needs_data
class TestServed:

    @pytest.fixture(scope="class")
    def built(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country(COUNTRY).crop_production()
        return df.reset_index()

    @pytest.fixture(scope="class")
    def raw(self):
        return get_dataframe(str(_root() / WAVE / "Data" / "s16c_me_bfa2018.dta"),
                             convert_categoricals=True)

    @staticmethod
    def _priced(df):
        return ((df["Quantity_sold"].fillna(0) > 0) & (df["Value_sold"].fillna(0) > 0)).astype(bool)

    def test_row_count_unchanged(self, built):
        assert len(built) == SERVED_ROWS

    def test_unit_sold_is_served_as_a_string_column(self, built):
        assert UNIT_SOLD in built.columns
        assert built[UNIT_SOLD].dtype == "string"
        assert built[UNIT_SOLD].isna().any(), "rows with no sale carry NA"
        assert set(built[UNIT_SOLD].dropna().unique()) == UNIT_LABELS

    def test_every_priced_row_carries_a_sold_unit(self, built):
        priced = self._priced(built)
        assert int(priced.sum()) == PRICED
        assert int((priced & built[UNIT_SOLD].notna()).sum()) == PRICED
        assert int(built[UNIT_SOLD].notna().sum()) == UNIT_SOLD_NON_NULL

    def test_sold_unit_differs_from_harvest_unit_on_the_measured_rows(self, built):
        """A disagreement is a FLAG; the count is pinned, not judged."""
        priced = built[self._priced(built)]
        same = (priced[UNIT_SOLD].astype("string") == priced["u"].astype("string")).fillna(False)
        assert int(same.sum()) == PRICED_AGREE
        assert int((~same.astype(bool)).sum()) == PRICED_DIFFER

    def test_raw_oracle_sold_unit_labels(self, built, raw):
        """s16cq16b, canonicalised by an independent reader of the u table,
        has the same label multiset as the served Unit_sold."""
        table = _u_table()
        oracle = raw["s16cq16b"].astype("string").map(
            lambda x: table.get(x, x) if pd.notna(x) else pd.NA).dropna()
        served = built[UNIT_SOLD].dropna()
        pd.testing.assert_series_equal(
            oracle.value_counts().sort_index(), served.value_counts().sort_index(),
            check_names=False, check_index_type=False, check_dtype=False)

    def test_raw_oracle_agreement_counts(self, raw):
        q = pd.to_numeric(raw["s16cq16a"], errors="coerce")
        v = pd.to_numeric(raw["s16cq17"], errors="coerce")
        priced = ((q > 0) & (v > 0)).astype(bool)
        hu, su = raw["s16cq12b"].astype("string"), raw["s16cq16b"].astype("string")
        assert int(priced.sum()) == PRICED
        assert int((priced & su.notna() & hu.notna()).sum()) == PRICED
        same = (hu == su).fillna(False).astype(bool)
        assert int((priced & same).sum()) == PRICED_AGREE
        assert int((priced & ~same).sum()) == PRICED_DIFFER
