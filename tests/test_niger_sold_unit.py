"""Niger ``crop_production`` carries the SOLD unit and keys the crop-level sale on it -- GH #824.

``u`` is the HARVEST unit; every Niger wave records the sale's unit in a
column the feature used to drop (``as02eq12b``, ``AS02EQ12B``, ``s16cq16b``,
``s16dq05b``), so ``Value_sold / Quantity_sold`` was a price per harvest unit
that is only right where the two units coincide.  Per the contract settled
on Uganda (canonical ``data_info.yml``, ``Unit_sold`` note; see
``tests/test_uganda_sold_unit.py``): a per-row COLUMN, not an index level;
canonicalised through the same ``u`` table; NA-bearing, never
sentinel-filled (``niger._finish_crop_production`` folds the table's
``Unknown`` back to NA); a disagreement is a data-quality FLAG, not an
identification.

Two wave shapes:

* 2011-12 / 2018-19: the sale is on the harvest row; ``Unit_sold`` is read
  straight off the b-column.
* 2014-15 / 2021-22: the sale block is CROP-level (no plot).  It is summed
  over ``(i, crop, Unit_sold)`` -- never across units -- and attributed only
  to a crop grown on ONE plot and sold in ONE unit.

And one source defect found on the way: the 2021-22 ``s16d`` sale file
REPEATS its crop-level lines once per plot (12,232 rows for 8,691
household-crop pairs; 5,786 of the 6,174 multi-pair rows byte-identical on
all 47 columns, and EVERY one of the 1,320 multi-pair rows carrying a sale
is).  The old script summed them and served a single sale 2-4x.  Exact
duplicates are now dropped BEFORE the sum: on the single-plot pairs
Value_sold 78,934,620 -> 73,206,995 FCFA, Quantity_sold 36,975 -> 32,420.5,
1,409 pairs either way (measured 2026-09-24, pre-fix warm parquet vs cold
rebuild).  See ``Niger/_/CONTENTS.org``, "crop_production: the SOLD unit".

Three families, split as in ``tests/test_niger_value_sentinels.py``:

* ``TestDeclared`` needs NO microdata: schema, the ``u`` table, and the
  wave scripts' source (the de-duplication precedes the sum, statically).
* ``TestFinishTail`` runs ``niger._finish_crop_production`` on a synthetic
  frame (no microdata).
* ``TestServed`` reads the raw files and builds the real table; skipped
  when Niger is unavailable.
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

COUNTRY = "Niger"
TABLE = "crop_production"
UNIT_SOLD = "Unit_sold"
WAVES = ("2011-12", "2014-15", "2018-19", "2021-22")
CROP_LEVEL_WAVES = ("2014-15", "2021-22")

#: 2021-22 s16d census (get_dataframe, convert_categoricals=False).
S16D_ROWS = 12232
S16D_COLUMNS = 47
S16D_PAIRS = 8691
S16D_MULTI_PAIRS = 2633
S16D_MULTI_PAIR_ROWS = 6174
S16D_EXACT_DUP_ROWS = 5786          # rows that have an identical twin (keep=False)
S16D_DROPPED = 3329                 # rows duplicated(keep='first') removes
S16D_MULTI_PAIR_SALE_ROWS = 1320    # ... every one of them an exact duplicate
S16D_DISTINCT_MULTI_ROWS = 388      # non-identical lines within a pair; none sells

#: 2014-15 AS2E2: one row per (household, crop) -- the sum there is a no-op.
AS2E2_ROWS = 5226

#: Served, 2026-09-24 cold rebuild.
UNIT_SOLD_ROWS = {"2011-12": 1294, "2014-15": 564, "2018-19": 3226, "2021-22": 1425}
W2021_SALE_PAIRS = 1409
W2021_VALUE_SOLD_PER_PAIR = 73_206_995
W2021_QUANTITY_SOLD_PER_PAIR = 32_420.5
W2021_VALUE_SOLD_BEFORE = 78_934_620      # what summing the repeats served

#: Sold-unit == harvest-unit share on PRICED rows (Quantity_sold > 0 and
#: Value_sold > 0) that know both units (Unit_sold non-null, u != 'Unknown'),
#: canonicalised through the `u` table, measured on the served table.
AGREEMENT = {"2011-12": (0.9869, 1293), "2014-15": (0.8943, 558),
             "2018-19": (0.9231, 3226), "2021-22": (0.8904, 1423)}
AGREEMENT_TOL = 0.002


def _root() -> Path:
    return countries_root() / COUNTRY


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    Delegates to the shared resolver in ``tests/conftest.py`` (exported
    ``AWS_*`` -> the user-config ``s3_creds`` -> the legacy in-tree
    ``countries/.dvc/s3_creds``).  This used to check only the in-tree
    file, which the library stopped writing in GH #1001, so a fresh
    checkout with user-config credentials silently skipped here.  Loaded by
    path because both import spellings are broken (see
    ``tests/test_tests_package_not_shadowed.py``).
    """
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


def _script(wave: str) -> str:
    return (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def niger_module():
    """``Niger/_/niger.py``, imported the way the wave scripts import it."""
    sys.path.insert(0, str(_root() / "_"))
    try:
        import niger
    finally:
        sys.path.pop(0)
    return niger


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
        assert UNIT_SOLD not in scheme["Data Scheme"][TABLE].get("index", []), (
            "Unit_sold is a per-row column, never an index level")

    @pytest.mark.parametrize("wave", WAVES)
    def test_every_wave_script_emits_unit_sold(self, wave):
        assert f"'{UNIT_SOLD}'" in _script(wave), f"{wave}: {UNIT_SOLD} not built"

    @pytest.mark.parametrize("wave", CROP_LEVEL_WAVES)
    def test_crop_level_sale_is_keyed_on_the_sold_unit(self, wave):
        src = _script(wave)
        assert "groupby(['i', 'crop', 'Unit_sold']" in src, (
            f"{wave}: the crop-level sale must be summed per (i, crop, Unit_sold), never across units")
        assert "one_unit" in src and "single" in src, f"{wave}: single-plot AND single-unit attribution"

    def test_2021_22_drops_exact_duplicates_before_the_sum(self):
        """Static source-order check (as ``tests/test_ghanalss_12b.py`` does):
        ``duplicated(`` must precede the ``groupby`` that sums the sale."""
        src = _script("2021-22")
        i_dedup = src.index("duplicated(keep='first')")
        i_sum = src.index("groupby(['i', 'crop', 'Unit_sold']")
        assert i_dedup < i_sum, "de-duplication must run before the sale is summed"
        assert "sold[~_dup]" in src and "soldn[~_dup]" in src, (
            "the labelled and the coded frame must be de-duplicated with the SAME mask")

    def test_u_table_maps_every_2021_22_sold_unit_spelling(self):
        """The s16d sold-unit labels are lower-case where the harvest labels
        are not; ``sac de 100kg`` was the one the table lacked."""
        df = df_from_orgfile(_root() / "_" / "categorical_mapping.org", name="u")
        table = dict(zip(df["Original Label"].astype(str).str.strip(),
                         df["Preferred Label"].astype(str).str.strip()))
        assert table.get("sac de 100kg") == "Sac de 100 kg", table.get("sac de 100kg")
        for raw in ("tiya", "tongolo", "panier", "Autres", "Sac de 50kg"):
            assert raw in table, f"sold-unit spelling {raw!r} not in the u table"

    def test_finish_tail_folds_unknown_back_to_na(self):
        src = (_root() / "_" / "niger.py").read_text(encoding="utf-8")
        start = src.index("def _finish_crop_production")
        body = src[start:src.index("\ndef ", start + 1)]
        assert "df['Unit_sold'].where(df['Unit_sold'] != U_NA, pd.NA)" in body


# ---------------------------------------------------------------------------
# fixture-level: the shared tail
# ---------------------------------------------------------------------------

class TestFinishTail:

    def _frame(self, unit_sold):
        return pd.DataFrame({
            "i": ["1", "1", "2"], "plot_id": ["1_1", "1_1", "1_1"],
            "crop": ["Mil", "Mil", "Riz"], "u": ["Kg", "Tiya", "Kg"],
            "Quantity": [10.0, 3.0, 5.0], "Quantity_sold": [4.0, None, None],
            "Value_sold": [800.0, None, None], **({} if unit_sold is None else {UNIT_SOLD: unit_sold}),
        })

    def test_unknown_becomes_na_and_dtype_is_string(self, niger_module):
        out = niger_module._finish_crop_production(self._frame(["Kg", niger_module.U_NA, None]), "2099-00")
        assert out[UNIT_SOLD].dtype == "string"
        assert out[UNIT_SOLD].tolist()[0] == "Kg"
        assert out[UNIT_SOLD].isna().tolist() == [False, True, True]
        assert UNIT_SOLD not in out.index.names

    def test_missing_column_yields_all_na_not_a_sentinel(self, niger_module):
        out = niger_module._finish_crop_production(self._frame(None), "2099-00")
        assert UNIT_SOLD in out.columns and out[UNIT_SOLD].isna().all()
        assert niger_module.U_NA not in set(out[UNIT_SOLD].dropna())


# ---------------------------------------------------------------------------
# raw files + built table
# ---------------------------------------------------------------------------

@needs_data
class TestServed:

    @pytest.fixture(scope="class")
    def s16d(self):
        return get_dataframe(str(_root() / "2021-22" / "Data" / "s16d_me_ner2021.dta"),
                             convert_categoricals=False)

    @pytest.fixture(scope="class")
    def built(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country(COUNTRY).crop_production()
        return df.reset_index()

    def test_s16d_repeats_its_crop_level_lines(self, s16d):
        """The measurement behind the de-duplication.  If a re-release stops
        repeating the lines, the drop becomes a no-op and these move."""
        assert s16d.shape == (S16D_ROWS, S16D_COLUMNS)
        pair = ["grappe", "menage", "s16dq01"]
        g = s16d.groupby(pair).size()
        assert len(g) == S16D_PAIRS
        assert int((g > 1).sum()) == S16D_MULTI_PAIRS
        assert int(g[g > 1].sum()) == S16D_MULTI_PAIR_ROWS
        dup = s16d.duplicated(keep=False)
        assert int(dup.sum()) == S16D_EXACT_DUP_ROWS
        assert int(s16d.duplicated(keep="first").sum()) == S16D_DROPPED
        multi = s16d.set_index(pair).index.isin(g[g > 1].index)
        sale = s16d["s16dq06"].fillna(0) > 0
        assert int((multi & sale).sum()) == S16D_MULTI_PAIR_SALE_ROWS
        assert int((multi & sale & ~dup).sum()) == 0, "a repeated sale line that is NOT an exact duplicate"
        distinct = s16d[multi & ~dup]
        assert len(distinct) == S16D_DISTINCT_MULTI_ROWS
        assert int((distinct["s16dq06"].fillna(0) > 0).sum()) == 0

    def test_2014_15_sale_block_is_one_row_per_household_crop(self):
        s = get_dataframe(str(_root() / "2014-15" / "Data" / "NER_2014_ECVMA-II_v02_M_STATA8"
                              / "ECVMA2_AS2E2P2.dta"), convert_categoricals=False)
        assert len(s) == AS2E2_ROWS
        assert s.groupby(["GRAPPE", "MENAGE", "EXTENSION", "AS02EQ110B"]).ngroups == AS2E2_ROWS

    def test_unit_sold_is_a_string_column_never_a_sentinel(self, built, niger_module):
        assert UNIT_SOLD in built.columns
        assert built[UNIT_SOLD].dtype == "string"
        assert niger_module.U_NA not in set(built[UNIT_SOLD].dropna().unique())
        assert built[UNIT_SOLD].isna().any(), "rows with no sale carry NA"

    @pytest.mark.parametrize("wave", WAVES)
    def test_unit_sold_populated_per_wave(self, built, wave):
        n = int(built.loc[built["t"] == wave, UNIT_SOLD].notna().sum())
        assert n == UNIT_SOLD_ROWS[wave], f"{wave}: {n} rows carry a sold unit"

    @pytest.mark.parametrize("wave", CROP_LEVEL_WAVES)
    def test_no_household_crop_is_served_in_two_sold_units(self, built, wave):
        g = built[(built["t"] == wave) & built[UNIT_SOLD].notna()]
        n = g.groupby(["i", "crop"])[UNIT_SOLD].nunique()
        assert int((n > 1).sum()) == 0, f"{wave}: {int((n > 1).sum())} pairs sold in two units"

    def test_2021_22_single_plot_sale_is_not_multiplied(self, built):
        g = built[(built["t"] == "2021-22") & built["Value_sold"].notna()]
        pairs = g.drop_duplicates(subset=["i", "crop"])
        assert pairs.groupby(["i", "crop"]).ngroups == W2021_SALE_PAIRS
        total = float(pairs["Value_sold"].sum())
        assert total == W2021_VALUE_SOLD_PER_PAIR, f"Value_sold on single-plot pairs {total:,.0f}"
        assert total < W2021_VALUE_SOLD_BEFORE
        assert float(pairs["Quantity_sold"].sum()) == W2021_QUANTITY_SOLD_PER_PAIR

    @pytest.mark.parametrize("wave", WAVES)
    def test_sold_unit_agrees_with_harvest_unit_at_the_measured_share(self, built, wave):
        """Where the two disagree the row is a FLAG, so the share is pinned
        (definition in ``AGREEMENT``'s comment) rather than asserted high."""
        g = built[built["t"] == wave]
        priced = g[(g["Quantity_sold"].fillna(0) > 0) & (g["Value_sold"].fillna(0) > 0)]
        both = priced[priced[UNIT_SOLD].notna() & (priced["u"] != "Unknown")]
        share, n = AGREEMENT[wave]
        assert len(both) == n, f"{wave}: {len(both)} priced rows know both units"
        agree = float((both[UNIT_SOLD] == both["u"]).mean())
        assert abs(agree - share) < AGREEMENT_TOL, f"{wave}: agreement {agree:.4f} vs {share}"
