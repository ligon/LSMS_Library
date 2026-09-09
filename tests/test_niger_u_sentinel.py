"""`Unknown` is the sentinel on Niger's `u` index level -- GH #842 / #847.

A NaN on a DECLARED index level is a deferred silent deletion: the row is
served, so no build-time guard fires, and it then vanishes in whichever
``groupby`` runs first (``_finalize_result``'s canonical de-dup, ``Feature()``
assembly, or the consumer's own aggregation), because pandas
``groupby(dropna=True)`` drops NaN keys.  Niger declares ``u`` in four table
indexes; before #842 it left the level NaN wherever the enumerator recorded an
amount and no unit.

Two families here, and the split is deliberate:

* ``TestRelabelMechanism`` needs NO microdata.  It pins the mechanism -- the
  country ``u`` categorical table maps every missing-unit spelling onto
  ``Unknown``, at read time, on an INDEX LEVEL -- so a future edit that moves
  the relabel somewhere else, or re-introduces the French ``Manquant``, goes
  red on a laptop with no S3 credentials.
* ``TestNoNullUnitIsServed`` builds the real tables and is skipped when Niger
  is unavailable.

MEASURED NUMBERS, not guesses.  Every count below comes from one cold rebuild
(private ``LSMS_DATA_DIR`` with only ``dvc-cache`` symlinked in,
``LSMS_COUNTRIES_ROOT`` pinned to the branch worktree), 2026-09-09, on the
commit that introduced them.  The before/after pair is recorded because the
delta is the point of the change:

    table             rows before -> after     u NaN before -> after   u=='Unknown' after
    crop_production      41,896 -> 46,341            441 -> 0                4,920
    food_acquired       239,031 -> 239,031            12 -> 0                   12
    plot_inputs          42,364 -> 42,364              0 -> 0                   75
    community_prices     15,423 -> 15,423              0 -> 0                    0

``crop_production`` GAINS 4,445 rows and that is not a bug: those rows were
already in the wave parquets and were being DELETED by the framework's NaN-key
collapse in the three waves whose index is non-unique (2011-12 +3,135,
2014-15 +350, 2021-22 +960; 2018-19's index is unique, so its 441 NaN-``u``
rows were served rather than deleted -- which is why #842 counted 441 and not
5,663).  3,485 of the recovered rows carry a REPORTED harvest Quantity.
``plot_inputs``' 75 and ``crop_production``'s 34 pre-existing ``Manquant``
rows are the same rows under the new spelling, not new ones.
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = "Niger"
SENTINEL = "Unknown"
RETIRED = "Manquant"

# Every Niger table declaring `u` in its index (read from data_scheme.yml by
# `u_tables` below; this is the expected set, so a new one cannot be added
# without updating the measured counts).
U_TABLES = ("crop_production", "food_acquired", "plot_inputs", "community_prices")

# Cold-rebuild measurements (see the module docstring for provenance).
EXPECTED_ROWS = {
    "crop_production": 46341,
    "food_acquired": 239031,
    "plot_inputs": 42364,
    "community_prices": 15423,
}
EXPECTED_SENTINEL = {
    "crop_production": 4920,
    "food_acquired": 12,
    "plot_inputs": 75,
    "community_prices": 0,
}
# Per-wave sentinel counts, so a wave-script regression cannot hide inside a
# country total that happens to add up.
EXPECTED_SENTINEL_BY_WAVE = {
    "crop_production": {"2011-12": 3146, "2014-15": 373, "2018-19": 441, "2021-22": 960},
    "food_acquired": {"2018-19": 0, "2021-22": 12},
    "plot_inputs": {"2011-12": 44, "2014-15": 15, "2018-19": 2, "2021-22": 14},
    "community_prices": {"2011-12": 0, "2014-15": 0},
}


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    Mirrors the helper in ``tests/test_uganda_crop_condition.py`` /
    ``tests/test_declared_spellings.py``; that duplication is the house pattern
    until a shared ``conftest.py`` lands.
    """
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


needs_data = pytest.mark.skipif(
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


# ---------------------------------------------------------------------------
# config fixtures (cheap -- no data build)
# ---------------------------------------------------------------------------

class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


@pytest.fixture(scope="module")
def u_tables() -> list[str]:
    """Every Niger table whose DECLARED index carries a `u` level.

    Read from ``data_scheme.yml`` rather than hardcoded, so adding a fifth
    `u` table makes the coverage test below fail loudly instead of silently
    leaving it unswept (CLAUDE.md, "Canonical Schema": never hardcode schema
    rules in tests).
    """
    path = countries_root() / COUNTRY / "_" / "data_scheme.yml"
    with open(path, encoding="utf-8") as f:
        scheme = yaml.load(f, Loader=_SchemeLoader).get("Data Scheme", {})
    found = []
    for table, spec in scheme.items():
        if not isinstance(spec, dict):
            continue
        idx = spec.get("index")
        if isinstance(idx, str) and "u" in [
            s.strip() for s in idx.strip("()").split(",")
        ]:
            found.append(table)
    return sorted(found)


def test_u_tables_are_the_swept_set(u_tables):
    """The sweep covers every declared `u` table -- no silent new arrival."""
    assert set(u_tables) == set(U_TABLES), (
        f"{COUNTRY} declares `u` in {sorted(u_tables)}, but this module sweeps "
        f"{sorted(U_TABLES)}.  A new `u` table must be filled with the "
        f"{SENTINEL!r} sentinel and its measured counts added here."
    )


# ---------------------------------------------------------------------------
# the relabel mechanism -- no microdata needed
# ---------------------------------------------------------------------------

class TestRelabelMechanism:
    """`Manquant` -> `Unknown`, on an index level, at read time.

    These run everywhere.  They pin the mechanism that was CHOSEN over two
    alternatives that were measured and rejected (both rejections are proved
    below, so a future reader does not have to re-derive them):

    * a ``spellings:`` block on `u` in the canonical ``data_info.yml`` --
      ``diagnostics._check_declared_spellings`` reads a spellings block as a
      CLOSED vocabulary and fails every value outside it, and the `u`
      vocabulary is deliberately open (GH #223);
    * a row in the global ``categorical_mapping/u.org`` -- it cannot win,
      because ``_row_union_categorical`` keeps the COUNTRY row on a collision.
    """

    def test_country_u_table_maps_every_missing_spelling_to_unknown(self):
        """Niger's `u` table resolves each missing-unit spelling to `Unknown`."""
        from lsms_library.country import Country

        table = Country(COUNTRY).categorical_mapping["u"]
        rows = table.set_index("Original Label")["Preferred Label"]
        for spelling in ("Manquant", "manquant", "produit absent", "Produit absent"):
            assert spelling in rows.index, (
                f"{spelling!r} is no longer an Original Label in {COUNTRY}'s `u` "
                f"table; a raw missing-unit label would pass through unmapped"
            )
            assert rows[spelling] == SENTINEL, (
                f"{spelling!r} maps to {rows[spelling]!r}, not {SENTINEL!r}"
            )

    def test_no_manquant_preferred_label_survives(self):
        """The retired spelling is gone from the served vocabulary.

        ``niger.py::_COMMUNITY_MISSING_UNITS`` keys on the Preferred Label this
        table produces, so the two must move together or Niger silently stops
        dropping CS07's product-absent rows.
        """
        from lsms_library.country import Country

        table = Country(COUNTRY).categorical_mapping["u"]
        assert RETIRED not in set(table["Preferred Label"]), (
            f"{RETIRED!r} is back as a `u` Preferred Label; see the header of "
            f"{COUNTRY}/_/categorical_mapping.org"
        )

    def test_relabel_applies_to_an_index_level(self):
        """A legacy parquet holding `Manquant` on `u` is relabelled on read.

        This is the half of the mechanism that a column-only relabel would
        miss, and `u` is only ever an index level.
        """
        from lsms_library.country import Country

        idx = pd.MultiIndex.from_tuples(
            [("2011-12", "h1", "1_1", "Mil", RETIRED),
             ("2011-12", "h2", "1_1", "Mil", "Kg")],
            names=["t", "i", "plot", "crop", "u"],
        )
        df = pd.DataFrame({"Quantity": [1.0, 2.0]}, index=idx)
        out = Country(COUNTRY)._apply_categorical_mappings(df)
        served = list(out.index.get_level_values("u"))
        assert served == [SENTINEL, "Kg"], served

    def test_sentinel_is_idempotent_under_the_relabel(self):
        """`Unknown` is not an Original Label, so it passes through unchanged.

        Niger writes the sentinel at BUILD time; the read-time mapper must not
        then map it onto something else.
        """
        from lsms_library.country import Country

        idx = pd.MultiIndex.from_tuples(
            [("2011-12", "h1", "1_1", "Mil", SENTINEL)],
            names=["t", "i", "plot", "crop", "u"],
        )
        df = pd.DataFrame({"Quantity": [1.0]}, index=idx)
        out = Country(COUNTRY)._apply_categorical_mappings(df)
        assert list(out.index.get_level_values("u")) == [SENTINEL]

    def test_fill_helper_fills_column_and_index_alike(self):
        """`niger.fill_missing_u` handles both shapes and adds no rows.

        The four call sites hand it `u` as a column (the two `_finish_*`
        tails) and as an index level (the EHCVM `food_acquired` hooks).
        """
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "niger_for_test", countries_root() / COUNTRY / "_" / "niger.py")
        niger = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(niger)

        assert niger.U_NA == SENTINEL

        as_col = pd.DataFrame({"u": ["Kg", None], "Quantity": [1.0, 2.0]})
        out = niger.fill_missing_u(as_col)
        assert list(out["u"]) == ["Kg", SENTINEL]
        assert len(out) == len(as_col)

        as_idx = as_col.set_index("u")
        as_idx.index = pd.MultiIndex.from_arrays(
            [["2011-12", "2011-12"], as_col["u"]], names=["t", "u"])
        out = niger.fill_missing_u(as_idx)
        assert list(out.index.get_level_values("u")) == ["Kg", SENTINEL]
        assert len(out) == len(as_idx)

    def test_spellings_route_would_close_an_open_vocabulary(self):
        """Why `u` is NOT declared with a `spellings:` block (measured)."""
        import lsms_library.diagnostics as diag

        idx = pd.MultiIndex.from_tuples(
            [("2018-19", "h1", "1_1", "Mil", "Kg"),
             ("2018-19", "h2", "1_1", "Mil", SENTINEL)],
            names=["t", "i", "plot", "crop", "u"])
        df = pd.DataFrame({"Quantity": [1.0, 2.0]}, index=idx)
        saved = diag._DECLARED_VOCABULARIES
        try:
            probe = dict(saved)
            probe["crop_production"] = dict(saved.get("crop_production", {}))
            probe["crop_production"]["u"] = (frozenset({SENTINEL}), {RETIRED: SENTINEL})
            diag._DECLARED_VOCABULARIES = probe
            check = diag._check_declared_spellings(df, "crop_production")
        finally:
            diag._DECLARED_VOCABULARIES = saved
        assert check.status == "fail" and "Kg" in check.message, (
            "declaring `u` via `spellings:` no longer closes the vocabulary; "
            "re-run the mechanism decision in .coder/ledger/842-niger-u-unknown.md"
        )

    def test_data_info_declares_the_canonical_sentinel(self):
        """The canon is written down where the index vocabulary lives."""
        from importlib.resources import files

        text = (files("lsms_library") / "data_info.yml").read_text(encoding="utf-8")
        head = text.split("Index Info:", 1)[0]
        assert "CANONICAL MISSING-UNIT SENTINEL" in head and SENTINEL in head, (
            "data_info.yml's `Single Index` section no longer declares "
            f"{SENTINEL!r} as the canonical `u` sentinel"
        )

    def test_global_u_org_cannot_override_a_country_row(self):
        """Why the global `u.org` route was rejected (measured)."""
        from lsms_library.country import _merge_categorical_tables

        g = pd.DataFrame({"Original Label": [RETIRED], "Preferred Label": [SENTINEL]})
        c = pd.DataFrame({"Original Label": [RETIRED], "Preferred Label": [RETIRED]})
        merged = _merge_categorical_tables({"u": g}, {"u": c})["u"]
        won = merged.set_index("Original Label")["Preferred Label"][RETIRED]
        assert won == RETIRED, (
            "the country row no longer wins a `u` key collision; the global "
            "categorical_mapping/u.org route may now be viable -- see the ledger"
        )


# ---------------------------------------------------------------------------
# the built tables -- data-gated
# ---------------------------------------------------------------------------

@needs_data
class TestNoNullUnitIsServed:
    """No Niger table serves a NaN on its `u` index level."""

    @staticmethod
    def _serve(table: str) -> pd.DataFrame:
        from lsms_library.country import Country

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return getattr(Country(COUNTRY), table)().reset_index()

    @pytest.mark.parametrize("table", U_TABLES)
    def test_no_nan_unit(self, table):
        flat = self._serve(table)
        assert "u" in flat.columns, f"{table} lost its `u` level"
        n = int(flat["u"].isna().sum())
        assert n == 0, (
            f"{COUNTRY}.{table}() serves {n} rows with a NaN `u`.  A NaN on a "
            f"declared index level is deleted by the next groupby with no "
            f"count anywhere (GH #842/#847) -- fill it with {SENTINEL!r}."
        )

    @pytest.mark.parametrize("table", U_TABLES)
    def test_retired_spelling_is_not_served(self, table):
        flat = self._serve(table)
        n = int((flat["u"].astype("string") == RETIRED).sum())
        assert n == 0, (
            f"{COUNTRY}.{table}() serves {n} rows with the retired {RETIRED!r} "
            f"unit label; {SENTINEL!r} is the canonical sentinel"
        )

    @pytest.mark.parametrize("table", U_TABLES)
    def test_sentinel_counts_are_the_measured_ones(self, table):
        flat = self._serve(table)
        assert len(flat) == EXPECTED_ROWS[table], (
            f"{COUNTRY}.{table}() has {len(flat)} rows, measured "
            f"{EXPECTED_ROWS[table]}"
        )
        n = int((flat["u"].astype("string") == SENTINEL).sum())
        assert n == EXPECTED_SENTINEL[table], (
            f"{COUNTRY}.{table}() serves {n} {SENTINEL!r} rows, measured "
            f"{EXPECTED_SENTINEL[table]}"
        )

    @pytest.mark.parametrize("table", U_TABLES)
    def test_sentinel_counts_by_wave(self, table):
        """Per-wave, so one wave's regression cannot cancel another's."""
        flat = self._serve(table)
        got = (flat.assign(_s=flat["u"].astype("string") == SENTINEL)
                   .groupby("t")["_s"].sum().astype(int).to_dict())
        assert got == EXPECTED_SENTINEL_BY_WAVE[table], (
            f"{COUNTRY}.{table}() sentinel counts by wave are {got}, measured "
            f"{EXPECTED_SENTINEL_BY_WAVE[table]}"
        )
