"""Burkina Faso 2014: C05 / C06 values are served as ``Expenditure`` (GH #1442).

Ruling (@ligon, 2026-11-04): ``autocons`` and ``cadeau``, which the file labels "Montant en
fcfa C05 / C06", are direct answers.  They are served as ``Expenditure`` on ``produced`` and
``inkind`` rows, with no derivation, as Uganda serves ``value_own`` and ``value_inkind``.  The
default ``basis='purchased'`` does not move.  ``mtt_dep`` (C02) stays unread: it is outlay, a
different question.

``test_config`` needs no data.  The oracle reads the four passage files directly and imports
nothing from ``Burkina_Faso/_/``.
"""
import importlib.util
import pathlib
import warnings

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
ROOT = countries_root() / COUNTRY
PASSAGES = {p: f"emc2014_p{p}_conso7jours_16032015.dta" for p in (1, 2, 3, 4)}
VALUES = {"purchased": "achat", "produced": "autocons", "inkind": "cadeau"}


def test_config_reads_each_block_value_and_not_c02():
    with open(ROOT / "2014" / "_" / "data_info.yml", encoding="utf-8") as f:
        myvars = yaml.safe_load(f)["food_acquired"]["myvars"]
    assert myvars["Expenditure"] == "achat"
    assert myvars["ProducedValue"] == "autocons"
    assert myvars["GiftValue"] == "cadeau"
    assert "mtt_dep" not in {v for v in myvars.values() if isinstance(v, str)}


@pytest.fixture(scope="module")
def raw():
    frames = []
    for passage, name in PASSAGES.items():
        d = get_dataframe(str(ROOT / "2014" / "Data" / name))
        frames.append(d.assign(visit=passage, i=[f"EMC_{int(z)}{int(m):03d}" for z, m in zip(d["zd"], d["menage"])]))
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country(COUNTRY).food_acquired(waves=["2014"])


@requires_s3
class TestOracle:
    @pytest.mark.parametrize("source,column", list(VALUES.items()))
    def test_each_households_money_in_each_passage_is_the_source_column(self, raw, served, source, column):
        value = pd.to_numeric(raw[column], errors="coerce")
        expect = value.where(value > 0).groupby([raw["i"], raw["visit"]]).sum()
        expect = expect[expect > 0]
        flat = served.xs(source, level="s").reset_index()
        got = flat.groupby(["i", "visit"])["Expenditure"].sum()
        got = got[got > 0]
        assert set(got.index) == set(expect.index)
        np.testing.assert_allclose(got.loc[expect.index].to_numpy(), expect.to_numpy())

    def test_a_money_only_block_is_a_row_with_no_quantity(self, raw, served):
        for source, column, quantity in (("produced", "autocons", "qautocons"), ("inkind", "cadeau", "qcadeau")):
            value = pd.to_numeric(raw[column], errors="coerce")
            qty = pd.to_numeric(raw[quantity], errors="coerce")
            part = served.xs(source, level="s")
            kept = part["Quantity"].notna() | part["Expenditure"].notna()
            assert kept.all()
            assert part["Quantity"].isna().any()
            # Nothing a passage 2-4 row could carry but money.
            later = part[part.index.get_level_values("visit") > 1]
            assert later["Quantity"].isna().all() and (later["Expenditure"] > 0).all()
            assert later.index.get_level_values("u").astype(str).unique().tolist() == ["Unknown"]
            # Every source row with money or a quantity reached the table, duplicates aside.
            assert len(part) <= int(((value > 0) | (qty > 0)).sum())

    def test_no_row_of_the_wave_carries_a_derivation_key(self, served):
        if "Derivation" in served.columns:
            assert served["Derivation"].isna().all()

    def test_the_default_basis_does_not_move(self, raw):
        achat = pd.to_numeric(raw["achat"], errors="coerce")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = ll.Country(COUNTRY).food_expenditures(waves=["2014"])
        assert set(frame.index.get_level_values("s").astype(str)) == {"purchased"}
        assert frame["Expenditure"].sum() == pytest.approx(achat[achat > 0].sum())

    def test_basis_total_is_the_three_blocks(self, raw):
        total = sum(pd.to_numeric(raw[c], errors="coerce").clip(lower=0).sum() for c in VALUES.values())
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = ll.Country(COUNTRY).food_expenditures(waves=["2014"], basis="total")
        assert frame["Expenditure"].sum() == pytest.approx(total)

    def test_a_reported_value_is_not_valued(self, raw):
        """A row with its own answer is `reported`, not priced by the median ladder."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = ll.Country(COUNTRY).food_expenditures(
                waves=["2014"], basis="total", valuation="median_price")
        sources = frame.attrs["valuation_sources"]
        assert sources["reported"] > 400_000 and sources["candidates"] < 2_000
