"""Benin 2018-19 plot_inputs: the purchase, the gift, and the missing unit.

Ruling (@ligon, 2026-11-13, "Fix all three now"): Purchased / Quantity_purchased read
s16bq08 / s16bq09a (16B.08 "Avez-vous acheté [INTRANT]?"), not s16bq05 / s16bq07a
(16B.05 "... sous forme de cadeau ou de don?"), which are served under the gift
columns.  GH #1118: a blank unit of use is the canonical ``Unknown``, never a minted
``Manquant``.

The static class needs no microdata; ``TestServed`` (``requires_s3``) compares the
served wave with the raw file, decoded by code here.
"""
from __future__ import annotations

import warnings

import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

WAVE = "2018-19"
SCRIPT = countries_root() / "Benin" / WAVE / "_" / "plot_inputs.py"


class TestScript:

    def test_no_minted_unit(self):
        assert "fillna('Manquant')" not in SCRIPT.read_text(encoding="utf-8")

    def test_the_purchase_columns_read_the_purchase(self):
        text = SCRIPT.read_text(encoding="utf-8")
        assert "purchased = src['s16bq08']" in text
        assert "'Quantity_purchased': pd.to_numeric(srcn['s16bq09a']" in text
        assert "received_as_gift = src['s16bq05']" in text


@pytest.mark.requires_s3
class TestServed:

    @pytest.fixture(scope="class")
    def frames(self):
        s16b = get_dataframe(countries_root() / "Benin" / WAVE / "Data" / "s16b_me_ben2018.dta",
                             convert_categoricals=False)
        s16b = s16b[s16b["s16bq02"] == 1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p = ll.Country("Benin").plot_inputs(waves=[WAVE])
        return s16b, p

    def test_every_applied_line_is_served(self, frames):
        s16b, p = frames
        assert len(p) == len(s16b) == 10605

    def test_purchase_and_gift_are_their_own_questions(self, frames):
        s16b, p = frames
        assert p["Purchased"].astype("boolean").sum() == (s16b["s16bq08"] == 1).sum() == 4719
        assert p["Received_as_gift"].astype("boolean").sum() == (s16b["s16bq05"] == 1).sum() == 383
        assert p["Quantity_purchased"].astype(float).sum() == pytest.approx(
            s16b["s16bq09a"].astype(float).sum())
        assert p["Quantity_received_as_gift"].astype(float).sum() == pytest.approx(
            s16b["s16bq07a"].astype(float).sum())
        assert p["Quantity"].astype(float).sum() == pytest.approx(s16b["s16bq03a"].astype(float).sum())
        # A unit is answered exactly where its quantity is.
        assert p["Unit_purchased"].notna().sum() == s16b["s16bq09b"].notna().sum()
        assert p["Unit_received_as_gift"].notna().sum() == s16b["s16bq07b"].notna().sum()

    def test_the_unit_columns_are_spelled_as_u_and_as_2021(self, frames):
        """The payload unit columns are served as the `u` level is (GH #1705 red team
        F2): a 2018-19 kilogram purchase reads 'Kg', as 2021-22's does, not the raw
        'Kilogramme' a filter on 'Kg' would miss."""
        _, p = frames
        path = countries_root() / "Benin" / WAVE / "Data" / "s16b_me_ben2018.dta"
        labels = get_dataframe(path, convert_categoricals=True)
        labels = labels[(get_dataframe(path, convert_categoricals=False)["s16bq02"] == 1).to_numpy()]
        u = set(p.index.get_level_values("u").astype(str))
        for col, raw in (("Unit_purchased", "s16bq09b"), ("Unit_received_as_gift", "s16bq07b")):
            served = p[col].dropna().astype(str)
            assert not served.eq("Kilogramme").any(), col
            assert served.eq("Kg").sum() == labels[raw].astype(str).eq("Kilogramme").sum() > 0, col
        assert "Kg" in u and "Kilogramme" not in u
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new = ll.Country("Benin").plot_inputs(waves=["2021-22"])
        assert "Kg" in set(new["Unit_purchased"].dropna().astype(str))

    def test_the_blank_unit_is_unknown(self, frames):
        s16b, p = frames
        u = p.index.get_level_values("u").to_series()
        assert not u.eq("Manquant").any()
        assert u.eq("Unknown").sum() == s16b["s16bq03b"].isna().sum() == 1
