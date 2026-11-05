"""GH #1513: Mali 2018-19 food labels reach ``harmonize_food`` (ruling 2026-11-05).

Every ``s07bq01`` label of that wave carries seven leading spaces and the ``mappings:``
lookup is exact, so ``j`` was served unmapped, against the labels of every other wave and of
the community table.  The wave's ``mapping.j`` strips before the lookup.

``TestRule`` needs no microdata.  ``TestServed`` does (``requires_s3``): its oracle reads
``s07b_me_mli2018.dta`` and the org table directly, and imports nothing from ``Mali/_/``.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

MALI = countries_root() / "Mali"
WAVE = "2018-19"
RAW = str(MALI / WAVE / "Data" / "s07b_me_mli2018.dta")
PADDED = "       Sucre (poudre ou morceaux)"
SERVED = "Sucre"


def _table():
    t = df_from_orgfile(str(MALI / "_" / "categorical_mapping.org"), name="harmonize_food")
    return dict(zip(t["Code"].astype(str).str.strip(), t["Preferred Label"].astype(str).str.strip()))


class TestRule:
    @pytest.fixture(scope="class")
    def strip(self):
        return ll.Country("Mali")[WAVE].formatting_functions["j"]

    def test_a_row_of_one_label_loses_its_padding(self, strip):
        assert strip(pd.Series([PADDED])) == PADDED.strip()

    def test_a_scalar_loses_it_too(self, strip):
        assert strip(PADDED) == PADDED.strip()

    @pytest.mark.parametrize("value", [np.nan, None, pd.NA])
    def test_a_missing_label_stays_missing(self, strip, value):
        assert pd.isna(strip(pd.Series([value], dtype=object)))

    def test_only_this_wave_strips(self):
        for wave in ("2014-15", "2017-18", "2021-22"):
            assert "j" not in ll.Country("Mali")[wave].formatting_functions

    def test_the_table_is_keyed_on_the_stripped_label(self):
        assert _table()[PADDED.strip()] == SERVED

    def test_the_chain_strips_then_maps(self):
        wave = ll.Country("Mali")[WAVE]
        mapping = wave.column_mapping("food_acquired", dict(wave.resources["food_acquired"]))
        (_, cols), = [kv for kv in mapping.items() if kv[0] != "df_edit"]
        source, steps = cols["idxvars"]["j"]
        assert list(source) == ["s07bq01"]
        function, table = steps
        assert table[function(pd.Series([PADDED]))] == SERVED


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope="class")
    def served(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country("Mali").food_acquired().reset_index()
        return df[df.t == WAVE]

    @pytest.fixture(scope="class")
    def raw(self):
        labels = get_dataframe(RAW)
        out = pd.DataFrame({
            "label": labels["s07bq01"].astype("string").str.strip(),
            "own": pd.to_numeric(labels["s07bq04"], errors="coerce"),
            "gift": pd.to_numeric(labels["s07bq05"], errors="coerce")})
        out["j"] = out["label"].map(_table())
        return out

    def test_the_file_is_padded(self):
        labels = get_dataframe(RAW)["s07bq01"].astype("string").dropna()
        assert labels.str.startswith(" ").mean() > 0.99

    def test_every_source_label_is_in_the_table(self, raw):
        assert raw.j.notna().all()

    def test_the_served_labels_are_the_tables(self, served, raw):
        assert set(served.j.astype(str)) == set(raw.j.astype(str))

    def test_no_served_label_is_padded(self, served):
        assert not served.j.astype(str).str.startswith(" ").any()

    @pytest.mark.parametrize("s,column", [("produced", "own"), ("inkind", "gift")])
    def test_a_fold_conserves_each_foods_quantity(self, served, raw, s, column):
        """Summed, not selected: a produced or gift quantity by food equals the file's."""
        rows = served[served.s.astype(str) == s]
        got = rows.groupby(rows.j.astype(str)).Quantity.sum().astype("float64")
        want = raw[raw[column] > 0].groupby("j")[column].sum().astype("float64")
        pd.testing.assert_series_equal(got.sort_index(), want.sort_index(), check_names=False,
                                       check_index_type=False, rtol=1e-6)

    def test_the_index_stays_unique(self, served):
        assert not served.duplicated(["t", "v", "i", "j", "u", "s"]).any()

    def test_a_food_the_table_renames_now_meets_the_community(self, served, raw):
        """What the strip is for: these foods could not join before, on any unit."""
        renamed = set(raw.loc[raw.label != raw.j, "j"].astype(str))
        community = ll.Country("Mali").community_prices().reset_index()
        community = community[community.t == WAVE]
        cells = set(map(tuple, community[["j", "u"]].astype(str).drop_duplicates().values))
        here = set(map(tuple, served[["j", "u"]].astype(str).drop_duplicates().values))
        assert {cell for cell in here & cells if cell[0] in renamed}

    def test_derivation_inputs_still_join(self, served):
        key = "Mali::food_acquired::7day-purchase-at-last-purchase-unit-value"
        inputs = ll.Country("Mali").derivation_inputs(key, wave=WAVE)
        assert set(inputs.index.get_level_values("j").astype(str)) == set(served.j.astype(str))
