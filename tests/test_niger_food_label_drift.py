"""Niger ``food_acquired.j``: cross-wave label drift folded, splits kept -- GH #833.

The two EHCVM waves spelled several items differently, so the served ``j``
carried near-duplicates (warm cache, rows): ``Chou`` 628 (2018-19) vs
``Choux`` 973 (2021-22); ``Sucre (poudre ou morceaux)`` 4,883 (2018-19) vs
``Sucre poudre`` 5,462 + ``Sucre morceaux`` 80 (2021-22); ``Aubergine,
Courge/Courgette`` 1,156 (2018-19) vs ``Aubergine,`` 452 (2021-22).

Read against the 2021-22 questionnaires (WB catalog 6276) two of the three
are an instrument SPLIT, not drift.  The decisions (``Niger/_/CONTENTS.org``,
"food_acquired: cross-wave label drift"):

* ``Choux`` -> ``Chou``: one vegetable, plural spelling.  Folded.
* ``Sucre poudre`` / ``Sucre morceaux`` -> ``Sucre (poudre ou morceaux)``:
  2018-19's label already says it covers both forms and cannot be split.
  Folded.
* ``Aubergine,`` -> ``Aubergine``, NOT folded into ``Aubergine,
  Courge/Courgette``: eggplant and courgette are different goods.  The crop
  rows (``Aubergine`` / ``aubergine``) follow.
* Code 166 (468 rows, 2021-22) CANNOT be decoded -- both questionnaires'
  product lists and the .dta value labels end at 165 -- and stays ``'166'``.

Two families, split as in ``tests/test_niger_value_sentinels.py``:

* ``TestTableFolds`` needs NO microdata: it pins the ``harmonize_food`` rows.
* ``TestServedLabels`` builds the real tables and is skipped when Niger is
  unavailable.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

COUNTRY = "Niger"
TABLE = "harmonize_food"

#: Original Label -> Preferred Label rows GH #833 changed or relies on.
FOLDS = {
    "89. Choux": "Chou",
    "Chou": "Chou",
    "134. Sucre poudre": "Sucre (poudre ou morceaux)",
    "135. Sucre morceaux": "Sucre (poudre ou morceaux)",
    "Sucre (poudre ou morceaux)": "Sucre (poudre ou morceaux)",
    "93. Aubergine,": "Aubergine",
    "Aubergine": "Aubergine",          # crop rows
    "aubergine": "Aubergine",
    # NOT folded: two goods bundled in 2018-19, split in 2021-22.
    "Aubergine, Courge/Courgette": "Aubergine, Courge/Courgette",
}
#: Labels that must no longer be served by any wave.
RETIRED = ("Choux", "Sucre poudre", "Sucre morceaux", "Aubergine,")

#: Served rows per (label, wave), measured on the 2026-09-24 cold rebuild.
#: 2021-22 sugar is 5,537, not 5,462 + 80 = 5,542: five households bought
#: both forms in the same unit, and the fold merges those rows at the
#: (t, v, i, j, u, s) grain (an additive, lossless sum).
SERVED = {
    ("Chou", "2018-19"): 628,
    ("Chou", "2021-22"): 973,
    ("Sucre (poudre ou morceaux)", "2018-19"): 4883,
    ("Sucre (poudre ou morceaux)", "2021-22"): 5537,
    ("Aubergine, Courge/Courgette", "2018-19"): 1156,
    ("Aubergine, Courge/Courgette", "2021-22"): 0,
    ("Aubergine", "2018-19"): 0,
    ("Aubergine", "2021-22"): 452,
    ("Courge/Courgette", "2021-22"): 2387,
    ("166", "2021-22"): 468,
}
CODE_166_ROWS = 468
CODE_166_SACHET_ROWS = 466


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


@pytest.fixture(scope="module")
def food_table() -> dict[str, str]:
    df = df_from_orgfile(_root() / "_" / "categorical_mapping.org", name=TABLE)
    return dict(zip(df["Original Label"].astype(str).str.strip(),
                    df["Preferred Label"].astype(str).str.strip()))


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestTableFolds:

    @pytest.mark.parametrize("raw,canon", sorted(FOLDS.items()))
    def test_row(self, food_table, raw, canon):
        assert food_table.get(raw) == canon, f"{raw!r} -> {food_table.get(raw)!r}"

    def test_no_retired_label_is_a_preferred_label(self, food_table):
        leaked = set(food_table.values()) & set(RETIRED)
        assert not leaked, sorted(leaked)

    def test_no_preferred_label_ends_with_a_comma(self, food_table):
        """'Aubergine,' was the 2018-19 bundle's truncation, not a name."""
        bad = sorted(v for v in food_table.values() if v.endswith(","))
        assert not bad, bad

    def test_code_166_is_not_named_by_inference(self, food_table):
        """No row decodes 166: the questionnaires and value labels end at
        165, and the sibling EHCVM-2 files label 166 country-specifically."""
        rows = {k: v for k, v in food_table.items() if k.split(".")[0].strip() == "166"}
        assert not rows, rows


# ---------------------------------------------------------------------------
# built tables
# ---------------------------------------------------------------------------

@needs_data
class TestServedLabels:

    @pytest.fixture(scope="class")
    def country(self):
        import lsms_library as ll
        return ll.Country(COUNTRY)

    @pytest.fixture(scope="class")
    def food_acquired(self, country):
        return country.food_acquired()

    def test_no_retired_label_is_served(self, food_acquired):
        j = food_acquired.index.get_level_values("j").astype(str)
        for lab in RETIRED:
            n = int((j == lab).sum())
            assert n == 0, f"j={lab!r} still served on {n} rows"

    @pytest.mark.parametrize("key", sorted(SERVED))
    def test_served_rows_per_label_and_wave(self, food_acquired, key):
        lab, wave = key
        sub = food_acquired.xs(wave, level="t")
        n = int((sub.index.get_level_values("j").astype(str) == lab).sum())
        assert n == SERVED[key], f"{wave} j={lab!r}: {n} rows, expected {SERVED[key]}"

    def test_folded_labels_join_across_the_two_waves(self, food_acquired):
        j = food_acquired.index.get_level_values("j").astype(str)
        for lab in ("Chou", "Sucre (poudre ou morceaux)"):
            waves = set(food_acquired[j == lab].index.get_level_values("t"))
            assert waves == {"2018-19", "2021-22"}, f"{lab!r} served in {sorted(waves)}"

    def test_code_166_stays_a_code(self, food_acquired):
        """468 rows, 466 in 'Sachet': the evidence CONTENTS.org records, and
        the only numeric-looking j in the table."""
        j = food_acquired.index.get_level_values("j").astype(str)
        numeric = sorted({x for x in j.unique() if x.isdigit()})
        assert numeric == ["166"], numeric
        rows = food_acquired[j == "166"]
        assert len(rows) == CODE_166_ROWS
        u = rows.index.get_level_values("u").astype(str)
        assert int((u == "Sachet").sum()) == CODE_166_SACHET_ROWS

    def test_crop_rows_follow_the_eggplant_label(self, country):
        crop = country.crop_production().index.get_level_values("crop").astype(str)
        assert int((crop == "Aubergine,").sum()) == 0
        assert int((crop == "Aubergine").sum()) > 0
