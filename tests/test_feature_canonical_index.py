"""Canonical-index assembly for `Feature()` -- GH #569 and GH #775.

`crop_production` had no `index_info` entry, so `Feature.__call__` fell through
to the MODAL index-shape filter: the shape carried by the most frames won, ties
broken by the order the caller listed the countries in.  Two consequences, both
measured on the warm corpus:

* a country that moved TOWARD the canonical index was the one excluded.  Adding
  the canonical `condition` level to Malawi (`feat/854-malawi-shipped-factors`)
  made Malawi's shape a singleton and dropped 131,379 rows -- 81% of the
  assembly in the red-team's four-country call
  (`slurm_logs/2026-09-09_epar_curation/REDTEAM_P2_854_malawi.org`, item 1b).
  Uganda, which has always had `condition`, was excluded for the same reason;
* the survivors depended on argument order (GH #775): the same four countries
  in three different orders returned three different single countries.

The fix is three declarative pieces in `data_info.yml` -- an `index_info` entry,
`level_aliases`, `missing_level_sentinels` -- plus a stated, order-independent
rule for which shape wins.  These tests pin all four.
"""
from __future__ import annotations

import itertools
import warnings

import pandas as pd
import pytest

from lsms_library.feature import (
    Feature,
    _align_to_canonical_levels,
    _canonical_index_levels,
    _fabricates_missing_levels,
    _harmonize_country_frame,
    _level_aliases,
    _missing_level_sentinels,
    _rename_index_levels,
    _select_kept_shape,
)

CANONICAL = ["t", "v", "i", "plot_id", "j", "u", "condition", "season"]


# ---------------------------------------------------------------------------
# The declarations themselves
# ---------------------------------------------------------------------------

class TestDeclarations:
    def test_crop_production_is_registered(self):
        assert _canonical_index_levels("crop_production") == CANONICAL

    def test_canonical_index_keeps_v(self):
        """Load-bearing: `country._compute_no_v_join` exempts any table whose
        canonical index omits `v`, so registering crop_production without it
        would silently strip `v` from every Country(...).crop_production()
        frame in the corpus."""
        from lsms_library.country import _no_v_join_tables
        assert "v" in _canonical_index_levels("crop_production")
        assert "crop_production" not in _no_v_join_tables()

    def test_level_aliases(self):
        assert _level_aliases("crop_production") == {"plot": "plot_id", "crop": "j"}

    def test_missing_level_sentinels_are_non_null_strings(self):
        sentinels = _missing_level_sentinels("crop_production")
        assert sentinels == {"u": "Unknown", "condition": "unknown_condition",
                             "season": "unknown_season"}
        for lvl, value in sentinels.items():
            assert isinstance(value, str) and value, lvl
            assert pd.notna(value), (
                f"{lvl}: a NULL index key is deleted by the next groupby "
                f"(CLAUDE.md 'Grain Collapse' 3b) -- the sentinel must be real")

    def test_sentinel_levels_are_canonical_levels(self):
        """A sentinel for a level the canonical index does not declare would
        never fire, and would read as a promise the assembly does not keep.
        Checked for EVERY table that declares sentinels (GH #569 added
        plot_labor / plot_inputs), not only the first one."""
        from lsms_library.feature import _index_info_section
        tables = sorted(_index_info_section().get("missing_level_sentinels") or {})
        assert {"crop_production", "plot_labor", "plot_inputs"} <= set(tables)
        for table in tables:
            canonical = set(_canonical_index_levels(table))
            assert canonical, f"{table}: sentinels declared but not registered"
            sentinels = _missing_level_sentinels(table)
            assert set(sentinels) <= canonical, table
            for lvl, value in sentinels.items():
                assert isinstance(value, str) and value and pd.notna(value), (
                    f"{table}.{lvl}: a NULL index key is deleted by the next "
                    f"groupby (CLAUDE.md 'Grain Collapse' 3b)")

    def test_alias_targets_are_canonical_levels(self):
        """An alias whose target is not canonical renames a level into one
        the assembly then drops as an 'extra' -- the opposite of its purpose."""
        from lsms_library.feature import _index_info_section
        for table in sorted(_index_info_section().get("level_aliases") or {}):
            canonical = set(_canonical_index_levels(table))
            targets = set(_level_aliases(table).values())
            assert targets <= canonical, (table, targets - canonical)

    def test_crop_production_does_not_use_the_pd_NA_fabrication(self):
        """#506's `fabricate_missing_levels` fills with pd.NA; the sentinel
        mechanism exists precisely because that is unsafe for a declared index
        level.  A feature must not be in both."""
        assert _fabricates_missing_levels("crop_production") is False
        assert _fabricates_missing_levels("interview_date") is True

    def test_no_feature_declares_both_mechanisms(self):
        from lsms_library.feature import _index_info_section
        section = _index_info_section()
        both = (set(section.get("fabricate_missing_levels") or [])
                & set(section.get("missing_level_sentinels") or {}))
        assert not both, f"features in both fill mechanisms: {sorted(both)}"


# ---------------------------------------------------------------------------
# Level renaming (GH #569)
# ---------------------------------------------------------------------------

def _frame(names, tuples, **cols):
    idx = pd.MultiIndex.from_tuples(tuples, names=names)
    return pd.DataFrame(cols or {"Quantity": [1.0] * len(tuples)}, index=idx)


class TestRenameIndexLevels:
    def test_plot_and_crop_are_renamed(self):
        """`plot_id` is canonical (@ligon 2026-09-10); `plot` is the alias."""
        df = _frame(["i", "t", "v", "plot", "crop", "u"],
                    [("h1", "2011", "c1", "p1", "Maize", "kg")])
        out = _rename_index_levels(df, _level_aliases("crop_production"),
                                   "X", "crop_production")
        assert list(out.index.names) == ["i", "t", "v", "plot_id", "j", "u"]
        assert len(out) == 1

    def test_noop_when_no_aliased_level_present(self):
        df = _frame(["t", "i", "plot_id", "j"], [("2011", "h1", "p1", "Maize")])
        out = _rename_index_levels(df, _level_aliases("crop_production"),
                                   "X", "crop_production")
        assert list(out.index.names) == ["t", "i", "plot_id", "j"]

    def test_clash_is_refused_and_warned(self):
        """A frame carrying BOTH `crop` and `j` means something by the
        distinction; renaming would make two levels share one name."""
        df = _frame(["t", "i", "crop", "j"], [("2011", "h1", "Maize", "Maize")])
        with pytest.warns(UserWarning, match="canonical target"):
            out = _rename_index_levels(df, {"crop": "j"}, "X", "crop_production")
        assert list(out.index.names) == ["t", "i", "crop", "j"]


# ---------------------------------------------------------------------------
# Missing-level alignment: promote-or-fabricate
# ---------------------------------------------------------------------------

class TestAlignToCanonicalLevels:
    def test_fabricates_absent_level_as_the_declared_sentinel(self):
        df = _frame(["t", "v", "i", "plot_id", "j", "u"],
                    [("2011", "c1", "h1", "p1", "Maize", "kg"),
                     ("2011", "c1", "h2", "p1", "Rice", "kg")])
        with pytest.warns(UserWarning, match="does not record"):
            out = _align_to_canonical_levels(
                df, CANONICAL, _missing_level_sentinels("crop_production"),
                "Ethiopia", "crop_production")
        assert list(out.index.names) == ["t", "v", "i", "plot_id", "j", "u",
                                         "condition", "season"]
        assert len(out) == len(df)
        assert out.index.is_unique
        assert set(out.index.get_level_values("condition")) == {"unknown_condition"}
        assert set(out.index.get_level_values("season")) == {"unknown_season"}

    def test_fabricated_level_is_never_null(self):
        df = _frame(["t", "v", "i", "plot_id", "j", "u"],
                    [("2011", "c1", "h1", "p1", "Maize", "kg")])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _align_to_canonical_levels(
                df, CANONICAL, _missing_level_sentinels("crop_production"),
                "X", "crop_production")
        for lvl in ("condition", "season"):
            assert not out.index.get_level_values(lvl).isna().any(), lvl

    def test_promotes_a_canonical_level_carried_as_a_column(self):
        """Mali / Nigeria / Tanzania report a real harvest unit in a `u`
        COLUMN.  Fabricating `u='Unknown'` over it would be a lie; promotion
        is lossless."""
        df = _frame(["t", "v", "i", "plot_id", "j"],
                    [("2012", "c1", "h1", "p1", "Maize"),
                     ("2012", "c1", "h2", "p1", "Rice")],
                    Quantity=[1.0, 2.0], u=["Kg", None])
        with pytest.warns(UserWarning, match="promoted column"):
            out = _align_to_canonical_levels(
                df, CANONICAL, _missing_level_sentinels("crop_production"),
                "Nigeria", "crop_production")
        assert "u" in out.index.names and "u" not in out.columns
        assert out.index.get_level_values("u").tolist() == ["Kg", "Unknown"]
        assert len(out) == 2 and out.index.is_unique

    def test_promotion_fills_nulls_with_the_sentinel_not_NA(self):
        df = _frame(["t", "v", "i", "plot_id", "j"],
                    [("2012", "c1", "h1", "p1", "Maize")],
                    Quantity=[1.0], u=[pd.NA])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _align_to_canonical_levels(
                df, CANONICAL, _missing_level_sentinels("crop_production"),
                "X", "crop_production")
        assert out.index.get_level_values("u").tolist() == ["Unknown"]

    def test_promotion_handles_a_categorical_column(self):
        """`get_dataframe` returns pandas categoricals from Stata/SPSS; a
        `fillna` on a categorical whose categories lack the sentinel raises."""
        df = _frame(["t", "v", "i", "plot_id", "j"],
                    [("2012", "c1", "h1", "p1", "Maize"),
                     ("2012", "c1", "h2", "p1", "Rice")],
                    Quantity=[1.0, 2.0],
                    u=pd.Categorical(["Kg", None], categories=["Kg"]))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _align_to_canonical_levels(
                df, CANONICAL, _missing_level_sentinels("crop_production"),
                "X", "crop_production")
        assert out.index.get_level_values("u").tolist() == ["Kg", "Unknown"]

    def test_noop_when_every_declared_level_is_present(self):
        df = _frame(CANONICAL, [tuple("abcdefgh")])
        out = _align_to_canonical_levels(
            df, CANONICAL, _missing_level_sentinels("crop_production"),
            "Uganda", "crop_production")
        assert out is df

    def test_report_suppresses_the_per_country_warning(self):
        df = _frame(["t", "v", "i", "plot_id", "j", "u"],
                    [("2011", "c1", "h1", "p1", "Maize", "kg")])
        report: dict = {}
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any warning would fail the test
            out = _align_to_canonical_levels(
                df, CANONICAL, _missing_level_sentinels("crop_production"),
                "Ethiopia", "crop_production", report=report)
        assert report["fabricated"]["Ethiopia"] == {
            "condition": "unknown_condition", "season": "unknown_season"}
        assert len(out) == 1

    def test_no_sentinels_declared_is_a_noop(self):
        df = _frame(["t", "i"], [("2011", "h1")])
        assert _align_to_canonical_levels(df, ["t", "i", "j"], {}, "X", "tbl") is df


# ---------------------------------------------------------------------------
# _harmonize_country_frame: rename + align, in that order
# ---------------------------------------------------------------------------

class TestHarmonizeCountryFrame:
    @pytest.mark.parametrize("names,extra", [
        # (Ethiopia) plot_id + j, no condition/season
        (["i", "t", "v", "plot_id", "j", "u"], {}),
        # (Malawi 6-level) plot + crop, no condition/season
        (["i", "t", "v", "plot", "crop", "u"], {}),
        # (GhanaSPS) plot_id + j + season, no condition
        (["i", "t", "v", "plot_id", "j", "u", "season"], {}),
        # (Uganda / Malawi 7-level) the full canonical shape, scrambled order
        (["i", "t", "v", "plot", "j", "u", "condition", "season"], {}),
    ])
    def test_every_real_corpus_shape_reaches_the_canonical_index(self, names, extra):
        df = _frame(names, [tuple(f"x{k}" for k in range(len(names)))], **extra)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _harmonize_country_frame(
                df, CANONICAL, "X", "crop_production", False,
                aliases=_level_aliases("crop_production"),
                sentinels=_missing_level_sentinels("crop_production"))
        assert list(out.index.names) == CANONICAL
        assert len(out) == 1

    def test_alias_runs_before_alignment(self):
        """Aligning first would see `plot_id` as missing on a country that
        spells it `plot` -- there is no `plot_id` sentinel, so the level would
        simply not be produced and the frame would still be excluded."""
        df = _frame(["t", "v", "i", "plot", "j", "u"],
                    [("2011", "c1", "h1", "p1", "Maize", "kg")])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _harmonize_country_frame(
                df, CANONICAL, "Uganda", "crop_production", False,
                aliases=_level_aliases("crop_production"),
                sentinels=_missing_level_sentinels("crop_production"))
        assert list(out.index.names) == CANONICAL
        assert out.index.get_level_values("plot_id").tolist() == ["p1"]

    def test_promotion_of_DISTINCT_values_refines_and_loses_nothing(self):
        """The REFINING case: two non-null, distinct `u` values on rows that are
        otherwise identical.  Promotion separates them, so nothing collapses.

        This is deliberately labelled as only half the story -- see the
        collision tests below.  An earlier version of this test was the ONLY
        coverage of promotion and asserted `is_unique`, which read as proof that
        promotion cannot collide.  It cannot collide *here*; it can collide when
        a null and the LITERAL sentinel share a row key."""
        from lsms_library.country import grain_reports
        before = len(grain_reports())
        df = _frame(["t", "v", "i", "plot", "j"],
                    [("2012", "c1", "h1", "p1", "Maize"),
                     ("2012", "c1", "h1", "p1", "Maize")],
                    Quantity=[1.0, 2.0], u=["Kg", "Bag"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _harmonize_country_frame(
                df, CANONICAL, "Nigeria", "crop_production", False,
                aliases=_level_aliases("crop_production"),
                sentinels=_missing_level_sentinels("crop_production"))
        assert len(out) == 2 and out.index.is_unique
        assert len(grain_reports()) == before

    def test_fabrication_alone_can_never_collide(self):
        """Branch 3 IS structurally safe: a constant level distinguishes
        nothing, so it cannot map two distinct keys onto one."""
        df = _frame(["t", "v", "i", "plot", "j", "u"],
                    [("2012", "c1", "h1", "p1", "Maize", "Kg"),
                     ("2012", "c1", "h1", "p1", "Maize", "Bag")])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _harmonize_country_frame(
                df, CANONICAL, "X", "crop_production", False,
                aliases={}, sentinels=_missing_level_sentinels("crop_production"))
        assert len(out) == 2 and out.index.is_unique


def _collision_frame():
    """Two rows identical but for `u`: one NULL, one the LITERAL sentinel.

    Filling the null with `'Unknown'` maps them onto the same index tuple.  No
    corpus country is in this state today (Mali / Nigeria / Tanzania carry 0
    literal `'Unknown'` in `u`), which is a fact about the data and not a
    property of the code -- a new wave could ship one tomorrow."""
    return _frame(["t", "v", "i", "plot_id", "j"],
                  [("2012", "c1", "h1", "p1", "Maize"),
                   ("2012", "c1", "h1", "p1", "Maize")],
                  Quantity=[1.0, 2.0], u=["Unknown", None])


class TestPromotionCollision:
    """The one way `_align_to_canonical_levels` can make an index non-unique.

    The docstring used to claim it could not, and `Feature.__call__` never
    checked `is_unique` after the concat -- so a real collision would have
    shipped a silently non-unique cross-country index.  It is now routed through
    the SAME audited collapse the core uses (GH #323)."""

    def test_collision_is_collapsed_and_a_grain_report_is_filed(self):
        from lsms_library.country import GrainCollapseWarning, grain_reports
        before = len(grain_reports("Nigeria", "crop_production"))
        with pytest.warns(GrainCollapseWarning):
            out = _align_to_canonical_levels(
                _collision_frame(), CANONICAL,
                _missing_level_sentinels("crop_production"),
                "Nigeria", "crop_production")
        assert out.index.is_unique, "the collision was not resolved"
        assert len(out) == 1
        after = grain_reports("Nigeria", "crop_production")
        assert len(after) > before, "no grain report was filed for the collapse"
        assert after[-1]["site"] == "Feature._harmonize_country_frame"

    def test_collision_is_fatal_under_grain_strict(self, monkeypatch):
        from lsms_library.country import GrainCollapseError
        monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
        with pytest.raises(GrainCollapseError):
            _align_to_canonical_levels(
                _collision_frame(), CANONICAL,
                _missing_level_sentinels("crop_production"),
                "Nigeria", "crop_production")

    def test_collision_is_never_a_silent_pass_through(self):
        """Whatever else happens, the frame handed to `pd.concat` is unique."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = _harmonize_country_frame(
                _collision_frame(), CANONICAL, "Nigeria", "crop_production",
                False, aliases=_level_aliases("crop_production"),
                sentinels=_missing_level_sentinels("crop_production"))
        assert out.index.is_unique

    def test_collision_is_recorded_in_the_aggregated_report(self):
        report: dict = {}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _align_to_canonical_levels(
                _collision_frame(), CANONICAL,
                _missing_level_sentinels("crop_production"),
                "Nigeria", "crop_production", report=report)
        assert report["promotion_collisions"]["Nigeria"] == {"u": "Unknown"}

    def test_unregistered_feature_is_untouched(self):
        """No index_info entry -> canonical_levels is [] -> nothing happens."""
        df = _frame(["i", "t", "plot_id", "crop"], [("h1", "2011", "p1", "Maize")])
        out = _harmonize_country_frame(df, [], "X", "an_unregistered_table", False,
                                       aliases={}, sentinels={})
        assert list(out.index.names) == ["i", "t", "plot_id", "crop"]


# ---------------------------------------------------------------------------
# _select_kept_shape: canonical preference + order independence (GH #775)
# ---------------------------------------------------------------------------

def _country_frame(country, names, n=1):
    inner = pd.MultiIndex.from_tuples(
        [tuple(f"{country}{k}{r}" for k in range(len(names))) for r in range(n)],
        names=names)
    df = pd.DataFrame({"Quantity": [1.0] * n}, index=inner)
    return pd.concat({country: df}, names=["country"])


class TestSelectKeptShape:
    def test_canonical_shape_wins_even_as_a_minority_of_one(self):
        """The whole point: a country that moved toward the declared index must
        not be the one excluded."""
        frames = [_country_frame("Uganda", CANONICAL, n=1),
                  _country_frame("A", ["t", "v", "i", "plot", "j", "u"], n=50),
                  _country_frame("B", ["t", "v", "i", "plot", "j", "u"], n=50)]
        kept, dropped, winner = _select_kept_shape(frames, CANONICAL)
        assert winner == tuple(["country"] + CANONICAL)
        assert [f.index.get_level_values("country")[0] for f in kept] == ["Uganda"]
        assert len(dropped) == 2

    def test_modal_rule_survives_for_an_unregistered_feature(self):
        frames = [_country_frame("A", ["t", "i", "x"]),
                  _country_frame("B", ["t", "i", "x"]),
                  _country_frame("C", ["t", "i"])]
        kept, dropped, winner = _select_kept_shape(frames, [])
        assert winner == ("country", "t", "i", "x")
        assert len(kept) == 2 and len(dropped) == 1

    def test_tie_is_broken_by_rows_not_by_argument_order(self):
        big = _country_frame("Togo", ["t", "i", "j"], n=100)
        small = _country_frame("Uganda", ["t", "i", "m"], n=3)
        for frames in ([big, small], [small, big]):
            _kept, _dropped, winner = _select_kept_shape(frames, [])
            assert winner == ("country", "t", "i", "j")

    def test_final_tie_break_is_lexicographic_and_stable(self):
        a = _country_frame("A", ["t", "i", "aaa"], n=5)
        b = _country_frame("B", ["t", "i", "zzz"], n=5)
        for frames in ([a, b], [b, a]):
            _kept, _dropped, winner = _select_kept_shape(frames, [])
            assert winner == ("country", "t", "i", "aaa")

    def test_survivors_are_identical_under_every_permutation(self):
        """GH #775, stated as a property.  On `development` the same four
        countries in three orders returned three different single survivors."""
        frames = {
            "Uganda": _country_frame("Uganda", ["t", "i", "a"], n=7),
            "Ethiopia": _country_frame("Ethiopia", ["t", "i", "b"], n=7),
            "Tanzania": _country_frame("Tanzania", ["t", "i", "c"], n=7),
            "Malawi": _country_frame("Malawi", ["t", "i", "d"], n=7),
        }
        results = set()
        for perm in itertools.permutations(frames):
            kept, _dropped, winner = _select_kept_shape(
                [frames[p] for p in perm], [])
            results.add((winner,
                         frozenset(f.index.get_level_values("country")[0]
                                   for f in kept)))
        assert len(results) == 1, f"argument order changed the survivors: {results}"

    def test_all_frames_agree_is_a_noop(self):
        frames = [_country_frame("A", ["t", "i"]), _country_frame("B", ["t", "i"])]
        kept, dropped, winner = _select_kept_shape(frames, [])
        assert kept == frames and dropped == [] and winner is None

    def test_single_frame_is_a_noop(self):
        frames = [_country_frame("A", ["t", "i"])]
        kept, dropped, winner = _select_kept_shape(frames, CANONICAL)
        assert kept == frames and dropped == [] and winner is None


# ---------------------------------------------------------------------------
# End-to-end, with synthetic countries
# ---------------------------------------------------------------------------

class _FakeCountry:
    """Just enough of `Country` for `Feature.__call__`."""

    _FRAMES: dict[str, pd.DataFrame] = {}

    def __init__(self, name, trust_cache=False):
        self.name = name

    def crop_production(self, currency=None, **kwargs):
        return self._FRAMES[self.name].copy()

    def plot_labor(self, currency=None, **kwargs):
        return self._FRAMES[self.name].copy()

    def plot_inputs(self, currency=None, **kwargs):
        return self._FRAMES[self.name].copy()


class TestFeatureEndToEnd:
    def test_every_corpus_shape_is_kept_including_a_seven_level_malawi(self, monkeypatch):
        import lsms_library

        shapes = {
            # the real per-country index shapes, measured warm on the corpus
            "Benin":      ["i", "t", "v", "plot", "crop", "u"],
            "Ethiopia":   ["i", "t", "v", "plot_id", "j", "u"],
            "GhanaSPS":   ["i", "t", "v", "plot_id", "j", "u", "season"],
            "Malawi":     ["i", "t", "v", "plot", "crop", "u", "condition"],
            "Tanzania":   ["i", "t", "v", "plot_id", "j"],
            "Uganda":     ["i", "t", "v", "plot", "j", "u", "condition", "season"],
        }
        frames = {}
        for name, names in shapes.items():
            df = _frame(names,
                        [tuple(f"{name}{k}" for k in range(len(names)))])
            if "u" not in names:            # Tanzania carries `u` as a column
                df["u"] = "kg"
            frames[name] = df
        _FakeCountry._FRAMES = frames
        monkeypatch.setattr(lsms_library, "Country", _FakeCountry)

        f = Feature("crop_production")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = f(list(shapes), currency=None)

        assert list(out.index.names) == ["country"] + CANONICAL
        assert sorted(out.index.get_level_values("country").unique()) == sorted(shapes)
        assert len(out) == len(shapes)
        assert out.index.is_unique
        assert out.attrs["canonical_alignment"]["promoted"]["Tanzania"] == {"u": 0}

    def test_survivors_do_not_depend_on_argument_order(self, monkeypatch):
        import lsms_library

        shapes = {
            "Ethiopia": ["i", "t", "v", "plot_id", "j", "u"],
            "Malawi":   ["i", "t", "v", "plot", "crop", "u", "condition"],
            "Uganda":   ["i", "t", "v", "plot", "j", "u", "condition", "season"],
        }
        _FakeCountry._FRAMES = {
            name: _frame(names, [tuple(f"{name}{k}" for k in range(len(names)))])
            for name, names in shapes.items()}
        monkeypatch.setattr(lsms_library, "Country", _FakeCountry)

        seen = set()
        for perm in itertools.permutations(shapes):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                out = Feature("crop_production")(list(perm), currency=None)
            seen.add(frozenset(out.index.get_level_values("country").unique()))
        assert seen == {frozenset(shapes)}


# ---------------------------------------------------------------------------
# Data-gated: the real corpus, when it is warm
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
@pytest.mark.slow
def test_warm_corpus_assembly_loses_no_rows():
    """Every country kept by `Feature('crop_production')` contributes exactly
    the rows `Country(name).crop_production()` returns, and nothing is
    collapsed.  Skips cleanly when the caches are not warm."""
    import lsms_library as ll
    from lsms_library.country import grain_reports

    countries = ll.Feature("crop_production").countries
    built: dict[str, int] = {}
    for name in countries:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = ll.Country(name, trust_cache=True).crop_production(
                    currency="index")
        except Exception:
            continue
        if isinstance(df, pd.DataFrame) and not df.empty:
            built[name] = len(df)
    if len(built) < 3:
        pytest.skip("fewer than three crop_production caches are warm")

    before = len(grain_reports())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = ll.Feature("crop_production", trust_cache=True)(sorted(built))

    assert list(out.index.names)[:1] == ["country"]
    assert out.index.is_unique
    per_country = out.index.get_level_values("country").value_counts().to_dict()
    for name, n in per_country.items():
        assert n == built[name], (
            f"{name}: Feature() returned {n} rows, Country() returned {built[name]}")
    assert len(grain_reports()) == before, "the assembly collapsed a grain"


def test_unnamed_index_is_left_alone():
    """A frame that arrives with an unnamed level cannot be round-tripped
    through reset_index/set_index; it is left for the #325 collapse warning."""
    df = pd.DataFrame({"Quantity": [1.0]}, index=pd.Index(["h1"]))
    out = _align_to_canonical_levels(
        df, CANONICAL, _missing_level_sentinels("crop_production"),
        "X", "crop_production")
    assert out is df


# ---------------------------------------------------------------------------
# plot_labor / plot_inputs (GH #569, ruling @ligon 2026-10-18)
# ---------------------------------------------------------------------------
#
# "(1) `plot_labor` keeps `stage` and `worker` as index levels, with a sentinel
# where a country lacks them.  (2) The seven EHCVM `plot_inputs` tables with no
# plot level are registered with a household-grain sentinel on `plot_id`
# through `missing_level_sentinels`, not excluded."
#
# Before registration both tables fell through to the MODAL shape filter:
# Feature('plot_labor') excluded GhanaLSS, GhanaSPS and Uganda (265,806 rows),
# Feature('plot_inputs') kept only the seven EHCVM countries and excluded the
# other seven (676,635 rows), measured warm 2026-10-19.

PLOT_LABOR = ["t", "v", "i", "plot_id", "stage", "source", "worker", "season"]
PLOT_INPUTS = ["t", "v", "i", "plot_id", "input", "j", "u", "season"]

# The real per-country index shapes, as `Country(c).<table>()` serves them
# (measured warm on the corpus, 2026-10-19).  Countries listed together share a
# shape.  `u_column` marks the six plot_inputs countries that carry `u` as a
# COLUMN, which the assembly promotes.
LABOR_SHAPES = {
    # Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Malawi, Mali,
    # Niger, Nigeria, Senegal, Tanzania, Togo
    "Benin":    ["i", "t", "v", "plot_id", "source"],
    "GhanaLSS": ["i", "t", "v", "plot_id", "stage", "source", "worker"],
    "GhanaSPS": ["i", "t", "v", "plot_id", "season", "stage", "source"],
    "Uganda":   ["i", "t", "v", "plot_id", "source", "season"],
}
INPUT_SHAPES = {
    # the seven EHCVM countries: no plot level at all
    "Benin":    (["i", "t", "v", "input", "crop", "u"], False),
    # Ethiopia, GhanaLSS
    "Ethiopia": (["i", "t", "v", "plot_id", "input", "j"], True),
    "Malawi":   (["i", "t", "v", "plot_id", "input", "crop", "u"], False),
    # Mali, Nigeria, Tanzania
    "Mali":     (["i", "t", "v", "plot_id", "input", "crop"], True),
    "Uganda":   (["i", "t", "v", "plot_id", "input", "j", "season"], True),
}


class TestPlotTableDeclarations:
    def test_plot_labor_is_registered_with_stage_and_worker(self):
        assert _canonical_index_levels("plot_labor") == PLOT_LABOR

    def test_plot_inputs_is_registered_with_plot_id(self):
        assert _canonical_index_levels("plot_inputs") == PLOT_INPUTS

    @pytest.mark.parametrize("table", ["plot_labor", "plot_inputs"])
    def test_canonical_index_keeps_v(self, table):
        """Both tables are household-grain and v-joined today (neither was in
        index_info nor in `skip_extra`).  `_compute_no_v_join` exempts a table
        whose canonical index omits `v`, so a v-less registration would strip
        `v` from every Country(...) frame of that table."""
        from lsms_library.country import _no_v_join_tables
        assert "v" in _canonical_index_levels(table)
        assert table not in _no_v_join_tables()

    def test_aliases(self):
        assert _level_aliases("plot_inputs") == {"crop": "j"}
        # every plot_labor country already spells plot_id / source
        assert _level_aliases("plot_labor") == {}

    def test_sentinels(self):
        assert _missing_level_sentinels("plot_labor") == {
            "stage": "Farm-level", "worker": "Farm-level",
            "season": "unknown_season"}
        assert _missing_level_sentinels("plot_inputs") == {
            "plot_id": "Farm-level", "u": "Unknown",
            "season": "unknown_season"}

    def test_shared_axes_share_one_sentinel_spelling(self):
        """`season` and `u` are the same axes crop_production carries; a
        second spelling of 'not recorded' on a shared axis would make the plot
        tables unjoinable with crop_production on it."""
        crop = _missing_level_sentinels("crop_production")
        for table in ("plot_labor", "plot_inputs"):
            s = _missing_level_sentinels(table)
            for lvl in set(s) & set(crop):
                assert s[lvl] == crop[lvl], (table, lvl)

    def test_structural_sentinel_is_not_the_epistemic_one(self):
        """GhanaLSS/_/CONTENTS.org: `Farm-level` (the axis is not resolved --
        the quantity is the total over it) and `Unknown` (the axis exists but
        was not fielded) "must not be merged"."""
        structural = {("plot_labor", "stage"), ("plot_labor", "worker"),
                      ("plot_inputs", "plot_id")}
        for table, lvl in structural:
            assert _missing_level_sentinels(table)[lvl] == "Farm-level"
        assert _missing_level_sentinels("plot_inputs")["u"] == "Unknown"

    @pytest.mark.parametrize("table", ["plot_labor", "plot_inputs"])
    def test_does_not_use_the_pd_NA_fabrication(self, table):
        assert _fabricates_missing_levels(table) is False


def _harmonize(df, table, country="X"):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return _harmonize_country_frame(
            df, _canonical_index_levels(table), country, table, False,
            aliases=_level_aliases(table),
            sentinels=_missing_level_sentinels(table))


class TestPlotLaborHarmonize:
    @pytest.mark.parametrize("country", sorted(LABOR_SHAPES))
    def test_every_real_shape_reaches_the_canonical_index(self, country):
        names = LABOR_SHAPES[country]
        df = _frame(names, [tuple(f"x{k}" for k in range(len(names)))],
                    PersonDays=[3.0])
        out = _harmonize(df, "plot_labor", country)
        assert list(out.index.names) == PLOT_LABOR
        assert len(out) == 1
        flat = out.index.to_frame(index=False)
        assert flat.notna().all().all()
        for lvl, sentinel in _missing_level_sentinels("plot_labor").items():
            if lvl not in names:
                assert flat[lvl].tolist() == [sentinel], lvl

    @pytest.mark.parametrize("names,level,values", [
        # GhanaSPS: two rows differing ONLY on stage -- the 60,607-row point
        (LABOR_SHAPES["GhanaSPS"], "stage", ("planting", "harvesting")),
        # GhanaSPS: two rows differing only on season
        (LABOR_SHAPES["GhanaSPS"], "season", ("major", "minor")),
        # GhanaLSS: two rows differing only on worker
        (LABOR_SHAPES["GhanaLSS"], "worker", ("male", "female")),
        # Uganda: two rows differing only on season
        (LABOR_SHAPES["Uganda"], "season", ("A", "B")),
    ])
    def test_rows_differing_only_on_a_kept_level_are_not_collapsed(
            self, names, level, values):
        from lsms_library.country import grain_reports
        before = len(grain_reports())
        base = {n: f"{n}0" for n in names}
        rows = [tuple(v if n == level else base[n] for n in names)
                for v in values]
        df = _frame(names, rows, PersonDays=[2.0, 5.0])
        out = _harmonize(df, "plot_labor")
        assert len(out) == 2 and out.index.is_unique
        assert sorted(out.index.get_level_values(level)) == sorted(values)
        assert out["PersonDays"].sum() == 7.0
        assert len(grain_reports()) == before


class TestPlotInputsHarmonize:
    @pytest.mark.parametrize("country", sorted(INPUT_SHAPES))
    def test_every_real_shape_reaches_the_canonical_index(self, country):
        names, u_column = INPUT_SHAPES[country]
        cols = {"Quantity": [4.0]}
        if u_column:
            cols["u"] = ["Kg"]
        df = _frame(names, [tuple(f"x{k}" for k in range(len(names)))], **cols)
        out = _harmonize(df, "plot_inputs", country)
        assert list(out.index.names) == PLOT_INPUTS
        assert len(out) == 1
        assert out.index.to_frame(index=False).notna().all().all()
        if u_column:
            assert out.index.get_level_values("u").tolist() == ["Kg"]
            assert "u" not in out.columns

    def test_ehcvm_household_records_get_the_farm_level_plot_sentinel(self):
        """The ruling's household-grain sentinel: an EHCVM row is the
        household's farm, never an observed plot, and it says so on `plot_id`
        rather than carrying a NULL (deleted by the next groupby) or a
        fabricated plot number."""
        names = INPUT_SHAPES["Benin"][0]
        df = _frame(names,
                    [("h1", "2018-19", "c1", "Seed", "Maïs en grain", "Kg"),
                     ("h1", "2018-19", "c1", "Urea", "(not crop-specific)", "Kg"),
                     ("h2", "2018-19", "c1", "Seed", "Sorgho", "Kg")],
                    Quantity=[10.0, 50.0, 7.0])
        out = _harmonize(df, "plot_inputs", "Benin")
        assert len(out) == 3 and out.index.is_unique
        plot = out.index.get_level_values("plot_id")
        assert plot.notna().all()
        assert set(plot) == {"Farm-level"}
        # the crop axis is renamed, not re-valued
        assert sorted(out.index.get_level_values("j")) == sorted(
            ["Maïs en grain", "(not crop-specific)", "Sorgho"])
        assert out["Quantity"].sum() == 67.0

    def test_u_column_nulls_become_unknown_not_NA(self):
        """GhanaLSS / Ethiopia / Mali / Nigeria / Tanzania carry `u` as a
        column with nulls (8,763 / 37,370 / 42,089 / 6,742 / 408 rows)."""
        names = INPUT_SHAPES["Mali"][0]
        df = _frame(names,
                    [("h1", "2017-18", "c1", "1_1", "Seed", "Mil"),
                     ("h1", "2017-18", "c1", "1_1", "Urea", "Mil")],
                    Quantity=[1.0, 2.0], u=["Kg", None])
        out = _harmonize(df, "plot_inputs", "Mali")
        assert len(out) == 2
        assert sorted(out.index.get_level_values("u")) == ["Kg", "Unknown"]

    @pytest.mark.parametrize("names,level,values", [
        # Malawi / EHCVM: one (input, crop) reported in two units
        (INPUT_SHAPES["Malawi"][0], "u", ("Kilogramme", "Gram")),
        # Uganda: one per-crop seed row in each season
        (INPUT_SHAPES["Uganda"][0], "season", ("A", "B")),
    ])
    def test_rows_differing_only_on_a_kept_level_are_not_collapsed(
            self, names, level, values):
        from lsms_library.country import grain_reports
        before = len(grain_reports())
        base = {n: f"{n}0" for n in names}
        rows = [tuple(v if n == level else base[n] for n in names)
                for v in values]
        cols = {"Quantity": [1.0, 3.0]}
        if "u" not in names:
            cols["u"] = ["Kg", "Kg"]
        df = _frame(names, rows, **cols)
        out = _harmonize(df, "plot_inputs")
        assert len(out) == 2 and out.index.is_unique
        assert sorted(out.index.get_level_values(level)) == sorted(values)
        assert len(grain_reports()) == before


class TestPlotTablesEndToEnd:
    @pytest.mark.parametrize("table,shapes", [
        ("plot_labor", {c: (n, False) for c, n in LABOR_SHAPES.items()}),
        ("plot_inputs", INPUT_SHAPES),
    ])
    def test_every_corpus_shape_is_kept(self, monkeypatch, table, shapes):
        """Before GH #569 the modal shape won: plot_labor kept 1 of these 4
        shapes and plot_inputs 1 of 5."""
        import lsms_library

        frames = {}
        for name, (names, u_column) in shapes.items():
            df = _frame(names, [tuple(f"{name}{k}" for k in range(len(names)))])
            if u_column:
                df["u"] = "Kg"
            frames[name] = df
        _FakeCountry._FRAMES = frames
        monkeypatch.setattr(lsms_library, "Country", _FakeCountry)

        canonical = _canonical_index_levels(table)
        seen = set()
        for perm in itertools.permutations(shapes):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                out = Feature(table)(list(perm), currency=None)
            assert list(out.index.names) == ["country"] + canonical
            assert len(out) == len(shapes)
            assert out.index.is_unique
            seen.add(frozenset(out.index.get_level_values("country").unique()))
        assert seen == {frozenset(shapes)}
        report = out.attrs["canonical_alignment"]
        assert "promotion_collisions" not in report


def _warm_assembly(table):
    """Country() frame shape per warm country, and the Feature() assembly."""
    import lsms_library as ll
    from lsms_library.country import grain_reports

    built: dict[str, tuple[int, set[str]]] = {}
    for name in ll.Feature(table).countries:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = getattr(ll.Country(name, trust_cache=True), table)()
        except Exception:
            continue
        if isinstance(df, pd.DataFrame) and not df.empty:
            built[name] = (len(df), set(df.index.names) | set(df.columns))
    if len(built) < 3:
        pytest.skip(f"fewer than three {table} caches are warm")
    before = len(grain_reports())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = ll.Feature(table, trust_cache=True)(sorted(built))
    return built, out, len(grain_reports()) - before


@pytest.mark.requires_s3
@pytest.mark.slow
@pytest.mark.parametrize("table", ["plot_labor", "plot_inputs"])
def test_warm_corpus_plot_table_assembly_keeps_every_country_and_row(table):
    """Every warm country is KEPT and contributes exactly the rows
    Country(name).<table>() returns; nothing collapses; a level a country does
    not carry at all arrives as its declared sentinel on every row."""
    built, out, n_grain = _warm_assembly(table)
    canonical = _canonical_index_levels(table)
    assert list(out.index.names)[: len(canonical) + 1] == ["country"] + canonical
    assert out.index.is_unique
    per_country = out.index.get_level_values("country").value_counts().to_dict()
    assert set(per_country) == set(built), sorted(set(built) - set(per_country))
    for name, n in per_country.items():
        assert n == built[name][0], (
            f"{name}: Feature() returned {n} rows, Country() returned {built[name][0]}")
    assert n_grain == 0, "the assembly collapsed a grain"
    flat = out.index.to_frame(index=False)
    sentinels = _missing_level_sentinels(table)
    for name, (_, carried) in built.items():
        sub = flat[flat["country"] == name]
        for lvl, sentinel in sentinels.items():
            if lvl not in carried:
                assert (sub[lvl] == sentinel).all(), (name, lvl)
