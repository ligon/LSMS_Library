"""GH #772 -- Uganda's ``earnings`` panel: shape and column name, without data.

The country-level ``Uganda/_/earnings.py`` used to assemble its ``(t, i)``
panel by stacking each wave's frame, collecting the results in a
``pd.DataFrame(dict)`` and stacking again.  Three defects rode on that round
trip, and only the first was visible to the suite:

1. the measure came out named ``earnings`` where ``data_scheme.yml`` declares
   ``Earnings`` (and ``lsms_library.currency`` registers ``{"Earnings"}`` as
   this table's monetary column), with the stacked column axis left behind as
   a constant ``level_1`` column.  That is what
   ``test_table_structure.py`` reported;

2. **the panel was a cross product.**  Under pandas 3 ``DataFrame.stack`` no
   longer drops all-null rows, so the dict-of-Series alignment materialised
   every household id in the union against every wave: 129,160 rows for
   22,447 real household-wave observations, 82.6% of the table fabricated.
   No guard could see it -- nothing is destroyed (rows are *manufactured*),
   and ``Earnings`` is not 100% null in any ``t`` slice, so neither
   ``null_read_audit`` site fires and there is no "mostly null" sanity check;

3. ``.squeeze()`` returns a *scalar* for a length-1 Series, which
   ``pd.DataFrame(dict)`` then broadcasts across every row.

Defect 2 is why these tests exist.  Defect 1 is caught by the structural
tests, but only where the microdata is available to build the table; defect 2
had no coverage at all.  Both are pinned here against synthetic waves, so the
whole file runs on a checkout with no data access.

``income.py`` gets the same treatment: it combined its sources with a plain
``+``, which aligns on the *intersection*.  The phantom rows were hiding that
-- ``[cols].sum(axis=1)`` turns an all-NaN row into ``0.0``, so the
intersection was very nearly the union -- and removing them without also
fixing the alignment would have silently dropped the enterprise-only
households the phantoms had been standing in for.
"""
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


UGANDA_ = Path(__file__).resolve().parent.parent / "lsms_library" / "countries" / "Uganda" / "_"

WAVES = ["2005-06", "2009-10", "2010-11", "2011-12",
         "2013-14", "2015-16", "2018-19", "2019-20"]


def _load_uganda_module():
    """Import the real ``Uganda/_/uganda.py`` (for the real ``id_walk``)."""
    spec = importlib.util.spec_from_file_location("uganda", UGANDA_ / "uganda.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["uganda"] = mod          # the script does `from uganda import ...`
    spec.loader.exec_module(mod)
    return mod


def _run_script(name, sources, tmp_path, monkeypatch, *, updated_ids=None):
    """Execute a country-level script with its I/O boundary stubbed.

    Only ``get_dataframe`` / ``to_parquet`` are replaced; everything the
    script actually computes is the shipped code.
    """
    import lsms_library.local_tools as lt

    written = {}
    monkeypatch.setattr(lt, "get_dataframe",
                        lambda fn, *a, **k: sources[fn].copy())
    monkeypatch.setattr(lt, "to_parquet",
                        lambda df, fn, *a, **k: written.__setitem__(fn, df))

    monkeypatch.chdir(tmp_path)
    (tmp_path / "updated_ids.json").write_text(json.dumps(updated_ids or {}))

    src = (UGANDA_ / name).read_text()
    exec(compile(src, str(UGANDA_ / name), "exec"), {"__name__": "__main__"})
    return written


@pytest.fixture
def wave_earnings():
    """One frame per wave, overlapping ids -- the panel structure that matters."""
    rng = np.random.default_rng(772)
    ids = [f"hh{k:05d}" for k in range(2000)]
    frames = {}
    for n, t in enumerate(WAVES):
        hh = sorted(rng.choice(ids, size=300 + 20 * n, replace=False))
        s = pd.Series(rng.integers(0, 5000, len(hh)).astype(float),
                      index=pd.Index(hh, name="i"))
        frames[t] = pd.DataFrame({"Earnings": s})
    # One wave keeps the pre-#772 'j' label, so a stale wave parquet written
    # before the rename still builds.
    frames["2010-11"].index.name = "j"
    return frames


@pytest.fixture
def earnings_panel(wave_earnings, tmp_path, monkeypatch):
    _load_uganda_module()
    sources = {f"../{t}/_/earnings.parquet": f for t, f in wave_earnings.items()}
    written = _run_script("earnings.py", sources, tmp_path, monkeypatch)
    assert list(written) == ["../var/earnings.parquet"]
    return written["../var/earnings.parquet"]


class TestEarningsPanel:

    def test_declares_the_canonical_column_and_nothing_else(self, earnings_panel):
        """`Earnings`, per data_scheme.yml -- and no `level_1` residue."""
        assert list(earnings_panel.columns) == ["Earnings"]

    def test_index_is_t_i(self, earnings_panel):
        assert list(earnings_panel.index.names) == ["t", "i"]

    def test_no_cross_product(self, earnings_panel, wave_earnings):
        """THE regression: one row per real household-wave, not per (id x wave).

        With these fixtures the cross product would be ~8x larger; on the real
        data it was 129,160 against 22,447.
        """
        assert len(earnings_panel) == sum(len(f) for f in wave_earnings.values())

        n_ids = len(set().union(*(set(f.index) for f in wave_earnings.values())))
        assert len(earnings_panel) < n_ids * len(WAVES)

    def test_introduces_no_nulls(self, earnings_panel):
        """A phantom row is a NaN row; the wave inputs have none, so nor should this."""
        assert earnings_panel["Earnings"].notna().all()

    def test_every_wave_survives_with_its_own_values(self, earnings_panel, wave_earnings):
        assert list(earnings_panel.index.get_level_values("t").unique()) == WAVES
        for t, f in wave_earnings.items():
            got = earnings_panel.xs(t, level="t")["Earnings"].sort_index()
            want = f["Earnings"].copy()
            want.index = want.index.rename("i")
            assert got.equals(want.sort_index())

    def test_one_household_wave_does_not_broadcast(self, wave_earnings, tmp_path,
                                                   monkeypatch):
        """`.squeeze()` on a length-1 Series returns a scalar.  It must not be used."""
        _load_uganda_module()
        frames = dict(wave_earnings)
        frames["2019-20"] = frames["2019-20"].iloc[:1]
        sources = {f"../{t}/_/earnings.parquet": f for t, f in frames.items()}
        out = _run_script("earnings.py", sources, tmp_path, monkeypatch)
        panel = out["../var/earnings.parquet"]
        assert len(panel.xs("2019-20", level="t")) == 1
        assert len(panel) == sum(len(f) for f in frames.values())


class TestEnterpriseIncomePanel:
    """The same defect, in the sibling table that shares the idiom.

    ``enterprise_income`` had no naming defect, so nothing in the suite ever
    reported it, but its cross product was larger: 78,040 rows of which
    64,105 (82.1%) were entirely null, against 13,935 real observations.

    It mattered in normal use because ``income.py`` names this table as a
    Makefile prerequisite.  Building ``income`` regenerated
    ``var/enterprise_income.parquet`` through the country script, while a
    direct ``enterprise_income()`` call on a cache lacking it went through
    the framework's wave concatenation and returned 13,935 -- the same API
    call answering two ways depending on cache provenance.
    """

    def test_no_cross_product_and_no_null_rows(self, tmp_path, monkeypatch):
        _load_uganda_module()
        rng = np.random.default_rng(7)
        ids = [f"hh{k:05d}" for k in range(500)]
        cols = ["revenue", "wagebill", "materials", "otherexpense",
                "profits", "losses"]
        frames = {}
        for n, t in enumerate(WAVES):
            hh = sorted(rng.choice(ids, size=60 + 5 * n, replace=False))
            frames[t] = pd.DataFrame(
                rng.integers(0, 900, (len(hh), len(cols))).astype(float),
                index=pd.Index(hh, name="j"),   # wave scripts label it 'j'
                columns=cols,
            )
        sources = {f"../{t}/_/enterprise_income.parquet": f
                   for t, f in frames.items()}
        out = _run_script("enterprise_income.py", sources, tmp_path, monkeypatch)
        panel = out["../var/enterprise_income.parquet"]

        assert list(panel.index.names) == ["t", "i"]
        assert list(panel.columns) == cols
        assert len(panel) == sum(len(f) for f in frames.values())
        assert not panel.isna().all(axis=1).any(), "phantom all-null rows"


class TestIncomeAlignment:
    """`income` must not depend on one source padding the other."""

    def _run(self, earnings, enterprise, tmp_path, monkeypatch):
        sources = {"../var/earnings.parquet": earnings,
                   "../var/enterprise_income.parquet": enterprise}
        out = _run_script("income.py", sources, tmp_path, monkeypatch)
        return out["../var/income.parquet"]["income"]

    @staticmethod
    def _frame(col, rows):
        idx = pd.MultiIndex.from_tuples([(t, i) for t, i, _ in rows], names=["t", "i"])
        return pd.DataFrame({col: [v for _, _, v in rows]}, index=idx)

    def test_a_household_in_only_one_source_is_kept(self, tmp_path, monkeypatch):
        earnings = self._frame("Earnings", [("2005-06", "hhA", 100.0)])
        enterprise = self._frame("profits", [("2005-06", "hhA", 50.0),
                                             ("2005-06", "hhC", 7.0)])
        income = self._run(earnings, enterprise, tmp_path, monkeypatch)
        assert income.loc[("2005-06", "hhA")] == 150.0
        # hhC has no earnings row at all.  Under the old plain `+` it aligned
        # to NaN and was dropped -- invisibly, while the phantom rows supplied
        # a 0.0 for it.
        assert income.loc[("2005-06", "hhC")] == 7.0

    def test_all_null_and_all_zero_rows_are_still_dropped(self, tmp_path, monkeypatch):
        earnings = self._frame("Earnings", [("2005-06", "hhB", 0.0)])
        enterprise = self._frame("profits", [("2005-06", "hhD", np.nan)])
        income = self._run(earnings, enterprise, tmp_path, monkeypatch)
        assert income.empty
