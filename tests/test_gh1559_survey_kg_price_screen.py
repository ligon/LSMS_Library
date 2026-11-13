"""GH #1559: a survey kilogram per unit against the mass its sale prices imply.

Ruling @ligon 2026-11-12, "GH1559 price-implied kg per unit for crops", "Also
build a rung", part (1): a REPORT-ONLY screen naming a survey kilogram per unit
(``Quantity_kg / Quantity``) outside 0.1x-10x of the price-implied kilograms per
unit, against a baseline of kg sales plus sacks at their stated mass.  It is in
the Site Q family (``quantity_audit``): the same ledger, accessor and strict
lever, its own warning subclass, and no cache hash.

The synthetic frames are Niger's shape: index ``(t, i, plot_id, crop, u,
condition)``, the harvest unit on ``u`` and the sale's own unit in ``Unit_sold``.
What the screen names on the real data is pinned at the bottom, cache-gated.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import _build_registry as R
from lsms_library import quantity_audit as Q
from lsms_library.quantity_audit import (
    SURVEY_KG_PRICE_BAND,
    QuantityImplausibleError,
    QuantityImplausibleWarning,
    SurveyKgPriceWarning,
    audit_survey_kg_prices,
    check_quantities,
    quantity_reports,
    survey_kg_price_factors,
)

INDEX = ["t", "i", "plot_id", "crop", "u", "condition"]
SITE = "survey-kg-price"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("LSMS_QUANTITY_STRICT", raising=False)
    Q._clear_quantity_reports()
    yield
    Q._clear_quantity_reports()


def _row(i, u, *, t="2021-22", crop="Mil", condition="unknown_condition",
         q=np.nan, kg=np.nan, sold=np.nan, value=np.nan, unit_sold=None,
         state=None, plot="1_1"):
    return {"t": t, "i": i, "plot_id": plot, "crop": crop, "u": u,
            "condition": condition, "Quantity": q, "Quantity_kg": kg,
            "Quantity_sold": sold, "Value_sold": value, "Unit_sold": unit_sold,
            "Condition_sold": state}


def _frame(rows):
    df = pd.DataFrame(rows).set_index(INDEX)
    for c in ("Quantity", "Quantity_kg", "Quantity_sold", "Value_sold"):
        df[c] = df[c].astype("float64")
    return df


def _market(t="2021-22", crop="Mil", *, kg_price=200.0, tiya_price=500.0,
            n_kg=10, n_sack=6, n_tiya=10, state=None, prefix="m"):
    """Sales: ``n_kg`` in Kg and ``n_sack`` in 100 kg sacks at ``kg_price`` per
    kilogram, ``n_tiya`` in Tiya at ``tiya_price`` per tiya (so 2.5 kg a tiya at
    the defaults).  Sale rows carry no survey kilograms of their own."""
    rows = []
    for k in range(n_kg):
        p = kg_price * (1 + 0.01 * (k - n_kg / 2))
        rows.append(_row(f"{prefix}kg{k}", "Kg", t=t, crop=crop, q=10.0, sold=10.0,
                         value=10.0 * p, unit_sold="Kg", state=state))
    for k in range(n_sack):
        rows.append(_row(f"{prefix}sac{k}", "Sac de 100 kg", t=t, crop=crop, q=2.0,
                         sold=2.0, value=2.0 * 100 * kg_price, unit_sold="Sac de 100 kg",
                         state=state))
    for k in range(n_tiya):
        p = tiya_price * (1 + 0.01 * (k - n_tiya / 2))
        rows.append(_row(f"{prefix}ti{k}", "Tiya", t=t, crop=crop, q=10.0, sold=10.0,
                         value=10.0 * p, unit_sold="Tiya", state=state))
    return rows


def _harvests(t="2021-22", crop="Mil", *, n=20, kg_per=2.5, condition="unknown_condition",
              prefix="h", extra=()):
    rows = [_row(f"{prefix}{k}", "Tiya", t=t, crop=crop, condition=condition, q=10.0,
                 kg=10.0 * kg_per) for k in range(n)]
    rows.extend(extra)
    return rows


def _site(reports=None):
    reports = quantity_reports() if reports is None else reports
    return [r for r in reports if r.get("site") == SITE]


def _named(reports):
    return sorted((r["wave"], r["direction"], o["i"], o["value"])
                  for r in reports for o in r["offenders"])


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------

def test_names_a_high_and_a_low_and_changes_nothing():
    df = _frame(_market() + _harvests(extra=[
        _row("big", "Tiya", q=10.0, kg=500.0),     # 50 kg a tiya: 20x
        _row("small", "Tiya", q=10.0, kg=2.0),     # 0.2 kg a tiya: 1/12.5
    ]))
    before = df.copy()
    with pytest.warns(SurveyKgPriceWarning):
        out = check_quantities(df, country="Testland", table="crop_production")
    assert out is df
    pd.testing.assert_frame_equal(df, before)
    reports = _site(quantity_reports(country="Testland", table="crop_production"))
    assert _named(reports) == [("2021-22", "high", "big", 50.0), ("2021-22", "low", "small", 0.2)]
    for r in reports:
        assert r["column"] == "kg_survey" and r["definition"] == "Quantity_kg / Quantity"
        assert r["band"] == [0.1, 10.0] and r["table"] == "crop_production"
        assert r["n_judged"] == 22 and r["rows"] == len(df)
        o = r["offenders"][0]
        assert o["implied"] == pytest.approx(2.5, rel=0.02)
        assert o["n_unit_sales"] == 10 and o["n_kg_sales"] == 16
        assert o["rung"] == "t,j,u" and o["u"] == "Tiya"
    high = next(r for r in reports if r["direction"] == "high")["offenders"][0]
    low = next(r for r in reports if r["direction"] == "low")["offenders"][0]
    assert high["ratio"] == pytest.approx(20.0, rel=0.02)
    assert low["ratio"] == pytest.approx(12.5, rel=0.02)
    assert high["quantity_kg"] == 500.0 and high["quantity"] == 10.0


def test_ordinary_rows_and_the_systematic_offset_are_silent():
    """Survey kilograms 1.4x the price-implied mass, the measured upper end of
    the offset, are well inside the band."""
    df = _frame(_market() + _harvests(kg_per=3.5))
    assert audit_survey_kg_prices(df, country="X") == []
    factors = survey_kg_price_factors(df)
    judged = factors["survey_over_implied"].dropna()
    assert len(judged) == 20 and judged.between(1.3, 1.5).all()


def test_the_warning_is_site_q_s_and_the_lever_is_site_q_s(monkeypatch):
    assert issubclass(SurveyKgPriceWarning, QuantityImplausibleWarning)
    df = _frame(_market() + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", "1")
    with pytest.raises(QuantityImplausibleError, match="price"):
        check_quantities(df, country="X", table="crop_production")


def test_each_screen_has_its_own_guard(monkeypatch):
    """A failure in the distributional audit must not skip the price screen, and a
    failure in the price screen must not silence the distributional one (red team F6).
    Either way the frame comes back unchanged and the failure is announced."""
    df = _frame(_market() + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))

    def boom(*_a, **_k):
        raise RuntimeError("boom")

    monkeypatch.setattr(Q, "audit_quantities", boom)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert check_quantities(df, country="X", table="crop_production") is df
    assert any("quantity plausibility audit could not run" in str(w.message) for w in caught)
    assert _named(_site()) == [("2021-22", "high", "big", 50.0)]
    monkeypatch.undo()

    Q._clear_quantity_reports()
    seen = []
    real = Q.audit_quantities
    monkeypatch.setattr(Q, "audit_quantities",
                        lambda *a, **k: seen.append(a[1]) or real(*a, **k))
    monkeypatch.setattr(Q, "survey_kg_price_factors", boom)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert check_quantities(df, country="X", table="crop_production") is df
    assert any("price screen could not run" in str(w.message) for w in caught)
    assert "Quantity_kg" in seen and _site() == []


def test_band_is_the_label_screen_s_band():
    from lsms_library.transformations import SURVEY_KG_LABEL_BAND
    assert tuple(SURVEY_KG_PRICE_BAND) == tuple(SURVEY_KG_LABEL_BAND) == (0.1, 10.0)


def test_only_crop_production_is_screened():
    df = _frame(_market() + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    assert audit_survey_kg_prices(df, country="X", table="plot_inputs") == []
    assert audit_survey_kg_prices(df, country="X", table="crop_production")


def test_a_frame_without_survey_kilograms_is_not_judged():
    df = _frame(_market() + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    assert survey_kg_price_factors(df.drop(columns="Quantity_kg")) is None
    assert audit_survey_kg_prices(df.drop(columns="Quantity_kg"), country="X") == []
    blank = df.assign(Quantity_kg=np.nan)
    assert survey_kg_price_factors(blank) is None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        check_quantities(blank, country="X", table="crop_production")
    assert not [w for w in caught if issubclass(w.category, SurveyKgPriceWarning)]


# ---------------------------------------------------------------------------
# What is judged, and against what
# ---------------------------------------------------------------------------

def test_a_unit_stating_its_mass_is_never_judged_here():
    """A sack row 1 kg per 100 kg sack is the LABEL screen's (kg_survey_off_label),
    not this one's: a stated-mass unit is in the baseline and gets no implied mass,
    so the two screens never name the same row."""
    df = _frame(_market() + _harvests(extra=[_row("sack", "Sac de 100 kg", q=4.0, kg=4.0)]))
    factors = survey_kg_price_factors(df)
    sack = factors.xs("sack", level="i")
    assert sack["kg_survey"].isna().all() and sack["kg_price_implied"].isna().all()
    assert audit_survey_kg_prices(df, country="X") == []


def test_the_baseline_reads_sacks_at_their_stated_mass():
    df = _frame(_market(n_kg=0, n_sack=6)
                + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    (report,) = audit_survey_kg_prices(df, country="X")
    o = report["offenders"][0]
    assert o["n_kg_sales"] == 6 and o["implied"] == pytest.approx(2.5, rel=0.02)


def test_a_volume_is_not_a_mass():
    """Litre sales enter no per-kg baseline (food ladder's _BASELINE_UNIT_CONVERSION
    has no litre either), so a market priced only in litres judges nothing."""
    rows = []
    for k in range(10):
        rows.append(_row(f"l{k}", "Litre", q=10.0, sold=10.0, value=2000.0, unit_sold="Litre"))
    rows += _market(n_kg=0, n_sack=0)
    df = _frame(rows + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    factors = survey_kg_price_factors(df)
    assert factors["kg_price_implied"].isna().all()
    assert audit_survey_kg_prices(df, country="X") == []


@pytest.mark.parametrize("n_kg, n_sack, n_tiya", [(4, 0, 10), (10, 0, 4)])
def test_thin_cells_are_silent(n_kg, n_sack, n_tiya):
    df = _frame(_market(n_kg=n_kg, n_sack=n_sack, n_tiya=n_tiya)
                + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    assert survey_kg_price_factors(df)["kg_price_implied"].isna().all()
    assert audit_survey_kg_prices(df, country="X") == []


def test_a_dispersed_unit_price_is_silent():
    rows = _market(n_tiya=0)
    for k, p in enumerate([20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000]):
        rows.append(_row(f"ti{k}", "Tiya", q=10.0, sold=1.0, value=float(p), unit_sold="Tiya"))
    df = _frame(rows + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    assert survey_kg_price_factors(df)["kg_price_implied"].isna().all()


def test_a_dispersed_baseline_is_silent():
    rows = _market(n_kg=0, n_sack=0)
    for k, p in enumerate([2, 5, 10, 50, 100, 200, 500, 1000, 2000, 5000]):
        rows.append(_row(f"kg{k}", "Kg", q=10.0, sold=1.0, value=float(p), unit_sold="Kg"))
    df = _frame(rows + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    assert survey_kg_price_factors(df)["kg_price_implied"].isna().all()


def test_a_wave_s_prices_never_judge_another_wave():
    df = _frame(_market(t="2014-15")
                + _harvests(t="2021-22", extra=[_row("big", "Tiya", t="2021-22", q=10.0, kg=500.0)]))
    assert survey_kg_price_factors(df)["kg_price_implied"].isna().all()
    assert audit_survey_kg_prices(df, country="X") == []


def test_another_crop_s_prices_never_judge():
    df = _frame(_market(crop="Sorgho")
                + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    assert audit_survey_kg_prices(df, country="X") == []


def test_the_sale_s_own_unit_prices_it():
    """A Tiya sale on a Botte harvest row is a Tiya price (GH #824)."""
    rows = _market(n_tiya=0)
    for k in range(10):
        rows.append(_row(f"ti{k}", "Botte", q=3.0, sold=10.0, value=5000.0, unit_sold="Tiya"))
    df = _frame(rows + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    (report,) = audit_survey_kg_prices(df, country="X")
    assert report["offenders"][0]["n_unit_sales"] == 10


def test_units_with_no_unit_are_neither_judged_nor_prices():
    rows = _market()
    for k in range(10):
        rows.append(_row(f"unk{k}", "Unknown", q=1.0, sold=1.0, value=1.0, unit_sold="Unknown"))
    rows += _harvests(extra=[_row("unknown", "Unknown", q=10.0, kg=5000.0),
                             _row("value", "Value", q=10.0, kg=5000.0)])
    df = _frame(rows)
    factors = survey_kg_price_factors(df)
    for i in ("unknown", "value"):
        assert factors.xs(i, level="i")["kg_survey"].isna().all()
    assert audit_survey_kg_prices(df, country="X") == []


def test_the_survey_s_sold_kilograms_are_not_read():
    """Quantity_sold_kg is the survey's own arithmetic on most sales (Niger 2021-22:
    939 of 1,134 equal the harvest's kg per unit), so it must not judge the survey."""
    df = _frame(_market() + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    a = survey_kg_price_factors(df)
    b = survey_kg_price_factors(df.assign(Quantity_sold_kg=1e9))
    pd.testing.assert_frame_equal(a, b)


def test_the_state_rung_comes_first_and_falls_back():
    """Shelled groundnut sells at 2.4x unshelled per kilogram.  Rows whose harvest
    state matches a priced sale state are judged on that state's cell; a state with
    too few sales falls back to the state-blind cell."""
    rows = (_market(crop="Arachide", kg_price=400.0, tiya_price=1000.0, state="shelled",
                    prefix="s")
            + _market(crop="Arachide", kg_price=160.0, tiya_price=400.0, state="unshelled",
                      prefix="u", n_tiya=3))
    rows += _harvests(crop="Arachide", condition="shelled", n=5, prefix="hs")
    rows += _harvests(crop="Arachide", condition="unshelled", n=5, prefix="hu")
    df = _frame(rows)
    factors = survey_kg_price_factors(df).reset_index()
    shelled = factors[factors["i"].str.startswith("hs")]
    unshelled = factors[factors["i"].str.startswith("hu")]
    assert (shelled["kg_price_rung"] == "t,j,state,u").all()
    assert shelled["kg_price_implied"].between(2.45, 2.55).all()
    assert (unshelled["kg_price_rung"] == "t,j,u").all()
    assert unshelled["kg_price_n_unit"].eq(13).all()


def test_a_slice_of_waves_names_the_same_rows():
    df = _frame(_market(t="2014-15") + _harvests(t="2014-15", extra=[
                    _row("big14", "Tiya", t="2014-15", q=10.0, kg=500.0)])
                + _market(t="2021-22") + _harvests(t="2021-22", extra=[
                    _row("big21", "Tiya", t="2021-22", q=10.0, kg=500.0)]))
    whole = [r for r in audit_survey_kg_prices(df, country="X") if r["wave"] == "2014-15"]
    part = audit_survey_kg_prices(df.xs("2014-15", level="t", drop_level=False), country="X")
    assert _named(whole) == _named(part) == [("2014-15", "high", "big14", 50.0)]


def test_a_feature_frame_keeps_countries_apart():
    a = _frame(_market() + _harvests(extra=[_row("big", "Tiya", q=10.0, kg=500.0)]))
    b = _frame(_market(tiya_price=5000.0) + _harvests(kg_per=25.0))
    pooled = pd.concat({"Alpha": a, "Beta": b}, names=["country"])
    reports = audit_survey_kg_prices(pooled, country="pooled")
    assert [(r["country"], o["i"]) for r in reports for o in r["offenders"]] == [("Alpha", "big")]
    assert reports[0]["rows"] == len(a)


# ---------------------------------------------------------------------------
# The cache: report-only code on the read path, in no build fingerprint
# ---------------------------------------------------------------------------

_ENTRY_POINTS = ("lsms_library.quantity_audit.audit_survey_kg_prices",
                 "lsms_library.quantity_audit.survey_kg_price_factors",
                 "lsms_library.quantity_audit.check_quantities")
_TOKEN = "SURVEY_KG_PRICE_MAX_SPREAD"


def _a_future_build_callable(df):
    """A module-level build-path caller of the screen, the call site the exclusion
    defends against (see test_quantity_screen's twin)."""
    return audit_survey_kg_prices(df, country="X")


def test_the_entry_points_are_excluded():
    for name in _ENTRY_POINTS:
        assert name in R._EXCLUDED_CALLABLES, name
    assert "_QUANTITY_LEDGER" in R._EXCLUDED_CONSTANTS


def test_the_exclusion_keeps_the_screen_out_and_this_test_can_fail(monkeypatch):
    with_exclusion = R._closure_parts(_a_future_build_callable, set())
    assert not [p for p in with_exclusion if _TOKEN in p]
    monkeypatch.setattr(R, "_EXCLUDED_CALLABLES",
                        R._EXCLUDED_CALLABLES - set(_ENTRY_POINTS))
    without = R._closure_parts(_a_future_build_callable, set())
    assert [p for p in without if _TOKEN in p], "the probe is vacuous"


def test_no_shipped_fingerprint_contains_the_screen():
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    assert parts
    assert not [p.split("=")[0] for p in parts if _TOKEN in p or "survey_kg_price" in p]


# ---------------------------------------------------------------------------
# The real data, cache-gated
# ---------------------------------------------------------------------------

def _built(country):
    import lsms_library as ll
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country(country).crop_production()
    return df, caught


@pytest.fixture(scope="module")
def niger():
    Q._clear_quantity_reports()
    df, caught = _built("Niger")
    return df, caught, _site(quantity_reports(country="Niger", table="crop_production"))


@pytest.fixture(scope="module")
def mali():
    Q._clear_quantity_reports()
    df, caught = _built("Mali")
    return df, caught, _site(quantity_reports(country="Mali", table="crop_production"))


#: (wave, direction) -> rows named on a cold build of this branch (burst job
#: 41275992, development f138a3d61 plus this screen).  A movement here is a data
#: change to explain, not a pin to bump.  Niger 2018-19 asks no kilograms, so it
#: has nothing to judge; Mali has 70 survey-kg rows in units stating no mass, and
#: one of them has a price-implied mass (no firing).
NIGER_FIRING = {("2011-12", "high"): 6, ("2011-12", "low"): 6,
                ("2014-15", "high"): 89, ("2014-15", "low"): 26,
                ("2021-22", "high"): 28, ("2021-22", "low"): 2}
NIGER_JUDGED = {"2011-12": 1932, "2014-15": 6394, "2021-22": 9020}
MALI_FIRING = {}
MALI_JUDGED = 1


def _count(reports):
    out = {}
    for r in reports:
        out[(r["wave"], r["direction"])] = r["n_implausible"]
    return out


@pytest.mark.requires_s3
class TestNiger:
    def test_the_screen_ran(self, niger):
        _, caught, _ = niger
        assert not [w for w in caught if "could not run" in str(w.message)]

    def test_it_names_rows_and_serves_them_as_recorded(self, niger):
        df, caught, reports = niger
        assert reports, "the screen named nothing on Niger"
        assert [w for w in caught if issubclass(w.category, SurveyKgPriceWarning)]
        served = df["Quantity_kg"].astype("float64")
        for r in reports:
            for o in r["offenders"]:
                assert (served == o["quantity_kg"]).any()
                assert o["u"] not in ("Kg", "Sac de 50 kg", "Sac de 100 kg", "Unknown")

    def test_the_firing_is_pinned(self, niger):
        assert _count(niger[2]) == NIGER_FIRING
        assert {r["wave"]: r["n_judged"] for r in niger[2]} == NIGER_JUDGED

    def test_the_ledger_names_what_the_served_frame_flags(self, niger):
        df, _, reports = niger
        factors = survey_kg_price_factors(df)
        flagged = factors["kg_price_flag"].notna().to_numpy()
        assert int(flagged.sum()) == sum(r["n_implausible"] for r in reports)

    def test_no_row_is_named_by_both_screens(self, niger):
        from lsms_library.transformations import harvest_kg_factors
        df, _, _ = niger
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            off_label = harvest_kg_factors(df)["kg_survey_off_label"].to_numpy(dtype=bool)
        flagged = survey_kg_price_factors(df)["kg_price_flag"].notna().to_numpy()
        assert off_label.any() and flagged.any()
        assert not (off_label & flagged).any()

    def test_a_one_wave_read_names_the_same_rows(self, niger):
        import lsms_library as ll
        _, _, reports = niger
        Q._clear_quantity_reports()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ll.Country("Niger").crop_production(waves=["2014-15"])
        part = _site(quantity_reports(country="Niger", table="crop_production"))
        whole = [r for r in reports if r["wave"] == "2014-15"]
        key = lambda rs: sorted((r["direction"], o["i"], o["plot"], o["j"], o["u"], o["value"])
                                for r in rs for o in r["offenders"])
        assert key(part) == key(whole)


@pytest.mark.requires_s3
class TestMali:
    def test_the_screen_ran(self, mali):
        _, caught, _ = mali
        assert not [w for w in caught if "could not run" in str(w.message)]

    def test_the_firing_is_pinned(self, mali):
        assert _count(mali[2]) == MALI_FIRING

    def test_one_row_is_judged(self, mali):
        df, _, _ = mali
        factors = survey_kg_price_factors(df)
        judged = factors["kg_price_implied"].notna() & factors["kg_survey"].notna()
        assert int(judged.sum()) == MALI_JUDGED
