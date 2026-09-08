"""Shared helpers for the five-source food-price comparison (PROTOCOL.org).

Every country agent imports these so the per-country outputs pool.  numpy,
pandas, plus ``metrics_miscellany`` / ``datamat`` from the side directory
named in ``_PYEXTRA`` (statsmodels is deliberately not used).  Self-test:

    PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra python slurm_logs/price_sources/common.py

Conventions
-----------
* A *price frame* is a DataFrame with a positive ``price`` column, the keys
  ``t``, ``j``, ``u`` and the geography columns named in ``geo_levels``
  (finest first), all as plain columns (reset the index first).
* ``pair`` strings are ``"<a>_vs_<b>"`` and ``gap_log = log p_a - log p_b``;
  ``a`` is the downstream (market-side) price so a marketing margin is
  positive.
"""
from __future__ import annotations

import math
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

SOURCES = ("unit_value", "reported_purchase_price", "own_consumption_valuation",
           "crop_sale_price", "community_price")

CELL_COLS = ["country", "t", "source", "geo_level", "geo", "j", "u", "n",
             "median_log_price", "sd_log_price", "median_price", "currency"]
GAP_COLS = ["country", "t", "pair", "geo_level", "geo", "j", "u", "n_a", "n_b",
            "median_log_a", "median_log_b", "gap_log", "ratio"]
MODEL_COLS = ["country", "t", "pair", "u_basis", "geo_level", "threshold_a",
              "threshold_b", "n_cells", "n_items",
              "n_geos", "median_gap", "iqr_gap", "share_abs_gap_gt_log1p5",
              "coef_within", "se_within", "coef_between", "se_between",
              "r2_fe", "resid_sd", "claim_30_50"]

CLAIM_LO, CLAIM_HI = math.log(1.3), math.log(1.5)

#: Residual degrees of freedom below which a fixed-effects slope is reported as
#: NaN instead of a number (see _fwl_slope).
MIN_DOF = 5


# ---------------------------------------------------------------------------
# geography
# ---------------------------------------------------------------------------

def attach_geo(df, country, geo_cols=("District", "Region")):
    """Join ``cluster_features`` geography onto a frame keyed by ``(t, v)``.

    ``df`` must carry ``t`` and ``v`` as columns.  Returns ``(out, n_missing)``
    where ``n_missing`` is the number of rows whose ``(t, v)`` has no
    ``cluster_features`` row -- report it, do not drop silently.  Only the
    geo columns the country actually has are attached.
    """
    cf = country.cluster_features().reset_index()
    cf["t"] = cf["t"].astype(str)
    cf["v"] = cf["v"].astype(str)
    have = [c for c in geo_cols if c in cf.columns]
    cf = cf[["t", "v"] + have].drop_duplicates(["t", "v"])
    out = df.copy()
    out["t"] = out["t"].astype(str)
    out["v"] = out["v"].astype(str)
    out = out.merge(cf, on=["t", "v"], how="left", indicator="_geo")
    n_missing = int((out["_geo"] == "left_only").sum())
    out = out.drop(columns="_geo")
    return out, n_missing


def geo_ladder(df, prefer=("v", "District", "Region")):
    """The geography levels present in ``df``, finest first."""
    return [g for g in prefer if g in df.columns]


# ---------------------------------------------------------------------------
# cells
# ---------------------------------------------------------------------------

def cell_medians(prices, geo_levels, *, keys=("t", "j", "u"), price="price",
                 threshold=1):
    """Median log price per ``(keys, geo_level, geo)`` at EVERY ladder level.

    Rows with a non-positive or missing price are excluded.  A ``national``
    level (``geo = 'all'``) is always appended.  Cells with fewer than
    ``threshold`` observations are dropped (default keeps all; the pairing
    step applies the real threshold so it is recorded in ``models.csv``).
    """
    p = pd.to_numeric(prices[price], errors="coerce")
    ok = p.notna() & (p > 0)
    base = prices.loc[ok, list(keys)].copy()
    base["_lp"] = np.log(p[ok].to_numpy(dtype=float))
    frames = []
    for level in list(geo_levels) + [None]:
        g = base.copy()
        if level is None:
            g["geo_level"], g["geo"] = "national", "all"
        else:
            g["geo_level"] = level
            g["geo"] = prices.loc[ok, level].astype(str).to_numpy()
        agg = (g.groupby(list(keys) + ["geo_level", "geo"], observed=True)["_lp"]
                .agg(n="size", median_log_price="median", sd_log_price="std"))
        frames.append(agg.reset_index())
    out = pd.concat(frames, ignore_index=True)
    out["median_price"] = np.exp(out["median_log_price"])
    out = out[out["n"] >= threshold]
    return out


def match_gaps(cells_a, cells_b, *, threshold_a=10, threshold_b=10, finest_only=True,
               order=("v", "District", "Region", "national")):
    """Pair two sources' cells on ``(t, j, u, geo_level, geo)``.

    Keeps pairs where ``n_a >= threshold_a`` and ``n_b >= threshold_b``.  The
    thresholds are PER SIDE because the sources differ in kind: a household
    unit value is a sample statistic (10 households, lowerable to 5), while a
    community price is a *measurement* -- one surveyed price per cluster and
    item (three vendor ``obs`` in GhanaLSS) -- so it is usable at ``n >= 1``.
    A symmetric 10 would push every community-price pair up to District or
    Region and lose the cluster-grain comparison this exercise is about.
    With ``finest_only`` each ``(t, j, u)`` keeps only the finest level at
    which it has any usable pair.
    """
    on = ["t", "j", "u", "geo_level", "geo"]
    m = cells_a.merge(cells_b, on=on, suffixes=("_a", "_b"))
    m = m[(m["n_a"] >= threshold_a) & (m["n_b"] >= threshold_b)].copy()
    if m.empty:
        return m.assign(gap_log=pd.Series(dtype=float), ratio=pd.Series(dtype=float))
    m["gap_log"] = m["median_log_price_a"] - m["median_log_price_b"]
    m["ratio"] = np.exp(m["gap_log"])
    if finest_only:
        rank = {lvl: k for k, lvl in enumerate(order)}
        m["_rank"] = m["geo_level"].map(rank).fillna(len(order))
        best = m.groupby(["t", "j", "u"], observed=True)["_rank"].transform("min")
        m = m[m["_rank"] == best].drop(columns="_rank")
    return m.rename(columns={"median_log_price_a": "median_log_a",
                             "median_log_price_b": "median_log_b"})


# ---------------------------------------------------------------------------
# the model: Frisch-Waugh-Lovell (Metrics_Miscellany), Bland-Altman regressor
# ---------------------------------------------------------------------------
# The fixed effects are partialled out exactly with Metrics_Miscellany's
# ``fwl_regression`` (github.com/ligon/Metrics_Miscellany, metrics_miscellany.org
# "Frisch-Waugh-Lovell Regression"): indicator blocks from ``utils.dummies``,
# residualise y and x on them, then ``estimators.ols`` with HC3 on the
# residuals.  The two packages (plus DataMat) live in a side directory, not the
# read-only venv image:  PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra
# (installed 2026-09-08; Metrics_Miscellany issue #4 records why a plain
# ``pip install git+`` does not work).
#
# Two things the first synthetic test taught (see the self-test below):
# 1. Do NOT regress ``log a - log b`` on ``log b``.  Noise in b sits in both,
#    and once the fixed effects absorb the true price variation that shared
#    noise is most of what is left -- a pure 35% markup came out with
#    beta = -0.11 (se 0.03).  Bland & Altman's remedy: regress the difference
#    on the MIDPOINT ``(log a + log b)/2``; with comparable noise in a and b
#    the spurious covariance cancels.
# 2. An additive per-unit cost shows up mainly BETWEEN items -- a fixed
#    handling cost is a big log gap on a cheap good and a small one on a dear
#    good -- and item fixed effects absorb exactly that.  So two
#    specifications: ``within`` (item + geo FE; identifies off spatial price
#    variation within an item) and ``between`` (geo FE only; the regressor is
#    the item's mean midpoint across its cells).
# 3. KNOWN LIMITATION of ``within``: the midpoint trick cancels the shared
#    noise only when a and b are equally noisy.  With unequal noise the
#    within slope has the sign of var(noise_a) - var(noise_b) under a pure
#    markup (synthetic, a slightly noisier: +0.08, se 0.03; a THIN b of two
#    observations per cell: -1.27, se 0.07, while between stays at -0.002).
#    ``between`` averages cell noise away over an item's many cells and is
#    the PRIMARY test; ``within`` is uninterpretable for a pair whose one
#    side is a measurement source at cluster grain (community prices, crop
#    sales at v).  Read it only where both sides are household samples with
#    >= 10 obs per cell, beside their ``sd_log_price`` in cells.csv.

_PYEXTRA = "/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra"
try:
    import datamat as dm
    from metrics_miscellany.estimators import fwl_regression, ols
    from metrics_miscellany.utils import dummies
except ImportError as exc:  # pragma: no cover - environment
    raise ImportError(
        "common.py needs metrics_miscellany and datamat, which are installed "
        f"outside the venv image.  Run with PYTHONPATH={_PYEXTRA} (colon-append "
        "your worktree path if you also need one)."
    ) from exc


def _fwl_slope(y, x, fe_blocks):
    """FWL slope of ``y`` on ``x`` after partialling out the effect blocks.

    ``y``, ``x``: 1-column DataFrames on a common index; ``fe_blocks``: list of
    indicator DataFrames (``dummies`` output) concatenated into one block.
    Returns ``(beta, se_hc3, resid_sd, y_resid_on_fe)``.  ``fwl_regression``
    pops the LAST dict entry as the regressor block, so the effects go last:
    first pass residualises y and x on them, second regresses y on x.
    """
    FE = dm.DataMat(pd.concat(fe_blocks, axis=1).astype(float))
    FE.columns = [f"fe{i}" for i in range(FE.shape[1])]
    # Identification guard (Nigeria, 2026-09-08: 17 cells against 16 geo
    # effects "fit" with r2 = 1.000000 and se = 9e-15; a cluster-grain pair
    # where every cell is its own geo is singular outright and raised
    # LinAlgError).  Effects consume rank; require MIN_DOF residual degrees
    # of freedom or report NaN -- a number here would be a lie.
    n_eff = int(np.linalg.matrix_rank(np.asarray(FE, dtype=float)))
    if len(y) - n_eff - 1 < MIN_DOF:
        return np.nan, np.nan, np.nan, None
    D = {"y": dm.DataMat(y), "x": dm.DataMat(x), "FE": FE}
    try:
        U, B = fwl_regression(D)
    except np.linalg.LinAlgError:
        return np.nan, np.nan, np.nan, None
    y_r, x_r = U["FE"]["y"], U["FE"]["x"]          # residualised on the effects
    beta_fwl = float(np.asarray(B["x"]["y"]).ravel()[0])
    b, V = ols(pd.DataFrame(np.asarray(x_r), index=y_r.index, columns=["x"]),
               pd.Series(np.asarray(y_r).ravel(), index=y_r.index), cov_type="HC3")
    beta = float(b.iloc[0, 0])
    if not math.isclose(beta, beta_fwl, rel_tol=1e-6, abs_tol=1e-8):
        warnings.warn(f"FWL recursion and ols disagree: {beta_fwl} vs {beta} "
                      "(numerical noise in a large dummy block?)")
    e = np.asarray(y_r).ravel() - beta * np.asarray(x_r).ravel()
    dof = max(len(e) - FE.shape[1] - 1, 1)
    return beta, float(math.sqrt(V.iloc[0, 0])), float(math.sqrt((e ** 2).sum() / dof)), np.asarray(y_r).ravel()


def fit_gap_model(gaps):
    """The two FWL specifications on matched cells.

    ``within``: ``gap_log ~ item FE + geo FE + beta_w * midpoint`` where
    ``midpoint = (median_log_a + median_log_b) / 2`` per cell.
    ``between``: ``gap_log ~ geo FE + beta_b * item_mean_midpoint``.
    A constant markup or an iceberg cost predicts both betas = 0; an additive
    per-unit cost predicts both < 0, the between one strongly.  ``r2_fe`` is
    the share of gap variance the item and geo effects explain on their own;
    ``resid_sd`` the within-spec residual SD.  Standard errors are HC3 on the
    residualised regression (``Metrics_Miscellany.ols``), unclustered -- say
    so if it matters for your country.
    """
    g = gaps.dropna(subset=["gap_log", "median_log_a", "median_log_b"]).copy()
    g = g.reset_index(drop=True)
    n = len(g)
    out = {"n_cells": int(n),
           "n_items": int(g["j"].nunique()) if n else 0,
           "n_geos": int(g["geo"].nunique()) if n else 0,
           "median_gap": float(g["gap_log"].median()) if n else np.nan,
           "iqr_gap": float(g["gap_log"].quantile(.75) - g["gap_log"].quantile(.25)) if n else np.nan,
           "share_abs_gap_gt_log1p5": float((g["gap_log"].abs() > math.log(1.5)).mean()) if n else np.nan,
           "coef_within": np.nan, "se_within": np.nan,
           "coef_between": np.nan, "se_between": np.nan,
           "r2_fe": np.nan, "resid_sd": np.nan, "claim_30_50": None}
    if n:
        out["claim_30_50"] = bool(CLAIM_LO <= out["median_gap"] <= CLAIM_HI)
    if n < 8 or g["j"].nunique() < 2:
        return out
    g["j"] = g["j"].astype(str)
    g["geo"] = g["geo"].astype(str)
    y = g[["gap_log"]].astype(float)
    mid = pd.DataFrame({"mid": (g["median_log_a"] + g["median_log_b"]) / 2.0})
    D_item, D_geo = dummies(g, ["j"]), dummies(g, ["geo"])
    tot = float(((y["gap_log"] - y["gap_log"].mean()) ** 2).sum())
    # within: item + geo effects, cell midpoint
    bw, sew, rsd, y_r = _fwl_slope(y, mid, [D_item, D_geo])
    out.update(coef_within=bw, se_within=sew, resid_sd=rsd,
               r2_fe=(float(1 - (y_r ** 2).sum() / tot) if (tot > 0 and y_r is not None) else np.nan))
    # between: geo effects only, the item's mean midpoint
    item_mid = pd.DataFrame({"item_mid": mid["mid"].groupby(g["j"]).transform("mean")})
    bb, seb, _, _ = _fwl_slope(y, item_mid, [D_geo])
    out.update(coef_between=bb, se_between=seb)
    return out


def model_row(gaps, *, country, t, pair, u_basis, threshold_a, threshold_b,
              geo_level="all"):
    """One ``models.csv`` row.  Call once with ``geo_level='all'`` on every
    matched cell and once per ``geo_level`` present (``gaps[gaps.geo_level ==
    lvl]``): ``finest_only`` mixes cluster ids, districts and regions in one
    ``geo`` effect, and the per-level rows show whether the answer depends on
    the rung."""
    row = {"country": country, "t": str(t), "pair": pair, "u_basis": u_basis,
           "geo_level": geo_level, "threshold_a": int(threshold_a),
           "threshold_b": int(threshold_b)}
    row.update(fit_gap_model(gaps))
    return row


def model_rows(gaps, **kw):
    """``model_row`` pooled plus one row per ``geo_level`` present."""
    rows = [model_row(gaps, geo_level="all", **kw)]
    for lvl in sorted(gaps["geo_level"].dropna().unique()) if len(gaps) else []:
        rows.append(model_row(gaps[gaps["geo_level"] == lvl], geo_level=str(lvl), **kw))
    return rows


# ---------------------------------------------------------------------------
# outputs
# ---------------------------------------------------------------------------

def _conform(df, cols, name):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing columns {missing}")
    return df[cols]


def write_outputs(outdir, *, cells, gaps, models):
    """Write the three CSVs with the fixed schemas; raises on a missing column."""
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    _conform(pd.DataFrame(cells), CELL_COLS, "cells").to_csv(outdir / "cells.csv", index=False)
    _conform(pd.DataFrame(gaps), GAP_COLS, "gaps").to_csv(outdir / "gaps.csv", index=False)
    _conform(pd.DataFrame(models), MODEL_COLS, "models").to_csv(outdir / "models.csv", index=False)
    return [outdir / f for f in ("cells.csv", "gaps.csv", "models.csv")]


# ---------------------------------------------------------------------------
# self-test
# ---------------------------------------------------------------------------

def _synthetic(kind, seed=0, n_geo=30, n_item=12, n_obs=25, n_obs_b=None):
    """Farmgate prices p_b; market prices p_a under a constant 35% markup
    (``kind='markup'``) or an additive cost of 40 per unit (``'additive'``)."""
    rng = np.random.default_rng(seed)
    n_obs_b = n_obs if n_obs_b is None else n_obs_b
    rows_a, rows_b = [], []
    base = np.exp(rng.normal(5, 0.8, n_item))            # item price levels
    for gi in range(n_geo):
        for ji in range(n_item):
            pb = base[ji] * np.exp(rng.normal(0, 0.15, n_obs))
            if kind == "markup":
                pa = pb * 1.35 * np.exp(rng.normal(0, 0.05, n_obs))
            else:
                pa = (pb + 40.0) * np.exp(rng.normal(0, 0.05, n_obs))
            for k in range(n_obs):
                rows_a.append(("2020", f"item{ji}", "Kg", f"v{gi}", f"R{gi % 4}", pa[k]))
            for k in range(n_obs_b):          # b may be a thin measurement source
                rows_b.append(("2020", f"item{ji}", "Kg", f"v{gi}", f"R{gi % 4}", pb[k]))
    cols = ["t", "j", "u", "v", "Region", "price"]
    return pd.DataFrame(rows_a, columns=cols), pd.DataFrame(rows_b, columns=cols)


if __name__ == "__main__":
    for kind, expect in (("markup", "both betas ~ 0, median gap ~ log 1.35 = 0.300"),
                         ("additive", "both betas < 0, between strongly")):
        for n_b, tb in ((25, 10), (2, 1)):     # dense b; then a thin measurement source
            a, b = _synthetic(kind, n_obs_b=n_b)
            ca = cell_medians(a, ["v", "Region"])
            cb = cell_medians(b, ["v", "Region"])
            g = match_gaps(ca, cb, threshold_a=10, threshold_b=tb)
            r = fit_gap_model(g)
            print(f"{kind:9s} b_obs={n_b:2d} thr=({10},{tb}) cells={r['n_cells']} "
                  f"finest={set(g['geo_level'])} median_gap={r['median_gap']:.3f} "
                  f"within={r['coef_within']:.3f} (se {r['se_within']:.3f}) "
                  f"between={r['coef_between']:.3f} (se {r['se_between']:.3f}) "
                  f"r2_fe={r['r2_fe']:.2f} claim={r['claim_30_50']}   <- {expect}")
    rows = model_rows(g, country="synthetic", t="2020", pair="a_vs_b", u_basis="native",
                      threshold_a=10, threshold_b=1)
    print("model_rows geo_levels:", [r["geo_level"] for r in rows])
    # under-identified: 12 cells, each its own geo -> NaN slopes, no exception
    tiny = g.groupby("j", observed=True).head(1).head(12).copy()
    tiny["geo"] = [f"g{k}" for k in range(len(tiny))]
    r = fit_gap_model(tiny)
    assert all(math.isnan(r[k]) for k in ("coef_within", "coef_between", "r2_fe")), r
    print(f"under-identified case: cells={r['n_cells']} within={r['coef_within']} between={r['coef_between']} (NaN, as it should be)")
    print("self-test done")
