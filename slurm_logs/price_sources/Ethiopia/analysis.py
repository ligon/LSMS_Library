#!/usr/bin/env python
"""Ethiopia -- five sources of food prices, compared (PROTOCOL.org).

Ethiopia has THREE of the five sources.  ``food_acquired`` carries no
``Price`` column, and the ESS instrument is why: Section 5A of the household
questionnaire asks a *quantity* for own production (Q5) and gifts (Q6) but a
*value* only for purchases (Q4, "How much did you spend?").  So source 2
(reported purchase price) and source 3 (own-consumption valuation) are
INSTRUMENT absences, not API gaps -- ``food_prices(units='unitprice')``
returns an empty frame.

  1  unit_value        food_prices(units='unitvalue'), s == 'purchased'
  4  crop_sale_price   crop_production: Value_sold / Quantity_sold
  5  community_price   community_prices: Price / Quantity

Two repairs are applied here and BOTH are analysis-side, because the defects
they work around live in ``lsms_library/country.py`` (out of scope):

  R1  ``_join_v_from_sample._v_to_str`` does ``str(int(float(x)))``
      (country.py:2331-2338), which destroys the leading zeros of Ethiopia's
      zero-padded EA ids: sample() '01010101601' arrives on food_acquired as
      '1010101601'.  Raw match against cluster_features.v is 9-46% per wave.
  R2  ``_join_v_from_sample`` runs BEFORE ``id_walk`` in ``_finalize_result``
      (country.py:2891 vs ~2910).  ``sample()`` is already id-walked, while
      the cached food_acquired parquet still carries the wave-native
      ``household_id2`` for 2013-14 / 2015-16, so only the 1,486 / 1,315
      urban-refreshment households join a ``v`` at all -- 68.5% / 66.6% of
      rows come back with v = NaN.  (``crop_production`` escapes this: its
      country script calls ``local_tools.id_walk`` itself.)

Both are fixed by dropping the framework-supplied ``v`` and re-joining
``sample()[['v']]`` on ``(i, t)`` at API level, where the i-key match is
1.0000 in every wave.  ``_v_repair`` asserts that.

Identification of the FWL slopes is ``common.py``'s job: since 2026-09-08 it
reports NaN for any spec with fewer than ``MIN_DOF`` residual degrees of
freedom rather than raising or returning a saturated "fit".  This script no
longer wraps it -- where a slope is NaN, read the medians and ``by_item.csv``.

Usage (from anywhere):

    export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
    export LSMS_BUILD_WORKERS=1
    taskset -c 6-8 .venv/bin/python analysis.py
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))          # slurm_logs/price_sources
import common                                  # noqa: E402

COUNTRY = "Ethiopia"
CURRENCY = "ETB"                               # single currency, all 5 waves
OUTDIR = HERE

# Geography ladder, finest first.  `District` is rebuilt as a Region|District
# composite: cluster_features ships a bare 1-2 digit code, 30 of whose 37
# distinct values occur in more than one Region (max 11), so the raw column
# is not a district identity.
GEO_LEVELS = ["v", "District", "Region"]

# Units that name no quantity.  A price "per Other (Specify)" cannot be
# compared with anything.  Both spellings are live in the corpus.
JUNK_UNITS = {"other (specify)", "other(specify)", "nan", ""}

# Per-side observation thresholds (PROTOCOL "Grain and geography").
# An ESS enumeration area holds 12 rural / 15 urban households, so a
# household-side source can never reach 10 observations per (cluster, item,
# unit) cell; 5 is the protocol's sanctioned floor for a thin source and is
# used for BOTH household-side sources here.  A community price is a
# measurement -- one surveyed price per cluster and item -- so it is usable
# at 1.
THR_HOUSEHOLD = 5
THR_COMMUNITY = 1

PAIRS = [
    # (pair name, source a = downstream/market, source b, thr_a, thr_b)
    ("unit_value_vs_crop_sale_price", "unit_value", "crop_sale_price",
     THR_HOUSEHOLD, THR_HOUSEHOLD),
    ("community_price_vs_crop_sale_price", "community_price", "crop_sale_price",
     THR_COMMUNITY, THR_HOUSEHOLD),
    ("unit_value_vs_community_price", "unit_value", "community_price",
     THR_HOUSEHOLD, THR_COMMUNITY),
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _kg_map():
    """Survey-supplied unit -> kg factors, lower-cased keys.

    ``Ethiopia/_/conversion_to_kgs.json`` is built from the ESS's own
    ``Food_CF_Wave{1..5}.dta`` conversion files (``mean_cf_nat``).  Using it
    -- rather than ``transformations._get_kg_factors``, whose factors are
    INFERRED from Expenditure/Quantity ratios -- keeps the per-kg view free
    of the circularity the protocol warns about: these factors never saw a
    price.  They are national means per unit and item-independent, so a
    piece/number unit converts coarsely; that is stated, not hidden.
    """
    import lsms_library
    from lsms_library.paths import countries_root
    p = Path(countries_root()) / COUNTRY / "_" / "conversion_to_kgs.json"
    d = json.load(open(p))
    return {str(k).strip().lower(): float(v) for k, v in d.items()
            if pd.notna(v) and float(v) > 0}


# Floor for the v-repair, per wave.  Not 1.0: `Ethiopia/_/CONTENTS.org`
# ("Household id emitted as i") records a measured orphan rate against the
# sample() spine for the plot/crop family -- "W1 0.0%, W2 0.7%, W3 0.7%,
# W4 4.2%, W5 2.0% -- a handful of farm households slightly off the
# cover-page spine".  crop_sale_price lands at 97.7% in 2018-19, inside that
# documented band.  The floor is here to catch a keyspace DRIFT (a 30-70%
# collapse, which is what defect R2 looks like), not to deny a known orphan.
V_REPAIR_FLOOR = 0.95


def _v_repair(df, sample_v, label):
    """Drop the framework's ``v`` and re-join it from ``sample()``.

    See the module docstring (R1 / R2).  ``df`` must carry ``i`` and ``t`` as
    columns.  Asserts the per-wave recovery clears ``V_REPAIR_FLOOR``.
    """
    out = df.drop(columns=[c for c in ["v"] if c in df.columns]).copy()
    out["i"] = out["i"].astype(str)
    out["t"] = out["t"].astype(str)
    out = out.merge(sample_v, on=["i", "t"], how="left")
    rate = out.groupby("t")["v"].apply(lambda s: s.notna().mean())
    bad = rate[rate < V_REPAIR_FLOOR]
    if len(bad):
        raise AssertionError(
            f"{label}: v-repair recovered <{V_REPAIR_FLOOR:.0%} of rows in "
            f"{dict(bad)} -- the i-keyspace has drifted; re-check before "
            "trusting any cluster-rung number (CONTENTS.org records that "
            "id_walk is order-dependent)."
        )
    print(f"  v-repair {label}: recovered "
          f"{out['v'].notna().mean():.4f} of rows; per wave "
          f"{ {k: round(float(v), 4) for k, v in rate.items()} }")
    return out


def _clean_units(df, label):
    """Drop rows whose ``u`` names no quantity, and report the count."""
    u = df["u"].astype(str).str.strip().str.lower()
    keep = ~u.isin(JUNK_UNITS) & df["u"].notna()
    n = int((~keep).sum())
    if n:
        print(f"  {label}: dropped {n} rows on an uninformative unit "
              f"({sorted(set(df.loc[~keep, 'u'].astype(str)))[:4]})")
    return df[keep].copy()


def _prep(df, country, label):
    """Attach geography, build the Region|District composite, report orphans."""
    out, n_missing = common.attach_geo(df, country, geo_cols=("District", "Region"))
    if n_missing:
        print(f"  {label}: {n_missing} of {len(df)} rows "
              f"({n_missing/len(df):.2%}) have a (t, v) with no cluster_features "
              "row -- counted, not dropped")
    # Region|District composite: the bare District code is not unique across
    # regions (30 of 37 codes appear in >1 Region).
    if "District" in out.columns and "Region" in out.columns:
        orphan = out["Region"].isna()
        out["District"] = (out["Region"].astype(str) + "|"
                           + out["District"].astype(str))
        # A row with no cluster_features match must NOT acquire a real
        # geography.  Without this the composite above stringifies its nulls
        # into a live "nan|nan" District (and cell_medians' own .astype(str)
        # does the same to Region), so orphan rows from two DIFFERENT sources
        # would meet in a phantom cell and be matched against each other.
        # Nulled here, they are dropped by the groupby at the District and
        # Region rungs and still counted at v and national.
        out.loc[orphan, ["District", "Region"]] = pd.NA
    return out


def _to_kg(df, kg, label):
    """Second, flagged view: price per kilogram, with ``u`` set to 'kg'."""
    f = df["u"].astype(str).str.strip().str.lower().map(kg)
    ok = f.notna() & (f > 0)
    out = df[ok].copy()
    out["price"] = out["price"].astype(float) / f[ok].to_numpy(dtype=float)
    out["u"] = "kg"
    print(f"  {label}: kg view keeps {ok.mean():.4f} of rows "
          f"({int(ok.sum())} of {len(df)})")
    return out


# ---------------------------------------------------------------------------
# the three sources
# ---------------------------------------------------------------------------

def build_sources(country):
    """Return {source name: price frame} with columns t, j, u, v, price."""
    W = warnings.catch_warnings
    with W():
        warnings.simplefilter("ignore")
        smp = country.sample().reset_index()[["i", "t", "v"]]
    smp["i"] = smp["i"].astype(str)
    smp["t"] = smp["t"].astype(str)
    smp["v"] = smp["v"].astype(str)
    smp = smp.dropna(subset=["v"]).drop_duplicates(["i", "t"])

    src = {}

    # ---- source 1: purchased unit value = Expenditure / Quantity per u ----
    with W():
        warnings.simplefilter("ignore")
        s1 = country.food_prices(units="unitvalue").reset_index()
    s1 = s1[s1["s"].astype(str) == "purchased"]
    s1 = s1.rename(columns={"Price": "price"})
    s1["price"] = pd.to_numeric(s1["price"], errors="coerce")
    s1 = _v_repair(s1, smp, "unit_value")
    src["unit_value"] = s1[["t", "j", "u", "v", "price"]]

    # ---- source 2 / 3: proven absent -------------------------------------
    with W():
        warnings.simplefilter("ignore")
        up = country.food_prices(units="unitprice")
    print(f"  food_prices(units='unitprice') -> {up.shape} rows: sources 2 and 3 "
          "are absent (food_acquired carries no Price column; ESS §5A asks a "
          "value only for purchases, Q4)")

    # ---- source 4: realised crop sale unit value -------------------------
    with W():
        warnings.simplefilter("ignore")
        s4 = country.crop_production().reset_index()
    vs = pd.to_numeric(s4["Value_sold"], errors="coerce")
    qs = pd.to_numeric(s4["Quantity_sold"], errors="coerce")
    keep = (vs > 0) & (qs > 0)
    print(f"  crop_sale_price: {int(keep.sum())} of {len(s4)} rows have "
          f"Value_sold>0 AND Quantity_sold>0 "
          f"(Value_sold>0 alone: {int((vs > 0).sum())})")
    s4 = s4[keep].copy()
    # Quantity_sold is attached by ethiopia.crop_production_for_wave ONLY where
    # the reported sale unit equals the harvest `u` ("no cross-unit fib"), so
    # this ratio is genuinely a price per native `u`.
    s4["price"] = vs[keep].to_numpy(dtype=float) / qs[keep].to_numpy(dtype=float)
    s4 = _v_repair(s4, smp, "crop_sale_price")
    src["crop_sale_price"] = s4[["t", "j", "u", "v", "price"]]

    # ---- source 5: surveyed community market price -----------------------
    with W():
        warnings.simplefilter("ignore")
        s5 = country.community_prices().reset_index()
    p = pd.to_numeric(s5["Price"], errors="coerce")
    q = pd.to_numeric(s5["Quantity"], errors="coerce")
    # ESS §10 records, per (EA, item, unit), a QUANTITY and the PRICE of that
    # quantity ("SECTION 10: MARKET PRICES", columns ITEM CODE | ITEM | UNIT |
    # UNIT CODE | QUANTITY | PRICE).  So the per-unit price is Price/Quantity;
    # Ethiopia has no NumberOfUnits column -- `Quantity` is that basis.
    s5 = s5.assign(price=p.to_numpy(dtype=float) / q.to_numpy(dtype=float))
    s5["t"] = s5["t"].astype(str)
    s5["v"] = s5["v"].astype(str)
    src["community_price"] = s5[["t", "j", "u", "v", "price"]]

    for k, v in src.items():
        src[k] = _clean_units(v, k)
    return src


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    import lsms_library as ll
    print(f"lsms_library: {ll.__file__}")
    country = ll.Country(COUNTRY)

    print("\n== building sources ==")
    src = build_sources(country)
    kg = _kg_map()

    print("\n== attaching geography ==")
    # cluster_features re-emits its GrainCollapseWarning on every call; capture
    # the set once and print each distinct message a single time.
    prepped = {}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for name, df in src.items():
            prepped[name] = _prep(df, country, name)
    seen = set()
    for w in caught:
        if w.category.__name__ != "GrainCollapseWarning":
            continue
        head = str(w.message).split(".")[0]
        if head not in seen:
            seen.add(head)
            print(f"  GrainCollapseWarning: {head}")

    print("\n== per-kg view ==")
    kgview = {name: _to_kg(df, kg, name) for name, df in prepped.items()}

    views = {"native": prepped, "kg": kgview}

    print("\n== cells ==")
    cells_all, cell_rows = {}, []
    for basis, frames in views.items():
        cells_all[basis] = {}
        for name, df in frames.items():
            c = common.cell_medians(df, GEO_LEVELS)
            cells_all[basis][name] = c
            r = c.copy()
            r["country"], r["source"], r["currency"] = COUNTRY, name, CURRENCY
            cell_rows.append(r)
            print(f"  {basis:6s} {name:18s} {len(c):7d} cells "
                  f"({c.groupby('geo_level').size().to_dict()})")
    cells = pd.concat(cell_rows, ignore_index=True)

    print("\n== gaps and models ==")
    gap_rows, model_rows = [], []
    for basis, per_source in cells_all.items():
        for pair, a, b, ta, tb in PAIRS:
            ca, cb = per_source[a], per_source[b]
            g = common.match_gaps(ca, cb, threshold_a=ta, threshold_b=tb)
            if not len(g):
                print(f"  {basis:6s} {pair:38s} 0 matched cells")
                continue
            g = g.copy()
            g["country"], g["pair"] = COUNTRY, pair
            gap_rows.append(g)
            for t, gt in g.groupby("t"):
                model_rows.extend(common.model_rows(
                    gt, country=COUNTRY, t=t, pair=pair, u_basis=basis,
                    threshold_a=ta, threshold_b=tb))
            print(f"  {basis:6s} {pair:38s} {len(g):5d} cells, "
                  f"waves {sorted(set(g['t']))}, "
                  f"levels {sorted(set(g['geo_level']))}")

    gaps = (pd.concat(gap_rows, ignore_index=True) if gap_rows
            else pd.DataFrame(columns=common.GAP_COLS))
    models = (pd.DataFrame(model_rows) if model_rows
              else pd.DataFrame(columns=common.MODEL_COLS))

    paths = common.write_outputs(OUTDIR, cells=cells, gaps=gaps, models=models)
    print("\nwrote:", *[str(p) for p in paths], sep="\n  ")

    # ---- the by-item table the protocol asks for in REPORT section 5 -----
    if len(gaps):
        by_item = (gaps.groupby(["pair", "u", "j"], observed=True)
                   .agg(n_cells=("gap_log", "size"),
                        median_gap=("gap_log", "median"))
                   .reset_index())
        by_item["ratio"] = np.exp(by_item["median_gap"])
        by_item["in_30_50"] = ((by_item["median_gap"] >= common.CLAIM_LO)
                               & (by_item["median_gap"] <= common.CLAIM_HI))
        by_item.sort_values(["pair", "u", "n_cells"], ascending=[True, True, False]) \
               .to_csv(OUTDIR / "by_item.csv", index=False)
        print(f"  {OUTDIR / 'by_item.csv'}")

    print("\n== headline ==")
    if len(models):
        m = models[(models.geo_level == "all") & (models.u_basis == "native")]
        cols = ["t", "pair", "n_cells", "n_items", "median_gap", "iqr_gap",
                "coef_between", "se_between", "coef_within", "r2_fe",
                "claim_30_50"]
        print(m[cols].to_string(index=False))


if __name__ == "__main__":
    main()
