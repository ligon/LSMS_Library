"""Inventory of food-price sources per country: community_prices shape and
label overlap with food_acquired; Price population by acquisition source s.
Warm-cache probe, read-only."""
import warnings, time, sys, json
warnings.simplefilter("ignore")
import pandas as pd, numpy as np
import lsms_library as ll

out = {}
def log(*a):
    print(*a, flush=True)

CP = ["Ethiopia", "GhanaLSS", "Malawi", "Mali", "Niger", "Nigeria", "Tanzania", "EthiopiaRHS"]
PRICE = ["GhanaLSS", "EthiopiaRHS", "Ethiopia", "Guatemala", "Nepal", "Malawi", "Mali", "Niger", "Nigeria", "Panama", "Tanzania", "Uganda"]

for name in sorted(set(CP) | set(PRICE)):
    rec = {}
    try:
        c = ll.Country(name)
    except Exception as e:
        log(f"## {name}: Country() failed: {e}"); continue
    t0 = time.time()
    try:
        fa = c.food_acquired()
        rec["fa_shape"] = list(fa.shape); rec["fa_index"] = list(fa.index.names); rec["fa_cols"] = list(fa.columns)
        idx = fa.index
        s_level = idx.get_level_values("s").astype(str) if "s" in idx.names else None
        fa_j = set(map(str, idx.get_level_values("j").unique())) if "j" in idx.names else set()
        rec["fa_n_j"] = len(fa_j)
        if "Price" in fa.columns and s_level is not None:
            g = pd.DataFrame({"s": s_level.values, "p": fa["Price"].notna().values, "q": fa["Quantity"].notna().values if "Quantity" in fa.columns else False,
                              "e": fa["Expenditure"].notna().values if "Expenditure" in fa.columns else False})
            agg = g.groupby("s").agg(n=("p", "size"), price=("p", "mean"), qty=("q", "mean"), exp=("e", "mean")).round(3)
            rec["price_by_s"] = agg.to_dict("index")
        elif "Price" in fa.columns:
            rec["price_by_s"] = {"(no s level)": {"n": len(fa), "price": round(float(fa["Price"].notna().mean()), 3)}}
        else:
            rec["price_by_s"] = "no Price column"
        if "u" in idx.names:
            rec["fa_u_top"] = [str(x) for x in idx.get_level_values("u").value_counts().index[:12]]
        log(f"## {name} food_acquired {fa.shape} in {time.time()-t0:.0f}s; price_by_s={rec['price_by_s']}")
    except Exception as e:
        rec["fa_error"] = f"{type(e).__name__}: {str(e)[:200]}"; fa_j = set(); log(f"## {name} food_acquired ERROR {rec['fa_error']}")
    if name in CP:
        t0 = time.time()
        try:
            cp = c.community_prices()
            rec["cp_shape"] = list(cp.shape); rec["cp_index"] = list(cp.index.names); rec["cp_cols"] = list(cp.columns)
            cj = set(map(str, cp.index.get_level_values("j").unique())) if "j" in cp.index.names else set()
            rec["cp_n_j"] = len(cj); rec["cp_j_in_fa"] = len(cj & fa_j); rec["cp_j_examples_not_in_fa"] = sorted(cj - fa_j)[:10]
            if "u" in cp.index.names:
                rec["cp_u_top"] = [str(x) for x in cp.index.get_level_values("u").value_counts().index[:12]]
            rec["cp_waves"] = sorted(map(str, cp.index.get_level_values("t").unique())) if "t" in cp.index.names else None
            pc = [col for col in cp.columns if "rice" in col.lower()]
            if pc:
                rec["cp_price_nonnull"] = round(float(cp[pc[0]].notna().mean()), 3)
            log(f"## {name} community_prices {cp.shape} in {time.time()-t0:.0f}s; j overlap {rec['cp_j_in_fa']}/{rec['cp_n_j']}; u={rec.get('cp_u_top')}")
        except Exception as e:
            rec["cp_error"] = f"{type(e).__name__}: {str(e)[:200]}"; log(f"## {name} community_prices ERROR {rec['cp_error']}")
    out[name] = rec
    json.dump(out, open("slurm_logs/price_sources/inventory.json", "w"), indent=1, default=str)
log("DONE")
