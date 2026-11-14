"""Compare served_side.py outputs: candidate dir vs base dir.
usage: served_compare.py <cand_dir> <base_dir> <out.json>   (run with the candidate clone)"""
import collections
import json
import os
import sys

import pandas as pd

from lsms_library import sampling as S
from lsms_library.derivations import MULTI_SEP

cd, bd, outp = sys.argv[1:4]
cs = json.load(open(os.path.join(cd, "summary.json")))
bs = json.load(open(os.path.join(bd, "summary.json")))
res = {"cand_head": cs.get("_head"), "base_head": bs.get("_head"), "calls": {}, "facts": {}}


def load(d, label):
    p = os.path.join(d, f"{label}.pkl")
    return pd.read_pickle(p) if os.path.exists(p) else None


for label in sorted(set(cs) | set(bs)):
    if label.startswith("_"):
        continue
    c, b = cs.get(label), bs.get(label)
    r = {}
    if c is None or b is None:
        r["missing_side"] = "cand" if c is None else "base"
        res["calls"][label] = r
        continue
    r["error_cand"], r["error_base"] = c.get("error"), b.get("error")
    fc, fb = load(cd, label), load(bd, label)
    if fc is not None and fb is not None:
        try:
            pd.testing.assert_frame_equal(fc, fb, check_exact=True)
            r["frame_equal"] = True
        except AssertionError as e:
            r["frame_equal"] = False
            r["frame_diff"] = str(e)[:1500]
        r["shape"] = list(fc.shape)
    r["cluster_warnings_equal"] = c.get("cluster_warnings") == b.get("cluster_warnings")
    r["n_cluster_warnings"] = [len(c.get("cluster_warnings") or []), len(b.get("cluster_warnings") or [])]
    if not r["cluster_warnings_equal"]:
        r["cluster_warnings"] = {"cand": c.get("cluster_warnings"), "base": b.get("cluster_warnings")}
    wc = collections.Counter(map(tuple, c.get("warnings") or []))
    wb = collections.Counter(map(tuple, b.get("warnings") or []))
    only_c, only_b = wc - wb, wb - wc
    if only_c or only_b:
        r["warnings_only_cand"] = [list(k) + [n] for k, n in only_c.items()][:20]
        r["warnings_only_base"] = [list(k) + [n] for k, n in only_b.items()][:20]
    r["sampling_reports_equal"] = c.get("sampling_reports") == b.get("sampling_reports")
    ac, ab = (c.get("attrs") or {}), (b.get("attrs") or {})
    sc, sb = ac.get(S.ATTRS_KEY) or {}, ab.get(S.ATTRS_KEY) or {}
    diff = {}
    for country in sorted(set(sc) | set(sb)):
        wcand, wbase = sc.get(country) or {}, sb.get(country) or {}
        for w in sorted(set(wcand) | set(wbase)):
            x, y = wcand.get(w), wbase.get(w)
            if x != y:
                diff[f"{country}/{w}"] = {
                    "cand_keys": sorted(x) if x else None, "base_keys": sorted(y) if y else None,
                    "cand_design_tag": (x or {}).get("design_tag"),
                    "base_design_tag": (y or {}).get("design_tag"),
                    "cand_holds": ((x or {}).get("served_strata") or {}).get("holds")}
    r["sampling_attrs_diff"] = diff
    r["resolution"] = {"cand": ac.get(S.ATTRS_RESOLUTION_KEY), "base": ab.get(S.ATTRS_RESOLUTION_KEY)}
    r["other_attrs_keys"] = {"cand": ac.get("other_attrs_keys"), "base": ab.get("other_attrs_keys")}
    res["calls"][label] = r

# ---- strata facts on the candidate frames -------------------------------------------
recs = {c: S.sampling_records(c) for c in ("Mali", "Niger", "Burkina_Faso", "Togo",
                                           "CotedIvoire", "Senegal", "Uganda")}
facts = res["facts"]
for country, rr in recs.items():
    for w, rec in rr.items():
        blk = rec.served_strata or {}
        f = load(cd, f"{country}__sample__{w}")
        fb = load(bd, f"{country}__sample__{w}")
        if f is None:
            facts[f"{country}/{w}"] = "no frame"
            continue
        flat = f.reset_index()
        d = {"holds": blk.get("holds"), "rows": len(flat), "has_strata_col": "strata" in flat,
             "base_has_strata_col": None if fb is None else "strata" in fb.reset_index()}
        if "strata" in flat:
            st = flat["strata"]
            pres = st.notna()
            d["non_null"] = int(pres.sum())
            d["distinct"] = int(st.dropna().nunique())
            d["dtype"] = str(st.dtype)
            d["values"] = sorted(map(str, st.dropna().unique()))[:80]
            if "Derivation" in flat:
                keys = flat["Derivation"].fillna("").astype(str).str.split(MULTI_SEP)
                allkeys = collections.Counter(k.strip() for ks in keys for k in ks if k.strip())
                d["derivation_keys_in_frame"] = dict(allkeys)
                if blk.get("derivation"):
                    hit = keys.apply(lambda ks: blk["derivation"] in [k.strip() for k in ks])
                    d["keyed_on_present"] = int(hit[pres].sum())
                    d["keyed_on_absent"] = int(hit[~pres].sum())
                strata_keys = [k for k in allkeys if "strat" in k]
                d["strata_keys_present"] = strata_keys
            if "v" in flat:
                per_v = flat.dropna(subset=["strata"]).groupby("v")["strata"].nunique()
                d["clusters"] = int(flat["v"].nunique())
                d["clusters_with_two_strata"] = {str(v): flat.loc[flat["v"] == v, "strata"].value_counts().to_dict()
                                                 for v in per_v[per_v > 1].index[:10]}
        facts[f"{country}/{w}"] = d

# cross-wave stratum changes for households in both waves
for country, (w1, w2) in {"Mali": ("2018-19", "2021-22"), "Senegal": ("2018-19", "2021-22"),
                          "Niger": ("2018-19", "2021-22"), "Togo": ("2018", "2021-22"),
                          "CotedIvoire": ("2018-19", "2021-22"),
                          "Burkina_Faso": ("2018-19", "2021-22")}.items():
    f = load(cd, f"{country}__sample__all")
    if f is None:
        continue
    flat = f.reset_index()
    a = flat[flat["t"] == w1].set_index("i")["strata"]
    b = flat[flat["t"] == w2].set_index("i")["strata"]
    both = a.index.intersection(b.index)
    a, b = a[~a.index.duplicated()], b[~b.index.duplicated()]
    ch = (a.reindex(both).astype(str) != b.reindex(both).astype(str))
    facts[f"{country}/cross {w1}->{w2}"] = {"households_in_both": int(len(both)),
                                             "change_stratum": int(ch.sum())}
    if country == "Togo":
        va = set(flat[(flat["t"] == w1) & (flat["strata"] == "Lomé commune Urbain")]["v"].astype(str))
        vb = set(flat[(flat["t"] == w2) & (flat["strata"] == "Grand Lomé Urbain")]["v"].astype(str))
        facts["Togo/lome grappes"] = {"2018": len(va), "2021-22": len(vb), "same_set": va == vb,
                                      "intersection": len(va & vb)}

# Burkina 2014 served on both sides
for side, d in (("cand", cd), ("base", bd)):
    f = load(d, "Burkina_Faso__sample__2014")
    if f is not None and "strata" in f.reset_index():
        st = f.reset_index()["strata"]
        facts[f"Burkina_Faso/2014 [{side}]"] = {"non_null": int(st.notna().sum()),
                                                "distinct": int(st.dropna().nunique()),
                                                "values": sorted(map(str, st.dropna().unique()))[:40]}
with open(outp, "w") as fh:
    json.dump(res, fh, indent=1, sort_keys=True, default=str)
bad = {k: v for k, v in res["calls"].items()
       if v.get("frame_equal") is False or v.get("error_cand") != v.get("error_base")
       or not v.get("cluster_warnings_equal", True)}
print("calls:", len(res["calls"]), "frame-unequal or error-differs or cluster-warn-differs:", len(bad))
for k, v in bad.items():
    print(" ", k, {kk: v.get(kk) for kk in ("frame_equal", "error_cand", "error_base",
                                             "n_cluster_warnings")})
print("sampling attrs diffs:", {k: list(v["sampling_attrs_diff"]) for k, v in res["calls"].items()
                                if v.get("sampling_attrs_diff")})
