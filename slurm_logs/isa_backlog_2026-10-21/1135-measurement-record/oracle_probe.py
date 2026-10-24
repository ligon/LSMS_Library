"""Raw-cover oracle for the #1135 red-team findings.  Aggregates only.

Usage: python oracle_probe.py <out.json>
"""
import json, os, subprocess, sys, warnings
import pandas as pd
import lsms_library as ll
top = os.environ["CLONE"]
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
from lsms_library.local_tools import get_dataframe
warnings.simplefilter("ignore")
root = ll.Country("Nigeria").file_path if hasattr(ll.Country("Nigeria"), "file_path") else None
out = {}


def pairs(country, a, b):
    s = ll.Country(country).sample(waves=[a, b])
    idx = s.reset_index()[["i", "t", "v"]]
    idx["t"] = idx["t"].astype(str)
    A = idx[idx.t == a].set_index("i")["v"].dropna().astype(str)
    B = idx[idx.t == b].set_index("i")["v"].dropna().astype(str)
    assert not A.index.duplicated().any() and not B.index.duplicated().any()
    both = A.index.intersection(B.index)
    return A.loc[both], B.loc[both]


def strict_k(a, b, synthetic=("moved-", "@")):
    """Earlier ids among changed real-both-sides pairs; how many map wholly
    and one-to-one to a single different id (over ALL linked households)."""
    syn = lambda s: s.map(lambda x: x.startswith(synthetic))
    ch = (a != b)
    real = ch & ~syn(a) & ~syn(b)
    L = pd.DataFrame({"e": a, "f": b})
    bwd = L.groupby("f")["e"].nunique()
    res = {"earlier_real_changed": int(a[real].nunique()), "one_to_one": 0,
           "not_one_to_one": []}
    for e in sorted(a[real].unique()):
        sub = L[L.e == e]
        fs = set(sub.f)
        realf = {f for f in fs if f != e and not f.startswith(synthetic)}
        if len(fs) == 1 and next(iter(fs)) != e and bwd.get(next(iter(fs)), 0) == 1:
            res["one_to_one"] += 1
        else:
            # aggregate description only: how this earlier id's linked
            # households are distributed over destination classes
            cls = sub.f.map(lambda f: "kept" if f == e else ("sentinel" if f.startswith(synthetic) else "real-new"))
            res["not_one_to_one"].append({
                "households_by_destination": {k: int(v) for k, v in cls.value_counts().items()},
                "n_real_new_ids": len(realf),
                "real_new_ids_shared_with_other_earlier_ids": int(sum(bwd.get(f, 0) > 1 for f in realf))})
    return res


# --- Nigeria movement markers on the raw covers ---------------------------
NG = {"2016Q1->2018Q3": ("2016Q1", "2018Q3", "2018-19/Data/secta_plantingw4.dta"),
      "2019Q1->2023Q3": ("2019Q1", "2023Q3", "2023-24/Data/Post Planting Wave 5/Household/secta_plantingw5.dta")}
croot = os.path.join(top, "lsms_library", "countries", "Nigeria")
for key, (a_, b_, rel) in NG.items():
    a, b = pairs("Nigeria", a_, b_)
    ch = a != b
    sent = a.str.startswith("moved-") | b.str.startswith("moved-")
    raw = get_dataframe(os.path.join(croot, rel), convert_categoricals=False)
    raw["i"] = raw["hhid"].map(lambda x: str(int(x)))
    raw = raw.drop_duplicates("i").set_index("i")
    o = out[key] = {"N": int(len(a)), "D": int(ch.sum()),
                    "D_sentinel": int((ch & sent).sum()), "D_real": int((ch & ~sent).sum()),
                    "N_matched_to_cover": int(raw.index.isin(a.index).sum()),
                    "ea0_among_D_sentinel": int((raw["ea"].reindex(a.index)[ch & sent] == 0).sum()),
                    "markers": {}}
    for var in ("hhsmlc", "hh_moved", "tracked_obs", "trackedw4", "trackedw5pp", "hhidyn"):
        if var not in raw.columns:
            continue
        col = raw[var].reindex(a.index)
        lab = col.map(lambda x: "NA" if pd.isna(x) else str(int(x)))
        o["markers"][var] = {
            "changed_sentinel": {k: int(v) for k, v in lab[ch & sent].value_counts().items()},
            "changed_real": {k: int(v) for k, v in lab[ch & ~sent].value_counts().items()},
            "changed_all": {k: int(v) for k, v in lab[ch].value_counts().items()}}
    o["strict_k"] = strict_k(a, b)

# --- strict K on every warning boundary ------------------------------------
B = {"GhanaLSS": [("1987-88", "1988-89")], "Ethiopia": [("2011-12", "2013-14")],
     "Tanzania": [("2010-11", "2012-13"), ("2012-13", "2014-15"), ("2014-15", "2019-20")],
     "Uganda": [("2005-06", "2009-10"), ("2009-10", "2010-11"), ("2011-12", "2013-14"), ("2015-16", "2018-19")]}
out["strict_k"] = {}
for c, prs in B.items():
    for a_, b_ in prs:
        a, b = pairs(c, a_, b_)
        k = strict_k(a, b)
        out["strict_k"][f"{c} {a_}->{b_}"] = {"N": int(len(a)), "D": int((a != b).sum()),
                                              "earlier_real_changed": k["earlier_real_changed"],
                                              "one_to_one": k["one_to_one"],
                                              "n_not_one_to_one": len(k["not_one_to_one"])}

# --- Ethiopia 2015-16 / 2018-19 shared ids ---------------------------------
s = ll.Country("Ethiopia").sample(waves=["2015-16", "2018-19"]).reset_index()
s["t"] = s["t"].astype(str)
va = set(s.loc[s.t == "2015-16", "v"].dropna().astype(str))
vb = set(s.loc[s.t == "2018-19", "v"].dropna().astype(str))
shared = sorted(va & vb)
cf = ll.Country("Ethiopia").cluster_features(waves=["2015-16", "2018-19"]).reset_index()
cf["t"] = cf["t"].astype(str); cf["v"] = cf["v"].astype(str)
agree = {}
for col in ("Region", "District", "Rural"):
    if col in cf.columns:
        x = cf[cf.v.isin(shared)].pivot_table(index="v", columns="t", values=col, aggfunc="first")
        if {"2015-16", "2018-19"} <= set(x.columns):
            x = x.dropna()
            agree[col] = [int((x["2015-16"].astype(str) == x["2018-19"].astype(str)).sum()), int(len(x))]
both_i = set(s.loc[s.t == "2015-16", "i"]) & set(s.loc[s.t == "2018-19", "i"])
out["ethiopia_2015_16_2018_19"] = {"keyspace_a": len(va), "keyspace_b": len(vb), "shared_v": len(shared),
                                   "shared_i": len(both_i), "attribute_agreement_on_shared_v": agree}
json.dump(out, open(sys.argv[1], "w"), indent=1, sort_keys=True)
print("ok")
