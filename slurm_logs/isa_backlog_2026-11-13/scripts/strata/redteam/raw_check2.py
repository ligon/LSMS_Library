"""Raw-file checks for the strata stream's native / absent records (candidate clone).
usage: raw_check.py <outdir>
Source files are fetched with data_access.get_data_file (local -> DVC -> WB) into
the private root; headers are read metadata-only; values with pandas.read_stata.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import warnings

import pandas as pd

warnings.simplefilter("ignore")
import lsms_library as ll  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
R = {}
try:
    import pyreadstat
except ImportError:  # pragma: no cover
    pyreadstat = None
R["pyreadstat"] = bool(pyreadstat)


def meta(rel):
    p = get_data_file(rel)
    if p is None:
        return None, None
    if pyreadstat is not None:
        try:
            _, m = pyreadstat.read_dta(str(p), metadataonly=True)
        except UnicodeDecodeError:
            _, m = pyreadstat.read_dta(str(p), metadataonly=True, encoding="latin1")
        return p, {"names": list(m.column_names),
                   "labels": dict(zip(m.column_names, m.column_labels)),
                   "var_to_labelset": dict(m.variable_to_label),
                   "value_labels": {k: {str(a): str(b) for a, b in v.items()}
                                    for k, v in m.variable_value_labels.items()},
                   "nrows": m.number_rows}
    with pd.io.stata.StataReader(str(p)) as r:
        vl = r.variable_labels()
        vals = r.value_labels()
    return p, {"names": list(vl), "labels": vl, "value_labels": vals}


def var_info(rel, var):
    p, m = meta(rel)
    if m is None:
        return {"file": rel, "error": "not fetched"}
    cand = [n for n in m["names"] if n.lower() == var.lower()]
    if not cand:
        return {"file": rel, "error": f"{var} not in file", "names": m["names"][:60]}
    v = cand[0]
    vl = m["value_labels"].get(v, {})
    return {"file": rel, "var": v, "var_label": m["labels"].get(v),
            "labelset": (m.get("var_to_labelset") or {}).get(v),
            "n_value_labels": len(vl), "value_labels": vl, "nrows": m.get("nrows")}


def _strkeys(o):
    if isinstance(o, dict):
        return {str(k): _strkeys(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_strkeys(v) for v in o]
    return o


RX = re.compile(r"strat|strate|domaine|\bzae\b|stratum", re.I)


def scan(wave_rel):
    from pathlib import Path
    root = Path(countries_root()) / wave_rel
    hits, files = [], []
    for sc in sorted(root.rglob("*.dta.dvc")):
        rel = str(sc.relative_to(Path(countries_root())))[:-4]
        files.append(rel)
        try:
            _, m = meta(rel)
        except Exception as e:  # noqa: BLE001
            hits.append({"file": rel, "error": f"{type(e).__name__}: {e}"})
            continue
        if m is None:
            hits.append({"file": rel, "error": "not fetched"})
            continue
        for n in m["names"]:
            lab = m["labels"].get(n) or ""
            if RX.search(n) or RX.search(lab):
                vl = m["value_labels"].get(n, {})
                hits.append({"file": rel, "var": n, "var_label": lab, "n_value_labels": len(vl),
                             "value_labels_head": dict(list(vl.items())[:8])})
    return {"n_files": len(files), "hits": hits}


R = json.load(open(os.path.join(out, "raw_headers.json")))

# ---- sweeps ----------------------------------------------------------------------------
S = {}
for wr in ("Mali/2017-18/Data",):
    S[wr] = scan(wr)
    print("scanned", wr, S[wr]["n_files"], "files,", len(S[wr]["hits"]), "hits", flush=True)
full = {}
for rel in ("Mali/2014-15/Data/EACIPOIDS.dta", "Mali/2014-15/Data/EACICONTROLE_p1.dta",
            "Mali/2014-15/Data/eaci2014_agregatconso.dta", "Mali/2014-15/Data/EACIS00_p1.dta",
            "Senegal/2021-22/Data/ehcvm_nsu_sen2021.dta", "Senegal/2021-22/Data/ehcvm_ponderations_sen2021.dta",
            "Senegal/2018-19/Data/ehcvm_ponderations_sen2018.dta", "Mali/2017-18/Data/eaci17_s00p1.dta"):
    _, m = meta(rel)
    full[rel] = None if m is None else {n: m["labels"].get(n) for n in m["names"]}
S["full_variable_lists"] = full
json.dump(S, open(os.path.join(out, "raw_sweeps2.json"), "w"), indent=1, default=str)

# ---- served vs raw ---------------------------------------------------------------------
V = {}


def served(c, w):
    s = ll.Country(c).sample(waves=[w]).reset_index()
    return s


def read(rel, cols, cat=True):
    p = get_data_file(rel)
    return pd.read_stata(str(p), columns=cols, convert_categoricals=cat)


# Niger 2011-12: per household, served strata == raw label of strate
try:
    s = served("Niger", "2011-12")
    raw = read("Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmamen_p1_en.dta",
               ["hid", "grappe", "strate"])
    raw["i"] = raw["hid"].map(lambda x: str(int(x)) if pd.notna(x) else None)
    m = s.merge(raw, on="i", how="left", indicator=True)
    V["Niger 2011-12"] = {
        "served_rows": len(s), "raw_rows": len(raw), "matched": int((m["_merge"] == "both").sum()),
        "equal_exact": int((m["strata"].astype(str) == m["strate"].astype(str)).sum()),
        "raw_distinct": sorted(map(str, raw["strate"].dropna().unique())),
        "raw_counts": raw["strate"].astype(str).value_counts().to_dict(),
        "served_counts": s["strata"].astype(str).value_counts().to_dict(),
        "served_i_head": s["i"].astype(str).head(3).tolist(),
        "examples_unequal": m.loc[m["strata"].astype(str) != m["strate"].astype(str),
                                  ["i", "strata", "strate"]].head(5).astype(str).values.tolist()}
except Exception as e:  # noqa: BLE001
    V["Niger 2011-12"] = f"{type(e).__name__}: {e}"

# Niger 2014-15: per grappe, served strata == the Conso file's strate; household coverage
try:
    s = served("Niger", "2014-15")
    rel = "Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2014_P1P2_Conso.dta"
    raw = read(rel, ["GRAPPE", "MENAGE", "EXTENSION", "strate"])
    per_g = raw.groupby("GRAPPE")["strate"].agg(lambda x: sorted(set(map(str, x.dropna()))))
    multi = per_g[per_g.map(len) != 1]
    g = per_g.map(lambda x: x[0] if len(x) == 1 else None)
    g.index = g.index.map(lambda x: str(int(x)))
    sv = s["v"].astype(str)
    got = sv.map(g)
    wel = read("Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2014welfare_T1c.dta",
               ["GRAPPE", "MENAGE", "EXTENSION"], cat=False)
    key = lambda d: set(zip(d["GRAPPE"].astype(int), d["MENAGE"].astype(int), d["EXTENSION"].astype(int)))  # noqa: E731
    kw, kc = key(wel), key(raw.dropna(subset=["GRAPPE", "MENAGE", "EXTENSION"]))
    V["Niger 2014-15"] = {
        "served_rows": len(s), "grappes_in_conso": int(per_g.shape[0]),
        "grappes_with_not_one_strate": int(len(multi)),
        "served_v_found_in_conso": int(got.notna().sum()),
        "served_equal_conso_strate": int((got.astype(str) == s["strata"].astype(str)).sum()),
        "welfare_households": len(kw), "welfare_households_in_conso": len(kw & kc),
        "raw_distinct": sorted(map(str, raw["strate"].dropna().unique()))}
except Exception as e:  # noqa: BLE001
    V["Niger 2014-15"] = f"{type(e).__name__}: {e}"

# Uganda early waves: label counts, raw (folded) vs served
fold = {"East rural": "East Rural", "North rural": "North Rural", "West rural": "West Rural",
        "Central rural": "Central Rural", "Other urban": "Other Urban", "KAMPALA": "Kampala",
        "kampala": "Kampala", "South-westrn": "South Western"}
for w in ("2005-06", "2009-10", "2010-11"):
    try:
        s = served("Uganda", w)
        raw = read(f"Uganda/{w}/Data/GSEC1.dta", None)
        col = [c for c in raw.columns if c.lower() == "stratum"][0]
        rawv = raw[col].astype("string").str.strip()
        V[f"Uganda {w}"] = {
            "served_rows": len(s), "raw_rows": len(raw),
            "raw_dtype": str(raw[col].dtype),
            "raw_counts": rawv.value_counts(dropna=False).to_dict(),
            "raw_counts_folded": rawv.map(lambda x: fold.get(x, x) if pd.notna(x) else x)
                                     .value_counts(dropna=False).to_dict(),
            "served_counts": s["strata"].astype("string").value_counts(dropna=False).to_dict()}
    except Exception as e:  # noqa: BLE001
        V[f"Uganda {w}"] = f"{type(e).__name__}: {e}"

# Mali 2017-18: raw code counts vs served code counts
try:
    s = served("Mali", "2017-18")
    raw = read("Mali/2017-18/Data/EACI17_ECHANTILLON.dta", None, cat=False)
    V["Mali 2017-18"] = {
        "served_rows": len(s), "raw_rows": len(raw), "raw_dtype": str(raw["strate"].dtype),
        "raw_counts": raw["strate"].value_counts(dropna=False).sort_index().astype(int)
                         .rename(lambda x: str(x)).to_dict(),
        "served_counts": s["strata"].astype(str).value_counts(dropna=False).to_dict()}
except Exception as e:  # noqa: BLE001
    V["Mali 2017-18"] = f"{type(e).__name__}: {e}"
json.dump(json.loads(pd.Series([0]).to_json()) and _strkeys(V), open(os.path.join(out, "served_vs_raw.json"), "w"), indent=1, default=str)
print("served vs raw done", flush=True)

# ---- documents -------------------------------------------------------------------------
D = {}
for rel in ("Senegal/2018-19/Documentation/ddi-documentation-english_microdata-4297.pdf",
            "Senegal/2021-22/Documentation/sen_ehcvm2_information_de_base.pdf"):
    try:
        p = get_data_file(rel)
        txt = None
        if p is not None and shutil.which("pdftotext"):
            txt = subprocess.run(["pdftotext", "-layout", str(p), "-"], capture_output=True,
                                 text=True, timeout=120).stdout
        if txt is None and p is not None:
            try:
                from pypdf import PdfReader
                txt = "\n".join((pg.extract_text() or "") for pg in PdfReader(str(p)).pages)
            except Exception as e:  # noqa: BLE001
                txt = f"<no text extractor: {e}>"
        lines = [ln.strip() for ln in (txt or "").splitlines() if re.search(r"strat", ln, re.I)]
        D[rel] = {"fetched": p is not None, "n_chars": len(txt or ""), "strat_lines": lines[:60]}
    except Exception as e:  # noqa: BLE001
        D[rel] = f"{type(e).__name__}: {e}"
json.dump(D, open(os.path.join(out, "docs.json"), "w"), indent=1, default=str)
print("done", flush=True)
