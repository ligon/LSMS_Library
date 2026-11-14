#!/usr/bin/env python
"""Render the per-country aggregate JSONs in out/ as Markdown tables (tables.md)."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")
ORDER = [("Senegal", "2018-19"), ("Senegal", "2021-22"), ("Mali", "2018-19"), ("Mali", "2021-22"),
         ("Niger", "2018-19"), ("Niger", "2021-22"), ("Burkina_Faso", "2018-19"), ("Burkina_Faso", "2021-22"),
         ("Benin", "2018-19"), ("Benin", "2021-22"), ("Togo", "2018"), ("Togo", "2021-22"),
         ("CotedIvoire", "2018-19"), ("CotedIvoire", "2021-22")]


def f(x, nd=2):
    if x is None:
        return "-"
    if isinstance(x, float):
        return f"{x:,.{nd}f}"
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)


def load():
    data = {}
    for fn in os.listdir(OUT):
        if fn.endswith(".json"):
            r = json.load(open(os.path.join(OUT, fn)))
            data[r["country"]] = r
    return data


def table(head, rows):
    s = "| " + " | ".join(head) + " |\n|" + "|".join("---" for _ in head) + "|\n"
    for r in rows:
        s += "| " + " | ".join(f(x) for x in r) + " |\n"
    return s


def main():
    D = load()
    W = [(c, w, D[c]["waves"][w]) for c, w in ORDER if c in D and w in D[c].get("waves", {})]
    o = []
    errs = [(c, w, v["error"]) for c, w, v in W if "error" in v]
    if errs:
        o.append("## Errors\n" + "\n".join(f"- {c} {w}: {e}" for c, w, e in errs) + "\n")
    W = [(c, w, v) for c, w, v in W if "error" not in v]

    # ---- check 1
    rows = []
    for c, w, v in W:
        g = v["check1_grid"]
        rows.append([c, w] + [g[k]["all"]["on_grid"] for k in ("s07bq03a", "s07bq04", "s07bq05", "s07bq07a")]
                    + [g["s07bq03a"][t]["on_grid"] for t in ("Unite", "Kg/Litre", "other")]
                    + [g["s07bq03a"]["all"]["n"]])
    o.append("### Check 1a. Quarter-grid share of positive values (%)\n" + table(
        ["country", "wave", "q3 all", "q4 all", "q5 all", "q7a all", "q3 unité", "q3 Kg/Litre", "q3 other", "n q3>0"], rows))
    rows = []
    for c, w, v in W:
        g = v["check1_grid"]
        r = [c, w]
        for k in ("s07bq04", "s07bq05", "s07bq07a"):
            r += [g[k][t]["on_grid"] for t in ("Unite", "Kg/Litre", "other")]
        rows.append(r)
    o.append("### Check 1b. Quarter-grid share by unit type, q4 / q5 / q7a (%)\n" + table(
        ["country", "wave", "q4 unité", "q4 Kg/L", "q4 other", "q5 unité", "q5 Kg/L", "q5 other",
         "q7a unité", "q7a Kg/L", "q7a other"], rows))

    # ---- check 2
    rows = []
    for c, w, v in W:
        k = v["check2_factor"]
        modes = []
        for r, x in k["by_region"].items():
            a = x["own_never_all"]
            if a.get("n", 0) >= 20:
                modes.append(a["mode"])
        nonone = sum(1 for m in modes if abs(m - 1.0) > 1e-9)
        pa, pk = k["pooled_all"], k["pooled_kglitre"]
        rows.append([c, w, pa.get("n"), pa.get("mode"), pa.get("share_at_mode"), pk.get("n"), pk.get("mode"),
                     pk.get("share_at_mode"), f"{len(modes) - nonone}/{len(modes)}", k["jamais_resid_pos"], k["jamais_q3_pos"],
                     k["q3_over_q7_purchase_only"]["kglitre"].get("mode"),
                     k["q3_over_q7_purchase_only"]["kglitre"].get("share_at_mode")])
    o.append("### Check 2a. q3/q4 on never-purchased own-production rows; Jamais residuals; q3/q7a on recent purchase-only same-unit rows\n" + table(
        ["country", "wave", "n own-never", "mode", "% at mode", "n Kg/L", "Kg/L mode", "Kg/L % at mode",
         "regions (n>=20) with mode 1.0", "Jamais resid>0", "Jamais q3>0", "q3/q7a Kg/L mode", "% at mode"], rows))
    for c, w, v in W:
        k = v["check2_factor"]
        rows = []
        for r, x in k["by_region"].items():
            a, b = x["own_never_all"], x["own_never_kglitre"]
            q = k["q3_over_q7_purchase_only"]["by_region_kglitre"].get(r, {})
            rows.append([r, a.get("n"), a.get("mode"), a.get("n_at_mode"), b.get("n"), b.get("mode"), b.get("n_at_mode"),
                         x["q3_kglitre_on_grid"]["share"], x["q3_unite_on_grid"]["share"], q.get("n"), q.get("mode"), q.get("share_at_mode")])
        o.append(f"#### Check 2b. {c} {w}, by region\n" + table(
            ["region", "own-never n", "mode", "n at mode", "Kg/L n", "Kg/L mode", "Kg/L n at mode", "q3 Kg/L on grid %",
             "q3 unité on grid %", "q3/q7a Kg/L n", "mode", "% at mode"], rows))
    rows = []
    for c in dict.fromkeys(c for c, _, _ in W):
        x = D[c].get("cross_wave_kglitre_median_ratio")
        if not x:
            continue
        for r, y in x["by_region"].items():
            rows.append([c, x["waves"], r, y["cells"], y["median_ratio"], y["p25"], y["p75"]])
    o.append("### Check 2c. Later/earlier wave median Kg/Litre q3, same (region, item, unit, size) cell with 10+ rows\n" + table(
        ["country", "waves", "region", "cells", "median ratio", "p25", "p75"], rows))

    # ---- check 3
    rows = []
    for c, w, v in W:
        k = v["check3_classes"]
        cl = k["classes"]
        rows.append([c, w, k["n_pos"], cl["G"]["share"], cl["H"]["n"], cl["H"]["share"], cl["L"]["n"], cl["L"]["share"],
                     k["decimal_rule_agreement_with_tolerance_rule"]])
    o.append("### Check 3a. H/L classes of positive q3 (H: off grid, 4+ decimals; L: off grid, up to 3)\n" + table(
        ["country", "wave", "n q3>0", "on grid %", "H n", "H %", "L n", "L %", "decimal rules agree %"], rows))
    rows = []
    for c, w, v in W:
        cl = v["check3_classes"]["classes"]
        for z in ("G", "H", "L"):
            x = cl[z]
            if not x.get("n"):
                continue
            s = x.get("slope_logq3_logeqadu1", {}) or {}
            rep = x.get("repeat3_within_item_unit_size", {}) or {}
            rows.append([c, w, z, x["n"], x.get("same_unit_size_as_last_purchase"), x.get("own_production_on_row"),
                         x.get("last_bought_over30_or_never"), x.get("expenditure_share_of_served"), s.get("slope"), s.get("resid_sd"), s.get("n"),
                         (rep.get("eqadu1") or {}).get("sig4"), (rep.get("eqadu1_jitter15") or {}).get("sig4"),
                         (rep.get("hhsize") or {}).get("sig4"), (rep.get("eqadu2") or {}).get("sig4")])
    o.append("### Check 3b. Class profiles (CIV statistics)\n" + table(
        ["country", "wave", "class", "n", "same unit+size as last purchase %", "own prod %", "last >30d/never %",
         "Expenditure % of served", "slope log q3 on log eqadu1", "resid sd", "slope n",
         "q3/eqadu1 repeats 3+ %", "jitter ±15% %", "q3/hhsize %", "q3/eqadu2 %"], rows))
    rows = []
    for c, w, v in W:
        b = v["check3_classes"].get("by_hhsize")
        if b:
            rows.append([c, w] + [b[k]["H_share"] for k in ("1", "2-3", "4-6", "7-12", "13+")])
    o.append("### Check 3c. H share (%) by household size\n" + table(["country", "wave", "1", "2-3", "4-6", "7-12", "13+"], rows))
    rows = []
    for c, w, v in W:
        u = v["check3_classes"]["by_unit_type"]
        rows.append([c, w] + [u[t][z] for t in ("Unite", "Kg/Litre", "other") for z in ("H", "L")])
    o.append("### Check 3d. H and L share (%) by unit type\n" + table(
        ["country", "wave", "unité H", "unité L", "Kg/L H", "Kg/L L", "other H", "other L"], rows))

    # ---- check 4
    rows = []
    for c, w, v in W:
        k = v["check4_served"]
        rows.append([c, w, k["served_purchased_rows"], k["expenditure_C"], k.get("recorded_C"), k.get("C_reconstructed_over_recorded"),
                     k["expenditure_A_gross"], k.get("recorded_A"), k["expenditure_na_share_of_purchased_rows"]])
    o.append("### Check 4a. Raw reconstruction of the served purchased rows, against CONTENTS.org\n" + table(
        ["country", "wave", "served purchased rows", "Expenditure C (FCFA)", "recorded C", "ratio", "A gross", "recorded A", "Expenditure NA % of rows"], rows))
    rows = []
    for c, w, v in W:
        k = v["check4_served"]
        b = k["by_q3_class"]
        rows.append([c, w, b["H"]["rows_share"], b["H"]["expenditure_share"], b["L"]["rows_share"], b["L"]["expenditure_share"],
                     k["off_grid_rows_share"], k["off_grid_expenditure_share"], k["phantom_jamais_rows"], k["phantom_jamais_rows_share"],
                     k["phantom_jamais_expenditure_share"], k["kglitre_rows_share"], k["kglitre_expenditure_share"],
                     k["kglitre_offgrid_rows_share"], k["kglitre_offgrid_expenditure_share"]])
    o.append("### Check 4b. Share (%) of served purchased rows and of their Expenditure touched\n" + table(
        ["country", "wave", "H rows", "H Exp", "L rows", "L Exp", "off-grid rows", "off-grid Exp", "phantom Jamais rows (n)",
         "phantom rows %", "phantom Exp %", "Kg/L rows", "Kg/L Exp", "Kg/L off-grid rows", "Kg/L off-grid Exp"], rows))

    # ---- discovery
    rows = []
    for c, w, v in W:
        d = v["discovery"]
        fs = d["files"]
        rows.append([c, w, fs["s07b"]["path"], fs["s07b"]["md5"], fs["s07b"]["rows"], (fs.get("welfare") or {}).get("md5"),
                     d.get("region_source"), d.get("merge_region_rate_pos"), d.get("merge_eqadu1_rate_pos"),
                     d["dtypes"].get("s07bq03a"), d.get("q3_pos_float32_exact_share")])
    o.append("### Sources and merges\n" + table(
        ["country", "wave", "s07b (from wave _/)", "md5", "rows", "welfare md5", "region from", "region match % (q3>0)",
         "eqadu1 match % (q3>0)", "q3 dtype", "q3>0 float32-exact %"], rows))
    open(os.path.join(HERE, "tables.md"), "w").write("\n".join(o))
    print("\n".join(o))


if __name__ == "__main__" and len(sys.argv) == 1:
    sys.exit(main())


# ---------------------------------------------------------------- compact tables for REPORT.md
def load_followup():
    F = {}
    for sub in ("out_followup", "out_followup_rerun"):   # rerun overrides
        p = os.path.join(HERE, sub)
        if not os.path.isdir(p):
            continue
        for fn in os.listdir(p):
            if fn.endswith(".json") and "_" not in fn.replace("Burkina_Faso", "BurkinaFaso").replace(".json", ""):
                r = json.load(open(os.path.join(p, fn)))
                if "country" in r:
                    for w, v in r["waves"].items():
                        if not v.get("errors") or (r["country"], w) not in F:
                            F[(r["country"], w)] = v
    return F


def compact():
    D = load()
    F = load_followup()
    W = [(c, w, D[c]["waves"][w]) for c, w in ORDER if c in D and w in D[c].get("waves", {}) and "error" not in D[c]["waves"][w]]
    o = []
    rows = []
    for c, w, v in W:
        g = v["check1_grid"]
        rows.append([c, w, g["s07bq03a"]["all"]["n"]] + [g[k]["all"]["on_grid"] for k in ("s07bq03a", "s07bq04", "s07bq05", "s07bq07a")]
                    + [g["s07bq03a"][t]["on_grid"] for t in ("Unite", "Kg/Litre", "other")])
    o.append("#### Table 1. Check 1, share of positive values on the quarter grid (%)\n" + table(
        ["country", "wave", "n q3>0", "q3", "q4", "q5", "q7a", "q3 unité", "q3 Kg/Litre", "q3 other"], rows))
    rows = []
    for c, w, v in W:
        k = v["check2_factor"]
        regs = [x for x in k["by_region"].values() if x["own_never_all"].get("n", 0) >= 20]
        modes = [x["own_never_all"]["mode"] for x in regs]
        non1 = sorted({m for m in modes if abs(m - 1) > 1e-9})
        mins = min((x["own_never_all"]["share_at_mode"] for x in regs), default=None)
        kl = [x["q3_kglitre_on_grid"]["share"] for x in k["by_region"].values() if x["q3_kglitre_on_grid"]["n"] >= 100]
        q7 = [x for x in k["q3_over_q7_purchase_only"]["by_region_kglitre"].values() if x.get("n", 0) >= 30]
        q7non1 = sorted({x["mode"] for x in q7 if abs(x["mode"] - 1) > 1e-9})
        rows.append([c, w, f"{len(modes) - len(non1 and [m for m in modes if abs(m-1)>1e-9])}/{len(modes)}",
                     ", ".join(f"{m:g}" for m in non1) or "none", mins,
                     f"{min(kl):.1f}-{max(kl):.1f}" if kl else "-",
                     f"{len(q7) - len([x for x in q7 if abs(x['mode']-1)>1e-9])}/{len(q7)}", ", ".join(f"{m:g}" for m in q7non1) or "none",
                     k["jamais_resid_pos"], k["jamais_q3_pos"]])
    o.append("#### Table 2. Check 2, the regional factor\n" + table(
        ["country", "wave", "regions with q3/q4 mode 1.0 (own-production, never bought, n>=20)", "modes other than 1",
         "lowest % at mode", "q3 Kg/Litre on grid, range over regions (%)", "regions with q3/q7a mode 1.0 (Kg/Litre purchase-only, n>=30)",
         "q3/q7a modes other than 1", "Jamais rows with residual > 0", "Jamais rows with q3 > 0"], rows))
    rows = []
    for c, w, v in W:
        k = v["check3_classes"]; cl = k["classes"]; f = F.get((c, w), {})
        hs = f.get("H_simple_fraction", {})
        H = cl["H"]
        s = (H.get("slope_logq3_logeqadu1") or {}) if H.get("n") else {}
        rep = (H.get("repeat3_within_item_unit_size") or {}) if H.get("n") else {}
        pcs = f.get("per_capita_signature", {})
        e = pcs.get("eqadu1", {}); hh = pcs.get("hhsize", {})
        byh = k.get("by_hhsize") or {}
        rows.append([c, w, cl["G"]["share"], H["n"], H["share"], hs.get("other"), cl["L"]["n"], cl["L"]["share"],
                     f"{s.get('slope')} ({s.get('resid_sd')})" if s.get("slope") is not None else f"n={s.get('n', 0)}",
                     f"{(rep.get('eqadu1') or {}).get('sig4')} / {(rep.get('eqadu1_jitter15') or {}).get('sig4')}" if rep else "-",
                     (byh.get("1") or {}).get("H_share"), (byh.get("13+") or {}).get("H_share"),
                     f"{e.get('repeat_share_offgrid')} / {e.get('repeat_share_offgrid_permuted')}" if e else "-",
                     (e.get("signature") or {}).get("rows", 0) if e else "-",
                     f"{hh.get('repeat_share_offgrid')} / {hh.get('repeat_share_offgrid_permuted')}" if hh else "-",
                     (hh.get("signature") or {}).get("rows", 0) if hh else "-"])
    o.append("#### Table 3. Check 3, the H/L classes and the per-capita signature\n" + table(
        ["country", "wave", "on grid %", "H n", "H %", "H not a simple fraction (n)", "L n", "L %",
         "H slope log q3 on log eqadu1 (resid sd)", "H q3/eqadu1 repeats, real / ±15% jitter (%)", "H % in 1-member hh", "H % in 13+ hh",
         "off-grid q3/eqadu1 repeats, real / permuted (%)", "eqadu1-signature rows", "off-grid q3/hhsize repeats, real / permuted (%)", "hhsize-signature rows"], rows))
    rows = []
    for c, w, v in W:
        k = v["check4_served"]; f = F.get((c, w), {})
        pcs = f.get("per_capita_signature", {})
        sig = []
        for den in ("eqadu1", "hhsize"):
            sg = (pcs.get(den) or {}).get("signature")
            sig.append(f"{sg['served_rows_share']} / {sg['served_expenditure_share']}" if sg else "-")
        nv = f.get("no_vague", {})
        rows.append([c, w, k["served_purchased_rows"], k["expenditure_C"], k.get("recorded_C") or "-",
                     k["off_grid_rows_share"], k["off_grid_expenditure_share"],
                     f"{k['phantom_jamais_rows']} ({k['phantom_jamais_rows_share']}%)", k["phantom_jamais_expenditure_share"],
                     sig[0], sig[1], f"{nv.get('q3_pos', 0)} ({nv.get('served_rows_share') or 0}%)" if nv else "-"])
    o.append("#### Table 4. Check 4, served purchased rows (raw reconstruction) and what each pattern touches\n" + table(
        ["country", "wave", "served purchased rows", "Expenditure C, FCFA", "recorded C (CONTENTS.org)", "off-grid q3, % rows", "off-grid q3, % Exp",
         "phantom Jamais purchases, n (% rows)", "phantom, % Exp", "eqadu1 signature, % rows / % Exp", "hhsize signature, % rows / % Exp",
         "rows with no vague, n (% served rows)"], rows))
    txt = "\n".join(o)
    open(os.path.join(HERE, "compact.md"), "w").write(txt)
    return txt


if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "compact":
    print(compact())
