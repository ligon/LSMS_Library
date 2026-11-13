"""Aggregate the harness jsonl logs: (1) id_walk applications per route/site,
(2) re-application evidence, (3) cold/warm/assume/feature frame agreement.

Usage: python analyze.py <logs_dir> <out.json>
"""
import collections
import glob
import json
import os
import sys

logs, out = sys.argv[1], sys.argv[2]
summary = {"walk_calls": collections.Counter(), "reapplication": [], "mixed": [],
           "finalize_noflag_walked": [], "finalize_flag_raw": [], "route_disagree": [],
           "errors": [], "dup_index": [], "results": 0, "per_country": {}}
fps = collections.defaultdict(dict)
for fn in sorted(glob.glob(os.path.join(logs, "*.jsonl"))):
    country = os.path.basename(fn)[:-6]
    pc = summary["per_country"].setdefault(country, collections.Counter())
    for line in open(fn):
        r = json.loads(line)
        if r["kind"] == "walk":
            site = r["site"][0].split(":")[0]
            summary["walk_calls"][f"{r['route']}|{site}"] += 1
            pc[f"walk|{r['route']}|{site}"] += 1
            verdicts = {v["verdict"] for v in r["cls"].values()}
            if "walked" in verdicts:
                summary["reapplication"].append([country, r["table"], r["route"], r["site"][:2], r["flag_in"], r["cls"]])
            if "mixed" in verdicts:
                summary["mixed"].append([country, r["table"], r["route"], r["site"][:2], r["cls"]])
        elif r["kind"] == "finalize":
            if "flag_in" not in r:
                continue
            verdicts = {v["verdict"] for v in r.get("cls_in", {}).values()}
            pc[f"finalize|{r['route']}|flag={r['flag_in']}"] += 1
            if not r["flag_in"] and verdicts & {"walked", "mixed"}:
                summary["finalize_noflag_walked"].append([country, r["table"], r["method"], r["route"], r["cls_in"]])
            if r["flag_in"] and verdicts & {"raw", "mixed"}:
                summary["finalize_flag_raw"].append([country, r["table"], r["method"], r["route"], r["cls_in"]])
        elif r["kind"] == "result":
            summary["results"] += 1
            if "error" in r:
                summary["errors"].append([country, r["table"], r["route"], r["error"][:160]])
                continue
            if r.get("dup_index"):
                summary["dup_index"].append([country, r["table"], r["route"], r["dup_index"]])
            fps[(country, r["table"])][r["route"]] = (r.get("rows"), r.get("frame_hash"), r.get("ti_hash"), r.get("ti_n"))
for (c, t), routes in sorted(fps.items()):
    vals = {k: v for k, v in routes.items()}
    if len({v[1] for v in vals.values()}) > 1:
        summary["route_disagree"].append([c, t, {k: [v[0], v[1][:8] if v[1] else None, v[3]] for k, v in vals.items()}])
summary["walk_calls"] = dict(summary["walk_calls"])
summary["per_country"] = {k: dict(v) for k, v in summary["per_country"].items()}
summary["fingerprints"] = {f"{c}/{t}": v for (c, t), v in fps.items()}
json.dump(summary, open(out, "w"), indent=1, default=str)
for k in ["results", "walk_calls"]:
    print(k, summary[k])
for k in ["reapplication", "mixed", "finalize_noflag_walked", "finalize_flag_raw", "route_disagree", "errors", "dup_index"]:
    print(f"== {k}: {len(summary[k])}")
    for row in summary[k][:40]:
        print("   ", str(row)[:300])
