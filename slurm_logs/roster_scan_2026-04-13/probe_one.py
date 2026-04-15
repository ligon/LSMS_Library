"""Per-country household_roster probe.

Runs ``Country(name).household_roster()`` via the Country API (so the
entire ``_finalize_result`` pipeline fires: kinship expansion, canonical
spellings, dtype coercion) and writes a single JSON diagnostic to the
per-country output directory.

Designed for multiprocessing.Pool fan-out; keep imports inside the worker
so every pool process builds its own module state rather than inheriting a
tainted parent cache.
"""
from __future__ import annotations

import json
import sys
import time
import traceback
import warnings
from pathlib import Path


def probe(country: str, out_dir: str) -> dict:
    out_path = Path(out_dir) / f"{country.replace(' ', '_')}.json"
    rec: dict = {"country": country, "status": "unknown"}
    t0 = time.monotonic()
    caught: list[str] = []

    try:
        import pandas as pd
        import yaml
        import lsms_library as ll
        from lsms_library.paths import COUNTRIES_ROOT
        from importlib.resources import files as _files

        # Load kinship labels (title-cased) once per worker
        kin_path = _files("lsms_library") / "categorical_mapping" / "kinship.yml"
        with open(kin_path, "r", encoding="utf-8") as f:
            kin_map = yaml.safe_load(f) or {}
        kinship_labels_title = {str(k).strip().title() for k in kin_map.keys()}
        kinship_labels_raw = {str(k).strip() for k in kin_map.keys()}

        # Capture warnings so we can surface "Unknown relationship labels"
        with warnings.catch_warnings(record=True) as wlist:
            warnings.simplefilter("always")
            c = ll.Country(country)
            df = c.household_roster()

        for w in wlist:
            caught.append(f"{w.category.__name__}: {w.message}")

        if df is None or (hasattr(df, "empty") and df.empty):
            rec.update(status="empty", rows=0, columns=[])
            rec["warnings"] = caught
            out_path.write_text(json.dumps(rec, indent=2, default=str))
            return rec

        cols = list(df.columns)
        rec["rows"] = int(len(df))
        rec["columns"] = cols
        rec["index_names"] = list(df.index.names)

        # ------------------------------------------------------------------
        # Sex
        # ------------------------------------------------------------------
        if "Sex" in cols:
            s = df["Sex"]
            uniq = sorted({str(x) for x in s.dropna().unique().tolist()})
            canonical = {"M", "F"}
            non_canonical = [v for v in uniq if v not in canonical]
            rec["sex"] = {
                "n_non_null": int(s.notna().sum()),
                "n_null": int(s.isna().sum()),
                "unique": uniq,
                "non_canonical": non_canonical,
                "non_canonical_counts": (
                    s[s.isin(non_canonical)].astype(str).value_counts().to_dict()
                    if non_canonical else {}
                ),
            }
        else:
            rec["sex"] = {"missing_column": True}

        # ------------------------------------------------------------------
        # Age
        # ------------------------------------------------------------------
        if "Age" in cols:
            a = df["Age"]
            a_num = pd.to_numeric(a, errors="coerce")
            rec["age"] = {
                "dtype": str(a.dtype),
                "n_non_null": int(a.notna().sum()),
                "n_null": int(a.isna().sum()),
                "n_non_numeric": int(a.notna().sum() - a_num.notna().sum()),
                "min": (None if a_num.notna().sum() == 0 else float(a_num.min())),
                "max": (None if a_num.notna().sum() == 0 else float(a_num.max())),
                "n_negative": int((a_num < 0).sum()),
                "n_gt_120": int((a_num > 120).sum()),
            }
            if rec["age"]["n_non_numeric"]:
                non_numeric_vals = (
                    a[a_num.isna() & a.notna()].astype(str)
                    .value_counts().head(10).to_dict()
                )
                rec["age"]["non_numeric_samples"] = non_numeric_vals
        else:
            rec["age"] = {"missing_column": True}

        # ------------------------------------------------------------------
        # Relationship -- check coverage vs kinship.yml
        # ------------------------------------------------------------------
        if "Relationship" in cols:
            r = df["Relationship"]
            uniq_raw = sorted({str(x).strip() for x in r.dropna().unique().tolist()})
            unknown = [
                v for v in uniq_raw
                if v.title() not in kinship_labels_title
                and v not in kinship_labels_raw
                and v not in {"", "<NA>", "nan", "None", "NaT"}
            ]
            # Count rows touched per unknown label (top 20)
            if unknown:
                mask = r.astype(str).str.strip().isin(set(unknown))
                unknown_counts = (
                    r[mask].astype(str).str.strip()
                    .value_counts().head(20).to_dict()
                )
            else:
                unknown_counts = {}
            rec["relationship"] = {
                "n_non_null": int(r.notna().sum()),
                "n_null": int(r.isna().sum()),
                "n_unique": len(uniq_raw),
                "unknown_labels": unknown,
                "unknown_label_counts": unknown_counts,
            }
        else:
            rec["relationship"] = {"missing_column": True}

        # ------------------------------------------------------------------
        # Kinship decomposition columns
        # ------------------------------------------------------------------
        rec["kinship_derived"] = {
            "Generation_present": "Generation" in cols,
            "Distance_present": "Distance" in cols,
            "Affinity_present": "Affinity" in cols,
        }
        if "Affinity" in cols:
            a = df["Affinity"]
            rec["kinship_derived"]["Affinity_unique"] = sorted(
                {str(x) for x in a.dropna().unique().tolist()}
            )
            rec["kinship_derived"]["Affinity_null_rate"] = (
                float(a.isna().mean()) if len(a) else None
            )

        # ------------------------------------------------------------------
        # Rogue / extra columns vs canonical roster schema
        # ------------------------------------------------------------------
        canonical = {"Sex", "Age", "Relationship", "Generation", "Distance", "Affinity"}
        extras = [c for c in cols if c not in canonical]
        rec["extra_columns"] = extras

        rec["status"] = "ok"
        rec["warnings"] = caught
        rec["elapsed_s"] = round(time.monotonic() - t0, 2)
    except Exception as e:
        rec["status"] = "error"
        rec["error"] = f"{type(e).__name__}: {e}"
        rec["traceback"] = traceback.format_exc()
        rec["warnings"] = caught
        rec["elapsed_s"] = round(time.monotonic() - t0, 2)

    out_path.write_text(json.dumps(rec, indent=2, default=str))
    return rec


if __name__ == "__main__":
    country = sys.argv[1]
    out_dir = sys.argv[2]
    rec = probe(country, out_dir)
    print(json.dumps({"country": country, "status": rec.get("status"),
                      "elapsed_s": rec.get("elapsed_s")}))
