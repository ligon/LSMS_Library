"""Per-(country, feature) probe for canonical LSMS Library tables.

Calls ``Country(name).<feature>()`` via the full API pipeline and writes a
JSON diagnostic to ``out_dir/{country}__{feature}.json``.

Designed for multiprocessing.Pool fan-out; all imports are inside the worker
function so every pool process builds its own module state.
"""
from __future__ import annotations

import json
import sys
import time
import traceback
import warnings
from pathlib import Path


def probe(country: str, feature: str, out_dir: str) -> dict:
    safe_country = country.replace(" ", "_")
    out_path = Path(out_dir) / f"{safe_country}__{feature}.json"
    rec: dict = {"country": country, "feature": feature, "status": "unknown"}
    t0 = time.monotonic()
    caught: list[str] = []

    try:
        import pandas as pd
        import yaml
        import lsms_library as ll
        from importlib.resources import files as _files

        # ------------------------------------------------------------------
        # Load canonical schema from data_info.yml
        # ------------------------------------------------------------------
        di_path = _files("lsms_library") / "data_info.yml"
        with open(di_path, "r", encoding="utf-8") as f:
            data_info = yaml.safe_load(f) or {}

        columns_schema = (data_info.get("Columns") or {}).get(feature, {})
        # spellings: {canonical: [variants]}
        all_spellings: dict[str, set[str]] = {}
        for col, col_def in columns_schema.items():
            if isinstance(col_def, dict) and col_def.get("spellings"):
                sp = col_def["spellings"]
                for canonical, variants in sp.items():
                    all_spellings.setdefault(col, {})[canonical] = set(
                        v for v in (variants or [])
                    )

        # ------------------------------------------------------------------
        # Check whether this country declares the feature
        # ------------------------------------------------------------------
        from lsms_library.paths import COUNTRIES_ROOT
        ds_path = COUNTRIES_ROOT / country / "_" / "data_scheme.yml"
        if not ds_path.exists():
            rec["status"] = "skip_no_scheme"
            rec["elapsed_s"] = round(time.monotonic() - t0, 2)
            out_path.write_text(json.dumps(rec, indent=2, default=str))
            return rec

        class _L(yaml.SafeLoader):
            pass
        _L.add_constructor("!make", lambda l, n: {"__make__": True})
        with open(ds_path, "r", encoding="utf-8") as f:
            scheme = yaml.load(f, Loader=_L) or {}
        ds = scheme.get("Data Scheme") if isinstance(scheme, dict) else None
        if not isinstance(ds, dict) or feature not in ds:
            rec["status"] = "skip_not_declared"
            rec["elapsed_s"] = round(time.monotonic() - t0, 2)
            out_path.write_text(json.dumps(rec, indent=2, default=str))
            return rec

        # ------------------------------------------------------------------
        # Call the API
        # ------------------------------------------------------------------
        with warnings.catch_warnings(record=True) as wlist:
            warnings.simplefilter("always")
            c = ll.Country(country)
            method = getattr(c, feature)
            df = method()

        for w in wlist:
            caught.append(f"{w.category.__name__}: {w.message}")

        # ------------------------------------------------------------------
        # Empty result
        # ------------------------------------------------------------------
        if df is None or (hasattr(df, "empty") and df.empty):
            rec.update(status="empty", rows=0, columns=[], index_names=[])
            rec["warnings"] = caught
            rec["elapsed_s"] = round(time.monotonic() - t0, 2)
            out_path.write_text(json.dumps(rec, indent=2, default=str))
            return rec

        cols = list(df.columns)
        rec["rows"] = int(len(df))
        rec["columns"] = cols
        rec["index_names"] = list(df.index.names)
        rec["dtypes"] = {c2: str(df[c2].dtype) for c2 in cols}

        # ------------------------------------------------------------------
        # Per-canonical-column checks
        # ------------------------------------------------------------------
        col_checks: dict[str, dict] = {}
        for col, col_def in columns_schema.items():
            if col not in cols:
                col_checks[col] = {"present": False}
                continue
            s = df[col]
            check: dict = {
                "present": True,
                "dtype": str(s.dtype),
                "n_non_null": int(s.notna().sum()),
                "n_null": int(s.isna().sum()),
                "non_null_rate": float(s.notna().mean()) if len(s) else None,
            }

            # Unique count (capped to avoid huge sets)
            try:
                uniq_vals = sorted({str(x) for x in s.dropna().unique().tolist()})
                check["n_unique"] = len(uniq_vals)
                check["sample_unique"] = uniq_vals[:20]
            except Exception:
                check["n_unique"] = None

            # Spellings violations
            if col in all_spellings:
                canonical_set: set[str] = set(all_spellings[col].keys())
                # Collect all variant strings too
                variant_map: dict[str, str] = {}
                for canon, variants in all_spellings[col].items():
                    for v in variants:
                        variant_map[v] = canon
                actual_strs = {str(x) for x in s.dropna().unique().tolist()}
                violations = [
                    v for v in actual_strs
                    if v not in canonical_set
                    and v not in variant_map
                    and v not in {"", "<NA>", "nan", "None", "NaT"}
                ]
                check["spellings_violations"] = violations[:50]
                check["n_spellings_violations"] = len(violations)
                if violations:
                    try:
                        mask = s.astype(str).isin(set(violations))
                        check["violation_counts"] = (
                            s[mask].astype(str).value_counts().head(20).to_dict()
                        )
                    except Exception:
                        check["violation_counts"] = {}

            col_checks[col] = check

        rec["canonical_columns"] = col_checks

        # ------------------------------------------------------------------
        # Extra (non-canonical) columns
        # ------------------------------------------------------------------
        canonical_names = set(columns_schema.keys()) if columns_schema else set()
        rec["extra_columns"] = [c2 for c2 in cols if c2 not in canonical_names]

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
    feature = sys.argv[2]
    out_dir = sys.argv[3]
    rec = probe(country, feature, out_dir)
    print(json.dumps({
        "country": country,
        "feature": feature,
        "status": rec.get("status"),
        "elapsed_s": rec.get("elapsed_s"),
    }))
