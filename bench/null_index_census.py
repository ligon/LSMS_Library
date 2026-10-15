"""GH #847 census: how many (country, table) cells carry a NaN on a declared
index level on the warm corpus?

Run: ``python bench/null_index_census.py``.  Reads every L2-country parquet
under ``data_root()`` (never builds), applies the guard's own
``audit_index_levels`` with the same union of country-scheme + canonical
levels ``Country._finalize_result`` passes, and reports per-cell counts.

The census is over WARM L2 parquets only -- a country with no warm cache is
not measured, matching how ``null_read_audit``'s sweep handled the corpus
(what is not built cannot be measured from the parquet side, and building it
here would make this script a corpus rebuild).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lsms_library.local_tools import data_root  # noqa: E402
from lsms_library.country import (  # noqa: E402
    _canonical_index_levels, _canonical_index_level_aliases,
    _declared_index_levels)
from lsms_library.null_index_audit import audit_index_levels  # noqa: E402
from lsms_library.yaml_utils import load_yaml  # noqa: E402


def _scheme_entry(root: Path, country: str, table: str):
    fn = root / country / "_" / "data_scheme.yml"
    if not fn.exists():
        return None
    try:
        with open(fn) as f:
            data = load_yaml(f)
    except Exception:
        return None
    return (data or {}).get(table)


def main() -> int:
    root = Path(data_root())
    canonical = _canonical_index_levels()
    aliases = _canonical_index_level_aliases()

    cells = 0
    fired = []
    for country_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        country = country_dir.name
        var = country_dir / "var"
        if not var.is_dir():
            continue
        for parquet in sorted(var.glob("*.parquet")):
            table = parquet.stem
            try:
                df = pd.read_parquet(parquet)
            except Exception as exc:
                print(f"  [unreadable] {country}/{table}: {exc}")
                continue
            cells += 1
            levels = list(_declared_index_levels(_scheme_entry(country_dir, country, table) or {}))
            for lvl in canonical.get(table, ()):
                if lvl not in levels:
                    levels.append(lvl)
            for a, c in (aliases.get(table) or {}).items():
                if c in levels and a not in levels:
                    levels.append(a)
            reports = audit_index_levels(df, levels, country=country,
                                         table=table)
            for rep in reports:
                fired.append(rep)

    print(f"\n=== {len(fired)} firing cells of {cells} warm L2 tables ===")
    total_rows = 0
    for rep in fired:
        bits = "; ".join(
            f"{f['level']}={f['n_nan']}"
            + (f" {sorted(f['waves'])}" if f.get("waves") else "")
            for f in rep["findings"])
        total_rows += sum(f["n_nan"] for f in rep["findings"])
        print(f"{rep['country']}/{rep['table']}: {bits}")
    print(f"total NaN-keyed (row, level) findings: {total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
