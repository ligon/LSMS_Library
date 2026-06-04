"""Uganda plot/parcel roster recon for plot_features (#167).

For each wave, probe AGSEC2A (owned parcels) and AGSEC2B (rented/leased
parcels) using lsms_library.local_tools.get_dataframe so we get DVC
auto-pull and consistent categorical handling.

Output: stdout report of columns, dtypes, and value labels for likely
tenure / soil / irrigation columns. Run as

    .venv/bin/python slurm_logs/recon_uganda_plot_2026-05-20.py \\
        > slurm_logs/uganda_plot_recon_2026-05-20.txt 2>&1
"""

from __future__ import annotations

import sys
import os
import traceback
from pathlib import Path

# Run from repo root; data paths are relative to each wave's Data/
REPO = Path(__file__).resolve().parents[1]
os.chdir(REPO)

from lsms_library.local_tools import get_dataframe

WAVES = [
    "2005-06", "2009-10", "2010-11", "2011-12",
    "2013-14", "2015-16", "2018-19", "2019-20",
]

# Candidate parcel-roster filenames per wave.  AGSEC2A = owned,
# AGSEC2B = rented/leased in LSMS-ISA Uganda convention.  2019-20
# uses a lowercase Agric/ subdir.
def candidates(wave: str) -> list[str]:
    base = f"lsms_library/countries/Uganda/{wave}/Data"
    if wave == "2019-20":
        return [
            f"{base}/Agric/agsec2a.dta",
            f"{base}/Agric/agsec2b.dta",
        ]
    return [
        f"{base}/AGSEC2A.dta",
        f"{base}/AGSEC2B.dta",
    ]


# Substrings hinting at columns we care about
HINT_HHID = ("hhid", "HHID")
HINT_PARCEL = ("parcel", "prcid", "parc")
HINT_PLOT = ("plot", "pltid")
HINT_AREA = ("acre", "area", "land", "size", "gps")
HINT_UNIT = ("unit", "_units", "ozza", "kawa")  # acre/ha unit codes
HINT_TENURE = ("tenure", "own", "rent", "share", "acqu", "title", "claim")
HINT_SOIL = ("soil", "quality", "type")
HINT_IRR = ("irrig", "water")
HINT_GPS = ("lat", "lon", "coord", "gps")


def hint_matches(col: str, hints: tuple[str, ...]) -> bool:
    low = col.lower()
    return any(h in low for h in hints)


def probe(path: str) -> None:
    print(f"\n## FILE {path}")
    try:
        df = get_dataframe(path)
    except Exception as e:
        print(f"  ERROR loading: {type(e).__name__}: {e}")
        traceback.print_exc(limit=2, file=sys.stdout)
        return

    print(f"  shape: {df.shape}")
    print(f"  cols ({len(df.columns)}): {list(df.columns)}")

    # Buckets of interest
    def show_bucket(name: str, hints: tuple[str, ...]) -> None:
        cols = [c for c in df.columns if hint_matches(c, hints)]
        if not cols:
            return
        print(f"  {name}:")
        for c in cols:
            dtype = str(df[c].dtype)
            nunique = df[c].nunique(dropna=True)
            nnull = df[c].isna().sum()
            print(f"    {c}  dtype={dtype}  nunique={nunique}  null={nnull}")
            # Show value distribution for low-cardinality / categorical cols
            if dtype == "category" or (dtype == "object" and nunique <= 20):
                vc = df[c].value_counts(dropna=False).head(15)
                for v, n in vc.items():
                    print(f"      {v!r}: {n}")
            elif nunique <= 12:
                vc = df[c].value_counts(dropna=False).head(12)
                for v, n in vc.items():
                    print(f"      {v!r}: {n}")
            else:
                # numeric — show range
                try:
                    desc = df[c].describe()
                    print(f"      describe: min={desc.get('min')!r} "
                          f"median={df[c].median()!r} max={desc.get('max')!r}")
                except Exception:
                    pass

    show_bucket("hhid candidates", HINT_HHID)
    show_bucket("parcel id candidates", HINT_PARCEL)
    show_bucket("plot id candidates", HINT_PLOT)
    show_bucket("area candidates", HINT_AREA)
    show_bucket("unit candidates", HINT_UNIT)
    show_bucket("tenure candidates", HINT_TENURE)
    show_bucket("soil candidates", HINT_SOIL)
    show_bucket("irrigation candidates", HINT_IRR)
    show_bucket("gps candidates", HINT_GPS)


def main() -> None:
    for w in WAVES:
        print(f"\n=== Wave {w} ===")
        for f in candidates(w):
            probe(f)


if __name__ == "__main__":
    main()
