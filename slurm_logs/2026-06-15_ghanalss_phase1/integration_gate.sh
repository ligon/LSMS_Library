#!/bin/bash
# Phase-1 integration gate: build all 7 GhanaLSS food_acquired wave parquets into
# one cache and check cross-wave canonical-schema consistency + a dry concat.
set -uo pipefail
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
DR=/global/home/users/ligon/.local/share/lsms_library
PY="$REPO/.venv/bin/python"
INTEG="/local/job${SLURM_JOB_ID}/cache-integration"
rm -rf "$INTEG"; mkdir -p "$INTEG"; ln -sfn "$DR/dvc-cache" "$INTEG/dvc-cache"
cd "$REPO"
WAVES="1987-88 1988-89 1991-92 1998-99 2005-06 2012-13 2016-17"
for w in $WAVES; do
  echo "=== build $w ($(date +%H:%M:%S)) ==="
  ( cd "lsms_library/countries/GhanaLSS/$w/_" && LSMS_DATA_DIR="$INTEG" "$PY" food_acquired.py ) 2>&1 | tail -2 || echo "!! build $w FAILED"
done
echo "=== cross-wave consistency check ==="
LSMS_DATA_DIR="$INTEG" "$PY" - <<'PY'
import pandas as pd, os
base=os.environ['LSMS_DATA_DIR']+"/GhanaLSS"
waves="1987-88 1988-89 1991-92 1998-99 2005-06 2012-13 2016-17".split()
CANON=['t','i','j','u','s','visit']
frames={}
for w in waves:
    p=f"{base}/{w}/_/food_acquired.parquet"
    try:
        df=pd.read_parquet(p)
    except Exception as e:
        print(f"{w}: READ FAILED {e}"); continue
    frames[w]=df
    s=sorted(map(str,df.index.get_level_values('s').unique())) if 's' in df.index.names else 'NO s'
    print(f"{w}: rows={len(df):>9}  idx={tuple(df.index.names)}  cols={list(df.columns)}  s={s}")
print("\n-- consistency --")
namesets={frozenset(df.index.names) for df in frames.values()}
print("index-name set uniform across waves:", len(namesets)==1, "->", namesets)
print("column sets:", {w:tuple(df.columns) for w,df in frames.items()})
# dry concat after reordering each to canonical level order
try:
    reordered=[df.reorder_levels([n for n in CANON if n in df.index.names]) for df in frames.values()]
    allc=pd.concat(reordered)
    print(f"DRY CONCAT ok: total rows={len(allc)}  idx={tuple(allc.index.names)}  index_unique={allc.index.is_unique}")
    print("per-wave row totals:", {w:len(df) for w,df in frames.items()})
    print("s distribution (all waves):")
    print(allc.index.get_level_values('s').value_counts().to_string())
except Exception as e:
    print("DRY CONCAT FAILED:", repr(e))
PY
echo "=== GATE DONE ($(date +%H:%M:%S)) ==="
