# REAL-BUILD PROTOCOL (mandatory for all feature-implementation + build-verifier agents)
# Validated 2026-06-04. Fixes the verification gap that let the plot_features make-path
# bugs (Malawi doubled DVCFS path, missing Makefile rules) pass per-country "verification".

## WHY (root cause)
The venv pins `lsms_library` to the MAIN checkout via a single-line `.pth`. Agents previously
"verified" by running wave scripts from the WORKTREE CWD — but `sys.path[0]=''` puts CWD first,
so even the MAIN venv imports worktree code there. is_this_feature_sane FALSELY passed; the
framework's `make` build then changes CWD and falls back to the main checkout / breaks relative
paths. => verification must use a WORKTREE-PINNED venv, run from a NEUTRAL CWD, on a COLD cache.

## RECIPE: worktree-pinned venv (~1.6s, node-local)
```bash
WT=$(git rev-parse --show-toplevel)
SRC=$(readlink -f /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.venv)   # /local/jobNNN/venv
DEST="$(dirname "$SRC")/venv_$(basename "$WT")"        # sibling, node-local (NOT on Lustre/$WT)
rm -rf "$DEST"; cp -a "$SRC" "$DEST"
printf '%s\n' "$WT" > "$DEST"/lib/python3.11/site-packages/lsms_library.pth   # generalize python3.11 if needed
# sanity (from neutral CWD!):
( cd /tmp && "$DEST/bin/python" -c "import lsms_library as ll; print(ll.__file__)" )  # MUST be under .../worktrees/<this wt>/
# ... do real builds with $DEST/bin/python ...
rm -rf "$DEST"   # cleanup when worktree done
```

## MANDATORY verification steps for an implementation/verifier agent
1. Build the pinned venv (above). Confirm `ll.__file__` is INSIDE the worktree FROM /tmp (neutral CWD).
2. Clear the feature's cache for the country: `lsms-library cache clear --country <C>` (or rm the var/ + wave _/ parquets), OR run with `LSMS_NO_CACHE=1` — so a stale L2 parquet can't shadow your code.
3. Build through the REAL framework API from a NEUTRAL CWD:
   `( cd /tmp && LSMS_NO_CACHE=1 "$DEST/bin/python" -c "import lsms_library as ll; df=ll.Country('<C>').<feature>(); from lsms_library.diagnostics import is_this_feature_sane; r=is_this_feature_sane(df, country='<C>', feature='<feature>'); r.summarize(); assert r.ok; print(df.shape)" )`
   This exercises the materialize:make build path (Makefile rules, run_make_target) — NOT a direct script run.
4. Also run `Feature('<feature>')(['<C>'])` to confirm cross-country aggregation sees it.
5. Only after the REAL build passes cold is the PR considered done.

## GOTCHAS
1. CWD masks the bug — ALWAYS validate from /tmp (or any dir without an `lsms_library/`).
2. Keep $DEST node-local (under /local/jobNNN/), never on Lustre/$WT — else every import round-trips Lustre.
3. Node-locality is ephemeral: if the agent lands on a new node the clone goes stale; recreate (1.6s).
4. Cache is shared across agents: use LSMS_NO_CACHE=1 (or LSMS_BUILD_BACKEND=make) to actually exercise the build.
5. Recipe assumes python3.11; check `ls "$DEST"/lib/` if the minor version differs.

## DELEGATED VERIFICATION
Every implementation PR gets an independent build-verifier agent that runs steps 1-4 on a COLD cache
from a fresh worktree of the PR branch, and reports pass/fail + the framework build output. The
coordinator does NOT inline-verify; the verifier red-teams the build.
