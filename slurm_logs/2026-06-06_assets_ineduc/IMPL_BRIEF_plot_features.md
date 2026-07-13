# Implementation brief — plot_features fill (2026-06-06)

Branch `feature/plot-features-fill` in the MAIN checkout. Venv ./.venv/bin/python.

## HARD RULES
- Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
  **NEVER `dvc pull`/`dvc fetch` CLI** (deadlocks). get_dataframe is lock-free.
- Edit ONLY your ASSIGNED country's dir. No other country/test/baseline/pyproject.
- Run NO git commands. Leave edits in the working tree; coordinator commits.
- Verify in this main checkout (.pth points here).
- **Scan the actual files — don't trust a section's *name*.** (Several triage labels
  were wrong: a "shocks" file was migration, a "housing" section was forest-products.)
  Confirm a file is a plot/parcel roster by reading its variable labels.

## Canonical plot_features (study Uganda / Nigeria / Malawi plot_features for the idiom)
- index **(t, i, plot_id)** — one row per (household, parcel/plot). i MUST match the
  roster's i; plot_id = the parcel/plot identifier.
- Columns (declare the FLEXIBLE subset that exists): `Area` (float), `AreaUnit` (str),
  `Tenure` (str), `TenureSystem` (str), `SoilType` (str), `Irrigated` (bool).
- **Area + plot identity are the core.** If a candidate has no per-plot Area and no
  real plot_id (e.g. it's crop-level keyed by crop, or household-level land totals),
  it is NOT plot_features — report **ABSENT** with the reason; do not fabricate a plot axis.
- Values as human-readable strings/bools/floats (convert_categoricals labels or a mapping).
  Do NOT emit `v`.

## Path choice
- Clean LONG (one row per (HH, plot)) → YAML.
- **WIDE** (one column per plot slot) → needs a reshape **script** (materialize: make).
- **Multi-subsection** (plot basics + area + soil across several files) → script merging
  the subsections on plot_id.

## Steps
1. Confirm the country does NOT already declare plot_features.
2. Read the roster block for the canonical i.
3. Load the plot/parcel module via get_dataframe; CONFIRM it's a plot roster (plot_id +
   area). Map to the canonical subset. Multi-wave: wire each wave with the module.
4. VERIFY: `LSMS_NO_CACHE=1` build `Country('<C>').plot_features()` + `is_this_feature_sane`.
   Confirm rows>0, index (t,i,plot_id), low orphan vs roster, report.ok True.

## Report (<280 words)
SCOPE DEVIATIONS first. Then IMPLEMENTED (files, per-wave source/plot_id/Area+cols, rows,
is_this_feature_sane.ok) OR ABSENT (data reason). Do not commit.
