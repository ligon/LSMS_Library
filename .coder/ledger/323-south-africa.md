# GH #323 — South Africa

**Class: INTENDED_AGGREGATION.** The collapse is lossless; 0 rows to recover.
The defect is that a *wanted* reduction was performed **silently and unasserted**.

## §1 The cell

| wave | table | source rows | declared index | dup rows | kept |
|---|---|---|---|---|---|
| 1993 | `cluster_features` | 8,809 | `(t, v)` | **8,454** | 355 |

Every other South Africa table is clean (dup = 0): `sample` 8,809, `housing` 8,808,
`household_roster` 43,687, `individual_education` 43,687, `assets` 35,187,
`interview_date` 8,809.

## §2 Why this is NOT the Guyana (INDEX_INCOMPLETE) case

`Data/STRATA2.dta` is a **household-level** stratification file:

- 8,809 rows, `hhid` **unique** (`is_unique == True`), carrying 355 distinct `clustnum`.
- `cluster_features` is by contract a **cluster-level** table: `_/data_scheme.yml`
  declares `index: (t, v)` with columns `Region` / `Rural`, both cluster attributes.
  **355 rows is the correct answer.**
- So reading it is inherently many-to-one (mean 24.8 households/cluster, range 3–120).

**Decisive test on the raw source:** across all 355 clusters, `newprov` has
`nunique == 1` and `type` has `nunique == 1` — **0/355 clusters vary on either**.
Consequently `groupby().first()` is byte-identical to a full `drop_duplicates()`
(355 rows, `.equals() == True`). The 8,454 dropped rows are **exact redundant
repetitions, not distinct entities**. The collapse is **lossless**.

Corroboration: `sample` reads the *same* STRATA2.dta at household level and does
**not** collapse (8,809 rows, 8,809 unique `(i,t)`) — exactly as predicted if
`hhid` is unique.

## §3 The actual defect, and the fix

`first` returns the right answer here **only by accident of the data being clean**.
That is a latent class-1 (silently WRONG) risk: a future wave, or a source
revision, in which one cluster's households disagree would silently bin a value.

**Fix (two parts):**

1. **`lsms_library/country.py` — make `aggregation:` load-bearing.** It was read by
   **zero** code paths (Malawi's own block said so: *"nothing reads this yet"*).
   Declaring without wiring would have been pure prose — the exact failure mode the
   standard names (*"prose is not enforcement"*). `_normalize_dataframe_index` now
   consults `aggregation:` at the duplicate-collapse point, keyed on the index
   level(s) being **collapsed away**. Reducers: `unique` (ASSERT-CONSTANT — raise if
   any column varies within a group) and `first` (historical default, stated
   deliberately). No policy ⇒ unchanged behavior + the existing #323 warning.

2. **`South Africa/_/data_scheme.yml`** declares `aggregation: {i: unique}` on
   `cluster_features`.

`unique`, not `first`: it converts the latent silently-WRONG risk into a loud
failure — the class-2 behavior the standard prefers.

### Deviation from the diagnosis (deliberate)

The diagnosis's step 2 said to drop `i: hhid` from the 1993 `cluster_features`
`idxvars`. **I kept it, on purpose.** The diagnosis itself concedes step 2 is
"hygiene, not the fix" and that dropping `i` does **not** remove the many-to-one
(you still get 8,809 rows indexed by `v`). But `i` is precisely the level that
names *what is being collapsed* — it is what makes the reduction **expressible and
enforceable**. Drop it and `dropped_levels` is empty, the policy never fires, and
the build falls back to the silent `first`, **re-arming the bug**. `idxvars` states
the SOURCE grain; `data_scheme` states the TARGET grain; `aggregation` states the
REDUCTION between them. A comment at both sites warns against "cleaning up" `i`.

## §4 Numbers

**Instrument validated on known positives before use** (L2-**wave** parquet, the
truth — `var/` is written post-collapse and reports zero):
Mali/2014-15 `household_roster` → 32,026 dups ✓ · Guyana/1992 `housing` → 311 ✓.

| | before | after |
|---|---|---|
| `cluster_features` rows (cold build) | 355 (silent, warned) | **355 (declared, asserted)** |
| duplicate index tuples | 8,454 collapsed silently | **0** |
| GH #323 warnings | 1 | **0** |
| vs. raw-source `drop_duplicates` | — | **355/355 clusters, `Rural` agrees 355/355** |

Row count is *unchanged by design* — 0 rows recover, because 355 is correct. What
changes is that the reduction is now **declared and machine-checked**.

**Guard is not vacuous** (mutation test on the real 8,809-row frame): unmutated →
collapses cleanly to 355; flip **one** household's `newprov` to a different valid
province code → **raises**. Validated on the rows where it matters, over all 355
groups, not a trivially-determined subset.

## §5 Regression

Static gate over **400 tables / 40 countries**: the new path fires only for a table
whose `aggregation:` key is a level **not** in its declared index. Exactly **one**
table qualifies — South Africa `cluster_features`. The 9 pre-existing
`visit: first` blocks (Albania, Benin, Burkina Faso, CotedIvoire, Guinea-Bissau,
Malawi, Niger, Senegal, Togo — all on `interview_date`) are **inert**: `visit` *is*
in their declared index, so it is never collapsed. Confirmed empirically (Malawi
69,484 / Albania 9,236 rows, `visit` retained) and pinned by a test.

## §6 Tests — `tests/test_declared_aggregation.py`

7 tests. **Pre-fix (stashed): 5 failed, 2 passed. Post-fix: 7 passed.** The 2 that
pass in both are the backward-compat guards (undeclared collapse still warns; a
policy naming a *declared* level stays inert).

## §7 Environment note (worth propagating)

CLAUDE.md says "`PYTHONPATH` alone does NOT redirect imports to a worktree." The
real mechanism is **cwd shadowing**: `lsms_library.pth` adds the main repo as a
plain `sys.path` entry (index 6), but `sys.path[0]` is `''`/script-dir. Running
from the main repo made cwd win. From a **neutral cwd**, `PYTHONPATH=<worktree>`
*does* win (verified: `lsms_library.__file__` resolves into `worktrees/`), so
library-code edits **can** be verified without building a worktree venv. Always
assert `'worktrees' in lsms_library.__file__`.

## §8 Tangential (not #323, not fixed)

`Region` is emitted as the raw `newprov` code stringified (`'1'`..`'9'`), not
decoded to province names, despite `data_scheme` declaring `Region: str`. Flagged
only; out of scope.
