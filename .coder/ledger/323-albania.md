# GH #323 — Albania `cluster_features`

Branch `fix/323-albania`, off `development` (2d3d5f71).

## §1 Task

`_normalize_dataframe_index` silently collapses a non-unique declared index with
`groupby().first()`. Albania's four affected cells, all `cluster_features`:

| wave | source rows | duplicate `(t,v)` tuples |
|------|------------:|-------------------------:|
| 2002 | 3,599       | 3,149                    |
| 2003 | 8,679       | 8,229                    |
| 2004 | 1,797       | 1,347                    |
| 2005 | 3,840       | 3,360                    |

## §2 Instrument validation (done FIRST)

The L2-**country** parquet (`var/`) is written POST-collapse and is a poisoned
instrument. Scanned the L2-**wave** parquets with the FULL declared index.
Validated on the brief's known positives before trusting any Albania number:

- `Mali/2014-15/household_roster` → 32,026 dup rows ✓ (expected 32,026)
- `Guyana/1992/housing` → 311 ✓ (expected 311)

Both hit, so the Albania figures above are trustworthy (they reproduce the brief).

## §3 Diagnosis — `EXTRACTION_BUG`, and a class-1 on top

`cluster_features` is CLUSTER grain (index `(t, v)`), but every wave extracts it
from a **household** cover page — in 2003, from the **person** roster — while
declaring an extra household `idxvars` entry that is not in the declared index.
The extra level is then dropped and the leftover duplicates collapsed. Nothing is
*missing* from the declared index; the extraction **over-emits** rows it should
never have produced.

Value-preservation, measured on the RAW `.dta` (not the poisoned cache):

| wave | column(s)             | clusters with >1 distinct value |
|------|-----------------------|--------------------------------:|
| 2002 | `m0_q1a`, `m0_ur`     | 0 / 450 → preserving            |
| 2003 | `STRATUM`             | 0 / 449 → preserving            |
| 2005 | `m0_stratum`, `m0_ur` | 0 / 480 → preserving            |
| 2004 | `m0_distr`            | **16 / 448 → NOT preserving**   |

**2004 is silently WRONG (class-1).** `m0_q01` is the household's *original 2002
PSU* (verified: 1,713 of 1,714 non-sentinel `(m0_q01, m0_q02)` keys exist in
2002's `(psu, hh)`); `m0_distr` is its *current* district. A household that moved
keeps its PSU code and acquires a new district, so `.first()` handed a mover's
district to the whole cluster.

`m0_orhh` does **not** identify the movers (all conflicted households are flagged
`yes` = original), so 2004 carries **no internal signal** that recovers a
cluster's district. The 2002 anchor is the only deterministic source.

Two findings the original diagnosis missed:

1. **Single-household mover PSUs.** 49 of the 448 real PSUs hold exactly one
   household. If that household moved, the PSU looks perfectly "unanimous" and
   **no within-2004 consistency check can see it**. PSU 223 is exactly this: one
   household, reported ELBASAN, truly GRAMSH. Only the 2002 anchor exposes it.
2. **2003 null-`v`.** 711 person rows carry a NULL PSU; the silent collapse
   turned them into a `v = NaN` cluster row. 2003 has **449** clusters, not 450.

Also: two administrative sentinels, `m0_q01 ∈ {995, 999}` (39 split-off + 44
moved/untraceable households; **0 of the 83** key into 2002), were promoted into
2 fabricated clusters — both labelled `BERAT`, pooling 83 households across 23
districts.

## §4 Prior art consulted

- `CLAUDE.md` — `sample()` / `v` ownership; `cluster_features` owns `v`.
- `lsms_library/data_info.yml` — canonical index for `cluster_features`.
- `country.py:_normalize_dataframe_index` — the #323 collapse + its warning.
- **Rejected: the `aggregation:` block.** `data_scheme.yml`'s `aggregation:` key
  is read by **nothing** in the library — it appears only in `_skip` sets
  (`country.py:2387`, `diagnostics.py:174`). Declaring the reduction there would
  be *prose, not enforcement* — precisely the failure mode the brief warns about.
  Hence an explicit, asserted reducer in code.

## §5 Fix

**Part 1 — make the grain explicit and CHECKED (all 4 waves).**
Dropped the extra household `idxvars` from each wave's `data_info.yml` and moved
`cluster_features` to `materialize: make` with a per-wave `_/cluster_features.py`.
Each reduces via `albania.cluster_reduce`, which **requires exactly one distinct
non-NA value per (cluster, column)**, emitting `<NA>` + a `RuntimeWarning`
otherwise, and drops null-`v` rows loudly. Deleting `i:` alone would NOT have
sufficed — the rows stay household-grain and would still be collapsed silently.

For 2002/2003/2005 the assertion passes unchanged: a pure no-op refactor turning
a silent collapse into a checked one.

**Part 2 — the 2004 semantic fix.**
- Sentinels 995/999 excluded from `cluster_features`; `sample.py` now emits
  `v = <NA>` for those 78 weighted households (they are kept — only their unknown
  cluster id is dropped).
- `Region` anchored on the **2002 psu→district map** (single-valued for all 450
  PSUs), translated to 2004's district *names* via a code→name map learned only
  from PSUs that are unanimous **and hold ≥2 households** (the ≥2 guard is
  essential — without it PSU 223's lone mover poisons the map). The map is
  **36 codes → 36 names, 0 ambiguous**; any unresolvable code yields `<NA>`, never
  a guess. Mojibake `KOR\x80E` normalized to `KORCE`.
- Majority vote was rejected: it **fails outright** on PSUs 43/44/47/52/53 (each a
  1-vs-1 KUKES/TIRANE tie). All five are 2002 code 17 = KUKES, so the anchor
  resolves them where `first()` and `mode()` cannot.

**Part 3 — the CLASS (the important one).**
`Wave.cluster_features()` (`country.py:1168`) collapses the household level with
`.first()` **before** `_normalize_dataframe_index` ever runs — so the #323
duplicate warning **never fires for `cluster_features`, in ANY country**, not even
on a cold build. Its comment asserted the false premise Albania 2004 disproves
("Region/Rural/District are invariant within a cluster by construction of the
LSMS-ISA sampling design").

The reducer is left alone (changing it would move data in countries not under
review), but the collapse is no longer silent where it is actually destroying
information. Cold-run blast radius of the new warning:

| country | waves warning | |
|---------|---|---|
| Guyana 1992, Uganda 2019-20, Kazakhstan 1996, Niger 2014-15 | 1 each | same class-1 as Albania 2004 |
| Malawi 2004-05 / 2013-14 / 2016-17 / 2019-20 | 4 | same |
| Kosovo, Tajikistan, Cambodia | 0 | collapse is value-preserving (low noise) |

## §6 Evidence

`Country('Albania').cluster_features()`, BEFORE → AFTER:

| wave | before | after | |
|------|-------:|------:|---|
| 2002 | 450 | 450 | values identical (checked, not assumed) |
| 2003 | 449 | 449 | values identical |
| 2004 | **450** | **448** | 2 phantom clusters removed |
| 2005 | 480 | 480 | values identical |

Duplicate `(t,v)` tuples: 0 → 0 at the API (the collapse always hid them); the
**source** duplicates 3,149 / 8,229 / 1,347 / 3,360 are now reduced by a checked
reducer instead of a silent `.first()`.

2004 `Region` corrected on **6** PSUs (validated where validation was NEEDED —
the ambiguous rows, not the ~430 clean ones):

| PSU | was | now | why |
|-----|-----|-----|-----|
| 16  | DURRES  | SHKODER     | 4 vs 1 mover |
| 223 | ELBASAN | GRAMSH      | **single-HH mover — invisible to any within-2004 check** |
| 259 | FIER    | MALLAKASTER | 3 vs 1 mover |
| 280 | TIRANE  | SKRAPAR     | 5 vs 1 mover |
| 297 | KOLONJE | KORCE       | 6 vs 1 mover |
| 344 | SARANDE | TEPELENE    | 3 / 2 / 2 split |
| 995, 999 | BERAT | **absent** | not clusters |

PSUs 43/44/47/52/53 stay KUKES — but now *by construction* (2002 code 17) rather
than by luck of source row order.

**ROWS_RECOVERED = 0, and that is the honest answer.** Albania is not a row-loss
case: `cluster_features` *should* be 450/449/448/480. The ~16k "dropped" rows were
redundant household/person copies of cluster attributes. The defect was (a) an
undeclared silent collapse in all 4 waves and (b) genuinely wrong values in 2004.
A correct fix *decreases* 2004 from 450 to 448.

## §7 Regression — nothing else moved

Albania, all tables, pristine `development` vs this branch:

- **Byte-identical**: `assets`, `housing`, `individual_education`, `plot_features`,
  `shocks`, `subjective_well_being`.
- `sample` / `household_roster` / `interview_date` / `household_characteristics`:
  the **only** changed column is `v` — 995/999 → `<NA>` (→ `Mover` sentinel in
  `household_characteristics`) on exactly 78 households / 373 roster rows. All
  other columns byte-identical, keyed on the stable `(t, i)`.
- Other countries (`cluster_features`, cold, md5 of full frame): Guyana, Uganda,
  Malawi, Kazakhstan, Niger, Kosovo, Tajikistan, Cambodia — **all identical**
  pristine vs branch. The `country.py` change adds a warning only; it moves no data.

## §8 Traps hit (for the next agent)

1. **The main checkout was on another agent's branch** (`fix/602-spellings`), not
   `development`. Comparing against it manufactured a phantom "Malawi regression"
   (`Rural` labels → floats). Baseline against a **pristine tree built from the
   merge-base commit**, never against whatever the shared checkout happens to be on.
2. **`countries_root()` follows the package**, so redirecting `sys.path` to a
   worktree also redirects the *config* — you cannot mix "worktree library +
   pristine config" by accident.
3. Sorting a frame by an index level you just changed (`v`) and then diffing
   positionally manufactures fake column diffs. Key the diff on a **stable** key.
4. `pytest`/`python -c` from cwd=worktree beats the `.pth` pin (sys.path[0] = cwd);
   `PYTHONPATH` alone does not. Assert `'worktrees' in lsms_library.__file__`.
5. Upgrade path verified: a warm **pre-fix** cache + post-fix code auto-rebuilds
   (v0.8.0 content hash covers `data_scheme.yml` + `data_info.yml` + scripts). No
   manual `cache clear` needed.

---

## Stripped to config-only (2026-07-13, GH #323 consolidation)

This branch (`fix/323-albania-config`, off `origin/development`) carries the
country work from `fix/323-albania` and **nothing else**. Per the decisions in
`slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`:

* **Stripped: 53 lines of `lsms_library/country.py`.** The original branch added
  a destruction warning to `Wave.cluster_features` (GH #323 **Site 2**). Site 2 is
  owned centrally by its own PR; core is not patched per-country. **D1: core does
  not aggregate.**
* **Kept, and it stands alone:** the four wave scripts, `albania.cluster_reduce`
  (a *country-level* helper — the right place for an explicit reduction), the
  2004 semantic fix, the sentinel-PSU drop, and `tests/test_albania_cluster_features.py`
  (20 tests, all country-level; none touches the stripped mechanism).
* No `aggregation:` key was ever added here — the original branch had already
  concluded the key is read by nothing and documents a policy without enforcing it.

Verified post-strip against the worktree config tree (`LSMS_COUNTRIES_ROOT`,
`LSMS_NO_CACHE=1`): clusters 450 / 449 / 448 / 480 for 2002 / 2003 / 2004 / 2005;
sentinels 995 & 999 absent; PSUs 43/44/47/52/53 -> KUKES; 16 -> SHKODER,
259 -> MALLAKASTER, 280 -> SKRAPAR, 297 -> KORCE, 344 -> TEPELENE;
223 -> GRAMSH (the lone-mover PSU); no mojibake; `sample()` leaks 0 sentinel `v`.
