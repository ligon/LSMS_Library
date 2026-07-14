# Prior-Art Ledger — GH #548 (panel-id collisions in bespoke `panel_ids.py`)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md` (§0 baseline).

**Search tier used:** ripgrep + git (floor). gitnexus not consulted (call-graph
here is one script + two `local_tools` functions; grep was sufficient and the
line anchors below were re-grepped, not remembered).

## §1 Task, restated

`GhanaLSS/_/panel_ids.py` is a **bespoke country-level script** (`panel_ids: !make`
in `data_scheme.yml:108`) that writes `_/updated_ids.json` + `_/panel_ids.json`
**directly**, instead of routing its `{cur_i: prev_i}` linkage through
`local_tools.panel_ids()` → `local_tools.update_id()` the way the YAML-path
countries (Malawi) do. `PANELC.DAT` is **person-level** and records GLSS1
households that **split** into two GLSS2 households; the script's
`dict(zip(cur_i, prev_i))` therefore encodes a **many-to-one** map (two live
1988-89 households → one 1987-88 id). `id_walk` renames both onto the same id and
`Country._normalize_dataframe_index`'s `groupby().first()` / additive `sum()`
collapses them — *inside* `load_from_waves`, i.e. **before** the L2-country
parquet is written, so the loss is baked into the cache and warm reads are silent.

**Not** the mechanism #548 states (it says "rename-onto-occupied"); the targets
`101332` / `114008` are 1987-88 ids and are absent from the live 1988-89 id set,
so #536's `cur_set` guard is a **no-op** here. Verified below.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `local_tools.update_id` | `lsms_library/local_tools.py:1731` | inverts `{cur: prev}`, and when N cur-ids share one prev, mints `prev`, `prev_1`, … (`_N` split convention). Carries the **GH #504** guard: never mint a suffix equal to a live target or an already-emitted id. | `tests/test_id_walk.py`, `tests/test_update_id*.py` | **REUSE** — this is the guard GhanaLSS bypasses |
| `local_tools.panel_ids` | `lsms_library/local_tools.py:1788` | wave-sequenced driver: builds the `RecursiveDict` chain from the **pre-suffix** prev-ids, then calls `update_id` per wave | via country builds | **REUSE** (Burkina uses the DataFrame form with a dummy prior-wave row) |
| `local_tools._close_id_map` / `id_walk` | `local_tools.py:1843` / `1899` | closure-resolve + apply the per-wave rename; idempotent since #547 | `tests/test_id_walk.py` | **DO NOT TOUCH** (blast radius = every panel country) |
| `Country._normalize_dataframe_index` | `country.py:~4158` | `groupby(idx).first()` / additive `sum()` — the *silent* collapse that turns a duplicate `(i,t)` into a wrong number | — | leave; it is the amplifier, not the cause |
| `Mali/_/panel_ids.py:122` (`cur_set` from the **full** cover) | — | the **#536** guard: skip a rename whose target is a *live current-wave* id | PR #546 | **EXTEND** to GhanaLSS as defense-in-depth (0 hits today) and to Burkina (latent) |
| `Burkina_Faso/_/panel_ids.py:65` | — | same guard but built from **panel-only** candidates (the *unfixed* #536 pattern) | — | **HARDEN** (byte-identical today) |
| `diagnostics._check_panel_ids_targets_exist` | `lsms_library/diagnostics.py:1372` | asserts a chain entry's two endpoints canonicalise to the **same** id | run via `check_panel_consistency` | **EXTEND** — false-fails on *every* split household (Malawi: 1410/5686 = 25% today) |

## §3 Definitions & conventions in force

- **Split-household id convention** = `prev_id`, `prev_id_1`, `prev_id_2`, … —
  minted by `update_id` (`local_tools.py:1731-1785`, docstring example). Malawi
  IHPS ships 896/996/1726 such ids across its three panel waves. GhanaLSS must
  use the same convention, not a bespoke one.
- **`panel_ids` (the `RecursiveDict`) stores PRE-suffix prev-ids** for the wave
  being processed: `local_tools.panel_ids:1838` does `recursive_D.update(...)`
  *before* `update_id` is applied at line 1839. Semantically right: `101332_1` is
  a **1988-89** canonical id; no such household exists in 1987-88. `updated_ids`
  (the rename map) carries the suffix; `panel_ids` (the lineage chain) does not.
- **`updated_ids` = per-wave `{raw_cur_id: post-walk canonical id}`**, consumed by
  `id_walk` (`local_tools.py:1899`) inside `_finalize_result`.
- Cache: `.py`/`.json` under a country `_/` are in `_BUILD_INPUT_SUFFIXES`
  (`country.py:448`) and feed `_table_cache_hash` (`country.py:~2285`) — editing
  `panel_ids.py` / `updated_ids.json` busts the L2-country hash for **every**
  GhanaLSS table, so the rebuild is automatic. Per `CLAUDE.md` §Cache Behavior.

## §4 Invariants & assumptions

- `STANDING.md §4` (all of it), plus:
- **`Country.updated_ids` is a `JSON_CACHE_METHOD`** (`country.py:75`) and
  `load_json_cache` prefers `data_root()/{C}/_/updated_ids.json` over the in-tree
  file (`country.py:2508-2511`) — and JSON caches carry **no content hash**. If
  such a copy exists it **shadows** the regenerated map and this fix is silently
  inert. Verified absent on this machine (`~/.local/share/lsms_library/GhanaLSS/_/`
  does not exist); re-check before believing any AFTER number.
- The rename map must stay **injective** after the fix (no two live cur-ids →
  one canonical id), and no minted `_N` id may equal a live 1988-89 cover id.
  Both are now **asserted in the script** — a class-1 failure is converted to a
  class-3 crash.
- GhanaLSS panel linkage exists **only** GLSS1↔GLSS2; the other five waves ship
  empty maps (`panel_ids.py` docstring). Anything that changes a non-1988-89
  wave is a bug in the fix.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| split-id minting (`_N`) | **reuse** `local_tools.update_id` | the tested guard (#504) the bespoke script bypasses; hand-rolling a suffixer is the reinvention this ledger exists to prevent |
| chain `RecursiveDict` + per-wave rename map | **reuse** `local_tools.panel_ids` (DataFrame form + dummy prior-wave row, the Burkina pattern, `Burkina_Faso/_/panel_ids.py:40-46`) | one call gives both artifacts with the framework's own suffix/chain semantics |
| rename-onto-occupied guard | **extend** the #536 full-cover `cur_set` (`Mali/_/panel_ids.py:122`) | 0 hits today for GhanaLSS; keeps the two complementary guards *both* present so the class is closed, not half-closed |
| split-tolerant chain check | **extend** `diagnostics._check_panel_ids_targets_exist` | it false-fails on the framework's own `_N` convention (Malawi 25%); without this the fix would *look* like a regression |
| `id_walk` / `_close_id_map` / `update_id` internals | **new: NO** | DANGER ZONE — patching them puts 13 panel countries in the blast radius for a defect in one bespoke script |

## §6 Open questions for the human

- **Niger's 59 duplicate `(i,t)` tuples are NOT this bug** (pre-walk, from
  `EXTENSION ∈ {0,1,2}` in `ECVMA2_MS00P1.dta` while the wave declares
  `i: [GRAPPE, MENAGE]`). Needs a separate issue: *household identity omits
  EXTENSION*. Not fixed here.
- Which split-off keeps the base id (`101332`) vs. `101332_1` is decided by
  `PANELC.DAT` row order (first-seen wins) — the same rule `update_id` applies
  everywhere. If Ghana's survey documentation designates a "continuing"
  household, that would be a better rule; no such variable is in `PANELC.DAT`.

---
### Phase 3 — verification

- `GhanaLSS/_/panel_ids.py` — **OK (anchored on §2/§5)**: now calls
  `local_tools.panel_ids()` (hence `update_id`); no hand-rolled suffixer, no
  touch to `id_walk`. `panel_ids.json` is byte-identical (§3: chain stores
  pre-suffix ids); `updated_ids.json` changes exactly 2/714 entries
  (`204922: 101332 -> 101332_1`, `255718: 114008 -> 114008_1`).
- `GhanaLSS` full-cover `cur_set` guard — **OK (anchored on §5)**: reuses #536's
  formula (`prev in cur_set and cur != prev`) over `Y00A.DAT` (the 1988-89
  cover, 3194 households — the same file the wave's `sample` is built from);
  0 skipped today → byte-stable. Plus two post-conditions that convert a
  future class-1 defect into a class-3 crash: the rename map must stay
  injective, and no canonical id may collide with a live 1988-89 id.
- `Burkina_Faso/_/panel_ids.py` — **OK (anchored on §2)**: `cur_set` now built
  from the full 2021-22 cover; both JSONs byte-identical (md5-verified), 95
  skipped chains unchanged.
- `diagnostics._check_panel_ids_targets_exist` — **OK (anchored on §3)**: accepts
  `canon_ci == f'{canon_pi}_{N}'` because that *is* the library's split
  convention. Malawi `panel_ids_targets_exist`: **fail (1410/5686 = 25%
  "inconsistent") -> pass** — 1410 false positives removed, and those entries
  now actually get their endpoint-existence check (0 missing). Burkina's
  pre-existing `1088/3132 prev` failure is **byte-identical** on pristine
  diagnostics (not caused, not masked).

**Measured (cold rebuild, isolated data root, BEFORE = pristine `development`):**

| table | before | after | note |
|---|---|---|---|
| sample | 56,328 | 56,330 | +2 |
| interview_date | 56,338 | 56,340 | +2 |
| household_roster | 246,584 | 246,594 | +10 persons |
| individual_education | 159,921 | 159,924 | +3 (rest of the recovered rows are all-NaN and dropped downstream) |
| food_acquired | 5,259,309 | 5,259,344 | +35 un-summed tuples |
| food_expenditures / food_prices / food_quantities | — | — | +29 / +35 / +35 |
| housing, plot_features, cluster_features, food_security | — | — | **0** (byte-identical) |

Every row **not** in the two split lineages is byte-identical, including all of
1987-88. GH #323 collapse warnings on a cold build: sample 2->0,
interview_date 2->0, household_roster 10->0, individual_education 8->0.
`food_acquired` (i=101332, t=1988-89) `Expenditure`: **15,850 -> 5,570**, with
the split-off household appearing as `101332_1` with 10,280.
