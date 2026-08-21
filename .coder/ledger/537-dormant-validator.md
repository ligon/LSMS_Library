# Prior-Art Ledger — GH #537: `validate_acquisition_source` is dormant

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites it,
> `CLAUDE.md`, and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git (floor). gitnexus not consulted; the symbol
under test has a zero-node call graph, which `git grep` establishes conclusively.

## §1 Task, restated

`transformations.validate_acquisition_source` enforces that the `s` (acquisition
source) index level of `food_acquired` and its three `_FOOD_DERIVED` children
(`food_expenditures`, `food_prices`, `food_quantities`) only carries values from
`transformations.S_VALUES`. Its docstring asserted it was "Called from
`Country._finalize_result`". **It had zero call sites, from its first commit.**
A guarantee that does not execute is worse than none, because it is trusted.
Decide: wire it in, or delete it — no third state, and stop the docstring lying.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | decision |
|--------|-----------|--------------|---------|----------|
| `S_VALUES` | `transformations.py:31` | the canonical `s` enumeration `(purchased, produced, inkind, other)` | not before this task | **reuse** — make it the single definition |
| `validate_acquisition_source` | `transformations.py:34` | the guard itself | **no** (0 refs in tests/bench/docs) | **extend** (wire + harden), not rewrite |
| `Country._finalize_result` | `country.py:2098` | the universal post-read pipeline; per §0 §2 the single place every table/country/read passes through | integration surface | **reuse** as the hook point |
| `Country._FOOD_DERIVED` | `country.py:3318` | derives the 3 food tables from `food_acquired`; each derived frame is passed back through `_finalize_result` (`country.py:3488`) | yes | **reuse** — one hook covers all 4 tables |
| `ethiopiarhs.food_acquired` | `EthiopiaRHS/_/ethiopiarhs.py:114` | df_edit hook; filtered `s` against its **own** `CANON_S` 3-tuple | no | **fix** — import `S_VALUES` |
| `Country._assert_built_required_columns` | `country.py:2355` | prior art for "loud post-build schema gate rather than silent wrong data" — the pattern this guard follows | yes (PR #243) | **cite**, don't duplicate |

No validator registry / dispatch table exists — `_finalize_result` calls its
checks inline, so wiring is a direct call, not a registration.

## §3 Definitions & conventions in force

- **Canonical `s`** = `S_VALUES = ('purchased','produced','inkind','other')`,
  `transformations.py:31`. Per its own comment, `data_info.yml` cannot express
  enumerated constraints on *index levels*, so this tuple — not the YAML — is the
  schema. Origin: GH #169, `slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org`.
- **Canonical schema** otherwise = `lsms_library/data_info.yml` (STANDING.md §3).
  Note `index_info` does **not** list `s` for `food_expenditures`, although the
  frame does carry it — a pre-existing doc/schema gap, out of scope here.
- **Derived food tables** are runtime-derived, not registered (`CLAUDE.md`
  §"Derived Tables") — so they cannot be guarded at the `data_scheme.yml` layer;
  the guard must sit on the read path.

## §4 Invariants & assumptions (the landmines)

- `_finalize_result` is **the highest-blast-radius function in the library**
  (§0 §2): every table, every country, every read. Anything raised here surfaces
  at API time for everyone. Guard must be a no-op when `s` is absent.
- It runs on **every read**, so an O(#rows) check is a real, silent perf
  regression — worst on `Feature('food_acquired')`, which assembles 20 countries
  in one call. (Measured: naive form = 819 ms on GhanaLSS's 5.26M rows.)
- **pandas never stores NaN in `MultiIndex.levels`** — a missing entry is code
  `-1`. This is exactly why the original `.dropna()`-based check was structurally
  blind to a NaN `s`, and why a levels-based rewrite must count codes explicitly.
- **`MultiIndex.levels` retains entries for filtered-out rows.** Reading unique
  values from `levels` without `remove_unused_levels()` yields false positives.
- Cache: `s` is not touched by `_finalize_result`, and this is a **post-read**
  guard, so **no cached parquet is invalidated and `LSMS_CACHE_SCHEMA` must NOT
  be bumped** (bumping would force a needless from-source rebuild of 20 countries).
  Consistent with `CLAUDE.md`: `_finalize_result` is *excluded by design* from the
  content hash.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| canonical `s` enumeration | **reuse** `S_VALUES` | it already exists; the bug is a *second* definition, so adding a third would be the worst outcome |
| the guard function | **extend** in place | logic is right; the defects are (a) never called, (b) NaN-blind, (c) O(#rows) |
| hook point | **reuse** `_finalize_result` | already the single post-read pipeline; `_FOOD_DERIVED` frames pass back through it, so one call covers all 4 tables |
| ERHS filter set | **reuse** `S_VALUES` (drop local `CANON_S`) | kills the drift; verified behaviour-neutral (no ERHS wave emits `other`) |
| loud-gate pattern | **cite** `_assert_built_required_columns` | same "crash rather than silently wrong" posture; no new machinery |

**Explicitly NOT built** (would have been reinvention): a validator registry, a
`data_info.yml` index-value-constraint mechanism, or a second enumeration.

## §6 Open questions for the human

- **EthiopiaRHS 1989 discards 686 of 2755 raw food rows (24.9%)** — 677 with a
  missing `source` code, 9 with unmapped codes 5/6/7. The drop is *deliberate and
  documented* (`ethiopiarhs.py` docstring), but was invisible at runtime; this
  task makes it `warn` with a count. **Should the NaN-source rows map to `other`
  rather than be deleted?** That needs the ERHS 1989 codebook and must not be
  guessed. Blocks nothing here; worth its own issue.
- The guard raises on **0 of 80** frames today. That is the expected steady state
  (it is a regression guard, not a bug detector) — but it means the issue's
  class-1 "silently wrong / reaches published work" severity is **not** supported
  by the data. Recommend reclassifying as class-2/4. See the PR body.

---
### Phase 3 — verification

- `validate_acquisition_source` — **OK (anchored on §2/§4)**: extended, not
  rewritten; reuses `S_VALUES`; NaN now read off level codes per §4; clean-path
  cost 819 ms → 3.2 ms per §4's perf invariant.
- `Country._finalize_result` — **OK (anchored on §2/§4)**: one added call at the
  tail; no-op without an `s` level; no cache-hash bump, per §4.
- `ethiopiarhs.food_acquired` — **OK (anchored on §5)**: local `CANON_S` deleted
  in favour of the imported `S_VALUES`; the CONTRADICTION (two competing
  definitions of canonical `s`) is resolved, not duplicated.
- No REINVENTION: no new enumeration, registry, or schema mechanism was added.
