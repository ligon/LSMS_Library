# Prior-Art Ledger — GH #819 (`_finalize_result` cluster join: `_v_to_str` + `id_walk` order)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md` (§0 baseline). Cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git (floor). The `gitnexus` MCP server did not
connect this session (`CONNECTION_CLOSED`), so the substitutes named in
`CLAUDE.md` §"Code intelligence: GitNexus is OPTIONAL" were used: `rg` over
`lsms_library/` + `tests/` for the blast radius of every symbol touched, and
`git log`/`grep` for the history of the two defects. Declared, not skipped.

## §1 Task, restated

`Country._finalize_result` (`lsms_library/country.py`) joins the sampling
cluster `v` onto every household-level table from `sample()`
(`_join_v_from_sample`), and separately re-keys the household id `i` through
`updated_ids` (`local_tools.id_walk`). Two defects in that pair leave two
thirds of Ethiopia ESS `2013-14` / `2015-16` `food_acquired` rows with
`v = NaN`, and the surviving third is the urban refreshment cohort:

1. `_v_to_str` normalises `v` with `str(int(float(x)))`, which rewrites a
   *string* id: Ethiopia's zero-padded 15-digit EA id `'010101088801601'`
   becomes `'10101088801601'` and no longer compares equal to
   `cluster_features.v` (measured raw match 9.1–46.3% by wave).
2. `_join_v_from_sample` merges on `(i, t)` *before* `id_walk` re-keys `i`,
   while `sample()` — finalised through the same method — is already walked.
   Only the never-re-keyed (urban refreshment) households match.

Scope: framework read-path only. No country config, no wave script, no
`transformations.py` / `local_tools.py` / `feature.py` / `data_info.yml`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._join_v_from_sample` | `country.py:2260` | joins `v` from `sample()` on `(i, t)`; owns `_v_to_str` | `tests/test_join_v_silent_skip_warn.py`, `test_no_v_join_declarative.py`, `test_sample.py` | **extend** (change the coercion only) |
| `Country._finalize_result` | `country.py:2838` | the single post-read pipeline; owns the step ORDER | `tests/test_finalize_order_797.py` (kinship/mappings order), `test_population.py` | **extend** (move one block) |
| `local_tools.format_id` | `local_tools.py:2197` | canonical string form for a household/cluster/person id; auto-applied to every `idxvars` entry — and therefore to `cluster_features.v` | `tests/test_id_format_id_canonical.py` | **reuse** — it *is* the rule `_v_to_str` was trying to express |
| `local_tools.id_walk` | `local_tools.py:2455` | per-wave `i` re-key through `updated_ids`; sets `attrs['id_converted']` after the concat | `tests/test_id_walk.py`, `test_panel_id_collisions.py` | reuse unchanged |
| `local_tools._close_id_map` | `local_tools.py:2399` | closure-resolves `{A:B, B:C}` so `id_walk` is idempotent | `tests/test_id_walk.py` | reuse unchanged |
| `Country._aggregate_wave_data` | `country.py:3260` | the caller; probes `self.updated_ids` at `:3293` **before** building, and per-wave `id_walk` at `:3606` | integration | untouched |

**Not reinvented:** no new id-normalisation helper, no second walk, no
re-derivation of `v`. `slurm_logs/price_sources/Ethiopia/analysis.py` shows the
*workaround* (re-join `sample()[['v']]` after the walk at API level); this task
fixes the framework so the workaround is unnecessary, rather than promoting it.

## §3 Definitions & conventions in force

- **`v`** = sampling-cluster id, owned by `sample()` and `cluster_features`,
  joined at API time — `STANDING.md §3`, `CLAUDE.md` §"`sample()` and Cluster
  Identity". Do not bake it into feature parquets.
- **`format_id` semantics** — `local_tools.py:2197-2224` docstring: numeric →
  decimal string; string → whitespace-stripped, trailing `.xxx` removed *only*
  when the whole pre-decimal string is numeric; **"Leading zeros are
  preserved."** Narrowed from `s.split('.')[0]` by GH #222.
- **`format_id` is applied to `idxvars` but NOT to `myvars`** — `CLAUDE.md`
  §"Gotchas with Teeth". `cluster_features` declares `v` as an `idxvar` (so its
  `v` is `format_id`-canonical); `sample` declares `v` as a *column*, so the
  API-time coercion in `_v_to_str` is the only place `sample().v` is
  canonicalised. That asymmetry is exactly why the two sides diverged.
- **Ethiopia EA id** — `Ethiopia/_/CONTENTS.org` §"Cluster (PSU) identifier":
  "The EA ID format is a long numeric string (e.g., \"010101088801601\")". A
  *string*, zero-padded, by the country's own documentation.
- **Ethiopia panel re-key** — `CONTENTS.org` §"Household ID changes across
  waves": W2/W3 use `household_id2`; `panel_ids`/`updated_ids` walk those back
  to the W1 `household_id` for the panel cohort, leaving the W2 urban
  refreshment cohort untouched. That cohort split is what makes the missing `v`
  non-random.

## §4 Invariants & assumptions

Repo-wide ones from `STANDING.md §4` apply unchanged. Task-specific:

- **`attrs['id_converted']` must be set exactly once and survive the merge** —
  `STANDING.md §4`, `CLAUDE.md` §"Panel ID Transitive Chains and the `attrs`
  Flag". The attrs rule is *"`attrs` survive only when every input agrees;
  any disagreement — including one side having none — yields `{}`"*, and the
  v-join is a **disagreeing** merge (pinned by
  `tests/test_population.py::TestVJoinIsADisagreeingMerge`: the right-hand
  `sample()` frame carries a population record the left frame does not).
  `_join_v_from_sample`'s explicit `result.attrs = dict(df.attrs)`
  (`country.py:2350`) is therefore load-bearing and must not be removed.
  Running `id_walk` *before* the join means the flag is set once (by `id_walk`)
  and carried across the merge by that existing copy — no second walk, no new
  attrs plumbing.
- **`id_walk` is idempotent by construction** (`_close_id_map`;
  `local_tools.py:2479-2487` docstring, `tests/test_id_walk.py`), so the flag is
  a performance hint, not a correctness gate. Losing it costs a redundant pass,
  not a collision — the `4db41a27` Burkina Faso failure mode is closed at the
  source.
- **`_finalize_result` is read-path**: it is in
  `_build_registry._EXCLUDED_CALLABLES`, so editing it moves **no** cache hash
  (`CLAUDE.md` §"The Population Record" states the same for `attach_population`;
  verified empirically for this change — `Country._table_cache_hash` identical
  under both interpreters).
- **The id_walk gate reads `self._updated_ids_cache`, not the property**
  (`country.py:2910`). The cache is populated by the eager probe in
  `_aggregate_wave_data` at `:3293`, which runs *before* the build and hence
  before every `_finalize_result` call on the normal path. The one caller that
  skips the probe is the `assume_cache_fresh` early return at `:3290` — see §6.
- **A left merge fans out if the right side has duplicate keys.** Walking `i`
  first makes W2/W3 panel rows meet `sample()`'s walked `(i, t)` for the first
  time, so `sample()` must be unique on `(i, t)` post-walk. Measured: 0
  duplicates in all five Ethiopia waves (and in Uganda, Burkina Faso,
  GhanaLSS). Row counts must be byte-identical before/after; they are.

## §5 Reuse decision

| quantity | decision (reuse / extend / new) | reason |
|----------|--------------------------------|--------|
| canonical string form of a cluster id | **reuse `format_id`** | it is already the rule applied to every `idxvars` entry, and therefore to the `cluster_features.v` the joined `v` must compare equal to. A bespoke "float-with-a-`.0`-tail" predicate would be a second, drifting spelling of a tested function — the §0 reinvention failure mode. |
| household-id re-key before the join | **reuse `id_walk`, move the existing call** | the walk already exists and is idempotent; nothing new is computed. Joining on a separately-walked key instead would run `id_walk`'s mapping twice per call and give `attrs['id_converted']` two places to be set. |
| cluster `v` on a household table | reuse `_join_v_from_sample` | `STANDING.md §5` |

## §6 Open questions for the human

- **`assume_cache_fresh=True` + a warm table parquet but a cold `sample`
  parquet.** That path (`country.py:3290`) returns *before* the `:3293`
  `updated_ids` probe, so `_updated_ids_cache` is `None` when the id_walk gate
  is evaluated. Today the v-join's re-entry into `sample()` can populate the
  cache as a *side effect*, after which the (later) id_walk fires — the table
  is walked but was joined pre-walk, i.e. the #819 bug. After this change the
  gate is evaluated first, so the table is neither walked nor mis-joined. Both
  are wrong; neither is made worse, and the accidental side-effect ordering was
  never a design. This is the same lazy-probe order-dependence already recorded
  in `Ethiopia/_/CONTENTS.org` §"Caveat: this table's country-level row count is
  ORDER-DEPENDENT" (63,139 vs 62,939 rows for `individual_education`). Fixing it
  properly means making the gate consult the `updated_ids` *property*, which
  would turn `id_walk` on for tables that silently skip it today and move row
  counts corpus-wide. Deliberately **out of scope** here.
- **`format_id` does not catch `OverflowError`.** The old `_v_to_str` did, and
  returned `str(x).strip()` for e.g. `float('inf')`. `'%d' % float('inf')`
  raises `OverflowError`, which now propagates. An infinite cluster id is
  corrupt data and being loud is the right default, but it is a behaviour
  change and is called out here rather than left for a reader to find.

---
### Phase 3 — verification (fill at task end)

- `Country._join_v_from_sample._v_to_str` → **OK (anchored on §2/§3/§5)**:
  replaced by a direct `format_id` call — reuse of the tested `idxvars` rule,
  not a new normaliser. No CONTRADICTION with §3 (`v` still lands as
  `pd.StringDtype`, still a single index level inserted after `t`).
- `Country._finalize_result` step order → **OK (anchored on §4)**: the `id_walk`
  block moved ahead of the v-join verbatim; `attrs['id_converted']` is still set
  exactly once (by `id_walk`) and still carried over the disagreeing merge by
  the pre-existing `result.attrs = dict(df.attrs)`. The `reorder_levels` block
  stays after the v-join, because that is the step that adds `v`.
- No REINVENTION: nothing new computes a cluster id, a household-id map, or a
  second walk.

**Blast radius, measured rather than argued** (raw records in
`slurm_logs/`-style scratch output; the scripts are three short readers over the
warm cache, no builds):

- *The reorder can only touch a country whose `updated_ids` actually renames an
  id AND that declares `sample`.* Enumerated: exactly **7** — Burkina Faso,
  Ethiopia, GhanaLSS, Malawi, Niger, Tanzania, Uganda. For all 7, `household_roster`
  is **byte-identical** across all 38 waves (rows / `v` NaN share / distinct `v` /
  duplicate index / distinct `i`), because YAML-path tables are already walked
  per wave by `_aggregate_wave_data:3606` before `_finalize_result` sees them.
  The defect bit **script-path country-level tables**, where the whole frame
  arrives unwalked — `food_acquired` is the instance. `food_acquired` is
  likewise byte-identical for the other 5; only Ethiopia moves.
- *The coercion touches every country with a `sample` table.* Swept all 36 that
  have one, comparing `format_id` against the old `str(int(float(x)))` on
  `sample()['v']` and asking which spelling `cluster_features.v` actually
  carries:

  | country | distinct `v` changed | matches `cluster_features` — new / old |
  |---|---|---|
  | Ethiopia | 1039 / 1305 | **1039 / 0** |
  | Liberia | 6 / 32 | **6 / 0** |
  | GhanaSPS | 97 / 334 | n/a — no `cluster_features` table |
  | the other 33 | 0 | — |

  **Liberia was a second, unreported instance of #819** and is fixed by the same
  line: `cluster_features.v` is `'032'`, `'052'`, …, and the old coercion served
  `'32'`, `'52'`. Its `sample().v` also contains `'102.0'` — a stringified float
  next to zero-padded strings in the same column — which is a live demonstration
  that the float-normalising half is still needed and still works (`'102.0'` ->
  `'102'`, matching `cluster_features`). GhanaSPS declares no `cluster_features`,
  so its 97 changes are neutral for matching; the served `v` now simply agrees
  with the `'001'`-style id its own `sample` table holds.
