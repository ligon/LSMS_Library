# Prior-Art Ledger — GH #1135 cluster-id measurement record (+ `redefined` gloss, #1246/#1248)

**Baseline:** development `57d255585`.  **Search tier used:** ripgrep + git
floor (the gitnexus MCP server did not connect in this session).

## §1 Task, restated

Two maintainer rulings (@ligon, 2026-10-23) on Site C, the cross-wave `v`
comparability warning (`lsms_library/sampling.py`, GH #942):

1. **#1135.** Each wave BOUNDARY's `panel:` block in `countries/{C}/_/sampling.yml`
   may carry an optional, typed measurement record with its own provenance.
   The `ClusterIdentityWarning`'s first line prints, per flagged boundary, what
   is measured -- cluster ids renumbered and the share of linked households
   whose `v` changes net of movers -- and says "not measured" otherwise.  The
   conditional Nigeria W5 numerator (86..106) is never printed as settled.
   Populate only from counts the repository already holds; cite each.
2. **#1246 / #1248.** Widen the documented gloss of `redefined` to cover "the
   same kind of unit, an independent draw, overlapping serials" (Burkina
   2014 -> 2018-19, Niger 2014-15 -> 2018-19).  No value changes.

Read-path and config only: nothing served changes, and the expected hash cost
is 0 (`.yml` is not a build input; the Site C callables are excluded).

## §2 Existing machinery (this task's area)

| symbol | path:line (base) | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `_PANEL_FIELDS` | `lsms_library/sampling.py:189` | frozen field names of a `panel:` block | `test_every_frozen_field_name_is_used_by_a_shipped_record` | **extend** by one field, `cluster_id_measurement` |
| `_check_panel` | `sampling.py:565` | validates continuity + its own provenance (`source_type`/`confidence`/`locator`) | `TestThePanelDeclaration` | **extend**: calls a new `_check_measurement` |
| `_check_vocab`, `_check_nested` | `sampling.py:540,550` | controlled-vocabulary and frozen-field-name refusals | yes | reuse |
| `_freeze` / `_FrozenDict` | `sampling.py:260,300` | immutable, JSON-able nested values | `test_records_are_immutable_all_the_way_down` | reuse (the new block is a nested mapping; nothing new needed) |
| `audit_cluster_identity` | `sampling.py:912` | SPAN rule: every incomparable boundary between first and last wave | `TestTheAudit` | **extend**: each boundary dict carries `measurement` |
| `_format_cluster_report` | `sampling.py:961` | the warning text | `TestTheWarning`, `TestTheReproduction` | **extend**: sizes on the first line; detail lines kept verbatim |
| `check_cluster_identity`, `outermost_call`, `_CLUSTER_LEDGER` | `sampling.py:882,994,991` | once-per-call, strict lever, report ledger | yes | reuse unchanged |
| `attach` / `merge_attrs` / `records_from_attrs` | `sampling.py:739,769,792` | `df.attrs` carrier incl. `Feature()` | `TestRecordsRideOnAttrs`, `TestFeatureCarriesTheRecord` | reuse (measurement rides inside `panel`) |
| `CLUSTER_ID_CONTINUITY` gloss | `sampling.py:217-223` | comment-level meaning of each value | — | **edit** (`redefined`) |
| existing counts | each boundary's `cluster_id_note` in the seven `sampling.yml`; `slurm_logs/isa_backlog_2026-10-21/framework1135-Nigeria-counts/counts-safe.json`; #1135 thread | the measured N / changed / renumbered figures | the notes are pinned by shipped-declaration tests | **reuse** (lift, never re-measure) |

Precedent for a typed, optional, provenance-carrying sub-record: the
`panel:` continuity reading itself (#942, DESIGN.org §3.3) -- its own
`source_type` / `confidence` / `locator`, "refused if absent".  The measurement
block copies that rule rather than inventing a new one.  Precedent for
"absence is written, not omitted": the exemplar's `_status` fields; here
absence of a count is printed "not measured".

## §3 Definitions & conventions in force

- Site C contract, frozen vocabulary, once-per-call rule, 0-hash design:
  `CLAUDE.md` §"Site C: `v` across a WAVE BOUNDARY"; module docstring and
  the comment block above `_CLUSTER_STRICT_ENV` in `sampling.py`.
- H2 SPAN / H3 no threshold, first line states the size: #1135 ruling
  2026-10-19 (comment 6281949507); typed record + "not measured" + 86..106
  never settled: #1135 ruling 2026-10-23.
- `derived-from-data` is never documentation: `sampling.py` module docstring.
- `v` = sampling-cluster id owned by `sample()` (STANDING.md §3).

Definitions this task fixes (in `sampling.py`, next to `_MEASUREMENT_FIELDS`):

- `linked_households` (N): households linked across the boundary with `v` on
  BOTH sides, as served by `Country(C).sample()` (post-`id_walk`).  Where a
  note gives a larger "linked" figure that includes households without `v`
  (Uganda 2,607 / 2,647; Tanzania 556 / 907), N is the with-`v` figure, the
  share's denominator -- the definition the #1135 H3 table used.
- `households_changed`: of N, those whose `v` differs (all causes, movers
  included).
- `households_changed_net_of_movers`: of those, the ones not classified as
  movers by the stated `mover_rule`; recorded only where a rule was applied.
- `clusters_renumbered`: distinct earlier-wave cluster IDS (not physical
  clusters) among linked households that map one-to-one to a different `v`.
  This is the definition the corpus already uses (GhanaLSS 79, Ethiopia 333,
  Tanzania 82 are all "cluster pairs").

## §4 Invariants & assumptions

- STANDING.md §4 inherited.  Warn, never fence or rewrite; no threshold; no
  allowlist; strict raises `ClusterIdentityError`.
- Nothing computed at warn time: the audit runs on arbitrary tables and a
  `sample()` fetch would recurse (diagnosis PROPOSAL.md).  Counts come from config.
- Every frozen field must be used by a shipped record (pinned test), so the
  measurement block may carry no field that no boundary uses.
- Nigeria's `moved-` sentinel (`ea == 0`) is NOT a complete movement
  classifier (counts-safe.json: W5 480 movers with a real EA; 254 `ea == 0`
  households report the original dwelling; the 112 real-id changes split
  86 original / 6 moved nearby / 20 no status).  So 112/5,067 is not "net of
  movers"; 86..106 is conditional on the W5 question's reference interval.
- Burkina 2014 -> 2018-19 shares 583 `i` by id COLLISION: N there is 0, cited
  to `panel_ids.py` / the cover, never to `sample()`.  Niger: 0 shared `i` by
  construction (`E_` prefix), not evidence; no-linkage cited the same way.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| N / changed / renumbered per boundary | reuse (lift from the current `cluster_id_note`) | already measured and committed; re-measuring is out of scope |
| net of movers | reuse for Nigeria 2018Q3 only (10/1,493) | the only boundary with a mover rule applied (sentinel + `hhsmlc` = same location for all 10); the 2026-10-19 ruling itself names the 0.67% |
| Nigeria 2023Q3 net of movers | not recorded as a count | 86..106 is conditional; 112 is sentinel-net only; both stay in the block's `note`, which the warning never prints |
| schema | extend `_PANEL_FIELDS` + new `_MEASUREMENT_FIELDS` | the freeze is widened by editing a frozenset with a reason |
| first-line formatter | extend `_format_cluster_report` | detail lines kept verbatim (tests match them) |

## §6 Open questions for the human

- None blocking.  Residual (not done here): net-of-movers counts for the other
  boundaries need movement classifications the repository does not hold
  (diagnosis "Twelve-boundary evidence requirements").

---
### Phase 3 — verification

(filled at task end)
