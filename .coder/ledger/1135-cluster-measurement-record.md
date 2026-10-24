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
  movers by the stated `mover_rule`; recorded only where ONE marker settles
  it -- nowhere today (see §4, review round).
- `clusters_renumbered`: distinct earlier-wave cluster IDS (not physical
  clusters; synthetic ids excluded) among linked households ALL of whose
  linked households go to one single different `v` that no other earlier id
  receives.  The corpus's "cluster pairs" usage (GhanaLSS 79, Ethiopia 333,
  Tanzania 82), made exact after the red team: Nigeria W5 is 11, not 12.
- `not_applicable`: a count that does not describe the boundary (a
  renumbering count across a change of KIND, synthetic bins, no linkage, or a
  wholesale re-layout with movers) -- printed "not applicable", distinct from
  "not measured".

## §4 Invariants & assumptions

- STANDING.md §4 inherited.  Warn, never fence or rewrite; no threshold; no
  allowlist; strict raises `ClusterIdentityError`.
- Nothing computed at warn time: the audit runs on arbitrary tables and a
  `sample()` fetch would recurse (diagnosis PROPOSAL.md).  Counts come from config.
- Every frozen field must be used by a shipped record (pinned test), so the
  measurement block may carry no field that no boundary uses.
- Nigeria's `moved-` sentinel (`ea == 0`) is NOT a movement classifier: it
  misses movers with a real EA (W5: 480) and marks non-movers (2018Q3: 40 of
  the 69 sentinel households answer `hhsmlc` = 1; 2023Q3: 140 of 231 answer
  `hh_moved` = 2).  Review round (red team + @ligon 2026-10-23): the net
  count depends on the marker -- 2018Q3 10 / 25 / 50..56 (sentinel /
  `tracked_obs` / `hhsmlc`), 2023Q3 112 / 150 / 226..249 (sentinel /
  `trackedw5pp` / `hh_moved`) -- so NEITHER boundary records a net count; the
  first round's 10/1,493 and "86..106" are withdrawn (the latter counted
  only real-id changes and was wrong under its own condition).
- Burkina 2014 -> 2018-19 shares 583 `i` by id COLLISION: N there is 0, cited
  to `panel_ids.py` / the cover, never to `sample()`.  Niger: 0 shared `i` by
  construction (`E_` prefix), not evidence; no-linkage cited the same way.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| N / changed / renumbered per boundary | reuse (lift from the current `cluster_id_note`) | already measured and committed; re-measuring is out of scope |
| net of movers | not recorded anywhere (review ruling 2026-10-23) | marker-dependent; the per-marker counts go in each Nigeria block's `note`; the field stays in the schema, its non-use named in `_MEASUREMENT_FIELDS_UNUSED` |
| K where it does not describe the boundary | `not_applicable` | Uganda x4, Burkina Faso, Niger, Tanzania 2019-20 |
| Tanzania 2012-13 -> 2014-15 K | 35, confidence medium | red-team count reproduced on the served sample; partly rests on the unverified r_hhid linkage |
| schema | extend `_PANEL_FIELDS` + new `_MEASUREMENT_FIELDS` | the freeze is widened by editing a frozenset with a reason |
| first-line formatter | extend `_format_cluster_report` | detail lines kept verbatim (tests match them) |

## §6 Open questions for the human

- None blocking.  Residual (not done here): net-of-movers counts for the other
  boundaries need movement classifications the repository does not hold
  (diagnosis "Twelve-boundary evidence requirements").

---
### Phase 3 — verification

- `_PANEL_FIELDS` + `_MEASUREMENT_FIELDS` / `_MEASUREMENT_COUNTS` — OK
  (anchored on §2 "extend"; the freeze test now covers the new block in both
  directions: every field used by a shipped record).
- `_check_measurement` — OK (§2 precedent: the panel's own-provenance rule;
  §4 counts typed and coherent).  11 mutants (each check removed, bool/float
  accepted, report drops the block, W5 records 106 as a count, ...) all killed.
- `audit_cluster_identity` `measurement` key — OK (§4 nothing computed at warn
  time; the block is copied from config).
- `_size_fragment` / `_share` / first line — OK (§3 ruling): prints what is
  measured, "not measured" otherwise; the W5 86..106 and 112 never reach the
  message (test + real-data check on all four Nigeria warnings).
- the 13 curated blocks — OK (§5 reuse): every `linked_households` /
  `households_changed` equals the served `sample()` count on a cold branch
  build (11 boundaries; Burkina Faso / Niger are N = 0 by absence of linkage).
- `redefined` gloss — OK (§1 item 2): comment/docs only.
- Cache: 0 of 549 / 40 / 2,109 (base `57d255585` vs branch `0b471ce87`).
  Served `sample` and `cluster_features` identical for all 7 countries.
  Evidence: `slurm_logs/isa_backlog_2026-10-21/1135-measurement-record/`.
- Review round (red team at `13d47fe4a`): Nigeria figures corrected against
  the raw covers (`oracle.json` in the evidence dir); validator tightened
  (K <= D <= N, block only on warning boundaries, string fields, plain-decimal
  YAML integers); "not applicable" vs "not measured"; SPAN qualifier on
  line 1; `unobservable` / `redefined` glosses reconciled.  All 26 red-team
  mutants retargeted plus 14 new: 39 killed, 1 equivalent (M04b).
