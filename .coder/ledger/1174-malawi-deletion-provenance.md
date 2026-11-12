# Prior-Art Ledger — Malawi nonseed deletion provenance (issue 1174)

Search tier: git + ripgrep + AST floor. Fresh GitHub development base:
`c964c6c4f25945dc14636fd094893b16b12180b7`. The verified source-integration baseline
is `3b28d293f71b962c3d878c2886be9ee794407915`. After its terminal checks, GitHub
development advanced to `bcd77200f4fd555a8dfc10656d3e7100ae03fd6f` with disjoint
Burkina Faso/Mali changes only. Root requested ordinary merges and byte/smoke
closure for that disjoint advance, preserving the frozen topic history without
rebase/force. The filesystem
origin's stale development ref is not a baseline. Inherits STANDING.md §§3–4, CLAUDE.md
Derived Values, and canonical lsms_library/data_info.yml.

## §1 Task, restated

Reconcile the remaining Module D all-payload-NA deletion with the existing
rows: 0 derivation registry and public derivation_inputs. Issue 1174 and its
one current comment were read, together with merged PRs 1422, 1523 and 1593
and their complete (empty) discussion timelines. The issue still names one
2010–11 DAP group with no ag_d38 answer. Establish current source evidence
before implementing; if already resolved, report that rather than patching.
The 1,355 registered Module G seed lines remain excluded. No quantity,
missingness, use, unit, eligibility, weight, food or whole-wave policy changes.

## §2 Existing machinery

| Symbol/path | Behavior and tests | Decision |
|---|---|---|
| malawi.py::_fertilizer_block | Existing two native type slots, selected enumerator-total quantities, nullable Applied from ag_d38; test_gh1174_malawi_input_use.py | reuse unchanged |
| malawi.py::assemble_plot_inputs | Existing full-key reduction; Quantity sum(min_count=1), Applied max; seed rows remain distinct | reuse unchanged |
| malawi.py::derive_seed_line_no_payload / inputs_seed_line_no_payload | Existing seed deletion predicate and raw original-name input replay | retain unchanged; follow its registry/replay pattern |
| malawi.py::_seed_source_reference | Logical file/release identity bound to declared sidecar, no raw-byte fallback | reuse for raw Module D input coordinates |
| 2010-11/_/plot_inputs.py | ag_d39a/f type; ag_d39d/i enumerator-total kg; ag_d38 use; full-record repeat policy | exact binding oracle, no edits |
| derivations.py rows: 0 / resolve_callable | Existing deletion disclosure, wave-local mapping callable resolution, no served stamp | reuse; test_derivations.py |
| Country.derivation_inputs | Raw variables at input grain, framework id_walk(state='raw') | reuse unchanged; test existing public input machinery |
| Country._finalize_result | Existing dropna(how='all') and post-read attrs summary | reuse unchanged |
| Current-base source collector / crop-country helpers | Merged 1510 hashes consumed override paths; merged 1475/1517 retains supplied country identity | retain upstream byte-exact; validate current-base compatibility |

## §3 Definitions and country grounding

The entire 5,373-line current CONTENTS.org is byte-exact to authenticated prior
full read (blob 5470d54c0180a32b31bd8abda2b425c093160038), including LOGBOOK,
WAITING and TODO. AGENTS.md/CLAUDE.md also have no delta from the prior fully
read 8558 grounding. Relevant CONTENTS §§4996–5058 were reread in full.

- Module D can name a fertilizer despite ag_d38=No or NA; the gate is an answer,
  not a row filter. The 49 False rows with positive total kg remain visible.
- “the one all-blank group with no ag_d38 answer (2010-11, DAP) is now all-NA
  payload and is not served” identifies the current provenance residual.
- “The 19 fertilizer groups stay NA” binds the native-quantity/blank-total
  disagreement. Do not substitute ag_d39b/g for enumerator totals ag_d39d/i.
- “Whether such a line is 'seed not used' or 'amount unknown' is not settled
  by the instrument” remains the seed ruling. A unit index alone is not payload.
- Historical “#1174 eligibility likewise remains unresolved” predates these
  explicit rulings; preserve that history and name the narrow residual rather
  than reopening eligibility. No new contradiction has been established.
- Older applied-unit helper commentary is qualified by dated country evidence:
  “those statements are contradicted by the separate native applied-unit,
  enumerator-total and timing columns.” Reuse the unchanged evidenced driver;
  do not reinterpret timing fields as units or alter unrelated comments here.

Registry contract: derivations.py says integer zero “names a deletion”; dropped
rows have no place to carry a key. Country.derivation_inputs returns ORIGINAL
variable names at input grain through get_dataframe, with public household-ID
rekeying. Existing seed entry omits Applied in columns; issue 1174 explicitly
requests that metadata correction without changing its predicate.

## §4 Invariants and assumptions

- Exact served native amounts/masks, row universe/full keys/units/Applied and
  original producers, reducers and finalizer. No invented zeros or kg estimates.
- Seed registry, replay, row exclusion and 1,355 baseline line count unchanged;
  add Applied to its declared metadata only. 2024 plot_inputs is unwired on this
  base; its future 15-line follow-up is outside this task and held whole wave.
- Full raw Module D input lines retain original columns and release positions;
  multiple slots/records contributing to a deleted group must all remain
  recoverable. A deleted group and a raw row are different counts.
- New registry disclosure changes attrs intentionally, not physical payload.
  Old registry entries and their input results remain exact.
- Sanctioned readers, private L2/shared L1 only, explicit package/config/head
  identity, single BLAS/build threads, pytest two loadfile workers. No DVC CLI,
  LSMS_NO_CACHE, shared rewarm, dependency/fixture weakening or GitHub writes.

## §5 Reuse decision

Extend the existing rows: 0 registry with a separate 2010–11 nonseed-group
record. The predicate documents the existing finalizer's all-payload-NA rule;
producer math, all three Module D projections, reducer and finalizer stay
unchanged. The wave-local replay assembles the existing projections with real
household IDs to find deleted full groups, then with release positions as
identities to retain their exact raw-line membership. It returns original
columns at the input grain, once per contributing release row. The existing
source-reference helper and public ID walk are reused. Applied is added only
to the existing seed record's declared metadata.

Current source confirms one deleted DAP group/one raw line among 19,265 Module
D records and 16,560 inorganic groups. Selected totals and ag_d38 are missing;
source MD5 is d143b71ea7c7f4efcac3b9afa0d6be48 (14,149,534 bytes). A source line
and a canonical group are distinct counts even though both are one here.
No use/routing interpretation follows from the missing answer.

Initial freeze 0efac3b72185dae8703db895dbc472e9a1bbeb8e is preserved in history.
After its terminal source/full-CI campaign, ordinary merge onto GitHub 3b28
produced source-verified freeze 1e8af2516869a861590cc0bc189840a87ba77337.
All four task files remain byte-exact reviewed 0ef; all incoming source-hash and
crop-country files remain byte-exact upstream. The later bcd advance contains
only the 1528 Burkina Faso/Mali topic, its ledger/country docs and tests.
It was ordinarily merged as 9f14d476906e1ad193bb3bb761e1350c9382c77f.
The complete Malawi tree, 58 shared-library files and eight relevant test files
are byte-exact source-verified 1e8. Root explicitly required byte/smoke closure
rather than another native/full campaign for this disjoint advance.

## Phase 3 — anchored verification

- **OK (§§2–4):** the predicate preserves every observed payload, including
  zero/False, and excludes seed/other waves. Raw replay reuses the three
  projections, full-key reducer, repeat policy, sidecar release identity and
  public ID handling. No alternate producer, quantity estimate or eligibility
  rule is introduced. The 19 blank-total groups remain NA under unchanged
  producer code and exact native-payload conservation.
- **OK (§§2/4/5):** independent source-free review on exact 0ef passes 171
  controls (108 existing + 63 independent), with nine source cases deselected,
  no P1/P2 blocker. Its scalar membership oracle, dtype/Parquet, full payload
  boundaries, public ID/wave modes and actual finalizer tests are distinct
  from author source evidence. Reviewer harness failures remain retained.
- **OK (§§4–5):** first cold job 41006209 completed 0:0, 20:30, peak RSS
  5,689,216K. Full offline CI on 0ef: 11,464 passed, 5,646 skipped, four
  xfailed, 697 warnings, 1,008.38s. This remains prior-head evidence.
- **OK (§§4–5):** merged 1e8 passes 332 relevant task/source/country-index/
  nitrogen controls and strict API audit. Cold current-base job 41009999
  completed 0:0 in 2:54, peak RSS 4,649,808K. Before decoding, all 18 held
  D/G/H sources (119,527,563 bytes) match declared MD5s/stable descriptors.
  Both heads use private cold L2 and the existing shared L1.
- **OK (§§3–5):** current-base/candidate frames are exact: 179,957 served
  plot-input rows, 181,313 prefinal rows, 57,118 sample rows, 37,076 nitrogen
  rows and 179,957 Feature rows. Native amounts/masks, full keys/units/Applied
  and old typed attrs are exact; only the declared registry metadata differs.
  The 67,553 served nonseed rows retain Applied True 67,489 / False 49 / NA 15.
- **OK (§§3–5):** all 1,356 hollow prefinal groups are disclosed: 1,355 seed
  lines (459/185/672/39) plus one DAP group. Seed inputs remain exact. The new
  public accessor returns the one original D line, with exact raw fields,
  public ID walk and replayed deleted full key; it joins to no served row.
  Default/Make parity passes with explicit wave-cache reuse. Feature native
  payload and old attrs match. Both fresh-process warm checks match typed
  output/attrs and retain all 14 cache-file bytes/epochs per head; snapshots
  are durable in integration/verification-1e8af251/.
- **OK (§§2/4/5):** measured own hash scope against 3b28 is 26 public-method
  country/26 wave cells: 22 declared country/22 declared 2010 wave cells plus
  four auto-derived method hashes each. All changed cells are Malawi; zero
  shared BTF/non-Malawi differences. Root's disjoint bcd merge leaves all
  relevant runtime/Malawi inputs byte-exact. No shared L2 write or rewarm.
- **OK (§§4–5):** exact 9f final smoke passes 108 controls, 14 warnings,
  9.85s; the complete incoming-path/byte proof is recorded. Initial author
  107-pass/one tuple-vs-list test assertion failure remains preserved; the
  correction changed only that assertion. All verification actors are terminal.

Evidence root: B/reports/1174-malawi-deletion-provenance-20261107/.
completed-0ef-receipt.json and integration/completed-receipt.json bind source,
CI, logs/XML, scripts and Slurm exits. Independent review is in the separate
review-1174-malawi-deletion-provenance-20261107/VERDICT.md. The integration byte
bindings and disjoint-base-byte-proof.json preserve exact head distinctions.
Raw logs/pickles are private; public reports carry aggregates only.

The pre-existing-wave provenance residual is complete. The conditional
2024–25/15-line seed follow-up belongs to issue 1275 and held whole-wave work;
it is NOT implemented or verified here. Root will carry that checklist before
narrowing/closing issue 1174. No new 2024 wiring, source eligibility, invented
zero, quantity recovery, weight48 or food-policy change is authorized.

## §6 Open questions

No scientific decision blocks this provenance correction. The source records
no use answer on the deleted DAP group; no inference about use or routing is
made. Root owns final configured GitHub CI, bounded integration acceptance,
PR publication and issue closure. Earlier full CI and independent review are
kept bound to their actual heads; no final-head full-suite rerun is claimed.
