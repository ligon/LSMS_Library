# Prior-Art Ledger — Malawi nonseed deletion provenance (issue 1174)

Search tier: git + ripgrep + AST floor. Fresh GitHub development base:
`c964c6c4f25945dc14636fd094893b16b12180b7`. Current integration baseline, freshly
fetched after terminal verification, is GitHub development
`3b28d293f71b962c3d878c2886be9ee794407915`; root requested an ordinary merge
preserving the frozen topic history, without rebase/force. The filesystem
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

The bounded sanctioned source probe confirms the residual on the current base:
19,265 Module D records (no repeats), 16,560 inorganic groups, exactly one DAP
group with missing Quantity and Applied, backed by one raw line with no ag_d38
answer or selected total. Source bytes match MD5
d143b71ea7c7f4efcac3b9afa0d6be48 (14,149,534 bytes). Probe and API audit exited 0.

Reuse the current finalizer and rows: 0 mechanism. Add a separate 2010–11
nonseed-group deletion record and predicate/input replay in the existing wave
mapping.py. Reuse all three existing Module D blocks and the assembler; leave
producer math untouched. Assemble the same projected blocks twice: once with
real household IDs to find deleted full groups, once with release positions as
temporary IDs to identify exactly which raw lines contribute to those groups.
Return original raw lines and their stable release coordinates, not projected
or invented quantities. Wave-local placement is already
supported by resolve_callable and narrows wave-hash scope; measure actual hashes
rather than predicting a table-only cache effect. Add Applied to the existing
seed record's columns as the issue requests. Prove raw input replay, full native
conservation, Default/Make/Feature/warm behavior and appropriate tests on a freeze.

The initial implementation freeze 0efac3b72185dae8703db895dbc472e9a1bbeb8e is
terminal-verified. Author focused checks: 108 passed; independent review: 171
passed (108 existing + 63 independent), with nine source cases deselected.
Cold job 41006209 completed 0:0: both APIs, Make/Feature, raw replay, unchanged
seed inputs and fresh warm checks pass; 14 private cache files unchanged per
head. Source frames preserve 179,957 plot-input rows, 57,118 sample rows and
37,076 nitrogen rows. All 1,356 prefinal hollow groups are 1,355 seed + one DAP.
Full offline CI: 11,464 passed, 5,646 skipped, four xfailed, 1,008.38s. Initial
test-only tuple/list assertion failure and reviewer harness failures remain
retained. These are exact 0ef/c964 receipts, not final current-base results.

Now normal-merge freshly fetched 3b28, preserving both upstream ledgers and all
upstream code. The four task implementation/test files must remain byte-exact
0ef. Malawi CONTENTS/AGENTS/CLAUDE have no upstream delta. Source collection
changes only hashing; nitrogen_kg's own AST is unchanged, but its country-index
helper changed upstream, so rerun the current-base cohort rather than assume
compatibility. Run bounded relevant 1510/1475/task controls, API audit, actual
hash census and fresh cold baseline/candidate APIs/Make/Feature/replay/warm
checks on the merged freeze. Configured final-head GitHub CI remains root's
gate; retain the complete earlier full-CI receipt as prior-head evidence.

This completes the existing-wave residual only. The issue's conditional
2024–25/15-line seed provenance follow-up belongs to issue 1275 and the held
whole-wave work; it is neither implemented nor verified here. Root will carry
that checklist before narrowing/closing issue 1174. No new 2024 wiring.

## §6 Open questions

No scientific decision is requested: registration documents an existing
deletion and does not decide whether fertilizer was used. Current-base
integration validation remains; the earlier baseline cohort is complete.
Stop any affected extension if its
eligibility/replay cannot be tied exactly to the current producer; document
the ambiguity instead of broadening the rule.
