# Stream: coping -- implement the #1562 coping contract

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/coping`, branch `feature/1562-coping-set-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/coping/`.
- The audit you build on (read it all, including `out/per_wave_instrument.tsv`):
  `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/1562-coping-audit/REPORT.md`.
  Its §6 has a draft canonical entry and §5 the measured hash costs.
- Rulings: `isa-decisions.org` entries "2026-11-13: Coping-strategy implementation,
  GH1562" (read every bullet) and the 11-12 "Coping-strategy contract, GH1562".
- For every country you touch, read the shocks passages of its `CONTENTS.org`.

## What the rulings require
1. **Canonical schema** (`lsms_library/data_info.yml`, shocks):
   - HowCoped0..2 are the household's REPORTED order.
   - New optional `HowCopedSet`: distinct labels, sorted, joined with
     `derivations.MULTI_SEP` ('+').
   - New optional `HowCoped3`, for Nigeria W1-W3.
   - Start from the audit's §6 draft and resolve its "[contingent]" sentences
     with the rulings.
2. **Read-time normalisation** (0 hashes; the module and its callables go in
   `_build_registry._EXCLUDED_CALLABLES`, pinned by a test, as `null_read_audit` is):
   - a label-class registry, as `.yml`, of slot fillers / missing codes and of
     "did nothing" labels, per country (and per wave where needed);
   - fillers and missing codes are served NA in their position and never enter
     the set (Uganda 96 'Other (specify)' stays a strategy);
   - "did nothing" is a set member only when it is the only answer; beside
     real strategies it is dropped from the set, while the positions keep what
     was recorded;
   - `HowCopedSet` is derived from the positions wherever the build did not
     write one.
3. **Build time:**
   - **EHCVM 2018-19** (Benin, Burkina, CIV, GNB, Mali, Niger, Senegal, Togo):
     - the hooks write the set from the `s14q05__k` grid and leave HowCoped0..2 NA;
     - state exactly which grid values count as chosen (values {0,1,3}, plus 2s on
       code 26 in Mali and Senegal), on evidence, and stop at a question if it
       is not established;
     - move CIV's hook from `cotedivoire.py` to `CotedIvoire/2018-19/_/mapping.py`,
       so later edits stay local (audit §5);
     - GNB 2018-19 codes 18-25: relabel per the Portuguese questionnaire
       (coordinator default; the BASES order puts questionnaire above
       variable-label), with the evidence in CONTENTS;
     - update `tests/test_shocks_extension_food_effects.py:92` and similar.
   - **EHCVM 2021-22 option 25:** keep the inferred slot, as a REGISTERED
     derivation with recoverable inputs (Benin 13, Niger 17 rows, plus any others).
     GNB 2021-22 shocks is being wired on `feature/gnb-labels-2021-20261113`.
     If the 2021-22 hook logic lives in shared code (`ehcvm.py`), GNB inherits
     your change.  If it does not, say so; Sue will extend it at integration.
   - **Nigeria:**
     - W1-W3 add `HowCoped3` from the 4th slot (s15aq5d);
     - W4 (s15aq7_*, "select all that apply") fills the set only, with
       positions NA;
     - serve shocks ONCE, under the post-harvest quarter, as
       `individual_education` pins it, removing the 11,275 duplicate rows;
     - check assets for the same unpinned shape; fix it if it is the same
       defect, and report it.
     - Stay out of Nigeria food and crop files: another stream edits them.
   - **Liberia:** move the forest products out of HowCoped* and the set into
     their own column(s).  Choose descriptive names, flag them for review, and
     update CONTENTS.
   - **Guatemala:** the single action stays HowCoped0 and fills the set.  Fix
     its stale "not yet wired" CONTENTS line.
4. Not in scope:
   - the cross-country harmonisation of labels;
   - the other audit residuals (Liberia's unserved shocks, Tanzania R1/R2,
     Uganda 2005-06 folds), apart from drafting them.

## Verify
- A cold base-vs-candidate build of `shocks` in every country you touch, in
  private roots.  Per wave, report:
  - rows;
  - fill of each position;
  - the set-size distribution;
  - rows whose positions changed, and why (filler, EHCVM emptied, the Nigeria pin);
  - that no other column moves.
- `Feature('shocks')`: a unique index; `HowCopedSet` present for every country
  that has coping.
- The tests touching shocks, `tests/test_schema_consistency.py`,
  `tests/test_derivations.py`, `tests/test_build_transform_hash.py`, and new
  tests for the read-time rules.
- Hash census.  Expected movement:
  - the EHCVM 2018-19 countries;
  - the EHCVM 2021-22 code-25 countries;
  - Nigeria, Liberia, Guatemala, if you edit their configs;
  - 0 `build_transforms_fingerprint`.
  Anything else is a finding.

## Deliver
Return `REPORT.md` and `PR_BODY.md` in your final message (the harness refuses
report files from sub-agents).  Commit as you go.
