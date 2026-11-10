# Mali queue refill 2026-11-09 v2

Search tier: ripgrep + Git, plus read-only primary GitHub issue metadata.
This is a fresh source-only dispatch, not recovery of the prior terminated job.

## §1 Task and source binding

Reconcile Mali's own portions of GH #1483, #1179 and #821 against the pinned
source and current issue discussions. Prepare only source-backed country-local
changes and bounded evidence plans. ROOT owns scientific decisions, execution,
integration and all GitHub publication. No subagents.

- Required and observed initial HEAD: `4381d701ffb06ab447a37c66362119d1c1e23009`.
- Checkout: `/local/job41069580/mirrors/dispatch_bfdaeec78b8d4396baf0ae3da755f958`.
- First action personally read `lsms_library/countries/Mali/_/CONTENTS.org`.
  The initial whole-file output truncated; subsequent bounded reads covered
  lines 1–3744 in full, including all later qualifications and TODO declarations.
- Original whole-file SHA256:
  `661729c8867e4f24f9893bf7bb952310e6e1c43383d24772dd2cfdc3d6de73fb`;
  3,744 lines. Any addition must conserve this entire byte prefix.
- Relevant idiosyncrasy: 2017 EAC-I's rural Lourd/Leger split is not EHCVM;
  EHCVM's household input roster and parcel fertilizer questions are distinct
  grains. A revisit of EAC-I enumeration areas does not establish household panel.
- Existing tension (preserve, do not silently dismiss): the old price summary
  says “community (market) price | 2014-15 ONLY”, while the 2026-10-30 entry says
  “2018-19 now uses the same canonical observation index.” GH #821's remaining
  issue is 2021 availability. Similarly, the old claim “carry no
  agriculture-input block” conflicts with the documented s16a/s16b sources.

## §2 Existing machinery and prior art

Inherits `.coder/ledger/STANDING.md`; read the shared prior-art-ledger skill,
AGENTS.md (CLAUDE.md byte-identical), README and task template.

| Source / symbol | Current role; verification already named | Decision |
|---|---|---|
| Mali `_/mali.py::plot_inputs_finalize`, `_input_labels` | EAC-I finalizer and existing input labels; `test_gh1118_mali_missing_input_units.py`, `test_mali_plot_inputs_crop_marker.py` | reuse; do not infer EHCVM binding |
| Mali `_/data_scheme.yml::plot_inputs` | currently EAC-I plot-grain declaration | diagnose own-wave extension requirements |
| `.coder/ledger/821-mali-community-prices.md`, `821-mali2021-availability.md` | merged 2018 implementation and narrowed 2021 hold | reuse; no loader replay |
| `.coder/ledger/822-mali-community-price-readings.md`, CONTENTS GH #1179 memo | restored three readings; approved gram/centilitre rule distinct from invalid codes | reuse; no eligibility repair |
| `tests/test_gh821_mali_s05_community_prices.py`, `test_gh1179_mali_community_quantity.py` | existing focused coverage | source read only; execution prohibited |
| GitHub issues #1483/#1179/#821 and all current comments | freshly read via primary tools: 0 / 2 / 1 comments | reconcile before any patch |

## §3 Definitions in force

Cite STANDING §3, AGENTS canonical schema/coverage sections and Mali CONTENTS.
`community_prices.Quantity` has its signed source-specific rules; EHCVM 2018
has no weight field. A regional price source has no invented grappe crosswalk.
The 2017 harvest-state card/export conflict and code 4 authority stay held.
No new unit, input, condition, eligibility, sampling, weight or default choice
is delegated to this worker.

## §4 Invariants and boundaries

STANDING §4 applies; dispatch is stricter: no project/dependency imports,
venv/image mount, tests or collection, provider/getter/parser/native rows or
headers, Country/Feature/build/audit harness, source acquisition, DVC operation,
shared cache mutation, shared framework/registry edits or other-country edits.
Only own country, uniquely named pure test source if cleared, and this ledger
may change. New fixtures or baseline/golden/expected/snapshot changes require
stopping that change and reporting through the existing absolute channel.
No scientific choice is implied by a source-only gate. Finite holds move to
the next independent issue, never idle waiting or speculative defaults.

Initial Git status contained only untracked root-level MESSAGES_TO_AGENT.txt
and QUESTIONS_FROM_AGENT.txt. They are not the shared absolute channels and
will be left alone and excluded from commits.

## §5 Reuse decisions / execution disclosure

- Reuse already merged work; do not redo 2018 prices, EAC-I reading restoration,
  approved Poids conversion, compiler cells or cluster measurements.
- Extend documentation when new source facts justify it. New EHCVM input
  mapping/grain choices require exact own-round authority and ROOT decisions.
- Static skill discovery: Make's wrapper invokes Poetry. The audited script is
  stdlib-only, but `python3 -S scripts/api_surface_audit.py --skills` failed
  before execution with `SyntaxError: future feature annotations is not
  defined` (old system Python). No package/runtime setup follows. Read on-disk
  skill frontmatter directly; no full api-audit pass is claimed.
- No tests run or collected. ROOT will execute pure controls after frozen
  source review. Test plans are not passing tests or data/API acceptance.

## §6 Questions and next steps

1. GH #1483: establish exact own-round s16b fields and s16a grain from public
   dictionaries/source, then distinguish mechanical work from reserved choices.
2. GH #1179: producer instructions/correction must identify exact passage,
   reading and field for unresolved units; no price/weight inference.
3. GH #821: exact Mali 2021 community/market instrument or release clarification
   remains necessary for C4; keep unsure. Do not rerun merged 2018 work.
4. Re-read the absolute steering channel after each issue/major step and before
   ending; append precise new holds/questions once in the paired channel.

### Phase 3 verification

Pending source reconciliation and finalization. This first ledger commit
precedes task implementation. No runtime acceptance is asserted.
