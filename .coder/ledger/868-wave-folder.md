# Prior-Art Ledger — #868 round-aware grain audit stamps

**Search tier:** ripgrep + git. Inherits STANDING.md and 323-framework.md.

## §1 Task
Preserve a reducer's explicit round name in `wave` while stamping its report
into the framework wave folder's parquet, including a subsequent L2-wave hit.

## §2 Existing machinery
- `build_transforms.build_context` scopes the country/table/folder around both
  YAML hook paths in `Wave.grab_data`; extend its report metadata.
- `_resolve_build_context` gives explicit kwargs precedence. Preserve it.
- `_cells_blanked_report` assembles the existing JSON-safe report; extend it.
- `country._wave_grain_audit` selects build reports for the wave footer; extend
  its folder selection, retaining old-report fallback and site/cache exclusions.
- `test_gh868_reduce_to_agreed_ledger.py` already tests cold stamp/warm replay
  through a synthetic country. Extend this fixture with explicit-round hooks.

## §3 Definitions
Per STANDING.md and CLAUDE.md grain-collapse policy, disclosure must survive
parquet reads. `wave` names the reported round; `Wave.year` is the build folder,
which can contain multiple rounds. `collapsed_rows` is rows minus groups, not
loss (`destroyed` and `nan_key_rows` remain zero for cells_blanked).

## §4 Invariants
Reuse the parquet grain footer and the single replay/strict choke point.
Never change reducer values or infer folder identity from the frame's `t`:
YAML `check_adding_t` can stamp the folder there, while a hook names a round.
Legacy reports without wave_folder still select on wave. Retain explicit wave
on replay. Runtime context must reset even after an exception.

## §5 Reuse
Extend existing context/report/filter and synthetic fixture. No new cache,
reducer, report mechanism, or survey mapping. Uganda prose alone needs current
#848 counts: twelve groups across five reports, with the twelfth a 2018-19
season-B sold-condition conflict; existing CONTENTS.org records that fact.

## §6 Open questions
None: approved release workplan explicitly chooses the wave_folder repair.

## Phase 3 verification
OK (§2–§5): the reducer, context precedence, footer writer and replay mechanism
are reused. The new synthetic round test and collapsed_rows assertion both
fail on b471342e7; the focused current suite passes (109 tests).
