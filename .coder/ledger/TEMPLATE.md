# Prior-Art Ledger — <issue # / task slug>

> Per-task ledger (copy of this template). Living, git-tracked snapshot of the
> machinery, definitions, and conventions that bear on THIS task. Edit in place;
> git history is the journal. Inherits the repo §0 baseline in `STANDING.md` —
> cite it, `CLAUDE.md`, and `lsms_library/data_info.yml` rather than re-copying.
> Keep the `§N` numbers; code comments and the Phase 3 report cite them.

**Search tier used:** <gitnexus | ripgrep+git floor — say which, and note if gitnexus was read-only>

## §1 Task, restated
<One paragraph in the repo's own vocabulary (country, wave, table, feature,
`data_info.yml` / `data_scheme.yml`, derived vs registered). If you can't restate
it without inventing terms, you don't understand it yet.>

## §2 Existing machinery (this task's area)
Search the concept *and its synonyms* for what already touches THIS table /
quantity / country — beyond the §0 baseline. Check: the country's `_/` config,
sibling countries' scripts, `transformations.py`, `country.py`, `local_tools.py`.

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
|        |           |              |         |                      |

## §3 Definitions & conventions in force
Cite the authoritative source — do not paraphrase. Most live in `STANDING.md §3`,
`lsms_library/data_info.yml`, or the country's `CONTENTS.org`. List only the ones
this task actually leans on, with their `path:line` / section.

- <term>: <local meaning>, `path:line` (or "per STANDING.md §3 / data_info.yml").

## §4 Invariants & assumptions
The landmines for THIS task. Reference `STANDING.md §4` for the repo-wide ones;
add any task-specific ones (e.g. "this wave's source has a `round` column",
"EHCVM: `v: grappe`, not `[vague, grappe]`").

- <invariant — and the `path:line` that enforces or assumes it>.

## §5 Reuse decision
For each quantity this task needs. "new" must justify why the §0 default or an
existing tested path doesn't fit.

| quantity | decision (reuse / extend / new) | reason |
|----------|--------------------------------|--------|
|          |                                |        |

## §6 Open questions for the human
- <question — and what decision it blocks>

---
### Phase 3 — verification (fill at task end)
Anchored check, not a general review. For each new/changed symbol report one of:
`CONTRADICTION` (violates a ledger entry), `REINVENTION` (duplicates existing
tested code — by formula, not just name), or `OK (anchored on §N)`. Anything not
tied to a ledger entry is out of scope — say so rather than padding.

- <symbol> — <verdict> (§N): <one line>
