# Prior-Art Ledgers

This directory holds **prior-art ledgers** for the LSMS Library, per the shared
`prior-art-ledger` skill (`~/.sucoder/skills/prior-art-ledger/SKILL.md`). A
ledger is a short, git-tracked inventory of the existing machinery,
definitions, and conventions that bear on a task. It is built *before* the work
(Phase 1), kept in context *during* the work (Phase 2), and is the *only* oracle
the verification step trusts (Phase 3). The point is narrow: catch the two
failures this repo is most prone to —

1. **Reinvention** — re-implementing something the library already implements and
   tests (a derived table, the `v`-join, kinship decomposition, a unit
   conversion).
2. **Contradiction** — violating a definition/convention already in force (the
   canonical schema, the IO sanctions, a cache invariant).

## When to open a ledger

Follow the skill: open one at the start of a **non-trivial** task that adds or
changes an estimator, statistic, transformation, derived table, or analysis —
i.e. anything that computes a quantity the codebase might already compute, or
whose correctness depends on a local definition. Skip it for one-line fixes,
docs, or throwaway scripts.

## Path convention (repo adaptation)

The skill's default is a single in-place `.coder/ledger.md`. This repo runs many
concurrent branches and agents (see the backlog-workflow / scrum-master-hpc
skills), so a single shared file would churn and conflict. **Use one ledger per
task:**

```
.coder/ledger/<issue-or-slug>.md     # e.g. .coder/ledger/245-uganda-food-units.md
```

Copy `TEMPLATE.md` to that path at task start. Commit the ledger in its **own**
commit, separate from code (`ledger(#245): food unit factors already live in
uganda.py:…; mark Quantity_kg as reuse`). The commit history is the journal —
do **not** append a running log inside the file; edit it in place to reflect
current understanding.

## The standing baseline — read `STANDING.md` first

`STANDING.md` is a repo-wide §0 baseline: the library's most reuse-prone
machinery, the definitions in force, and the load-bearing invariants, each with
a `path:line` anchor. **Per-task ledgers inherit it** — your §3/§4 should *cite*
`STANDING.md`, `CLAUDE.md`, and `lsms_library/data_info.yml` rather than
re-copy them. Spend the per-task effort where it actually varies: §2 (search for
existing machinery touching *your* table/quantity) and §5 (the reuse / extend /
new decision).

## Search tier

Record the tier you used (skill "Tooling" section). In this repo:

- **gitnexus** (`mcp__gitnexus__*`) is the preferred tier for "what calls /
  depends on this" — *when its index is writable*. It is sometimes mounted
  read-only in agent sessions (FTS-ensure errors on every Bash call); when it
  is, drop to the floor and say so.
- **Floor:** `git` + `ripgrep` (+ `python -m ast` / `ast-grep`). Always works.
  `STANDING.md` itself was seeded at the ripgrep floor.

`STANDING.md` line numbers are anchored to a stated commit and *will* drift —
re-grep the symbol name if a line looks off.
