# Prior-Art Ledger — GH #323 (Mali `pid` identifier)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git floor (gitnexus MCP not reachable from this
worktree session). Every claim below was checked against the SOURCE `.dta` via
`get_dataframe()` — never against a cached parquet, which is written
POST-collapse and therefore cannot show the evidence (see §4).

**Scope.** This ledger covers ONLY the `pid` identifier fix. The
`interview_date` / `visit` work that shared branch `fix/323-mali` is a separate
change requiring a new `{const: value}` core primitive; it is deliberately NOT
in this branch and is not assessed here.

## §1 Task, restated

Mali's `household_roster` and `individual_education` are declared with index
`(t, i, pid)` in `countries/Mali/_/data_scheme.yml`. In the 2014-15 wave both
wire `pid` in `idxvars` to source column `s01q`. `s01q` is not the person's
identifier. The declared index is therefore non-unique in the source, and
`_normalize_dataframe_index` reduces it with `groupby().first()` — silently
deleting rows. Fix the identifier so the declared index is unique at source, and
prove the recovered rows are real. Config-only: `countries/Mali/**`, no core.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py` | collapses a non-unique DECLARED index with `groupby().first()` — the mechanism that ate the rows | yes | **do not touch** (core; owned by PR #614) |
| `df_data_grabber` | `lsms_library/local_tools.py` | builds `idxvars`/`myvars` from one source file; auto-applies `format_id` to `idxvars` | yes | reuse as-is |
| `pid(value)` | `countries/Mali/_/mali.py:13` | Mali's `pid` formatter; expects a 3-component composite | — | reuse (`[grappe, menage, s01q00]` is 3 components) |
| `people_last7days` (2014-15) | `countries/Mali/2014-15/_/people_last7days.py:48` | **already** builds pid as `mali_pid([grappe, menage, s01q00])` and asserts `(t,i,pid)` uniqueness | asserts | **prior art — this is the correct wiring, already in the repo** |
| `roster_to_characteristics` | `lsms_library/transformations.py` | derives `household_characteristics` from `household_roster`; applies the MonthsSpent residence filter | yes | untouched; consumes the fix |

The single most important entry is the fourth: the correct `pid` formula was
**already written and asserted** one directory over, in this same wave, against
this same source file. `data_info.yml` and `people_last7days.py` had simply
drifted out of the same keyspace. Nothing new was invented here.

## §3 Definitions & conventions in force

- `household_roster` / `individual_education` index: `(t, i, pid)` —
  `countries/Mali/_/data_scheme.yml:18,26`. Unchanged by this task.
- `pid` = the *person*. Per `lsms_library/data_info.yml` (Index Info), `pid` is
  the within-household individual key; `i` is the household.
- Source variable labels, read from `EACIIND_p1.dta` metadata (ground truth, not
  paraphrase):
  - `s01q00` → `"Numero d'ordre"` — the person's own roster line number.
  - `s01q` → `"Code du répondant"` — the roster line of whoever ANSWERED
    section 1 on the household's behalf. A household-level fact.
  - (For contrast, the same file's `s01q06` → `"Code du père dans le menage"`,
    another line-number *reference*, mis-wired once before as education in
    GH #171. Same family of bug: a reference column mistaken for a value.)
- EHCVM convention (`CLAUDE.md`): Mali is `v: grappe`, `i: [grappe, menage]`.
  Preserved; only the `pid` component changed.

## §4 Invariants & assumptions

- **A declared index must be unique in the source.** Duplicates on a declared
  index mean the IDENTIFIER IS BROKEN or a LEVEL IS MISSING — not "reduce me".
  Per `SkunkWorks/grain_aggregation_policy.org` and decision D1 of
  `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`, core does **not**
  aggregate, and no reducer may be declared to paper over this. Mali is the case
  that argument is made from: no reducer is correct — `first` keeps one person,
  `sum` is meaningless on `Sex`.
- **The L2 parquet is written POST-collapse.** A warm read cannot see the lost
  rows: the cached index is already unique. Every measurement in this ledger was
  taken with `LSMS_NO_CACHE=1`. (This is *why* #323 was closed once and stayed
  broken — `CLAUDE.md` cache section; DESIGN §"Why the old warning could not
  fire".)
- **Core is owned centrally.** `lsms_library/*.py` is off-limits on this branch
  (PR #614 Site 1/3, a Site-2 PR, a shared-helpers PR). Verified: this branch's
  diff touches one file, `countries/Mali/2014-15/_/data_info.yml`.
- `format_id` is auto-applied to `idxvars` (`CLAUDE.md`), so the new
  `[grappe, menage, s01q00]` composite is formatted exactly as the old one was;
  no formatter change needed.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| Mali 2014-15 `pid` | **reuse** | Adopt the formula `people_last7days.py:48` already uses and asserts: `mali_pid([grappe, menage, s01q00])`. No new machinery. |
| the collapse itself | **do not touch** | Core (`_normalize_dataframe_index`). Owned by PR #614 under decision D1. |
| `aggregation:` YAML key | **rejected** | Would contradict §4. Declaring `{pid: first}` here "puts a signature on the corpse" (DESIGN D1). |
| `household_characteristics` | **no change** | Derived at runtime; it picks the fix up for free. |

## §6 Open questions for the human

- 469 of 3,804 households have a `s01q` ("Code du répondant") that VARIES across
  member rows — i.e. different respondents answered for different members. That
  is a property of the source instrument and is now simply unused by `pid`. It is
  not carried into any table. If the respondent identity is ever wanted, it is a
  `myvars` column, never an index level. **No decision blocked.**
- The `interview_date` / `visit` half of the original `fix/323-mali` branch is
  deferred to its own PR (it needs a `{const: value}` idxvars primitive in core).
  Not addressed here.

---
### Phase 3 — verification

Anchored on the ledger, `LSMS_NO_CACHE=1`, `LSMS_COUNTRIES_ROOT=<worktree>`.

- **`Mali/2014-15/_/data_info.yml` `household_roster.idxvars.pid`** — `OK
  (anchored on §2, §3, §5)`. Now `[grappe, menage, s01q00]`, the formula
  `people_last7days.py:48` already used. Not a reinvention; a re-convergence.
- **`Mali/2014-15/_/data_info.yml` `individual_education.idxvars.pid`** — `OK
  (anchored on §2, §3)`. Same source file, same block, same bug.
- **No `aggregation:` key added; no `lsms_library/*.py` touched** — `OK
  (anchored on §4)`.

Evidence (all cold-build):

| check | before | after | source truth |
|---|---|---|---|
| `household_roster` 2014-15 rows | 5,149 | **37,175** | 37,175 rows in `EACIIND_p1.dta` |
| members/HH (mean) | 1.354 | **9.773** | 9.7726 |
| `(t, i, pid)` unique **in source** | **NO** — 5,149 groups over 37,175 rows | **YES** — 37,175/37,175 | — |
| `individual_education` 2014-15 rows | 2,093 | **4,097** | 4,097 rows with non-null `s02q23` |
| `household_roster` total (all waves) | 188,706 | 220,732 | +32,026 people |

The "index unique" test on the *returned* frame is `True` both before and after —
that is precisely the disease (§4): the collapse **makes** it unique. The
load-bearing check is uniqueness of the declared key **in the source**, which is
the row above.

Corroboration, spot-checked against raw `EACIIND_p1.dta`:
- HH `grappe=1, menage=3`: source n=9, ages `[0,1,2,3,5,7,20,28,43]`, 6M/3F.
  API n=9, identical ages, 6M/3F, `pid` = `1003001…1003009`.
- HH `grappe=1, menage=6`: source n=15. API n=15, identical 15-age vector, 9F/6M.
- `s01q00` enumerates 1..n cleanly in **all 3,804** households; every household's
  `s01q` value is itself one of that household's `s01q00` lines — confirming
  `s01q` is a *reference into* the person numbering, not a person id.
- Derived `household_characteristics` 2014-15: 3,781 HH, 36,614 persons, mean
  9.68, max 84 (roster max 84). `log HSize` for HH 1003 = 2.197225 = ln(9) and
  for HH 1006 = 2.708050 = ln(15) — matching the 9- and 15-member source
  households exactly. The 561-person / 23-HH shortfall vs. 37,175 is the
  documented MonthsSpent residence filter (`CLAUDE.md`), not a loss.

Blast radius, checked exhaustively — every Mali (wave × table) declaring `pid`,
uniqueness of the declared key **in the source**:

| wave | table | pid cols | rows | uniq(i,pid) | |
|---|---|---|---|---|---|
| 2014-15 | household_roster | `[grappe, menage, s01q00]` | 37,175 | 37,175 | fixed |
| 2014-15 | individual_education | `[grappe, menage, s01q00]` | 37,175 | 37,175 | fixed |
| 2017-18 | household_roster | `[grappe, exploitation, codeid]` | 94,071 | 94,071 | already OK |
| 2017-18 | individual_education | `[grappe, exploitation, codeid]` | 80,600 | 80,600 | already OK |
| 2018-19 | individual_education | `[numind]` | 46,014 | 46,014 | already OK |
| 2021-22 | individual_education | `[numind]` | 43,472 | 43,472 | already OK |

2018-19 / 2021-22 `household_roster` declare `pid` inside a `dfs:` sub-block
(`s01q00a`, `membres__id`); their returned row counts are byte-identical before
and after and equal their source row counts (46,014 / 43,472), so they carry no
collapse. **2014-15 was the only affected wave.**

Tests: `tests/test_roster_residence_filter.py` + `tests/test_schema_consistency.py`
— 231 passed against the worktree config tree.
