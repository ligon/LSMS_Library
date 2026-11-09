# Prior-Art Ledger — GH709 historical threshold correction

**Base:** `86077a65f225c42e2b03005e4b5eb15ec8a509a8`.
**Search tier:** ripgrep + git; documentation and metadata only. No GitNexus,
package import, survey/source/cache read, reader execution, or corpus validation.
Inherits [STANDING.md](STANDING.md), especially §§3–4, and the byte-identical
repository `AGENTS.md` / `CLAUDE.md`. Task-specific instructions prohibit the
API/hash/test suites; skill descriptions were enumerated as text instead.

## §1 Task, restated

Append a dated correction to `slurm_logs/settlement_thresholds/FINDINGS_threshold_rules.org`,
preserving its entire original byte prefix. Supersede the first bullet under
“Data-quality landmines found along the way” (lines 685–690 at base), which calls
`1.7498005798264095e+100` corruption, a float artifact and undocumented, and
mistakes survey year for the file-format boundary. Link the precise later
sentinel findings. No new measurement, scientific adjudication or policy change.

## §2 Existing machinery and evidence

| Anchor at base | Existing record | Verification / reuse |
|---|---|---|
| `slurm_logs/settlement_thresholds/FINDINGS_threshold_rules.org:683` | Historical paragraph to correct | Extend by append only; static original-prefix comparison, no prose tests |
| `slurm_logs/sentinel_audit/FINDINGS_1.7498e100.org:94` | Bit pattern: exact `2^333`, IEEE754 `0x54c0000000000000` | Reuse established report, not a new census |
| Same report, `:125`, `:153`, `:270` | Reader mechanism and source history; `double` and format ≤105, not survey year | Reuse with explicit attribution; report cites installed pandas 3.0.2, upstream PR #59325/milestone 3.0 and absence from v2.2.3 |
| Same report, `:329`, `:465` | Self-correction to prior blanket “undeclared” claim and disclosed coverage limits | Reuse qualified record; do not restore blanket negatives or current counts |
| Same report, `:574` | Reader owns conditional missing conversion; no unconditional repository sentinel list | Preserve mechanism and policy boundary; no implementation added or executed |
| `pyproject.toml:10`; `CLAUDE.md:988` | Current source declares `pandas >=3.0`; repository targets pandas 3.0+ | Text inspection only; do not repeat the report's old dependency-floor state as current |
| Supplied `current-issue.json` / `current-comments.json` | Full open GH709 body and its one supplied comment, including self-corrections | Read both bodies; narrowed append-only work takes precedence over stale header and original broader recommendations |

No existing execution tests are needed or run for this prose change. This
checkout is shallow and exposes only the pinned base commit: upstream tag/PR
history is attributed to the later report, not independently verified here.

## §3 Definitions and conventions in force

Per `STANDING.md` §3 and `CLAUDE.md` “Pandas 3.0 Targets”, keep current dependency
state distinct from historical observations. The later report's “The bit
pattern” and “Mechanism” establish a documented Stata system-missing (`.`)
marker for `double` in `.dta` formats 104/105. Its reader condition is format
≤105 **and** storage type `double`; this is not a survey-year rule. Its
installed-stack observations and version-history assertions remain attributed
to that report; they establish no universal verdict about arbitrary occurrences.

## §4 Invariants and assumptions

Inherit `STANDING.md` §4 and `CLAUDE.md` “Data Access”; this task authorizes no
data access at all. Only this ledger and the historical threshold report may
change. Preserve all other Git modes/types/blobs and the report's original
bytes. Use actual UTC date. Add no country-specific interpretation, threshold
list, unconditional sentinel filter, reader/config/dependency-floor change,
normalization, coverage verdict, upstream filing/contact or current census.
Do not imply other historical conclusions were remeasured. No imports of the
library or readers; prepared runtime may run stdlib-only static checks with
site initialization disabled. Import identity is supplied setup metadata,
not a newly executed package-import assertion.

## §5 Reuse decision

| Item | Decision | Reason |
|---|---|---|
| Marker diagnosis and provenance | Reuse | Precise later findings already document it, including qualifications |
| Historical threshold report | Extend | Append makes the correction visible without altering historical evidence |
| Reader and scientific/data policy | Reuse unchanged | Existing type-and-format reader mechanism owns conversion; no new rule authorized |
| Validation | Reuse git/stdlib static checks | Byte-prefix, pinned-link and tracked-object comparisons directly verify this task |

## §6 Open questions and authority

The charter's explicit “OWNED paths ONLY” names this ledger, while its later
FIRSTNEW instruction names `709-threshold-correction-remote-task.md`. Recorded
the conflict in the absolute shared `QUESTIONS_FROM_AGENT.txt`; absent contrary
steering, use this explicitly owned path and do not create the unowned alias.
The charter already authorizes proceeding after surfacing this ledger checkpoint;
no further routine approval is needed. No open scientific question is introduced.

### Phase 3 — verification

UNRUN. Initial ledger-only commit precedes the document append. Final qualification
will be a separate ledger-only commit after static validation.
