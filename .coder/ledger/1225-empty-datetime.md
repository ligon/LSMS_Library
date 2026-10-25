# Prior-Art Ledger - GH1225 empty read-time datetime coercion

**Search tier:** ripgrep and git; no GitNexus tool is available.
**Base:** `0102af8cf696127ef410bb7b530db557ee90b9b8`.
Inherits `STANDING.md`, especially sections 3 and 4.

## §1 Task, restated

Make the read-time `_coerce_datetime_column` accept a zero-row Series with
either an ordinary Index or a MultiIndex. A non-datetime empty input must
become `datetime64[us]` without losing its index, name or attrs. An already
datetime input remains a literal pass-through at its existing resolution.
This is the narrow shared helper defect reported in GH1225, not a change to
wave parsing, country config or populated date interpretation.

## §2 Existing machinery

| Machinery | Source | Existing coverage | Decision |
|---|---|---|---|
| Read-time coercion | `lsms_library/country.py:7169`, `_coerce_datetime_column` | `tests/test_gh798_datetime_mixed.py`; `TestReadTime` in `tests/test_gh1045_parse_datetime.py` | Extend the existing helper only. |
| Already-datetime pass-through | `lsms_library/country.py:7211` | `TestReadTime.test_datetime64_is_a_pass_through`; mixed-date module's resolution test | Keep this check before the empty-input branch. |
| Stored date dtype and empty parser precedent | `lsms_library/datetime_parse.py:540`, `parse_datetime`, especially its zero-row branch at line 584 | `TestRound2.test_empty_input_with_a_multiindex` in `tests/test_gh1045_parse_datetime.py` | Reuse the `datetime64[us]` contract; do not invoke the build parser or change its calendar/report rules. |
| Declared and canonical dtype entry points | `lsms_library/country.py`, `_enforce_declared_dtypes` and `_enforce_canonical_dtypes` | `tests/test_gh798_datetime_mixed.py::test_both_enforcers_use_the_helper` | Keep callers and exception policy unchanged; add empty-frame integration controls. |
| Read/build fingerprint boundary | `lsms_library/_build_registry.py:228` and `tests/test_build_transform_hash.py:585` | `test_empty_constructor_dtype_code_is_not_in_any_build_closure` | Reuse existing exclusion and regression; no exclusion changes. |

## §3 Definitions and conventions in force

- `AGENTS.md`, "Calendar quantities": dates store `datetime64[us]`; month
  and year are separate nullable integer quantities, not dates.
- `AGENTS.md:641` and the helper's current early return: an already-datetime
  column passes through literally, including its stored resolution.
- `datetime_parse.py:556`: the build parser requires a calendar and owns
  parse reports and derivations. This read-time empty case has no values
  to interpret and must not introduce a calendar default or a report.
- `STANDING.md` sections 3 and 4 remain authoritative for schema, sanctioned
  I/O and read-path transformations. This task requires no data I/O.

## §4 Invariants and assumptions

- Preserve populated mixed-format parsing, per-wave year-last evidence,
  missingness, warning classes and timezone refusal unchanged.
- Preserve the full index (including unused MultiIndex levels, code dtypes
  and names), Series name and attrs; do not replace it with a default index.
- Do not mutate the input Series or coerce an already-datetime input to a
  different resolution, even when empty.
- Keep the change after the existing datetime pass-through and before
  string extraction/alignment, where the zero-row MultiIndex currently fails.
- Limit tracked changes to this ledger, the helper in `country.py`, and
  `tests/test_gh798_datetime_mixed.py`. No country source/cache access,
  dependency changes, build fingerprint exclusions or test weakening.

## §5 Reuse decision

Extend the existing helper with an empty-input cast to the parser's existing
microsecond dtype. Prefer `s.astype('datetime64[us]')` to a newly constructed
Series because it carries pandas' existing index/name/attrs propagation.
Verify that propagation explicitly rather than assuming it. Reuse the
existing mixed-date test module and helper/caller coverage.

## §6 Open questions for the coordinator

None about date semantics. Production edits await coordinator review of this
ledger and the narrow reuse plan. First add the empty-index regressions and
run them against the unchanged base; then apply the minimal helper fix and
run the affected hermetic module. Exclude the two warm-corpus cases by name
to honor the no-data-access scope, and retain that qualification.

## Phase 3 verification

Pending. No implementation or runtime success is claimed by this ledger.

Sue
