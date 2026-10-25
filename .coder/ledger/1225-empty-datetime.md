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
| Read-time coercion | `lsms_library/country.py:7169`, `_coerce_datetime_column` | `tests/test_gh798_datetime_mixed.py`; `TestReadTimeCast` in `tests/test_gh1045_parse_datetime.py` | Extend the existing helper only. |
| Already-datetime pass-through | `lsms_library/country.py:7211` | `TestReadTimeCast.test_datetime64_is_a_pass_through`; mixed-date module's resolution test | Keep this check before the empty-input branch. |
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

Extend the existing helper with a typed empty Series constructor, following
the existing empty-input constructor in `parse_datetime` (`datetime_parse.py`,
section 2). Preserve `s.index` and `s.name` explicitly, copy `s.attrs`, and
keep the literal already-datetime pass-through before this branch. A cast
from the input dtype is unsuitable: independent review demonstrated that
an empty `timedelta64[ns]` Series rejects conversion to `datetime64[us]`.
Zero rows require no interpretation of the source dtype. Do not introduce
an intermediate object cast to work around that unsupported conversion.
Reuse the existing mixed-date test module, adding timedelta cases for all
three index shapes and both dtype enforcers.

## §6 Open questions for the coordinator

None. The coordinator reviewed ledger commit `b405437ef` and released the
original narrow plan before test or production edits. Independent review
then found the empty-timedelta cast failure in implementation
`e7c2931030eadf275772f9c7a3336d34b7edbf87`. The coordinator preauthorized
the explicit empty-constructor amendment above, still within the same three
tracked paths. Amendment plan commit `45805deec` preceded production/test
edits. The final source/test pin is
`5123365b9a3d3fe93004399dd23b4bf2c913d0f2`; original and amended red/pass
logs remain separate and unchanged.

## Phase 3 verification

- `_coerce_datetime_column`: OK (sections 2-4). The original cast preserved
  tested object/string inputs but failed for empty timedelta input. The
  amended implementation reuses the parser's empty-constructor precedent,
  explicitly preserving index, name and attrs. The already-datetime early
  return and all populated parsing code remain unchanged.
- Empty metadata and dtype tests: OK (sections 3-4). Nine object/string/timedelta cases
  cover an ordinary Index, an empty MultiIndex and a MultiIndex retaining
  unused string/integer levels. Full Series/index equality, name, attrs,
  input preservation and absence of warnings are checked explicitly.
  Twelve already-datetime empty cases retain literal identity and the
  original second/millisecond/microsecond/nanosecond resolution.
- Caller integration: OK (section 2). Twelve object/timedelta empty-frame cases exercise both
  dtype enforcers and verify the full frame/index and attrs.
- Discriminating control: with production still byte-identical to base
  `0102af8cf`, the new tests gave **12 failed, 12 passed, 19 deselected**
  in 3.42 seconds. Ordinary empty input returned seconds instead of
  microseconds; empty MultiIndex input raised the reported alignment error,
  swallowed by the enforcers. Existing-datetime pass-through controls passed.
- Original candidate: **49 passed, 2 deselected** in 4.03 seconds. This includes the
  complete hermetic mixed-date module, the six GH1045 read-time controls,
  its build-parser empty-MultiIndex control and the existing structural and
  measured read/build-fingerprint regression. Zero test failures or skips.
  The two warm-corpus tests were explicitly deselected; no source-data
  build, acquisition, shared-cache mutation or corpus census ran.
- Amendment discriminating control: after an explicit Git diff assertion
  that `country.py` still matched `e7c293103`, the new timedelta selection
  gave **9 failed, 43 deselected** in 2.27 seconds. The direct helper raised
  `TypeError`; the enforcers silently retained `timedelta64[ns]`. The log is
  `control/fix1225/e7-timedelta-regressions.log` beneath this job's local root.
- Amended candidate: **58 passed, 2 deselected** in 3.63 seconds, zero test
  skips/failures. This is the entire original finite selection plus nine
  timedelta controls. The log is
  `control/fix1225/amended-candidate-hermetic-tests.log`. No data/census ran.
- Both invocations emitted the same Google API Core FutureWarning about
  future Python 3.11 support before pytest started. No new helper warning
  was observed. Full logs remain private in
  `/local/job40498884/control/fix1225/{base-new-tests,candidate-hermetic-tests}.log`.

The original and amended candidate selections were:

```text
tests/test_gh798_datetime_mixed.py
tests/test_gh1045_parse_datetime.py::TestReadTimeCast
tests/test_gh1045_parse_datetime.py::TestRound2::test_empty_input_with_a_multiindex
tests/test_build_transform_hash.py::test_empty_constructor_dtype_code_is_not_in_any_build_closure
-k 'not warm_interview_date' -q -p no:cacheprovider
```

All runs used `/local/job40498884/venv_img/bin/python` from this worktree,
with `PYTHONPATH` naming this worktree, `LSMS_SKIP_AUTH=1`, private
`LSMS_DATA_DIR`/`LSMS_CONFIG_DIR` under `control/fix1225`, and
`OPENBLAS_NUM_THREADS=OMP_NUM_THREADS=MKL_NUM_THREADS=LSMS_BUILD_WORKERS=1`.
Before calling `pytest.main` in the same process, each run asserted:

```python
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
```

The original verification did not cover empty timedelta input. Independent
review found that contradiction; the amended reuse plan and discriminating
tests resolve it without changing populated parsing. No remaining
contradiction or reinvention was found against this ledger. Country data
behavior and a full-corpus numerical hash census are outside this check.

Sue
