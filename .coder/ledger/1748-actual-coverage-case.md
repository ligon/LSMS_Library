# Prior art for #1748 actual sample-coverage attribution

## 1. Task
Give the existing all-waves assertion a static public test name for the
actual failing country, including isolated call-phase errors. Preserve the
sample result and every existing assertion. No protected output change.

## 2. Existing machinery
- `tests/test_sample.py`: `SAMPLE_COUNTRIES`, the availability skip,
  `_get_sample`'s one-frame-per-country cache, and
  `TestSample.test_covers_all_waves` are the existing coverage contract.
- `scripts/private_ci_check.py`: `_checked_in_tests` loads regular
  committed test source and traverses class and method definitions.
  `_junit_failure_locations` strips parameters and emits only exact
  checked-in functions and counts. Covered by
  `tests/test_gh932_safe_ci_failure_locations.py`.
- #1788 at 98e9e3c52 adds 46 separate static ISA wave probes sharing the
  same sample. A transient one-time error from the original comparison
  can still fail just the uninformative generic case.
- Discovery used git, rg and the actual pytest collection.

## 3. Definitions
Coverage still means `Country.waves` minus the existing explicit exclusions
compared against the same sample frame's unique `t` values. See
`CLAUDE.md`, `lsms_library/data_info.yml`, and `STANDING.md`.

## 4. Invariants
Keep per-file scheduling, previous absence skips and separate noncoverage
tests unchanged. No assertion message, parameter, paths from data, cache
metadata or new public channels. No change to builds, country contracts,
the private workflow or shared caches. See `STANDING.md` for IO rules.

## 5. Reuse
Reuse the original method body and availability fixture. Extend only the
pytest routing. Static method definitions are necessary because generated
names and parameter text are not admitted by the existing wrapper. Unknown
future countries still use discovery and the original assertion.

## 6. Open questions
This does not prove any past run's cause, or diagnose a cache stage.
The independently green read repair and wave probes remain separate.
