# LSMS Library v0.7.4

Patch release over v0.7.3. One fix; **no data-API changes**.

## Fix
- **`diagnostics.food_acquired_u_code_leaks` is now NaN-safe.** A *categorical*
  `u` index level with a `NaN` kept the NaN as a `float` through `.astype(str)`
  (object dtype coerces `NaN -> 'nan'`), feeding `re.match` a float and raising
  `TypeError: expected string or bytes-like object, got 'float'`. It surfaced
  only in the `data-tests` CI job (which builds from a fresh DVC pull → a
  categorical `u` with a NaN, and is skipped on PRs), on the v0.7.3 `master`
  merge. A `NaN` `u` means no unit was recorded — not a leaked code — so it is
  now correctly excluded. Adds a categorical-NaN regression test.
  (PR #451; `master` cherry-pick `d981c918`.)

## Notes
- **No change to `Country().food_acquired()` or any data output vs v0.7.3** —
  this is purely a fix to an audit/diagnostics helper.
- Version is derived from the git tag via poetry-dynamic-versioning.
