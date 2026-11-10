# Prior-art ledger — GH #932 bounded full-data workers (private donor)

**Search tier:** Git + ripgrep (floor). Source pin:
`9f89151ddd783a07fa57b9277da3677f7024ce7c`.

## §1 Task, restated

Apply Root's 2026-11-10 design in
`reports/932-bounded-data-parallelism-design-20261110/PLAN.md` under the
external campaign root: the dedicated PR workflow must invoke
`poetry run pytest -n 2 --dist=loadfile` with its entire existing suffix and
private capture preserved. This is a draft donor for integration review.

## §2 Existing machinery

| Path / symbol | Existing behavior and verification | Decision |
|---|---|---|
| `.github/workflows/pr-data-validation.yml`, full-suite step | Direct serial pytest inside private capture; eligibility, exact merge/parents, credentials, prebuild and thread/build pins | Extend only pytest command prefix |
| `Makefile`, `PYTEST_ARGS` | File-grouped xdist; direct workflow does not inherit it | Reuse policy with explicitly bounded two workers |
| `tests/test_gh932_pr_data_validation.py::test_existing_data_recipe_is_reused_without_weaker_test_or_credential_gate` | Whole-step equality to advisory CI after removing the sole approved diagnostic flag; environment/deadline checks | Extend existing normalization for the exact approved pytest prefix only |
| `tests/test_gh932_safe_ci_failure_locations.py` | Invented Git/JUnit sources and actual wrapper subprocess; privacy, cleanup and failures | Reuse unchanged |
| `tests/test_private_ci_check.py` / `scripts/private_ci_check.py` | Fake stream/XML payloads, actual failing child pytest, startup/signal statuses and private cleanup | Reuse unchanged |

Complete three relevant test files and wrapper were read before edits. Prior
ledgers: `932-ready-pr-data-validation.md`, `932-safe-ci-failure-locators.md`,
`1294-private-ci-output.md`; their historical sequential policy is superseded
only for this dedicated workflow by Root's exact design.

## §3 Definitions & conventions in force

Inherit `STANDING.md §3/§4`, `CLAUDE.md` and `lsms_library/data_info.yml`.
No library, survey/configuration, schema or scientific definition changes.
Fresh read-only GH #932 and all comments (none returned) on 2026-11-10 retain
the maintainer's full-data requirement and pending real platform acceptance.
Two workers/loadfile changes scheduling only; it establishes no speedup.

## §4 Invariants & assumptions

Preserve every following pytest option and test selection, private stdout/
stderr/JUnit wrapper, diagnostic opt-in, failure status, credential/eligibility
gates, exact checkout, concurrency/name, prebuild and timeouts. Keep BLAS/OMP/
MKL and `LSMS_BUILD_WORKERS=1`. Owned files are this ledger, the dedicated
workflow and the existing recipe test only. Prove library/config Git
mode/type/blob identity against the pin; no corpus metadata census.

Freeze source before finite pure verification. Use the approved same-image
runtime in the worker's own allocation, exact checkout/config identity, private
empty data/config/state/cache/tmp, false credentials and user/network namespace.
No actual data, acquisition, provider, benchmark, live CI, public publication,
settings, dependency/image/master or shared cache mutation is authorized.

## §5 Reuse decision

Reuse existing tests and wrapper. Normalize only the exact bounded scheduling
prefix alongside the existing diagnostic normalization, then retain the full
legacy recipe equality and environment/deadline assertions. No mirror tests or
new CI framework. Finite selection: exactly the three existing test files in
§2, with repository conftests disabled and explicit xdist/timeout plugins.

## §6 Open questions

None blocking. Root explicitly authorizes these pure checks without another
permission loop; integration/publication and the final full gate remain with
the integration lead/Root.

### Phase 3 — verification

- OK (§2–§5): frozen product/test head `036675ce63eed946d1a6b16662ed3d146970226a`
  ran exactly the three existing files once: **121 passed, 0 failed/errors/skips**,
  pytest 9.11 s, outer 9.77 s, native exit 0 and process group closed. Durable
  dispatch evidence includes direct log, JUnit, selection and identity receipts.
- OK (§4): approved image SHA/generation verified; one private extraction and
  unchanged helper. Exact checkout/library/config/data paths asserted. Controller
  and two workers used MAIN, proper rootdir, no conftests, BLAS/build 1, xdist 2
  with loadfile, unmapped nonroot user and separate network namespace. The pure
  tests imported no LSMS package; separate helper/identity imports used private
  empty data/config and absent credentials. Those roots contain no files.
- OK (§2/§5): recipe normalization proves the sole approved command change;
  all other recipe/env/deadline comparisons remain. Existing synthetic wrapper
  tests preserve nonzero child/startup/signal statuses, private output and cleanup.
- One host Python 3.6 preflight failed on `subprocess(text=True)` before any
  extraction/import/test; its receipt is preserved. Correcting the operator to
  `universal_newlines=True` required no source or test-body retry.
- No data acceptance, benchmark, speedup, native Windows or live CI claim.
  Integration and the composed full-data gate remain Root-owned.
