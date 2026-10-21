# Uganda sibling cache writes: implementation boundary

Baseline: `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Scope and ruling: issue #1062 and the maintainer's #788 ruling make the
framework authoritative for country tables assembled from waves.

1. Make `nutrition.py` write exactly its requested output. Give it a small
   `fct`/`nutrition` target selector; direct script invocation defaults to
   nutrition. Separate the Make rules and pass the requested target explicitly.
   Preserve the existing food-quantity aggregation, food-column intersection,
   nutrient matrix and multiplication exactly. Computing the companion matrix
   in memory is permitted; writing the companion parquet is not.
2. Remove the legacy country `food_acquired.parquet` Make prerequisite from
   these rules: the script already obtains framework-authoritative quantities
   through `Country('Uganda').food_quantities()`.
3. In `income.py`, first request each sibling through the Country API, then
   read its verified raw country cache with the sanctioned reader. This keeps
   the existing pre-finalize arithmetic and avoids changes from API-time `v`,
   currency, labels or identifiers. Preserve the existing earnings/enterprise
   sum and union-of-households behavior. If this cannot preserve those values,
   report the discrepancy before widening the design.
4. Remove the raw compiler prerequisites from income's Make rule. Replace
   earnings/enterprise target recipes with thin calls to their existing Country
   API, with `LSMS_BUILD_BACKEND=dvc` explicit in those subprocesses. Retain the
   listed Make targets so `make all` stays reachable. This prevents the make
   backend from falling back through the retained historical scripts or
   recursively calling itself. Do not rewrite their numeric tests or remove
   historical scripts merely for cleanup.
5. Census Uganda recipes and the other countries' rules that consume `var/`
   siblings. Report concrete analogous cases. Fix only Uganda in this branch;
   draft a precise residual if cross-country findings remain.

Non-goals: #771 credential/API work, FCT scientific changes, #1078 resolution
without a separate reproducer, cache promotion, dependency/baseline changes.
Do not print or copy any existing literal key into reports or new code.

Verification: failing synthetic side-effect tests first; existing Uganda
earnings/income tests; actual cold private-cache builds in alternating
fct/nutrition and earnings/income orders; exact frame/index/dtype comparisons;
verify untouched siblings retain bytes, footer hash and coverage. Run the
relevant Feature audit. Editing Uganda's Makefile costs one Uganda rewarm;
measure and report hashes rather than claiming it is table-scoped.
Include Make-backend/no-recursion and `make all` reachability controls.
Historical compilers applied `id_walk` before writing: raw cache identity and
final API identity must be measured separately, not assumed interchangeable.

Root design by Sue; implementation must first provide a per-task prior-art
ledger and report any contradiction with the country documentation.

Implementation adjustment approved by dispatcher before live verification:
keep the earnings/enterprise var targets reachable as thin calls to their
existing Country API, explicitly pinning `LSMS_BUILD_BACKEND=dvc` in that
subprocess. Merely deleting the recipes breaks `make all`; retaining only the
historical scripts also permits explicit make-backend fallback to the legacy
compiler. The pinned wrapper preserves both entry points and prevents
recursion without schema changes. Historical script tests remain intact;
only their income IO stub learns the new API materialization boundary.

Independent-review amendment (starting 58ccfc2ca): valid one-wave earnings
and enterprise-income caches under inherited make bypass the wrapper recipes
because their target files already exist. The sibling public API calls inside
income.py must temporarily set LSMS_BUILD_BACKEND=dvc themselves. Use local
try/finally to restore the exact original string or absent key after each call,
on success or exception; then continue the existing raw read/arithmetic.
Do not change LSMS_NO_CACHE, public methods, cache policy or numeric operations.
The new regression seeds valid 2011-12 caches through the dvc API and asks for
all-wave income under make; initial income wrongly has 1,810 rows/one wave
instead of the exact 14,740-row/eight-wave reference.
