# Prior-Art Ledger -- GH #1062 Uganda sibling writes

Search tier: ripgrep + git + source/test reads; no indexed MCP tool exposed.
Baseline: f8d236694. Author: Sue, 2026-10-21.

## 1. Task restated

Make Uganda country-only nutrition/FCT/income builds write only their requested country table, preserving the existing arithmetic and framework-authoritative raw siblings. Follow the #788 maintainer ruling (2026-10-17, reaffirmed 10-18). The dispatcher's concrete boundary is `slurm_logs/isa_backlog_2026-10-21/1062-design.md` on the campaign branch.

## 2. Existing machinery

- `Uganda/_/nutrition.py`: original kg filtering, `(i,t,j)` sum, common food-column intersection and exact nutrient matrix product. It writes both FCT and nutrition; preserve calculations, select only the output. Tested indirectly by approved `tests/test_uganda_invariance.py`; add synthetic actual-script side-effect controls.
- `Uganda/_/income.py`: source totals at raw index grain, sum duplicate source keys, `.add(fill_value=0)` union and zero/null suppression. Reuse unchanged. `tests/test_gh772_uganda_earnings_shape.py::TestIncomeAlignment` pins union/zero behavior.
- `Country._aggregate_wave_data` / normal `earnings()` and `enterprise_income()` build/validate/stamp raw country parquets. Reuse for sibling materialization; discard finalized return values, read raw via sanctioned reader to preserve arithmetic/index semantics.
- `local_tools.get_dataframe` (`local_tools.py:1686`) sanctioned source/cache reader and `paths.data_root` (`paths.py:92`) authoritative cache location. Reuse, do not bypass with bare pandas IO.
- `local_tools.to_parquet` (`local_tools.py:3225`) redirects script output to data root. Reuse existing call, exactly one requested country output.
- `Uganda/_/Makefile:139-143`: income raw sibling prerequisites and coupled nutrition/FCT recipe. Extend routing only; retain historical compilers/tests, disconnect concatenate-only recipes from normal country builds.
- `tests/test_gh772_uganda_earnings_shape.py`: actual historical earnings/enterprise scripts and income arithmetic under stubbed IO. Preserve all assertions; extend IO stub for new materialization boundary.

## 3. Definitions and conventions

Inherit `.coder/ledger/STANDING.md` sections 3/4, `CLAUDE.md` cache/IO sanctions and `lsms_library/data_info.yml` schema. Country table hashes cover Uganda's Makefile: this change costs one Uganda re-warm. Script outputs are initially hashless; the framework verifies/stamps the requested table. A script cannot assert framework freshness for an unrequested companion.

Uganda `CONTENTS.org:1917-2050` documents two builders and coupled output metadata loss. Its historical statement that the authority decision remains open predates the explicit #788 ruling; preserve the history and add a dated resolution. The same file's #967 approval and BLAS-specific exact fingerprint notes remain in force. They do not authorize changing nutrient operands or arithmetic.

## 4. Invariants and assumptions

- Preserve every nutrition/FCT operand, food intersection, operation order, index/columns/dtypes and exact numbers on this machine. No #771 API/FCT-source work, key movement, density change, fixture/baseline refresh or tolerance change.
- One requested script output; no sibling bytes/footer/hash/coverage changes on repeated requested-table builds once inputs are valid. Sibling creation through its normal API is allowed when absent/stale, and must leave a verified stamped raw cache.
- Income arithmetic consumes raw pre-finalize frames, avoiding API-time v/currency/label changes. Preserve union-of-households and current duplicate/source aggregation.
- Physical private L2 per code revision; only L1 may be shared read-only. All real build comparisons use the same node, image interpreter and pinned numeric threads; assert checkout/config realpath.
- Do not infer #1078 nine-cell corruption resolved from this correction alone.

## 5. Reuse decision

- **Reuse** nutrition/FCT existing operand construction/product; add only requested-output selection.
- **Reuse** normal Country sibling builders and sanctioned raw reader; **extend** income orchestration without new numeric transform.
- **Extend** Make rules to select outputs and remove raw compiler prerequisites; retain historical scripts/tests.
- **New** actual-script IO-boundary regression tests, needed because existing GH772 tests do not detect companion writes or ensure framework materialization precedes raw reads.
- **Reuse** content-hash inventory and Feature audit scripts for verification; no cache-hash mechanism change.

## 6. Open questions and contradictions

No scientific decision required within the concrete boundary. Documented historical authority-open statements are superseded by the explicit #788 ruling, not silently dismissed. If exact private baseline/candidate values differ, stop and report before changing arithmetic. Cross-country analogous consumers and #1078 remain separately reported residuals.
