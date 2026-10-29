# Ethiopia compiler test temporary-directory ownership

Search tier: Git/ripgrep; inherits STANDING.md and1287-ethiopia-nine-public-extraction.md.

## §1 Task

Fix four test-fixture root checks on exact public3c50ebd8e6434d1ab75df1781f5f08010d741511 so ordinary CI needs no TMPDIR environment variable. Keep nine compiler files and all333behavior assertions/profile observers unchanged.

## §2 Existing machinery

Pytest tmp_path owns each test's synthetic directory; request.getfixturevalue('tmp_path_factory').getbasetemp() is pytest's authoritative base. ethiopia_declared_concat.py and ethiopia_retirement_concat.py receive request/tmp_path from their fixture callers. Community-price and food-coping fixtures already receive them inline. Reuse those actual pytest facilities, not CI environment mutation, skipped tests or a new filesystem abstraction.

## §3 Definitions

The source/config/provider/IO definitions in the extraction ledger and STANDING.md remain in force. A test-private root is a descendant of that test's tmp_path, itself under the actual pytest base; TMPDIR is a host preference, not pytest's per-test ownership contract.

## §4 Invariants

All four checks must verify actual pytest ownership while preserving private file/directory modes, actual Country/Wave metadata, invented leaves, source/config receipts and unchanged data oracles. No existing behavioral assertion is removed or loosened. The guarded runner must OMIT TMPDIR and pin trusted tempfile.tempdir plus explicit pytest basetemp to its owned runtime before imports; each JUnit case must certify the variable absent, so the guard cannot mask the portability defect. No product, framework, science, CI/workflow, dependency or baseline changes.

## §5 Reuse decision

Extend the four fixture-root ownership checks to the existing request/tmp_path machinery. No new helper layer is needed. Reuse the reviewed guard/launcher with only new source/path/identity and unset-environment proof changes.

## §6 Evidence and gates

Retain old333guarded passes at8a3dd4 and failed CI at public3c50 as distinct receipts. CI run40876684587/job123457181650 reports6461passed/4611skipped/333errors, each new compiler fixture failing on mandatory TMPDIR. This is a fixture environment failure, not evidence of a product regression. Root source review precedes nine NEWcandidate-only <=120s/core42/8GiB/n0/thread1 populations. Actual-source/API/Make/cache/scientific acceptance remains outside scope.

The corrected aea9678ae5e69bfdc000e00632f250e5cb6f651c composition completed
333unique passes across nine complete zero-denial guards. Root independently
checked999JUnit proofs that TMPDIR was absent at setup/call/teardown, the
333provenance audits, exact source/configuration/runtime bindings and empty
production delta. All behavior assertions and profiling observers remain.
Durable SEALED-v1 manifest4db9534a20fa3ba4113cea26942061443d74a502307e2151acd6b913e43aa22f
binds181ordinary files and source/full-failed-CI custody. This ledger-only
follow-up changes no tested code/test bytes. The old failed CI remains failed;
new public-head CI must run independently. No real-data acceptance is implied.
