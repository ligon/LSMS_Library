# Niger compiler fixture bytecode policy (PR1316)

## §1 Task and public base

Public base e8a5681c8fbe6442683ea1d60b8f1ecc25c073b6 retains the verified
six-compiler implementation. CI run40878793600/job123464435095 reports
6607passed/4611skipped/four legacy fingerprint failures. Root inspected the
primary diff: all14source/config entries remain equal; only the private
Niger/_/__pycache__/niger.cpython-311.pyc file appears after helper import.
The earlier154guarded passes had bytecode writing disabled in their setup;
those passes and failed CI remain distinct evidence, not rewritten history.

## §2 Existing machinery (Git/ripgrep/AST floor)

Reuse tests/test_niger_concatenator_refusal.py::
test_concatenator_accepts_a_yaml_declared_wave_with_no_script. It copies actual
compiler/helper source, builds invented YAML and zero-byte markers, runs the
real compiler and fingerprints EVERY fixture file before/after. It also checks
one writer, exact frame and copied source/config stability. pytest monkeypatch
already restores test-local sys/env/function state; use it for the interpreter's
documented sys.dont_write_bytecode flag. Do not filter cache files or alter
fingerprints, failure assertions, production code or CI/dependency settings.

## §3 Definitions and country constraints

Inherit STANDING.md §§3-4, CLAUDE.md and the1287public-extraction ledger.
Full Niger CONTENTS was read during recovery; this public base's bytes equal
the fully reviewed22b22candidate. Preserve2014EXTENSION/split-off semantics,
genuine ECVMA plot_features non-declarations and native duplicate grain.
This test-only fixture policy changes no country data, source, index or cache
contract. Actual declarations remain native-script/YAML union, not file output.

## §4 Invariants and narrow authorized change

Only this four-case fixture explicitly monkeypatches sys.dont_write_bytecode
True before private helper import/runpy. Keep complete before==after assertion,
all fixture files in fingerprints, exact source bytes/frame and one writer.
Do not skip/xfail, ignore cache files, change scientific expectations, change
production or widen bytecode admission. A flag restored by monkeypatch is a
fixture isolation choice; it must work when the test body starts with ordinary
bytecode writing enabled, rather than depending on the outer guard setting.

## §5 Reuse and finite proof

REUSE monkeypatch and existing four cases. EXTEND only the fixture's explicit
read-only instrumentation. New four-case-only guard/launcher reuses reviewed
Niger-v2 strict source/private metadata/provider/profile protections and
--assert=plain; source/import/collection start protected with dont_write True.
At pytest_runtest_call set False just before each body and record/assert it;
observe actual body entry False and actual runpy compiler entry True. A teardown
hook wrapper restores True after monkeypatch cleanup, retaining strict absent
bytecode probes and unchanged file assertions. No arbitrary pyc admission.
Root reviews full source/guard before one bounded four-case execution at
core44/8GiB/120s/n0/thread1. Do not rerun the unchanged other150cases.

## §6 Remaining gate

No execution is authorized until root's new source/guard release. CI full-suite
failure remains historical and is repaired only by later exact-head CI success.
Production source is byte-equal to publice8a; source/API/Make/rewarm/science
acceptance remains as qualified in PR1316. Root alone publishes to the same PR.
