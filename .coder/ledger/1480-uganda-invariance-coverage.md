# Prior-Art Ledger — GH1480 Uganda invariance coverage

Base:3b28d293f71b962c3d878c2886be9ee794407915, freshly fetched GitHub development. Filesystem development111b is stale and untouched. Search tier: git/ripgrep/AST. Author tree/branch fix/1480-uganda-invariance-coverage-20261108 is isolated; all1528 trees/heads remain preserved.

## §1 Task

Expose the actual comparison coverage of every currently declared Uganda baseline entry/table, and add opt-in COMPLETE verification that refuses missing required comparisons. Current inventory is53 entries/13 tables/265 pinned fields, computed from the manifest, never a fixed target. Do not refresh/relax scientific fingerprints, source values, fixtures, numerical rules or dependencies. Ordinary unit runs must not build or newly access shared caches.

## §2 Existing machinery

| Machinery | Existing contract / decision |
|---|---|
| tests/test_uganda_invariance.py _load_baseline, _fingerprint, _canonical_dtypes | Reuse raw SHA256 hash_pandas_object, five-field checks, narrow existing dtype equivalences. No new fingerprint engine. |
| _expected_coverage / _coverage_skip_reason | Reuse pre/post-read wave-footers; explicit short manifest cannot compare; missing footer still follows existing comparison contract, never guessed from t. GH1113. |
| test_parquet_matches_baseline | Retain scientific assertions and the requires_s3 collection gate; extend its shared inspection result so reporter/CLI count each pinned field, including failures, errors and non-comparisons. Existing15 hash-variant controls remain meaningful. |
| Pytest terminalreporter.stats / TestReport.user_properties / record_property | Reuse native reporter and xdist-serialized properties for a terminal coverage summary, including credential-gated/setup skips without IO. No new global build/report framework. |
| Explicit read-only command + Make recipe | Add a small COMPLETE operator over the SAME inspection routine, required private data-root argument and optional JSON report. It reads only named existing cache files and never materializes or falls back to shared configuration. No new implicit build behavior. |
| tests/fixtures/uganda_baseline.json | Read-only dynamic inventory. Current53 keys/13tables, no error/null entries; one permitted measured nutrition alternate. Generator never invoked. |
| conftest.py / tests/conftest.py | Existing root option compatibility and credential gate; ordinary unit data gate stays, no new cache access in reporter. Retired purge is not reintroduced. |
| Makefile existing PYTHONPATH / pytest loadfile controls | Extend with explicit private-root verification target only; no dependency update or cache clear. |

## §3 Definitions and grounding

Inherit STANDING.md§§3–4, CLAUDE.md Data Access/cache contracts and canonical data_info.yml. Full CURRENT Uganda CONTENTS5969lines/378120bytes, including LOGBOOK/WAITING/TODO, read BEFORE test/source analysis. Grounding receipt records its SHA256 and all read ranges.

The record says “Do not ‘fix’ it by re-adding a build step to the invariance module without measuring what that costs.” Fresh issue1480 provides measured prior cost137–238s/4–8cores plus5s checks, and names reports/baseline-refresh/build_all.py. We add no build behavior; any source cohort is separately authorized private verification, with its actual cost recorded.

Retired session purge historically made53 skips; alphabetical builds and test_uganda_tables cleanup still explain partial caches. Shape equality is not content equality or hash freshness. The historical nutrition hold is explicitly LIFTED by Oct15 approval, not a new permission gate: preserve primary12d25…50e3a and exactly the measured Haswell alternatef3d3…d6. The specific-current approval supersedes old “do not regenerate” prose; it does NOT authorize broader baseline refresh. Earlier sibling-write history is superseded by GH1062 requested-output routing; no live-defect claim follows from old paragraphs alone.

Other idiosyncrasies remain: v changes EA→parish, while ea is design origin; 2009 movers use GPS bins/unlocated markers. Design-intent “one-third ... each wave” conflicts with DDI “This same sample was used ...2015/16”; both retained as qualified evidence. WAITING829 x100 crop-sale correction remains a release caveat, not a request to change data. Country/API/native stages and their keys differ; raw fingerprints are not scientific or population acceptance. Historical “crop not registered”/country-axis veto paragraphs are superseded by later canonical axes and1475 row-country routing. None is repaired here.

## §4 Invariants

- Preserve manifest bytes, all primary/finite alternate hashes, null-as-unpinned debt, dtype equivalences, exact value hashing, pre/post coverage gates and native/scientific definitions.
- Inventory all manifest entries dynamically; errors, unknown/malformed keys, absent files, short coverage, credential/setup skips, deselections/not-run and unreadable parquets are explicit non-comparisons, not passes. Unexpected cache paths are reported separately, never silently admitted into the baseline scope.
- Count every field: pinned required, checked-pass, checked-fail, not-compared and unpinned. Full required coverage and successful invariance are distinct; a mismatch may complete coverage while failing verification.
- COMPLETE fails missing required comparisons or any mismatch/error. Default pytest keeps source credential gating; reports its gaps explicitly without new file scans/builds. Explicit read-only CLI can inspect already-built private parquets without source credentials.
- No implicit source acquisition/build, shared-L2 reads/writes/rewarm, DVC CLI, dependencies, fixture/baseline writes, new masks/tolerance or GitHub writes. ALL test commands use explicit private data/config/false credentials, exact mounted main interpreter/cwd/PYTHONPATH/countries-root assertions, threads/build1 and pytest2/loadfile.

## §5 Reuse and verification

Reuse existing fingerprint, comparator rules, coverage gate and Pytest report/property transport. Extend the existing actual test with one shared structured inspection result and a thin assertion adapter, so CLI and pytest cannot disagree by formula. A small explicit-root COMPLETE command is new because no existing completeness operator/reporter was found; it does no materialization. Preserve/update actual-test invocation in hash-variant controls solely for report-property plumbing, without removing any assertion.

Before accepting: hermetic empty/partial/full/mismatch/error/unpinned/unknown-key cases, entry/field accounting, source-gated and deselected pytest summaries, strict exit semantics, xdist and report-only no-build/shared-cache guards. Baseline digest and all production/config fingerprints stay unchanged. Source verification, if needed, is one bounded13-table private cohort (dynamic current table set), never a baseline regeneration; pre-existing baseline failures are reported honestly. Root owns independent review and final GitHub/merge.

## §6 Open questions

None blocks the authorized test-only change. Scientific baseline mismatches are separate evidence/residuals; they cannot be repaired by this task. The ledger is committed/surfaced before implementation.
