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
| Pytest terminalreporter.stats / TestReport.user_properties / record_property / native collection + workeroutput | Reuse native report/property transport for comparisons and native collected/deselected scope union for workers that execute no Uganda test; no cache IO or double counting. |
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


### Phase3 — verified handoff

Ledger-first9907f626e9ffaac999fc11249382929372442b98 preceded implementation;
original5093db0d6628086305ca471306ea73c4bd7a979e, reporter-fix
e0ef0be2ef9051c9e62e42cde0bc246ee1a6cfb8 and all original review/source
artifacts remain preserved. Final tested integration is
4f861a4bbbae3d0df2bd9a7c6ab901f340afce19, ordinary merge with fresh GitHub
8505ee8a05359f66a820534c5af2ab3873671a27. All9 task paths are byte-exact to
reviewede0ef;18 incoming BFA/Mali1528 and Malawi1174 paths are disjoint/exact.
Filesystem development111b remained untouched. Only the authorized filesystem
fix/1480-uganda-invariance-coverage-20261108 ref was fast-forward published.

Implemented: shared structured inspection retains all original scientific
oracles; native pytest reports plus actual collection/deselection evidence
count every declared baseline entry/field. Explicit-root read-only COMPLETE
CLI/Make target never builds/acquires/stamps/refreshes or falls back to a
shared root. Ordinary reporter does no cache discovery. Unknown files are
not inventoried there; the explicit operator names extras separately.

Independent5093 review found a real P2: when xdist deselected every collected
Uganda comparison, no TestReport reached the controller and no section was
printed. Reporter-onlye0ef records collection before selection, carries it
through native workeroutput and unions worker scope. Collected-but-unexecuted
comparisons remain visible; a never-collected module stays quiet. Original
changes-requested verdict and reproducer remain unchanged. Separate closure:
10passed67.76s, actual serial/n2 all-deselected/partial/never-collected/zero-
selected controls plus dedup/no-IO checks; no remaining finding. No source
campaign replay was needed for this hook fix.

Author focused frozene0ef:70passed,53credential-skipped,157.47s pytest /
159.158s wrapper. Default summary explicitly INCOMPLETE0/53 entries,0/265
required fields, verification NOTPASSED. This includes9 new selection controls,
original finite hash/structure controls, empty/partial/full/mismatch/error,
missingness/unknown-key/unpinned/report-output and conftest compatibility.
All use private data/config, false credentials, unshare-rn, exact mounted-main
package/countries-root/head assertions, BLAS/build1 and pytest2/loadfile.
The earlier v3 run overlapped an edit and remains NON-AUTHORITATIVE; its
checks were repeated on frozen code. No invalid attempt is promoted.

Separate source job41011740 COMPLETED0:0 at exact5093, freeco_carleton
--cold4CPU32G: all13 dynamically inventoried ordinary getters succeeded in
237.040s; COMPLETE exit0 matched all53 entries/265 pinned fields, zero unpinned.
The37 extra generated wave files are outside the shipped baseline scope and
reported separately. Every private cache file's bytes/mode/nsmtime and the
manifest digest remained unchanged through the read-only operator. The receipt's
operator_seconds10.762 includes inventory/snapshot closure, not pure CLI time;
no separately measured pure-operator time is claimed. No baseline was generated.
This validates the current manifest cohort, not every Uganda feature, source
truth, historical freshness or population interpretation.

Comparator/CLI/Make/hash-variant controls/manifest are byte-exact5093 after the
reporter fix; entry_result/summarize/summary_lines ASTs are unchanged. Original
fingerprint/dtype/coverage-gate functions and all5 assertion ASTs match3b28.
Manifest SHA25603252ade67950b3677b72c8b27fb1cfe6cc6c7127f1ed1bca746dddc701a866d.
Library arithmetic/config inputs remain outside this test-only change;
CONTENTS is documentation excluded from build hashes. No tolerance/skip,
fixture/scientific baseline, dependency, source policy or default was relaxed.

Full isolated offlineCI job41014455 COMPLETED0:0, exact integrated4f861:
11632passed,5652skipped,4xfailed,692warnings;1384.84s pytest/1389.313s wrapper,
24m54s Slurm. Freeco_carleton4CPU32G, own image mount, private EMPTY data/config,
false credential gate/unshare, BLAS/build1 and pytest2/loadfile. Head-after
matches and code-clean-after is true. The node-local submission-cwd warning
preceded helper clone/cd; exact runtime cwd/package/config/mount checks passed
before test launch. This is offline CI, separate from source COMPLETE above.
Strict metadata API/skill audit on integrated4f861 exit0 with private roots.

All scientific/source/review/CI actors closed before this ledger-only update.
Final whole-tree byte proof binds every non-ledger tracked path to tested4f861;
therefore the final docs freeze needs no code/source replay. Durable evidence:
B/reports/1480-uganda-invariance-coverage-20261108/{REPORT.md,SOURCE-RECEIPT.json,
reporter-fix-byte-ast-proof.json,integration-byte-proof.json,offline-integrated/}
and B/reports/review-1480-uganda-invariance-coverage-20261108/{VERDICT.md,
REPORTER-CLOSURE.md,closure-e0ef/}, where B is the task burst root.

Remaining root-owned gates: final-head GitHub unit/Windows checks, PR review,
expected-head publication/merge and issue closure. No GitHub writes, shared-L2
rewarm/clear or scientific policy decision by this worker. No scientific
residual introduced; inherited country caveats in§3 remain qualified.
