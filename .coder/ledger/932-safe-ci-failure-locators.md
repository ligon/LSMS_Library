# GH932 — static public failure locators, private CI payloads

## Task and source
ROOT approved a minimal diagnostic source fix on exactc94300df37dce2cbd99019ccb09c28c2509158f2 after actual full-data42748441466 reported16 failures but deleted all private traces/identifiers. Preserve those sixteen as unclassified; do not reconstruct or rerun. Search tier: rg+Git. Ledgerfirst before product changes.

## Reuse and authority
Reuse scripts/private_ci_check.py capture/cleanup/status/count boundaries and tests/test_private_ci_check.py; inherit STANDING/CLAUDE canonical definitions, no LSMS data/API change. Existing1294 ledger§3 explicitly prohibited arbitrary XML identifiers; ROOT now permits ONLY static checked-in repository test file and declared test function validated by source AST. Never publish parameters, arbitrary classname, text/messages/trace/properties or captured output.

## Bounded implementation
Opt-in --public-test-failures on PR full-data wrapper only. Legacy nightly/manual/Windows calls unchanged/counts-only. Capture exact Git HEAD before child; use immutable Git blobs for only failed-case candidate paths after child. Validate relative public test path, bounded source and qualified declared AST function/class, never import test/library modules. Emit static file/function only, aggregate numeric failures/errors separately; unmapped counts only. Bound32 public locations/1MiB per source/existing16MiB XML; no new dependencies/artifacts/network/data/credential operations. Captured streams/XML still deleted; child argv/status and full-suite coverage unchanged.

## Finite proposed controls (UNRUN)
Invented Git/checked-in source + mocked JUnit: malicious absolute/traversal/symlink paths, fake/mismatched class/function, parameter-sensitive names, exact static matches, collection errors, unknown/skips, source drift after pinned revision, oversized/malformed/DTD XML and bounded summaries. Actual wrapper subprocess proves private fake sentinels/deletion/exit plus static locator output. Reuse meaningful existing wrapper controls, properroot/no conftest or LSMS imports/private config and source/network checks. ROOT reviews exact source/design before runtime. No workflow triggering/settings/fullsuite/census/raw/provider work.

## Status
Source-only preparation. Actual16 failures remain unclassified. No runtime/acceptance claims.
