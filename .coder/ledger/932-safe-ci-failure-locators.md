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

## Actual finite verification
Frozen472f actor90031 CLOSED0/chunka421e3: exact36PASS/0F/E/S,31.32s pytest.
The23 new controls plus13 selected existing wrapper controls ran once; no full
data/workflow trigger/native/API/census or LSMS package import occurred. MAIN,
properroot/noconftest/explicitxdist+timeout/autoload-off, nonroot unmapped user+
network namespace and private empty config/data were asserted for controller+
2workers. Source/HEAD/import bindings stayed exact and clean; sockettripwires0.
Legacy noflag counts-only output/status/privacy/cleanup and workflow recipes
remain unchanged. Opt-in real wrapper proves fake stream/message/parameter
sentinels stay private while a checked-in public function locator is emitted;
malicious/missing/mismatched/collection identities are numeric count-only.
Git blob regular-mode/source revision/AST and XML/size/output bounds controls pass.
This does not recover/classify the old16 failures or prove the next full-data
suite will pass. Source-mapping is conservative: inherited/dynamic/unrecognized
cases remain unmapped; all failure statuses remain authoritative. ROOT owns
integration/publication/next necessary full-run and administrative decisions.

## Affected recipe oracle
After the closed36PASS run, ROOT source-reviewed one omitted existing control:
test_existing_data_recipe_is_reused_without_weaker_test_or_credential_gate
expected PR reused steps byte-for-byte identical to legacy, so the newly approved
PR-only diagnostic flag necessarily conflicted. This was a source-identified
oracle relation, not an observed old-test failure. Test-only5b06673 asserts
exactlyone standalone diagnostic flag in PRfullwrapper/allPRjobs, nonelegacy;
removes ONLY that literal token from copied run text, then retains every original
whole-step/env/timeout equality. Product/workflow/existing othertests exact472f.
ONLY that1 control actor87921 CLOSED0/chunk65c381:1PASS/0F/E/S,1.51s pytest.
36initial+1targeted, not a fresh37run. Three identities/no LSMS imports/0network
tripwires/private0/sourceclean+bindingssame. No full-data/CI rerun or settings.
