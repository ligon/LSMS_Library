# GH1051A: optional diagnostic export in ordinary pytest

## §1 Scope / authority
ROOT2026-11-09 authorizes test-only correction from publicationef7620a7c837d699a1cc47733ea9d073415ca8b1. Actual1665CI42649627278/unit129272140909 has13332PASS/5661SKIP/4X/28teardownERRORs. Fixture unconditionallyrequiresDIAGNOSIS_OBSERVATIONS, setbyownprivateoperator butabsentordinaryCI. Independentreview2d4d5f37b08783c211c930976e59514fb66e10a9 confirmsP2; productionnointroducedP1/P2. OriginalCI/author78/source preserved, not relabelled.

## §2 Reuse
InheritSTANDING/sourceidentity/privatepytest recipe. Existingrequest.node.user_properties alreadyretainsobservation; externalJSON export isoptionaldiagnosticconvenience, not testsemantics. No newfallbackframework.

## §3 Contract / invariants
Useos.environ.get foroptionaloutputroot, conditionalizeONLYJSONexport. PreservealltestassertASTs, fixtureuser_properties, privateinputintegrityassertions andstate/pathcleanup; no source/product/config/census change. Missingexportmustnotraise orskipcleanup.

## §4 Verification
FreezebeforeONEwhole28case module, standardpytestDIAGNOSIS_OBSERVATIONS explicitlyUNSET/privateconfigdata/MAIN/cwdidentity/2loadfile/thread1/unshare. No78replay/product/census/raw/API. Recordactualterminal/JUnit+sourceobjectproof; Rootownsreview/CIpublication/carries.

## §5 Open boundaries
Old78passedunderexplicitexportenv; not portableCIacceptance. Newboundedrun mustproveabsent-envcase whiletest/integrity/cleanup remain. No productionrepairorhashimpact claimed.
