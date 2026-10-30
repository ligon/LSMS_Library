# GH #1146 test-side coverage ledger

§1 Fix only the first two accepted #1113 follow-ups. Refuse pinning a Uganda
var parquet with a present incomplete coverage manifest. Skip Nigeria sold-unit
wave-count checks when their present manifest lacks the required PH waves.
Leave the optional cold-rebuild behavior unchanged; no library/cache policy.

§2 Reuse production read_parquet_cache_waves and #1113's expected-coverage /
coverage-skip rule in tests/test_uganda_invariance.py:144–172, including its
before/after metadata checks around a concurrent atomic replacement. Generator
build_manifest currently has no gate; both Nigeria raw/warm count tests repeat
the unguarded2011Q1/2013Q1 assumptions. Search tier git/rg; STANDING inherited.

§3 Country caveats read first incl live TODO/WAITING. Uganda's historical helper
uses j=household/i=item;2015 gsec1 bridge and244+21 versus current265null-v are
separate. Nigeria quarter-specific food/crop vocabularies, PH-only sold units
and size-agnostic units remain; old retained u=NA prose is superseded by1118.
No counts, source semantics or baseline values are repinned. Coverage is recorded
by footer manifest, not guessed from observed t levels or row counts.

§4 #1113 deliberately compares manifest-absent legacy/Make outputs as before;
only PRESENT short manifests are incomplete. Preserve that engineering rule.
Var expects all current Country(Uganda).waves dynamically; wave-native entries
remain unchanged. Superset coverage is sufficient; zero-row declared waves can
be covered. Incomplete generation raises before main writes the output JSON.

§5 Extend existing scripts/tests only; no shared framework. Both Nigeria tests
carry the same required-wave assertions, so use one local test-only gate before
and after relevant reads. New invented parquet/footer tests exercise partial,
full/superset/absent/empty coverage, both search roots, no baseline overwrite and
skip-before-API. No actual source/cache/API build or old cohort replay.

§6 No unresolved scientific decision. Root requested this scoped engineering
preparation and independent review; no new user permission or cold rebuild.


## Verification

Root independently reviewed the complete 9fb45746 source/test diff and accepted
its #1113 semantics. One bounded selection of only the new invented footer tests
passed: 21 tests, no failures/errors/skips, 4.07 seconds pytest / 6.377 seconds
outer. Approved image, exact source/countries roots, private data/config/DVC
site cache, core38 / 8GiB / thread1 / 120+5; source unchanged and group closed.
No survey read, country/API/cache build, old cohort or baseline regeneration
occurred. JUnit and source/command/log bindings are preserved by the author.
Optional issue item3 remains #1113's existing skip; no cold rebuild was added.
