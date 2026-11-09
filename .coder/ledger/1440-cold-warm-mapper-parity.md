# Prior-Art Ledger — GH1440 cold/warm mapper parity correction

**Base:**a72fac624290cff75ae013379827486a55afd412. Git/ripgrep/AST floor.
**State:** ROOT-approved narrow correction, ledger before code/runtime.

## §1 Task
Correct the independent synthetic cold/warm mismatch in the candidate market
lookup. Raw Wave labels must use the same existing Country mapper exactlyonce;
already-finalized Country fallback labels must not be mapped again. Project
only the requested column, preserving all other keys/columns and error/NA rules.

## §2 Machinery
`Country._market_lookup`3212–3237 selects Country fallback versus raw concat.
`Country._apply_categorical_mappings` uses `_categorical_key_column` and
`_augment_numeric_code_keys`493/3855, unlike the old manual warm dictionary.
Reuse that method, not a new parser/key-normalizer or copied mapping helper.
The existing all-wave fallback, `_add_market_index` HHpriority/NA/attrs,
Make/freshness/readgates and country declarations remain unchanged.

Full Pakistan CONTENTS personally read at11714 and authenticated wholefile
exacta72:362lines, including all TODO vocabulary/headings (no LOGBOOK/WAITING
entries). PIHS1991singlecrosssection, REGIONS300of301clusters and no guessed
geography remain. Catalog raising-factor delivery gap and frame/representativity
qualifications remain; no source fact was newly discovered.

## §3 Definitions
Inherit STANDING§3–4, AGENTS market/categorical mapping/cache interfaces and
original1440 ledger. No new scientific/default/normalization rule is authorized.
Existing mapper semantics determine labels identically on both routes.

## §4 Invariants
Change only raw requested-column mapping; check missing column before
projection; keep all-wave fallback unmodified and finalized labels unremapped.
Do not remap t/v/i/other columns, modify native/source frames, add/drop/aggregate
rows, swallow loader failures or change sample HHpriority/degraded/error/attrs.
No schema/hash exclusions/Make/provider/raw/sharedcache/fullsuite/census work.
Independent a72FAIL and fixture failures remain immutable in externalreview.

## §5 Reuse
Call self._apply_categorical_mappings on cf[[column]] then assign only that
column back. No new helper/grammar. Preserve all original tests; add the eight
independent pure controls as positive regressions (seven healthy +numericparity).
Existing key-resolver duckfixture must bind the real mapper; its market-only
structural assertion should prove delegated resolver reach, while all other
caller assertions and exact positional/declared-label values remain unchanged.

## §6 Review ownership
The prior independent reviewer is now correction author. ROOT/1452 must review
this child independently. ExternalpriorP2 review manifestbf3fdae8c1bdde151441312ed3d9d6bdccf5cc922ed198cebe5b586a833925ee
under reports/review-1440-pakistan-cold-market-20261109 in durableburstroot.

## Phase3
Pending source freeze and meaningful pure regression run. No original122
cohort replay or source/actualPakistan build is authorized here.
