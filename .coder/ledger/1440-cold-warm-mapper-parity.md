# Prior-Art Ledger — GH1440 cold/warm mapper parity correction

**Base:**a72fac624290cff75ae013379827486a55afd412. Git/ripgrep/AST floor.
**State:** narrow correction tested/frozen f2a12155e049856cad822a5edf241a0853a68e2c; independent review/release pending.

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
- **OK (§2–§4):** raw requested column passes through the existing Country
  mapper exactlyonce; already-finalized fallback skips it. Existing missing-column
  and empty checks precede projection. No normalizer/helper/schema/Make change.
  Production outside `_market_lookup` remains byte-exacta72.
- **OK (§4–§5):** nine pure regression cases pass: all seven earlier healthy
  independent controls, now-positive own numeric-Code/string-label cold/warm
  case, and explicit projection isolation of t/v/District/unrelated metadata.
  Typed NA/payload/index/attrs/input immutability and exact loader failures stay.
  The existing non-idempotent chain case proves no second mapping.
- **OK (§5):** key-resolver duckreceiver binds the actual mapper. Only the
  market caller structural pin follows that concrete one-column delegation
  through the unchanged resolver; all other caller/assertions and exact
  positional/declared-key expectations remain.
- **Actual frozen run:**16PASS/0fail/error/skip,7.30s pytest14.687s wrapper/outer0,
  MAIN/private fresh roots/unshare/threads1/n2loadfile;three exact package/config
  identities. Datafiles0/sourceclean; import created a private config/s3_creds
  placeholder, so configuration is not claimed empty. No failed correction run.
- Original independent a72 numeric-parity FAIL and both earlier fixture failures
  remain frozen. Its seven healthy unique cases are now author regressions;
  this is not independent acceptance of this correction. ROOT/1452 review next.
- No original122 replay, actualPakistan/raw/provider/Make/build/source/warm/cache
  campaign, fullsuite or hashcensus. Complete cache impact UNMEASURED.
  Receipts: reports/1440-mapper-parity-correction-20261109 in durable
  isa-sm-20261103 burst root. Final ledger-only child preserves tested bytes.
