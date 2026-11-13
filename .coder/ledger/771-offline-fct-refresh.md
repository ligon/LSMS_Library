# #771: deliberate FDC refresh, offline nutrition builds

Ledger first on merged development dc23e320. Root assigned this preparation after re-reading maintainer ruling6368101158 (2026-10-23): move key to existing config/env lookup, DO NOT rotate, commit Ethiopia fct_usda.csv from one deliberate API run, replace silent fallback with a hard error naming refresh. No acquisition is executed during source/test preparation. Uganda owner confirmed no competing #771 edit; its historical #1000 FCT residual is separate.

## Prior art and definitions in force

- Inherit STANDING.md, CLAUDE.md, canonical data_info and the per-country CONTENTS/LOGBOOK/WAITING caveats. Ethiopia's TODO115 says a complete FCT has not been assembled despite a nutrition skeleton; Food_CF/conversion_to_kgs contains mass factors, not nutrient composition. Uganda's historical FCT-density/source-note disputes and resolved #967 direction decision are not authority to change densities here.
- Existing config.get('fdc_api_key') already supplies FDC_API_KEY → user config → default precedence. Reuse it, not a new config loader, tracked-key fallback or key rotation. scripts/dvc_remote_reachability.py loads config.py by file to avoid package authentication; reuse that source-only lookup pattern for the explicit refresh.
- Current Ethiopia fct_tools.nutrient_df uses curated Preferred Label/FDC ID rows, existing fooddatacentral.nutrients responses, and harmonize_nutrient applies the existing nutrient-label/energy fallback/zero-fill and x10 per100g→perkg. Reuse exactly. Uganda fct_addition has its own existing harmonizer; retain it and its already versioned fct_usda.csv. Do not borrow Uganda or Tanzania values as Ethiopia's missing USDA component.
- Current Ethiopia nutrition executes live nutrient_df on every build, before Country; Uganda reads its tracked CSV but silently refreshes if missing, after querying food quantities. Both contain an old tracked key literal. Never print/copy those literal values or user configuration into reports. Review diffs must redact removed literals.
- GhanaLSS563 is the offline/versioned-input precedent. Uganda1062 already separates fct/nutrition requested outputs; preserve target-specific writes and the existing quantity filters/intersections/matrix product.

## Narrow implementation

Normal builds read only their versioned USDA component; absent snapshot raises FileNotFoundError naming scripts/refresh_fct.py --country COUNTRY before any Country/food or network call. Ethiopia saves/loads the existing harmonized per-kg component, so do not scale it twice. Uganda's existing CSV parsing and numerical values stay unchanged. No change to food acquisition, kg conversion, density choices, nutrient vocabulary, recall or fixtures.

An explicit refresh script reads only public curation/nutrient metadata, obtains the key through config.get, calls the original country nutrient/harmonizer helpers, validates full requested-label coverage and atomically writes a component CSV. The deliberate acquisition must use the exact frozen curation/request inventory, finite request timeout/no automatic retry, retained raw public responses/provenance and secret-safe error handling. It must not instantiate Country, run Make or read survey caches. Ethiopia acquisition is root-reviewed separately before one run; Uganda live refresh is not authorized merely by implementing a CLI choice.

## Acceptance and remaining work

Meaningful invented tests must prove no network or food call on missing/corrupt snapshots, key lookup precedence without exposing values, no implicit refresh, original unit conversion/harmonizer reuse, finite exact request scope, no partial final CSV on failure, secret-safe errors and unchanged requested-output nutrient arithmetic. No invented nutrient table establishes actual FCT acceptance. A successful authorized acquisition creates the authentic Ethiopia component, which receives independent scientific/source review before commit/publication. #1171 still needs separately qualified cold/default/Make raw/served/sibling evidence; #1357's anonymous-household exclusion remains unchanged.

## Public extraction

Prepared on development0b1effb2 after accepted deliberate acquisition and actual-source offline consumer. Product bytes are selected explicitly from tested sourceb6c6456d; no private operator or ancestor is included. Ordinary public/runtime qualifications follow below before final review.

## Closed validation and limitations

The source selection is byte-exact b6c6456d. Reviewed synthetic source66e421 passed28 tests (25 new plus3 existing routing); later9 metadata/setup checks and1 request-start journal check have separate receipts, not a final whole-suite replay. Original initial4pass/1fail from inherited parser cache is retained; explicit refresh bypasses only that cached parser alias in a scoped finally and keeps an owned distinct-ID response memo. Request identity/error/atomicity/cache-restoration/one-ULP controls preserve the existing harmonizer, not a new nutrient completeness policy.

Deliberate acquisition used exactly10 distinct FDC requests, no automatic retry; all responses returned200 with exact requested integerfdcId. Snapshot10x21/1652B SHAedaaa57a5d51b50e6c4be09e176dedd049bdd17b3df9e23382940cf04cc76a74 is authenticated against retained raw public responses; same existing key was privately migrated if absent, never rotated or printed. Existing country CSV build-input hashing suffices; coarse country-wide invalidation is expected.

Actual-source offline consumer: sourceb6c6456d, correctedoperator8224e150, command955ff837; actor0/35.6058s outer0/36.4406s, ECHILDclosed. All5 strict mandatory food/served/nutrition/transport predicates and7 input immutability checks pass; no network/parser/observer denials. Closed private22files83,490,658B manifest59b852f5b466bb7214298e3fb409c71434e9ae6521c1422b81f8b20dac223a66. Original failed consumer preserved: falsely memory-origin saved parquet replies caused compatibility aggregation, changed masks/values and reversed columns; not an ordering-only failure. Source-bound independent review accepted the one-token provenance correction. This validates default cold country over saved native/sample prerequisites and authentic offline nutrient arithmetic, not native cold/Make/Feature/all-wave nutrition API freshness or whole1171 retirement. Uganda actual acquisition/runtime was not repeated.

The settled1357 ruling continues excluding anonymous rows from household-keyed tables; no inferred IDs/orphans or eligibility change. Ref771/1171; root owns scope reconciliation.
