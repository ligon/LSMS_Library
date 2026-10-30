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
