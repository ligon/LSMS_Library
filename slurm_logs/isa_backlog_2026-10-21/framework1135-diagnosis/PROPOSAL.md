# GH #1135 concrete proposal - DRAFT, no implementation approval

Pinned f8d236694c26fe9e709c5548f02c70f74ea72c33. Sue (GPT-6).

Preserve the existing twelve boundary declarations, SPAN selection, no-threshold emission, return values, strict behavior, nesting/deduplication, sampling metadata carrier and report ledger. Extend only typed metadata transport and message formatting. Do not call Country.sample() from warn time: the audit runs on arbitrary household/item tables, a frame may skip the very boundary endpoints it spans, and a sample fetch would add dependencies/recursion/cost without solving mover identification.

## Smallest representation

Add one optional `panel.cluster_id_measurement` mapping, validated by the existing `_check_panel` path. Keep all current records valid when it is absent. Suggested bounded fields (names are the concrete draft tested privately):

```yaml
cluster_id_measurement:
  linked_households: null
  households_changed_net_of_movers: null
  clusters_renumbered: null
  mover_definition: null
  cluster_count_definition: null
  source_type: derived-from-data
  confidence: low
  locator: 'measurement/proof artifact, with source/version and served-vs-native layer'
  unavailable_reason: 'No verified movement-separated measurement is prepared'
```

The three quantities are independent optional integers; an unavailable quantity is null, never zero or inferred from prose. Present counts must be real nonnegative Python integers (reject booleans, floats including integral-looking values, fractional values, strings, NaN and infinity). The numerator must not exceed a known denominator. Zero linked households makes the share unavailable, not 0% or100%. No size threshold gates a warning. A known count needs its own existing-vocabulary source_type/confidence/locator; the nested provenance distinguishes a derived measurement from GhanaLSS's documentary panel-continuity claim. A known numerator requires a documented mover definition; a cluster count requires a count definition. Partial or all-null blocks require an explicit unavailable reason; absence of the entire block has the legacy fallback reason "no structured measurement recorded". Reject unknown nested fields.

Reuse `_freeze`, SamplingRecord JSON/pickle/equality, attach/merge/records_from_attrs and the report ledger. There is no new public measurement API, table, canonical column, sample payload, parquet schema field, reducer, identifier or build runner.

## Quantity definitions to bind to evidence

1. N: unique *served, post-id_walk household links* across the declared adjacent boundary, with nonmissing i and v at both endpoints, following Country.waves order. Assert each household has one unambiguous v per wave before intersection; do not silently choose first among conflicting values. Count raw tracking lines separately and never substitute them for N.
2. R: linked households whose served v changes for the declared identity/renumbering event, with actual movers' changes removed from the numerator. N remains the full linked nonnull-v denominator. This is why historical Nigeria's10/1493 is0.67%, not10 divided by a nonmover-only denominator, and not79/1493. An unclassified movement status is not evidence of nonmovement; report unavailable or a documented partial bound rather than silently treating it as false. When movement and renumbering coexist on a row, preserve a proof of the event classification instead of attributing both automatically.
3. K: number of *distinct earlier real sampling clusters proven to have been renumbered* at this boundary, supported by a verified old/new cluster relation after separating movement. Count an earlier cluster once even if it has many household links or multiple successor IDs; carry ambiguity as unavailable. Do not count changed households, unique destination IDs, `(old,new)` pairs, GPS bins, moved-singletons or all administrative names as K. For a `redefined` EA-to-parish boundary a physical-cluster renumbering count may be inapplicable; say unavailable with that reason instead of asserting a fictitious bijection. A paired-cluster floor (GhanaLSS79) is explicitly an observed lower bound, not the total retained design clusters.

Historical h2h3 supplies exact N and a narrower P (changed except moved-prefix sentinel), not R/K under these definitions in general. Keep P as historical provenance outside production R; do not label it net of all movers.

## First-line format

Keep ONE physical first line, with one deterministic fragment per flagged boundary, including boundaries whose endpoints are not both returned. Use country wave order, semicolon separation, no overall truncation that drops the last boundary. All fragments must name both endpoints and both requested metrics. Exact examples below are formatter targets using historical/synthetic inputs, not claims of newly verified current data:

`Nigeria/sample: v not comparable; 2016Q1 -> 2018Q3: 1 cluster renumbered; 10/1493 linked households (0.67%) change v net of movers; 2019Q1 -> 2023Q3: 12 clusters renumbered;112/5067 linked households (2.21%) change v net of movers.`

For a legacy or unverified boundary:

`Uganda/sample: v not comparable; 2011-12 -> 2013-14: clusters renumbered unavailable (identity redefined); linked-household share net of movers unavailable (movement not separated).`

A100% synthetic control must print its exact ratio and100.00%, without converting this to a warning threshold. Keep current detailed continuity/provenance/note/remedy/strict text below the first line. Existing `in_frame=False` qualification remains. Store the exact nested measurement in every boundary report so consumers do not parse the formatted fraction or rounded percentage. Do not parse `cluster_id_note`, rounded artifact shares or prose to compute a count.

## Compatibility

- Existing sampling.yml files without the optional block load unchanged; malformed new blocks still fail direct config validation. The existing audit's generic failure warning remains as-is.
- Existing public/Feature frames retain their complete values, null masks, order, dtypes, index/column names and non-sampling attrs. Curated new measurement metadata intentionally adds nested attrs; it does not rewrite measurements.
- Existing parquet JSON metadata remains readable as plain dictionaries. Old parquet records do not need migration; normal finalization reads current config. Nested nullable integer fields already round-trip exactly in the private representation probe. No cache hash stamp/coverage bypass is permitted.
- Do not edit data_scheme.yml, per-wave data_info.yml, country modules, source parsers, shared reducers, `_build_registry` or any fingerprint exclusion. Sampling.yml stays a sibling excluded by its current suffix policy. Current closure tests show sampling callables outside build fingerprints; the actual approved implementation must still measure all549Country hashes and39framework fingerprints in separate exact-source baseline/candidate processes. The expected0/0 is grounded but not yet a measured candidate result.

## What is verifiable now, and the honest partial boundary

All reporting mechanics, schema validation, legacy fallback and synthetic exact rates can be implemented/tested from held code alone if separately approved. Exact historical N/P are reproducible from the prepared artifacts for all twelve boundaries, but source reconstruction and true mover-adjusted R/K are not. No numeric boundary record is currently approved by this diagnosis for current production population solely from historical prose/aggregates.

The smallest concrete numeric follow-up is the two Nigeria boundaries. Its formatter already provides a precise sentinel predicate (`cluster_id`: ea=0 -> moved-{hhid}); exact historical69/231 excluded and10/112 remaining are held. Verify source-backed whole-EA LGA changes and current served links before curating1/12cluster counts. Required declared sample covers are2015-16/Data/secta_plantingw3.dta,2018-19/Data/secta_plantingw4.dta,2023-24/Data/Post Planting Wave 5/Household/secta_plantingw5.dta (use the exact spaced path from data_info.yml), plus any actual panel-identity prerequisites. Use source labels and code paths to establish the actual status scope; if ea=0 is not a complete movement indicator, keep the residual explicit.

A read-path/schema-only patch with unavailable metrics everywhere, or a later Nigeria-only population of measured sizes, is a PARTIAL #1135 implementation. It cannot claim all twelve literal numeric promises are fulfilled. Keep #1135 open and narrow its remaining count/provenance work in the same PR-readiness phase. The human's SPAN/no-threshold choices remain settled. No implementation has been authorized in this task.

## Required implementation gates if later approved

- Private `test_1135_contract_draft.py` is runnable now and discriminates baseline:4fail/1pass. It separates loader rejection from report data loss and first-line omission; its known numbers are synthetic controls. Add an independently verified100%case after schema approval.
- Add invalid-count/cross-field tests, null/zero-denominator fallback, multiple flagged boundaries all on the first line, correct country ordering, skipped endpoints, no note parsing, explicit prefix-only-vs-actual-mover counterexample, moved-singleton removal from numerator only, cluster-pair/household/GPS-bin count confusion, immutable nested JSON/parquet/Feature attrs transport. Keep the existing97pure tests and all strict/nesting/data-identity controls.
- Preserve the seven existing source-backed tests as data cases, not silently skipped success claims. If source-backed verification is approved, root first chooses physically copied current-hash sample/panel/companion prerequisites; gate SHA/MD5, coverage, identity and quiescence before calls. An L2 footer claim alone is not a source-cold proof. No new .dvc/tmp is allowed without root preparation.
- Record exact source/proof paths and versions alongside any curated R/K, including unknown movement cases and linkage ambiguity. Fail on ambiguous current served keys before counting, rather than copying historical first-wins behavior.
- Inspect the actual549/39counterfactual result; preserve all source/config/fixture modes and hashes; normal current-hash warm reads must leave cache bytes/modes/mtime exact. No source build/full scanner sweep is justified until a specific evidence gap requires it and is approved.

## Residual draft for root, if a partial patch is chosen

Title: `#1135 residual: verify mover-adjusted cluster-renumbering sizes beyond the bounded reporting mechanism`.

Tier: B diagnosis/measurement; data acquisition only if a specifically required source is genuinely unheld. SPAN and no threshold are settled. Historical H2/H3 excluded only moved-prefix sentinel IDs, and did not count clusters. Obtain verified served-household pair relations, actual movement classifications/periods and physical-cluster renumbering evidence for each unfilled boundary, preserving distinctions between rendering/kind/synthetic IDs and physical movement. Do not alter v, sample universe or data payloads. Keep every unavailable metric explicit until independently measured. The reporting mechanism/numeric subset, if reviewed but unmerged, must be described as reviewed but unmerged, never production-resolved.

Sue
