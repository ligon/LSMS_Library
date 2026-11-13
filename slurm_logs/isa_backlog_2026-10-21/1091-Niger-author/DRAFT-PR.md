Niger's country community-price Make target bypassed the framework selector:
even a request for 2011-12 alone, or an unsupported EHCVM wave, returned both
ECVMA waves. Retire its concatenate-only script and explicit country rule so
both backends use the existing wave aggregator. Keep both wave scripts and
all source mappings, masks, grain and IDs unchanged.

Both backends now return 32,352 / 13,691 rows for the supported singletons and
zero for unsupported requests; all 46,043 existing readings remain exact at
matched stages. Make now carries the authoritative normalized country attrs.
Existing empty-state and CLI serialization distinctions are documented.

Validation: 114 focused tests and 12 real built assertions; the 17 new tests
give 12 meaningful parent failures / five passes. Source-backed cold, fresh
warm and populated requests, 72 exact frame/attrs and axis comparisons, three
complete Arrow artifact comparisons, food/derived/Feature and healthy Mali
controls pass. All three scoped scanner records exactly match the parent.
Detailed private evidence retains an auxiliary collector's unrecorded initial
cache delta as an explicit limit; subsequent observations do not explain it.
The first freeze also replaced its private detailed aggregate with the safe
export. Original per-probe evidence is unchanged; both freeze attempts and a
separately named reconstruction are retained, without an original-byte claim.
Independent review must assess both qualifications.

Cache cost was measured before builds: 23 Niger API hashes change, 526 other
API hashes and 39 framework fingerprints stay exact. Default warm reads make
zero cache changes; Make can rewrite requested hashless native wave files
with identical bytes/modes and changed mtimes. No broad rebuild was run.

Addresses only the Niger part of GH1091; Tanzania and independently handled
Mali require separate reconciliation. GH1196 remains unchanged. Independent
review is pending; this is a draft description, not a PASS or published PR.

Stack base checked from live PR1176 metadata:
`fix/1088-niger-quantity-sentinel-20261021` at
`d602f8d63520e521a730f85551b366c4d45d9a35`.
