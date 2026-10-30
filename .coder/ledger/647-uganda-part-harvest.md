# Prior-Art Ledger — GH647 Uganda partial harvest

**Diagnosis base:** 5a4b2ac623d55bb31b8908260ad54efb34070cd6. Search tier: git + ripgrep.
**Public docs extraction base:** 1542a886ff67c642fb746ca41fe87994141b59cb.

## §1 Task
Adjudicate the omitted 2019-20 AGSEC5A `_11`/`_21` harvest and sale families from the official questionnaire/manual and dictionary. The issue requests an explanation, not unconditional inclusion. Record the independently reviewed documentation and source-backed alternatives; no production policy selected. This public extraction changes documentation only and preserves merged #1291 notes verbatim; it includes no new runtime, tests or data reads.

## §2 Existing machinery
- `Uganda/_/uganda.py:1292` `_production_observations` preserves reported IDs, actual mapped condition slots or explicit release-row coordinates. Tests: `test_gh1198_uganda_production_observations.py`.
- `uganda.py:1327` `crop_production_for_wave` decodes each condition slot with its own harvest/sale units, conditions and reported factor; source positions precede selection. Tests: `test_uganda_crop_condition.py`, `test_crop_kg_factor.py`.
- `uganda.py:1906` `CROP_COLMAPS`: 2019 A/B both map ordinary-family slots 1/2; PART remains unmapped. 2018 A maps one slot with no harvest unit; B maps two slots with verified own vocabularies. Reuse declarations, do not infer symmetry.
- `uganda.py:1865` existing PART-sale omission comment; country CONTENTS GH647 notes explicitly say no verdict.
- GH1198 ledger and `SkunkWorks/grain_aggregation_policy.org:23`: native observation grain retained; aggregation belongs in explicit transforms.
- Tanzania #846 document-only pypdf6.1.1 wheel, SHA256 7781f99493208a37a7d4275601d883e19af24e62a525c25844d22157c2e4cde7, reusable reader precedent, no installation.

## §3 Definitions
Inherit STANDING.md §3, canonical data_info.yml and Uganda data_scheme.yml crop declaration. `condition` describes physical state (fresh/dry and form), not interview completeness. A/B are module labels, not universal calendar order. `production_obs` distinguishes source observations without implying additivity. CONTENTS documents switched labels and missing-source units, so names alone are not authority.

## §4 Invariants
Inherit STANDING.md §4. Preserve every existing key/value, condition/unit/factor pairing, source locator, conditional legacy null-key boundary and household walk. No new shared axis, sum, exclusion, parcel mapping or value inference from suffixes. Preserve GH1290 nine unsupported keys, GH1297 unresolved parcel and GH689 disclosure. Historical GH647 counts are not a fresh source census or a valid sum across unlike units.

## §5 Reuse
Reuse exact official PDF documentation and public dictionary; pinned zipimported pypdf only in isolated document process. Reuse source maps/observation policy as constraints. Documentation may extend the reason for omission. The independent review supports the qualified diagnosis. Any production inclusion/default-total proposal still needs a concrete source-supported contract; no new implementation is selected here. Family selection must precede both reported-factor pool construction and aggregation, because appending observations can change other rows' factors too.

## §6 Questions
The paper asks about actual harvest; condition differs from completeness; the bounded three sibling extracts lack the PART family. Unresolved: does 5A1 Q6 report additional harvest since ordinary 5A, or a revised/cumulative amount including it? Does the same relationship hold for Q7 sale quantity and Q8 sale value, and which recorded CAPI/version/row relationship resolves the 5 No and 38 missing follow-ups? Existing #647 retains this question for human review; preservation of source reports does not choose a default total.

### Phase 3 — source adjudication
- OK (§3/§4): no production/config/test bytes change. The only country change documents evidence and the unresolved relation; CONTENTS is excluded from build hashes.
- CONTRADICTION to a mutually-exclusive full/PART interpretation (§6): all 97 PART-quantity physical rows also carry ordinary quantities, and initial harvest=2; the ordinary-family label is not an exclusive completeness class.
- OK (§5): exact declared PDFs match sidecar MD5s; pinned document-only reader extracted 89 pages with no parser warnings. First -S/base-prefix harness stop is preserved separately.
- OK (§4): exactly four cached declared source files, 11,481,271 B, standard reader/private cache/network-isolated finite actor; no builder/API or aggregation executed. Counts distinguish 97 rows / 100 slot reports / 100 flag answers / 138 initial partial rows. Marginal date distributions do not prove per-row chronology or disjoint intervals.
- Open (§6): neither public manual nor selected DDI supplies the incremental/cumulative 5A1 rule. Existing #1198 observation preservation cannot establish additivity; harvest_kg sums across this coordinate. Independent Ethiopia review rejects blind append/replacement/No filtering and requested the date-evidence qualification, incorporated before final freeze.

- Public docs extraction: independent review authenticated 115 ordinary artifacts and 77 selected DDI variables, plus both exact PDFs/all 89 saved pages and 12 independently re-extracted critical pages. The peer performed no project imports, raw/cache/private-witness reads or tests. No full Country/API acceptance is claimed. Only this ledger and a new CONTENTS appendix change; merged #1291 appendix is retained byte-for-byte.
