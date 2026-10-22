# Prior-Art Ledger -- GH1198 Uganda production observations

**Search tier:** git + ripgrep floor. This ledger precedes all new prototype
functions. Source baseline: f8d236694c26fe9e709c5548f02c70f74ea72c33.
Inherits [STANDING.md](STANDING.md), the repository CLAUDE.md and data_info.yml.

## §1 Task, restated

Design and privately test a lossless observation coordinate for Uganda
crop_production. No production, canonical-schema or public-axis change is
approved. Preserve each eligible native production record/condition slot and
its quantity, own reported factor, sales fields and attributes before the
analyst's harvest reduction. Equal factors and partial reports also retain
identity. GH849's factor-column wiring remains separate.

## §2 Existing machinery

| Machinery | Source | Existing coverage and decision |
|---|---|---|
| crop_production_for_wave / CROP_COLMAPS | countries/Uganda/_/uganda.py:1230,1620 | Reuse pure field decoding, eligibility and dtypes; tests/test_crop_production_kgfactor.py and condition tests. Private projection must intercept before the seven-key sum/first block at1579. |
| Source identity and field oracles | frozen private Uganda/849/{DIAGNOSIS.org,PROPOSAL.md,source-identity.json,all-production-key-audit.json} | Reuse only after receipt gates and guarded physical copies; source release/row/slot locators are evidence, not an approved API. Strict artifact addendum separately accepted. |
| _normalize_dataframe_index / Country._finalize_result | country.py:8394,3815 | Reuse with deep-copied declaration in private controls; existing grain/index/population tests. Preserve id_walk and id_converted, never invent an ID formatter. |
| _align_to_canonical_levels / _harmonize_country_frame | feature.py:277,649 | Reuse canonical union/sentinel alignment and extra-level/drop controls; tests/test_feature_canonical_levels.py. A country-only coordinate is insufficient. |
| harvest_kg_factors / harvest_kg / yield_kg | transformations.py:4282,4698,4888 | Reuse reported-factor screens, ladder, survey medians and explicit analyst reduction; tests/test_harvest_kg_shipped_factors.py and harvest-factor tests. No new conversion estimator. |
| get_dataframe / get_data_file | local_tools.py; STANDING §2/§4 | Reuse sanctioned source access only if decoding is needed. Saved raw/native artifacts may instead be copied with exact receipts. |

## §3 Definitions and conventions

- CLAUDE.md:515: "Duplicates on a declared index mean the IDENTIFIER IS BROKEN
  or a LEVEL IS MISSING -- fix the index; do not declare a reducer."
- data_info.yml:107 currently declares crop grain
  `(t,v,i,plot_id,j,u,condition,season)`; sentinels at276 and Feature's
  alignment implementation are existing extension points, not approval to edit.
- Grain policy, SkunkWorks/grain_aggregation_policy.org:23: maximal instrument
  grain belongs in access/composition; explicit transformations may aggregate.
  The document labels its broader composition mechanism partly implemented.
- Uganda CONTENTS full f8 SHA256
  `774d5bc13c35ca5a6d9543766648f88dbc1efe508d73aa388d5ac6e81146256c`
  (3604 lines, including task/logbook entries). A/B names modules rather than
  calendar-season order; 2013 value vocabularies override swapped labels;
  2018 mixes legacy/wide names and A has no harvest-unit source; 2019 slots
  are condition observations. Sales unit/condition is separate.
- KgFactor is a rate on the SAME observation, not additive or a reported total.
  Existing finite/positive, cap250, kilogram1% and calendar-year screens stay.
- CONTENTS:2945 already identifies missing record ordinal; historical claims
  of "small and in the right direction" (2435), harmless repeated rows
  (helper1573) and older ladder wording conflict with frozen849 findings.
  Treat these as dated contradictions, never permission to discard evidence.

## §4 Invariants and assumptions

- STANDING §4 applies. Source/config/runtime/fixtures stay exact f8. No cache
  stamp, bypass, acquisition, shared-cache mutation or promoted private parquet.
- Retain EVERY eligible observation, even equal payloads, absent factors or
  missing native IDs. A physical row ordinal is CONSTRUCTED/source-order and
  bound to exact file release provenance, never survey-reported identity.
- 2011/2013/2015 production IDs and 2018/2019 actual slots require separate
  namespace and uniqueness auditing. Unkeyed2009/2010 require full-row-repeat
  classification, not automatic deletion. Future ID conflicts stay explicit.
- Existing null-key losses360/43 and 2015 sales x100 remain separate. No
  Quantity/factor/unit/eligibility correction is included in this phase.
- Feature drops undeclared extras and collapses; proposed declaration must be
  tested through actual existing normalization/alignment with copied metadata.
- Explicit zero-tolerance full-frame, axis, dtype, mask and recursive typed
  attrs comparisons; pandas default floating tolerances are insufficient.

## §5 Reuse decision

| Quantity | Decision | Reason |
|---|---|---|
| Source decoding, slots, screens | reuse | Pure helper and frozen source oracles already encode them. |
| Observation coordinate | new design only | Existing seven keys omit native production/slot identity; no existing crop coordinate represents it. |
| Country / Feature alignment | extend privately | Deep-copy declarations and call current hooks; no new global flag. |
| Per-row kg then harvest/yield | reuse | Existing transformations implement both conversion and analyst reduction. |
| Comparator | synthetic unless gated | Do not claim a healthy Niger real-cache comparator without full current-hash/coverage receipts. |

## §6 Open engineering decisions for root

Root must review the concrete coordinate name/type/provenance, missing/duplicate
ID behavior and honest unresolved sentinel before any public-schema change.
This phase will test one minimal design and report measured downstream effects
or precise executable remaining controls; no vague policy blocker is asserted.

### Phase 3 verification

Pending private projection and source-grain census. Production and schema edits
are forbidden in this phase. Sue (GPT-6).
