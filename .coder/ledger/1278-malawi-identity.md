# Prior-Art Ledger — GH #1278 Malawi 2019-20 identity

Search tier: ripgrep + git floor; no GitNexus server used. Diagnosis pin: 10ad99d62.
Public documentation extraction base: 5a4b2ac623d55bb31b8908260ad54efb34070cd6.

## §1 Task
Reconcile the seven ignored `function: cs_i` declarations with actual Malawi
2019-20 identifiers, preserving IHPS membership and independent IHS5 observations.
The original investigation inventoried parser contexts and checked bounded
cached-source joins. This extraction records its qualified results only; no
new production, runtime, tests or data reads are part of the extraction.

## §2 Existing machinery
- `Wave.column_mapping`, country.py:1464: only mapping/mappings activates a
  formatter; `Wave.grab_data` applies per-file overrides before extraction.
- `local_tools.df_data_grabber`: format_id on index variables; reuse actual reader.
- Malawi/2019-20/_/mapping.py:17: cs_i would prepend cs-19-; currently unused.
- Malawi/2016-17/_/data_info.yml: working mapping:cs_i; scripts use cs-17-.
- Malawi/2019-20/_/{food_acquired,plot_features,plot_inputs,plot_labor,crop_production,
  livestock,anthropometry,people_last7days,food_coping,months_food_inadequate}.py:
  explicitly use bare cross-sectional case_id to match sample/roster.
- Malawi/_/panel_ids.py reads only Panel YAML blocks and reuses panel_ids;
  JSON source crosswalks are unchanged by PR1265 (merged 9105d5d90).
- tests/test_gh1260_malawi_panel_geo.py covers geo joins and status rule;
  GH1260 closed by merged PR1279, not by an identifier change.
- derivations.py: REQUIRED_FIELDS and DerivationRecord.resolve consume function:
  legitimately in derivations.yml; do not replace those keys.

## §3 Definitions
Inherit STANDING.md §3/§4, CLAUDE.md and lsms_library/data_info.yml.
Malawi CONTENTS.org:1932-1946 says only Data/Panel is wired into the panel;
Cross_Sectional keeps its own ids. 2010-11 case_id is panel canonical identity.
2016-17 is cs-17-prefixed; that precedent does not prove a partial 2019 migration safe.
Niger/_/niger.py:43 also adds E_ to prevent matching its separate ECVMA panel.
Equal labels across independent draws prove neither physical continuity nor
physical distinctness; provenance must establish linkage.

## §4 Invariants and contradictions
CONTENTS.org:74: "That asymmetry is a deliberate, already-recorded decision";
CONTENTS.org:2719: "function: cs_i is not a recognised key" and comments
"describe intent, not behaviour". Preserve both as historical evidence.
The panel-ids skill's table labels IHS4/IHS5 as panel waves, whereas country
notes distinguish independent IHS and IHPS halves. Use the actual Panel wiring.
Do not infer panel membership from coincident bare strings across samples.
No raw IDs/frames in Git/evidence. Verified copies only; offline, private roots;
all thread counts 1; source builds <=300s, warm <=60s, pytest -n0.
Strict comparisons must inspect dtype, missing masks, exact nonmissing values.

## §5 Reuse decision
No namespace policy selected. The reversible bare-ID removal experiment
matched existing script producers but retained two cross-sample coincidences.
The alternative cs-19 migration changes longitudinal set-intersection counts
and requires every YAML and script producer to change together; seven key
replacements alone orphan ten scripts and four additional YAML tables.
Original YAML restored byte-for-byte; candidate patch is evidence only.
Use existing formatter, reader, id_walk, hash machinery; no new identity policy.

## §6 Review questions
Independent Ethiopia source/receipt review supports the bounded diagnosis.
Root owns the existing-issue update and maintainer-policy escalation. Two
IHS5 strings match the earlier IHS3 sample without linkage evidence; retain
that qualification without reclassifying independent cross-sectional households.
The worker/peer issue snapshot was open, no comments (2026-10-30, before
the root review update). The original issue explicitly permits removing
dead declarations if bare is intended. No automatic closure.

## Phase 3
- OK (§2): saved 8 actual-parser synthetic tests passed at the diagnosis pin;
  seven ignored identifier keys
  versus 36 legitimate derivation registrations across 265 YAML files. One
  unrelated malformed example (no function key) is explicitly recorded.
- OK (§3/§4): actual cached-source 2019 sample has 11,434 unique IHS5 and
  3,178 unique IHPS identifiers, disjoint before/after panel canonicalisation.
  Checked household-grain outputs join on their i keys; cluster_features
  has household-grain merge inputs but itself is cluster-grain. No aggregation
  or sample reclassification added.
- OPEN IDENTITY POLICY (§4): two IHS5 strings occur in IHS3 2010
  sample, despite none being in any updated_ids key/value. On actual sample
  spine, panel_attrition counts 1,454 current versus 1,452 with only IHS5
  prefixed; all sample payloads/rows and panel IDs remain exact. This is not
  an all-wave Country.panel_attrition roster acceptance run. The defensible
  finding is two unsubstantiated string-based longitudinal matches; the
  physical households are not proved either identical or distinct.
- OK (§2): pure build_frame/panel_ids reconstruction equals committed maps
  (5,686 panel; 4,000/2,508/3,178 updated), no JSON writes.
- LIMITED (§4): candidate comparisons retained full-frame failures caused by
  initial missing 2004 sample dependency (v subsequently added) and plot i
  dtype string→str. Native array/mask/dtype comparisons and tiny/nextafter
  controls are explicit; no blanket API-frame equality claim.
- OK (§5): YAML-only candidate changes 25/25 Malawi non-JSON country hashes
  and 25/125 enumerated wave-table hashes, all 2019-20. Final docs/ledger-only
  change restores build input bytes and introduces no production rewarm.
- Independent source/receipt review is complete with qualified support; policy
  ruling, selected repair and complete verification remain outstanding. Outcome
  scientific_disagreement; GH1278 stays open. No tests/data replay accompanied
  the public docs-only extraction.
  Aggregate report and draft are in dispatch ad30c7b83dc24ab8b01696180964b318
  evidence/REPORT.md; raw operands stay in sibling private-witnesses.
