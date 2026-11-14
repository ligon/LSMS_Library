# Stream: gnb -- Guinea-Bissau labels, then 2021-22 wiring (one GNB re-warm)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/gnb`, branch `feature/gnb-labels-2021-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/gnb/`.
- Read before editing: `lsms_library/countries/Guinea-Bissau/_/CONTENTS.org` (all
  of it).  The instrument is in Portuguese.  Also read the CONTENTS.org of
  Togo, Benin and CotedIvoire, the 2021-22 sections at least: their 2021-22
  waves landed in v0.17.0 (PR #1711) and are your precedent.

## Phase A: 2018-19 labels (spec: Q1 and Q3 of
`/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/reconcile-20261112/REPORT.md`, lines ~248-280)
Issues: #1107, #1539, #1119 (GNB half), #1226 (GNB product-94 half), the GNB
item of #1483, and #1118 (GNB site).
1. `harmonize_input`: map the four Portuguese inorganic labels to
   Urea / Phosphate / NPK.  Handle DAP as the Togo precedent does, or as Other,
   and say which.  Add kg routes for `Quilograma` / `Grama`.
2. `harmonize_seed_crop` from the country's own `s16bq01` labels.  Seed rows
   with a recorded crop are served `(not crop-specific)` today, which breaks the
   ruled contract.
3. #1539: map `Djagatu` to `Jaxatu` and `Beringela` to `Aubergine`, and drop the
   trailing comma.
4. #1226: bridge community-price product 94 to 95 by code.
5. #1118: count the `Manquant` rows first.  Then make
   `2018-19/_/plot_inputs.py:126` fill `'Unknown'`.
6. Fix the stale comments at `_/data_scheme.yml:155` and
   `2018-19/_/crop_production.py:18,120`.
Verify:
- `nitrogen_kg(GNB)` has more than 0 matched rows.
- `plot_inputs` is frame-equal to base apart from the label columns.
- `seed_kg` is conserved.
- `Feature('plot_inputs')` has a unique index.
- `community_prices` gains the product-94 rows and nothing else moves.
- The per-wave `u == 'Manquant'` count is 0.

## Phase B: wire Guinea-Bissau 2021-22 (#1697's GNB part)
Ruling: "EHCVM-2 2021-22 wiring, Benin / Cote d'Ivoire / Togo / Guinea-Bissau -- GO",
plus "EHCVM-2 2021-22 strata, GH1575 extension" (region x milieu) and
"Undocumented producer constructions in EHCVM food quantities".
- Order: `sample`, `household_roster`, `cluster_features`, then food
  (`food_acquired`, with #1598's own-country NSU weights), `shocks` (#1556's
  ruling), `assets` (#1566's s12q09 check).  Nothing else.  One commit per table.
- Copy the Togo / Benin / CIV 2021-22 wiring (data_info.yml, mapping.py,
  derivations.yml entries, tests) rather than inventing.  Read it on
  development first.
- The wave's own weights are served.  It is a new wave, and the ruling allows
  that.  No panel IDs: the 2021 same-cluster revisit is not a household panel
  unless a ruling says so.
- **Before serving any food quantity**, test the released quantities for
  producer constructions like those found in the other EHCVM-2 waves:
  - Togo: s07bq03a = 1.115 x a regional multiplier;
  - CIV: H/L outlier replacement;
  - Mali: a rice block with q3 = 1.4 kg x household size;
  - Senegal: salt at 0.02 kg x household size;
  - Niger: s07bq07a filled where CAPI disables it.
  The method and code are in
  `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-live-s07bq03a-check/`
  and `ehcvm2-s07bq03a-docsearch/`.  If you find a construction, do not serve
  the affected rows as household answers.  Stop food at that point and bring it
  back as a question with the evidence.  The other tables continue.
- Disclose that adding a wave moves GNB's cross-wave kg inference pool, as
  Malawi 2024-25 did.
Verify:
- A cold build of each new table.
- `bench/feature_audit/scan.py` restricted to Guinea-Bissau, if its scope fits
  your private roots.
- 2018-19 frame-equal to base, apart from the Phase-A label changes and the
  disclosed kg-pool movement.

## Tests and census
- `tests/test_*guinea*.py` / `*gnb*` (find them), the EHCVM tests the
  Togo/Benin/CIV 2021-22 work touched (`tests/test_ehcvm_7day.py` and others),
  `tests/test_derivations.py`, `tests/test_schema_consistency.py`,
  `tests/test_build_transform_hash.py`.
- A hash census.  Expected: Guinea-Bissau only.

## Deliver
`REPORT.md` and `PR_BODY.md`.  Write Phase A's report section as soon as
Phase A is done, then continue.  Commit as you go.
