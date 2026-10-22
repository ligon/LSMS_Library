# Prior-Art Ledger - GH1119 Burkina Faso seed-crop labels

Search tier: ripgrep and git. Actual parent is reviewed GH1123 head
`cfd0782766068da6dc7e7605334e957f988a4ab1`; PR1189 remains unmerged in the
root's 2026-10-22 preparation receipt. This ledger precedes production edits
and new diagnostic functions. Inherit `STANDING.md`, `CLAUDE.md` and
`lsms_library/data_info.yml` rather than restating their contracts.

## Section 1. Task

Reproduce GH1119's Burkina Faso named seed-crop vocabulary mismatch against
the actual Org parser, held native input/crop sources and normal Country API.
If confirmed, change only two Preferred Label cells in harmonize_seed_crop:
the maize and cowpea seed targets become the existing crop-production labels.
Preserve every native input identity and all payloads, including GH1123's
source-qualified unknown-code-21 identity and uniqueness guard. No other
country, source classification, marker policy, grain or science is authorized.

## Section 2. Existing machinery

| Machinery | Location | Existing coverage | Decision |
|---|---|---|---|
| Org parser / table resolver | `local_tools.py:2417,2735` | `test_orgfile_missing_table.py`, categorical-resolution tests | Reuse actual named table parser |
| Native input builder and seed resolver | `Burkina_Faso/2018-19/_/plot_inputs.py:40,59,94` | `test_burkina_faso_plot_inputs_identity.py`, reviewed GH1123 source oracle | Preserve extraction, resolver and guard |
| Native crop builder | `Burkina_Faso/2018-19/_/crop_production.py` | Existing agriculture/Feature tests; held-source diagnosis required here | Reuse existing harmonize_food lookup |
| Country key / finalization | `Burkina_Faso/_/burkina_faso.py:ehcvm_i`, `country.py:_finalize_result` | GH1123 all-source and panel/sample review | Preserve all identities and full typed attrs |
| Seed mass reduction | `transformations.py:5569` (`seed_kg`) | `test_863_transforms.py`, `test_gh1093_declared_unit_consumers.py`, `test_gh1159_pooled_reducers.py` | Reuse; measure at actual household grain |
| Feature assembly and audit | `feature.py`, `bench/feature_audit/scan.py` | Cross-country tests and accepted GH1123 audit | Reuse aliases, sentinels, complete warning accounting |
| Content hashes / read gates | `STANDING.md` section 2; `country.py:_table_cache_hash`, `Wave._input_hash` | Cache suite; GH1123 actual 549/39 inventory | Reuse; measure before source builds |

## Section 3. Definitions and country idiosyncrasies

- The 2018-19 input roster is household x input slot, with no parcel axis;
  `data_scheme.yml:218-238` and CONTENTS's dated GH1123 section are authoritative.
- EHCVM `i` is the injective ehcvm_i formatter and `v` is grappe. Vague is a
  sample split, not another input measurement. Do not allocate household seed
  totals to crop parcels or assert a parcel-level join.
- The 2014 EMC has four independent food recalls, with quantities only in
  passage 1. The 2021-22 EHCVM is a half-panel restricted to original EAs,
  with distinct panel/cross-sectional weights. Neither has a current
  plot_inputs builder; do not add coverage.
- Native 2018 code 21 is unlabelled on 602 applied rows; its existing Seed
  classification is unverified (#1187). The qualified crop token preserves
  identity and must not borrow 2021's different code meaning.
- Purchased / Quantity_purchased are existing API names, not verified
  purchase semantics: their gift/donation bindings are separately owned by
  #1188. This task must preserve them exactly.

## Section 4. Invariants and evidence boundaries

- Production boundary: exactly two Preferred Label cells, no original key or
  other table/cell/comment edit. Prove source labels and actual crop vocabulary
  before changing them. Preserve all 22,877 native input lines and their keys,
  multiplicities, numeric values, units, dtypes, masks and full typed attrs
  except the two intended labels.
- Maintain GH1123 identity safeguards and all other crop labels. Verify
  injectivity on actual held labels, including equal-payload observations.
- CONTENTS retains contradictory sampling counts (10,224/6,961/3,218 versus
  the prior actual 10,411/7,010/3,227) and conflicting French/English split
  descriptions; quote/re-measure rather than declaring notes stale.
- The old harmonize_input comment calls code 21 Autres semences, contrary to
  the dated GH1123 evidence. Record this contradiction; do not expand the
  production edit into source-classification curation.
- Sanctioned IO only, physically copied held sources, terminal MD5/SHA/size/
  mode/mtime/inode/nlink gates, exact source/import/data/config identities,
  private roots, active socket/write guards and unshare -rn. No acquisition,
  DVC CLI, shared mutation, bypass, cache stamps or in-tree payloads.
- Actual 549 API and 39 framework hash census precedes source rebuilds.
  Country-wide invalidation is an operational cost, not proof every output
  was rebuilt. Fresh-process warm receipts must preserve real backend writes.
- Full records, IDs and warnings stay private. Safe exports have a dedicated
  directory and never replace private aggregate filenames. Save per-call
  endpoints before assertions; retain failed attempts under distinct names.

## Section 5. Reuse decision

Use the existing Org mapping, parser, seed resolver and crop vocabulary.
Add only meaningful focused parser/vocabulary/injectivity regressions and an
additive dated country note. The original GH1123 source test contains two
expected crop spellings; report the necessary two-value expectation alignment
before changing that test, without weakening its source/payload assertions.
Reuse existing seed_kg, Country/Feature assembly, scanner and strict typed
frame comparisons. No new resolver, reducer, quantity or plot identity.

## Section 6. Questions and residuals

No human classification decision is needed for the authorized two labels if
actual source reproduction agrees. Any contrary source evidence stops scope
expansion for root review. Senegal, Guinea-Bissau extraction, marker policy,
GH1187 and GH1188 remain separate. Final live parent/issue reconciliation is
required before publication; root supplies live-state coordination.

## Verification

Pending source reproduction and the actual prebuild inventory. No candidate
or completed fix is claimed by this grounding ledger.

Sue
