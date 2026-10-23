# Burkina Faso #1123 — diagnosis only

The current Country path drops **183 distinct reported input line-items** from Burkina Faso 2018–19: source/wave **22,877**, served **22,694**. A non-injective crop map breaks the declared identity. The issue’s **163 destroyed / 150 conflicting tuples** reproduces, but excludes another **20 equal-payload pairs between distinct native input codes**. Preserve all 183.

No tracked file, ref or GitHub resource was changed. HEAD remains `a63d26d7f7cdc5204931c1fbe334c1fcc6cea8fb`; all current 549 country hashes and 39 framework fingerprints match runtime baseline f8. This is a concrete proposal awaiting root approval, not an implemented fix.

## Native grain and loss mechanism

The actual builder is [2018-19/_/plot_inputs.py](/local/job40255479/worktrees/isa-Burkina_Faso/lsms_library/countries/Burkina_Faso/2018-19/_/plot_inputs.py:94). The source has one applied record per `(grappe, menage, s16bq01)`; all 22,877 rows pass `s16bq02 == 1`. There are zero duplicated native keys, zero missing native keys and zero null declared keys. No plot/parcel column exists. Extraction and dictionary mapping preserve rows one-for-one; there is no join or merge fanout in this builder.

The declared country grain is `(t,i,input,crop,u)`, explicitly household × input in [data_scheme.yml](/local/job40255479/worktrees/isa-Burkina_Faso/lsms_library/countries/Burkina_Faso/_/data_scheme.yml:215). All seed types become `input='Seed'`; the crop mapping pools four occurring native slots into `Autre crop`:

| Native slot | Source label | Applied rows |
|---|---|---:|
| 15 | Semences d'autres céréales | 195 |
| 19 | Plants/boutures de tubercules | 80 |
| 20 | Autres semences | 883 |
| 21 | No Stata label; read as literal `21` | 602 |

There are 170 duplicate declared-key groups containing 353 source records: 157 pairs and 13 triples. Every colliding group consists of distinct native slots; none repeats the same source input code. All collisions are `Seed / Autre crop / Kilogramme`.

| Case | Groups | Extra source rows lost |
|---|---:|---:|
| Conflicting non-null payload | 150 | 163 |
| Equal measured payload, distinct native slots | 20 | 20 |
| Complementary-only missingness | 0 | 0 |
| Total | 170 | 183 |

The current core uses the most complete observed row, with source-order tie-breaking. An independently computed selection matches the complete pre-final Country frame, values and payload dtypes, after the framework’s index-string dtype normalization. Thus changing the selector or adding Purchased to the key would not repair the identity. Equal measurements do not make two native slots the same source line.

All 183 discarded rows report `Purchased=False`, missing `Quantity_purchased` and a quantity in the literal source unit `Kilogramme`. Their reported quantities total **2,409.58 kg**:

| Native slot | Lost rows | Reported kg lost |
|---|---:|---:|
| 15 | 32 | 590.50 |
| 19 | 7 | 147.00 |
| 20 | 89 | 740.48 |
| 21 | 55 | 931.60 |

These are reported quantities by native input code, not a claim that code 21 is physically seed. No sentinel, clipping, rounding, unit inference or quantity curation is proposed.

## Household namespace and country record

Read all 1,424 lines of Burkina CONTENTS first, including TODO/WAITING/LOGBOOK records. The EMC 2014 uses `zd/menage` and four food recall passages; EHCVM uses `grappe/menage`, with vague a sample split. The 2021–22 sample is a half-panel restricted to households remaining in their original EA, with distinct panel/cross-sectional weights and no replacement floor.

This script correctly uses [ehcvm_i](/local/job40255479/worktrees/isa-Burkina_Faso/lsms_library/countries/Burkina_Faso/_/burkina_faso.py:18), not the older `i` formatter: grappe + literal `0` + two-digit-minimum menage. Among the 4,509 source agricultural households it yields 4,509 unique IDs, zero sample orphans; the older formatter differs for 1,329 households. Do not re-key households. Source grappe maps to at most one vague in this input file; no repeat-visit level is missing here.

CONTENTS records live caveats rather than blanket completeness: 2014 province variation within cluster, missing 2014/2021 cluster GPS, 2021 parcel coordinates not yet wired, 2014 food exposure and interview-result conflicts, and source/served quantity distinctions. The live #1148 entry quotes the official report’s claim that all p1 households participated and the contradictory retained status results; neither is declared stale or adjudicated here. The generic food skill’s EHCVM “distinct rounds” example is not a license to turn this sample split into another recall axis; the country record governs this survey.

A direct contradiction exists in the current crop-table comment at [categorical_mapping.org:616](/local/job40255479/worktrees/isa-Burkina_Faso/lsms_library/countries/Burkina_Faso/_/categorical_mapping.org:616):

> `'21' is Burkina's trailing 'Autres semences' slot.`

Held 2018 metadata binds **20**, not 21, to `Autres semences`; no dictionary entry binds 21. Held 2021 metadata binds 21 to `Plants d'hévéa, de teck` and 22 to `Autres semences`. This is a result to record, not permission to borrow a later-wave meaning.

The existing `harmonize_input` binding of literal 21 to **Seed is legacy-only and unverified**. The proposed fix retains it for a bounded identity repair; it does not endorse that classification. A separate source-authority residual is drafted.

## Exact proposed boundary — pending root approval

Change exactly three Preferred Label cells in `harmonize_seed_crop`:

| Original Label | Current target | Proposed target |
|---|---|---|
| Semences d'autres céréales | Autre crop | Autre céréale |
| Plants/boutures de tubercules | Autre crop | Autre tubercule |
| 21 | Autre crop | Unknown [Burkina_Faso 2018-19 s16bq01=21] |

Leave `Autres semences`, bare `20`, the input map including its legacy Seed classification, household IDs, units, payload extraction and canonical grain unchanged. The two named splits reuse the human-authored Benin/Togo fix `36170a57b04317e55716919eb5eb1054b5b43102`. **Those two changes alone leave 98 duplicate extra rows**; a private negative control rejects that incomplete copy.

The third target is an explicitly source-qualified unknown, preserving native code identity without naming a physical crop. Existing precedent is visible native-code fallback: Tanzania’s crop builder prefixes unresolved codes with `crop_` ([tanzania.py:1259](/local/job40255479/worktrees/isa-Burkina_Faso/lsms_library/countries/Tanzania/_/tanzania.py:1259)); Niger passes unmatched source crop labels through; Uganda records accepted unresolved numeric unit codes. These support preserving a visible source identity. No exact house spelling for this qualified unknown-crop token was found: **the exact spelling is a representation choice for root approval**.

Add the existing Benin/Togo uniqueness-guard logic after `_finish_plot_inputs` and the nonempty assertion, immediately before [to_parquet at line 124](/local/job40255479/worktrees/isa-Burkina_Faso/lsms_library/countries/Burkina_Faso/2018-19/_/plot_inputs.py:123). Its error should report country/wave and duplicate count without exporting raw keys. A future duplicate must fail before cache output even if the measured payload is equal. Do not add Purchased or another axis, sum unlike categories, or rewrite the shared selector.

Also correct the inaccurate mapping comment, add the genuine source-code/coverage idiosyncrasies to CONTENTS and add focused regression tests. No country-module, schema, data-scheme, shared-framework or comparator-country edit is needed.

## Private proposal evidence and all-wave effects

Applying those exact three cell changes **in private frames only** gives 22,877 unique native-grain rows and zero null keys. Crop labels change on 877 rows; every other key, measurement, unit and payload dtype is exact row-for-row. All 183 currently discarded native lines return with full payloads, including the 20 equal-payload lines.

The unchanged current finalizer retains all 22,877 rows and payloads. Its existing public unit spelling `Kilogramme -> Kg` (global `categorical_mapping/u.org:40`) is accounted for in this comparison; no unit conversion or value change is introduced. The actual Benin guard AST rejects the baseline, rejects the incomplete two-split control, accepts the full proposal and rejects a future even equal-payload duplicate. Distinct native identities with equal payloads remain separate.

Only `Burkina_Faso/2018-19/_/plot_inputs.py` currently consumes `harmonize_seed_crop`. There is no 2014 or 2021–22 plot_inputs builder; normal public subset requests currently return zero rows for each. The 2018-qualified token therefore reaches **no other current wave key**. It must not become a universal code-21 mapping when future wave coverage is added.

Those coverage gaps are real source opportunities, not solved here:

| Wave | Held input source | Raw rows | Native key observation | Current plot_inputs output |
|---|---|---:|---|---:|
| 2014 | emc2014_agri_couts_intrants.dta | 138,242 | 36 rows on repeated `(zd,menage,W1)` keys; separate diagnosis needed | 0 |
| 2018–19 | s16b_me_bfa2018.dta | 22,877 | Unique `(grappe,menage,s16bq01)`; all applied | 22,694 |
| 2021–22 | s16b_me_bfa2021.dta | 70,994 | Native key unique; 12,223 applied rows also unique; changed 22-slot dictionary | 0 |

The country Org mapping file is a whole-country build input. A private hash-scope probe confirms a change there moves all **24 Burkina country/table hashes**, while **525 other-country hashes and all 39 framework fingerprints remain exact**. This is a scope probe, not an actual candidate-content hash receipt. An implementation must measure its actual hashes and cold/warm builds after approval. Documentation is excluded from hashing; guard changes remain within the already affected BF set.

## Current public Feature and healthy comparators

Actual held-source scripts and normal Country builds reproduce Benin 10,605 rows and Togo 13,730 rows. Their pre-write guards succeed. Togo’s three source rows beyond 13,730 have all measured payloads absent; its raw wave and Country cache keep 13,733, while the deliberate public empty-payload filter removes those three. This is separate from BF’s nonempty lost rows.

Current `Feature('plot_inputs')(['Burkina_Faso','Benin','Togo'], currency=None)` serves 47,029 rows: 22,694 / 10,605 / 13,730. Every country frame matches its public Country counterpart after the existing canonical alias/sentinel alignment; **additional Feature loss is zero**. Warm parquet byte hashes and nanosecond mtimes remain unchanged.

The existing harness was scoped to these countries and phases 1/2: eight warning records, no failure/error/skip. Plot_inputs has no phase-2 kwarg grid, which is recorded rather than pretending additional variants were tested. Full warnings remain private. The BF loss occurs before Feature assembly; no shared Feature change is indicated.

## Verification and limits

Five source blobs (44,225,375 bytes) were physically copied from held L1 and checked against current sidecar MD5s. Sixteen companion caches were physically copied and verified against the approved 1,519-file seed manifest; six country-level companion parquets also match current expected hashes and full wave coverage. **No plot_inputs cache was seeded**: actual scripts and the first normal Country requests built it from held source. No acquisition, DVC CLI, shared-cache clearing/promotion or write was used.

The four 2018 instrument sidecars have no held blobs: two household XLS questionnaires and two nomenclature PDFs. Exact paths/MD5s are recorded in `instrument-manifest.json`. No instrument binding for code 21 is claimed and none was fetched.

Private checks: **11 positive/adversarial tests pass**, zero skips; **two deliberate conservation tests fail** on the baseline and incomplete two-split proposal; **four scoped existing Benin/Togo tests pass**, zero skips. The complete tracked SHA256 manifest, clean pinned HEAD and private L1/companion integrity proofs are in `validation.json`. All commands use the mounted image, exact cwd/PYTHONPATH/config identity, private data root, active offline namespace and one numerical/build/Make worker.

`LEDGER-DRAFT.md` is ready for root review before a tracked ledger commit. `DRAFT-ISSUES.md` separates other-country #1123 work, #1119 vocabulary joins and the new code-21 source-authority question. `COMMANDS.sh` documents reproducible commands. Raw frames, source IDs, complete warnings and scanner diagnostics stay private.

Sue
