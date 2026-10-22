# Prior-Art Ledger - GH1188 Burkina Faso input purchase/gift routing

**Search tier used:** ripgrep + git, with stdlib static inspection planned. No GitNexus or memory-query tool is exposed. Actual source parent is `3dc9d28510dd783c648413987011832669f8bcf7` (PR1203 over PR1189, root freshly reports both unmerged). This initial ledger precedes any new helper, source/schema analysis or runtime import. The complete 1,486-line country CONTENTS was read first, including its TODO entries and dated caveats. AGENTS and CLAUDE are byte-identical; standing/prior ledgers and applicable skills anchor this record.

## §1 Task, restated

Static phase0 diagnosis only: map Burkina Faso 2018-19 `plot_inputs` used, purchased and gift quantities, flags and their own units to the existing canonical/country interface and actual consumers. Identify a coherent source-preserving representation and the smallest later held dependency set, using current source/config and old aggregate metadata receipts only. Do not implement a flag-only change, place purchased quantity under the used-unit axis without authority, or choose a new representation before root reviews concrete options.

Live GH1188 is open with zero comments. Its prior measured scope is 22,877 applied rows, native `(grappe,menage,s16bq01)` uniqueness, 802 gift yes versus 7,689 purchase yes, 8,297 flag disagreements, 8,371 quantity/mask differences, and 23 gift-unit / 193 purchase-unit differences from usage units among known-unit records. These counts are inherited evidence to reproduce after runtime release, not new measurements or permission to infer code meanings.

## §2 Existing machinery

The paths below come from the already-reviewed GH1123/GH1119 ledgers and country notes. Current line anchors and complete callers will be verified after this ledger-first commit; no new implementation is presumed.

| machinery | prior anchor / evidence | existing coverage | reuse decision |
|---|---|---|---|
| Native input builder | `countries/Burkina_Faso/2018-19/_/plot_inputs.py`; GH1123 ledger §2 | `test_burkina_faso_plot_inputs_identity.py`, full native identity review | preserve native extraction, eligibility, grain, row order and guard |
| Crop/input mapping and household key | `Burkina_Faso/_/categorical_mapping.org`; `burkina_faso.ehcvm_i` | GH1123 identity tests and GH1119 label tests | preserve code21 identity, Maïs/Niébé improvements and injective household namespace |
| Canonical/country field declarations | `lsms_library/data_info.yml`; `Burkina_Faso/_/data_scheme.yml` | `tests/test_schema_consistency.py` | inspect existing unit/purchase/gift columns before proposing extension |
| Native input consumers | `transformations.py:seed_kg` (prior GH1119 anchor5569), nitrogen/rate consumers to enumerate | `test_863_transforms.py`, `test_gh1093_declared_unit_consumers.py`, `test_gh1159_pooled_reducers.py` | reuse existing usage-quantity semantics; no new reducer/conversion |
| Country normalization/finalizer | `country.py:_normalize_dataframe_index`, `_finalize_result`, `_join_v_from_sample`, `local_tools.id_walk` | GH1123/GH1119 full native/public and typed-attrs controls | preserve existing identifiers, attrs and loss reporting |
| Feature alignment | `data_info.yml:Index Info`; `feature.py` | Cross-country feature tests, scoped accepted GH1123/GH1119 audit | inspect column/axis transport without changing shape |
| Source/writer/cache machinery | `STANDING.md` §§2-4; `local_tools.get_dataframe`, `to_parquet`; hash/read gates | source/cache suite and prior actual full census | later reuse only after explicit guarded runtime release |
| Prior aggregate metadata receipts | GH1188 / country dated section and `review-1123-Burkina_Faso` evidence | independently measured q05/07/08/09 semantics and unit disagreements | read existing metadata only; no held payload open/rehash/copy/decode |

## §3 Definitions and conventions in force

Inherit `STANDING.md` §3, CLAUDE and `lsms_library/data_info.yml`; do not duplicate or infer canonical schema rules. The live issue, dated CONTENTS sections and prior ledgers distinguish existing API column names from source meanings.

- The roster is household x input slot, with no parcel axis. `vague` is a sample split; `ehcvm_i`, not the older `i` formatter, supplies the reviewed household key. The source has no parcel allocation to infer.
- CONTENTS quotes the builder's `s16bq05 purchased?` / `s16bq07a quantity purchased` comments, then contradicts them with q05's source label: "16B.05. Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?" Actual purchase fields are q08/q09a with unit q09b; gift quantity q07a has unit q07b; the current row unit comes from used quantity q03b.
- Code21 is unlabelled on 602 applied 2018 records and retains `Unknown [Burkina_Faso 2018-19 s16bq01=21]`. Its input=Seed classification is explicitly unverified legacy curation (GH1187). The differently labelled 2021 code21 is not authority for 2018.
- GH1119's source-backed seed targets Maïs/Niébé and all other mapping cells remain in force. Country counts and raw bindings retained by earlier identity work do not certify purchase semantics.
- The country notes retain contradictory historical sample counts and French/English split descriptions. They also record the one-household grappe298 exception to the general one-vague-per-grappe assertion. Quote the dated evidence rather than declaring these notes stale or changing IDs.

## §4 Invariants and authorized boundary

Inherit `STANDING.md` §4. Preserve all 22,877 rows, native identities, 602 code21 lines, crop labels, input categories, usage quantities/units, eligibility, sampling/ID/attrs, row order and grain. No conversion factor, parcel allocation, aggregation, source deletion, new wave, code21 curation or shared axis is authorized.

Only the task ledger/notes and private `C/Burkina_Faso/1188` may be written. Phase0 permits git/rg/read and stdlib `-S` only. No project/numpy/pandas/pyarrow import, test, API, source payload open/rehash/copy/decode, network acquisition, DVC CLI, shared-cache mutation, dependency/venv/Slurm change or GitHub write. Older skill examples suggesting cache bypass, shared warming or source inspection do not release this stricter task hold. Root owns representation choice, subsequent runtime release, GitHub publication and acceptance; a human owns merge.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| Retain source usage measurements and native identities | reuse | reviewed current builder and map define this boundary |
| Represent independent purchase/gift quantities and units | inspect / extend only if required | current schema and sibling precedents must be mapped; do not invent an axis or silently pool unlike units |
| Usage-based seed/fertilizer transformations | reuse | preserve existing Quantity/u consumers without adding conversion assumptions |
| Source authority / later runtime proof | reuse | old aggregate metadata for static phase; own genuine gates and guarded bounded verification only after release |

## §6 Questions for root

Pending static census: which coherent existing-column representation can preserve gifts and independently unit-tagged purchases without changing the row grain? If an existing canonical contract is missing, which explicit schema/design boundary should be approved? Source mappings, concrete options, minimal declared dependency counts and evidence limits will be delivered before any implementation or runtime action.

### Phase 3 verification

Not run: this is the ledger-first grounding commit. Static source/caller diagnosis follows; runtime, implementation and scientific decisions remain held.

Sue

## Phase0 static findings and root direction (2026-10-22)

The ledger-first commit was `7eb9cfe34`; all subsequent work has been source/config/old aggregate metadata inspection with stdlib only. The private source census and plan are `C/Burkina_Faso/1188/static-source-census.json`, `held-provider-metadata.json` and `PHASE0.md`. No production/schema/test edit, project/numerical import, provider payload open/hash/copy/decode, test/API or cache write has occurred.

Root's concrete direction is country-local wide payload fields at unchanged grain: retain `Quantity <- q03a` and `u <- q03b`; correct `Purchased <- q08` and `Quantity_purchased <- q09a`; add exactly four optional fields, `Unit_purchased <- q09b`, `Received_as_gift <- q05`, `Quantity_received_as_gift <- q07a`, `Unit_received_as_gift <- q07b`. Each source unit uses its own decoded labels and only exact curated formatter keys. Missing flags/amounts/units stay missing; no code5/cart equivalence, zero/False fill, u substitution, conversion or q09c monetary field. This records technical direction, not production/runtime release.

Static inspection found 14 country `plot_inputs` entries, no shared `Columns.plot_inputs` declaration and no existing independent input purchase/gift unit field. `data_info.yml:921`'s nullable `crop_production.Unit_sold` supplies the secondary-unit storage precedent, not input-specific semantics. Country concatenation and Feature's union-of-columns transport admit the local fields without a new axis. Preserve the builder's native identity guard and extend its explicit payload whitelist. Country optional-column parsing already exists (`country.py:1020`, `diagnostics.py:194`); its extended dict declarations are not coerced by scalar-only `country.py:7103`, so the native writer must supply nullable dtypes explicitly. Feature can drop entirely null columns; empty-frame column order/dtypes require later observation, not a static equality claim.

The usage consumers `nitrogen_kg` (`transformations.py:5446`), `seed_kg` (`:5569`) and `fertilizer_rate` (`:6773`) use existing `Quantity`/`u`; no purchase/gift-specific registered consumer was found. Current identity tests encode the wrong q05/q07a purchase oracle and must later change while retaining all reviewed usage/native/ID/map/attrs/guard assertions. Sibling EHCVM builders share that textual binding, but BF source metadata cannot adjudicate other countries' variables.

The smallest native oracle source is 2018 s16b (988943 bytes). Normal cold Country/Feature proof needs six distinct source files, declared **6335825 bytes per copy set**: s16b2018; welfare2014; cover+weights2018; cover+panelweights2021. `_join_v_from_sample` (`country.py:3055`) calls all-wave `sample()` even for an input singleton. Current sidecar MD5s/sizes match old independent receipts. Current lstat metadata of the six held endpoints under `review-1123-Burkina_Faso/data-candidate/dvc-cache` confirms declared size, regular/nonsymlink/nlink1; no provider bytes were opened. Current binary MD5/SHA/magic/physical authentication remains required after release. Optional market/cluster sources would be a separately scoped two-file extension, not part of this minimal set.

Old metadata independently distinguishes q03b's code5 `Charrettée` from q07b/q09b's `Charrette`. Numeric equality is not unit equivalence. New unit columns do not automatically receive the finalizer's name-matched global u normalization, so decoded native labels can differ from public usage-axis aliases. Preserve that explicit paired-column meaning and verify exact source strings later.

Additional contradiction retained: the categorical-map historical comment calls code21 Autres semences (`:565`), while its dated note (`:618`) and CONTENTS (`:1400`) say code20 is labelled and code21 unlabelled. Code21's classification stays GH1187. CONTENTS (`:418`) says SOURCE.org lacks metadata keywords, while all three current files contain catalog IDs; report rather than silently rewrite. None of these contradictions permits changing identities, mappings, code21 classification or sample science.

The metadata helper's original exit1 rejected an extensionless tracked Makefile; its original source/stop record remain. Additive v2 permits the exact Makefile basename and completed exit0. No runtime/test/data prefix was run or replayed. The next step is root's review of the exact source plan and guarded source-only diagnosis release; production edits remain held until source diagnosis and explicit boundary release.

Sue
