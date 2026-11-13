Burkina Faso #1058 is a reproduced Tier B coordinate omission at immutable baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`. No fix is implemented or committed. The concrete boundary proposed in PROPOSAL.md preserves the existing household namespaces, parcel eligibility, panel mapping and area policy.

The complete Burkina CONTENTS (1,376 lines), Mali (1,246), Niger (1,557), and Senegal (749) were read before the corresponding source/header checks, including live TODO/WAITING/LOGBOOK locations. Burkina has three TODO entries and Senegal two; neither Mali nor Niger has a live TODO/WAITING heading or LOGBOOK drawer in these baseline files. Relevant standing-ledger, backlog, review and cross-country assembly guidance was applied at the git/ripgrep/AST floor. Existing country notes and prior read guidance remain authoritative; no tracked note was edited.

Burkina's 2014 EMC is a different instrument with four food recall passages; 2018 is EHCVM with a sample split; 2021 is a half-panel constrained to households still in their original EA, without a replacement floor. The script's identity distinction is deliberate: 2018 passes `ehcvm_i`, while 2021 passes `_old_i` wrapping country `i`. That distinction is preserved. Mali also mixes EAC-I rural holdings/urban households with EHCVM; its 2021 panel has top-ups and a regional booster. Niger's pre-EHCVM EXTENSION and panel namespace decisions are separate from its EHCVM plot IDs. Senegal's 2021 panel/top-up mix and two excluded EAs must not be confused with the English BID's mistranslation as two excluded “administrative regions.”

There are two concrete documentation/code tensions to retain:

- The Burkina helper at `_/burkina_faso.py:179` says “EHCVM s16a has no decimal-degree parcel GPS”; `_/data_scheme.yml:116` repeats the claim. CONTENTS:756 explicitly corrects it for 2021 and names `s16aq46__latitude`/`__longitude`. The measured source confirms the dated correction, without making the blanket comments a reason to reinterpret another wave.
- The country concatenator says “Cached parquets store pre-transformation data” and describes storing a pre-walk index. That script itself does not call id_walk, but the actual default framework route calls the tested mapper in `country.py:5349` before the country write. The actual raw country parquet carries `id_converted=True`. Native script keys and raw-country/public keys are distinct proof layers. No mapper or namespace is changed here.

The sampling-document tensions recorded in CONTENTS also remain: the French report splits the ZD sample, whereas the English BID says it “randomly divided each enumeration area into two equal groups”; 2021 planned counts add to 3,730 beside a stated 3,510. No universe, backcasting or sample decision is inferred from coordinate availability.

All source access used get_dataframe and get_data_file, with metadata-only inspection of the latter's private held-file result. Fifteen necessary sidecars/blobs were physically copied and checked against their current sidecar MD5 plus SHA256/mode/mtime receipts. The first preparation gate stopped on nine blobs absent from the selected API11/1123 private copies; all nine were found already held in release/shared L1, verified and physically copied. The original gap/failure files are retained. There is no current missing-source gap and no acquisition.

| Source | Raw rows | Retained native parcels | Placeholders filtered | q46 coordinate headers |
|---|---:|---:|---:|---|
| Burkina 2018 | 12,441 | 12,441 | 0 | absent |
| Burkina 2021 | 8,467 | 7,350 | 1,117 | present |
| Mali 2021 | 9,924 | 6,638 | 3,286 | absent |
| Niger 2021 | 5,729 | 5,729 | 0 | absent |
| Senegal 2021 | 6,112 | 6,112 | 0 | absent |

Every filtered Burkina 2021 row lacks both field and parcel numbers. There are no partially missing parcel identifiers, retained null native-key cells, or repeated retained `(grappe,menage,field,parcel)` tuples. The two coordinates are float64 with no value-label missing codes. Their variable labels explicitly say “Coordonnées GPS de la parcelle” and identify Latitude/Longitude. In all 8,467 raw rows there are 4,180 paired positions, zero partial pairs and 4,287 missing pairs. Every position is on a retained parcel; the retained set has 4,180 pairs and 3,170 missing pairs. None of the 1,117 filtered placeholders has coordinates.

Among retained parcels, q45 has 4,190 Yes, 3,151 No and nine missing answers. All 4,180 positions occur among Yes answers; ten other Yes parcels have GPS area but no position. The proposed mapping leaves those positions missing. q47 is an area, distinct from q46 coordinates; its current GPS/estimate selection, unit conversion and plausibility clamp are unchanged. The sibling header/variable-label search finds only q45 and q47 as GPS-related fields in the held Mali, Niger and Senegal 2021 files. This is a held-distribution finding, not a claim about unpublished questionnaires or future releases.

The default normal cold build started with zero Burkina L2 parquets and produced plot_features plus its normal sample support. It returns 19,784 plot rows: 12,434 in 2018 and 7,350 in 2021. Native helper/wave/raw-country counts are 12,441 and 7,350. Exactly seven 2018 raw-country rows have all six old payload columns missing; their exact key set is the difference from the public table. They remain excluded. No 2021 row is removed. Formatted native household identities are injective in the held sources; the tested id_walk changes 1,756 distinct 2021 household tokens, with zero resulting parcel collisions. Both public waves have zero duplicate keys, null key cells or household orphans against normal sample().

The private source-to-builder projection follows the same source mask and wave formatter, then the existing id_walk and its attrs. After the unchanged finalizer, every old public key, value, null mask, dtype, axis and logical attribute is exact; the total remains 19,784. The projection adds 4,180 source pairs in 2021 and missing coordinates in 2018. It is a proposal proof, not a candidate implementation or persisted candidate cache. The actual 2018 wave is proposed to retain its original six-column output; missing country-level coordinate columns arise from concatenating the 2021 output.

Senegal supplies the healthy assembly comparator, using six physically copied parquets whose actual current hashes and full two-wave coverage were verified before probes. Its normal Country table has 13,824 rows. Feature(plot_features) for Burkina and Senegal has 33,608 rows, unique canonical keys and zero further loss; each country's full payload is exact under the declared canonical index reorder. A fresh process using a separate physical private data copy returns exact complete Country and Feature frames, including logical attrs, with zero data-file byte/mode/mtime changes.

The scoped phases 1/2 scanner records 13 warnings and no failure/error/skip: six QuantityImplausible warnings on existing area diagnostics, two NullRead warnings, one PopulationHeterogeneity warning, two diagnostics noting API-added `v` beyond the country scheme, and two constant AreaUnit diagnostics. This accounts for the entire finding multiset. plot_features has no phase-2 kwarg grid, so this invocation has zero phase-2 kwarg cases. The scanner does not discover the omitted undeclared coordinate columns. Full warning messages and diagnostics remain private.

The required-coordinate baseline test fails once as expected. Fourteen private source/projection controls pass, including eight deliberate bad projections (zero fill, coordinate-based row dropping, erroneous index alignment, axis swap, area-as-position, wrong 2021 household formatter, extra row and coordinate shift). Twenty-one relevant existing tests pass without skips: plot-axis contracts and four cluster-GPS configuration checks. These are diagnosis controls; the approved implementation will still need actual candidate source/cold/warm/Feature verification and focused production regression tests.

Read-only hash capture measures all 549 baseline country cells and 39 framework tags exact to the immutable f8 inventory. Actual hash-input observations show all 24 Burkina cells reading the proposed country module/scheme inputs. Their anticipated invalidation is therefore 24 Burkina cells, 525 other-country cells unchanged and zero framework changes. This is an observed dependency footprint, not fabricated candidate-content hashes. CONTENTS is excluded from content hashes.

Initial guarded cold attempts were preserved. The newly created checkout lacked DVC's hardwired ignored tmp directory, so Repo initialization was correctly denied before any build. Root verified the ignore rule and prepared only that ignored path as a symlink to owner-only `checkout-dvc-tmp` under this private task root. The guard permits descendants only while that exact realpath binding holds; all other checkout writes remain denied. DVC site state is private too. A second stop distinguished native from already-walked country keys; a later private finalizer-call argument error was completed without another cold rebuild. A fresh physically empty Burkina L2 root provided the final normal cold evidence. No source/cache read bypass, manual stamp or production callable replacement was used.

Freeze proof checks the diagnosis checkout, immutable baseline, prior #1123 writer and old Burkina lane by complete tracked SHA/mode/symlink/head/clean snapshots. Prior #1123 evidence (258 files) is exact. All selected original L1/L2 receipts remain exact, private cold/warm data trees are unchanged after probes, no in-tree parquet or actual outbound attempt exists, and no task process remains. validation.json and freeze-proof.json contain safe counts; full rows, keys, positions, attrs and logs stay under the owner-only private task root.

The exact successful probe commands are below. Preparation and cold diagnosis require an empty private data root; do not replay them over this preserved evidence. A fresh replay needs physical harness/source copies and its own root-approved private DVC metadata target. `run.sh` supplies mounted-image Python, matching cwd/PYTHONPATH/countries/data realpaths, unshare -rn, active socket/write guards, and all numerical/build/Make thread limits. The private scripts use one worker; no auto-sized parallelism is used.

```sh
P=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1058
TASK_DATA_ROOT="$P/data-cold-verified" bash "$P/run.sh" "$P/prepare.py" > "$P/preparation-verified-cold-private.log" 2>&1
bash "$P/run.sh" "$P/diagnose.py" > "$P/diagnosis-verified-private.log" 2>&1
bash "$P/run.sh" "$P/complete_projection.py" > "$P/completed-projection-private.log" 2>&1
bash "$P/run.sh" "$P/hash_scope.py" > "$P/hash-scope-private.log" 2>&1
bash "$P/run.sh" -m pytest -q -p no:cacheprovider "$P/test_baseline_negative.py" > "$P/baseline-negative-private.log" 2>&1
bash "$P/run.sh" -m pytest -q -p no:cacheprovider "$P/test_projection_controls.py" > "$P/proposal-controls-private.log" 2>&1
bash "$P/run.sh" -m pytest -q -p no:cacheprovider tests/test_plot_id_canonical.py tests/test_burkina_faso_cluster_gps.py::test_2018_19_geo_subdf_maps_the_coordonnes_gps_columns tests/test_burkina_faso_cluster_gps.py::test_2014_declares_no_geo_subdf tests/test_burkina_faso_cluster_gps.py::test_no_wave_asks_for_lat_dd_mod tests/test_burkina_faso_cluster_gps.py::test_optional_matches_the_waves_that_lack_gps > "$P/existing-tests-private.log" 2>&1
bash "$P/run.sh" "$P/api_feature.py" initial > "$P/initial-api-feature-private.log" 2>&1
bash "$P/run.sh" bench/feature_audit/scan.py --features plot_features --countries Burkina_Faso Senegal --phases 1,2 --jobs 1 --build-timeout 120 --out "$P/scanner-findings-private.jsonl" > "$P/scanner-private.log" 2>&1
bash "$P/run.sh" "$P/prepare_warm.py" > "$P/preparation-warm-private.log" 2>&1
TASK_DATA_ROOT="$P/data-warm" bash "$P/run.sh" "$P/api_feature.py" warm > "$P/warm-api-feature-private.log" 2>&1
bash "$P/run.sh" "$P/finish.py" > "$P/final-proof-private.log" 2>&1
```

The baseline negative command intentionally exits 1. The originally executed diagnose.py reached the finalizer-argument harness error after completing the successful cold source builds; complete_projection.py completed those already-built frames without rebuilding. The private diagnose.py now contains the corrected argument for a fresh replay. Preserved failure logs distinguish that correction, the native/country identity proof stop, the initial source-location gate, and the original DVC tmp denial.

Sue.
