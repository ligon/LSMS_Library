PASS for the combined nine-fix integration at `483b4aa117c2c05ac00bf023adab97e47782df46`. All requested Country frames and attrs match their frozen corrected review references exactly. The combined Feature frames, complete country parts and attrs also match. A fresh Python process repeats every call without changing any parquet. No production discrepancy was found.

Independent verification by Sue, 2026-10-21. Runtime checkout: `/local/job40255479/worktrees/isa-integration`. Private evidence/cache: this directory. No payload from the separate running cohort's mutable `integration/data` cache was read, copied or written.

| Normal Country request | Rows | Frozen corrected reference | Complete comparison |
|---|---:|---|---|
| Ethiopia.plot_inputs() | 180,900 | review-1118-Ethiopia candidate cold | Exact; 37,370 Unknown units |
| Tanzania.plot_inputs() | 9,698 | review-1118-Tanzania candidate cold | Exact; 408 Unknown units; all 19 no-amount rows |
| Malawi.plot_inputs() | 180,184 | review-1109-v2 fixed cold | Exact, including reviewed organic amounts, units and Applied |
| Uganda.plot_inputs() | 84,216 | review-1118-Tanzania healthy Uganda comparator | Exact |
| Uganda.food_acquired() | 355,571 | review-1077 candidate default | Exact |
| Uganda.food_acquired(labels={'u':'Code'}) | 355,571 | review-1077 candidate Code | Exact |
| Mali.community_prices() | 155,931 | review-822 candidate cold served | Exact; all native reading slots |
| Niger.community_prices() | 46,043 | review-1088 candidate cold | Exact; reviewed quantity missing-code correction |
| Nigeria.food_acquired() | 595,725 | review-1144 after-data, including reviewed #1034 runtime | Exact; source-backed quarter item vocabulary |

"Exact" means `assert_frame_equal(check_exact=True, check_index_type=True, check_column_type=True)` plus complete attrs equality: every value and missingness mask, key, multiplicity, index/column order and dtype was checked. Row counts are supplementary. No unit/item vocabulary, numeric value, attr or serialized path was normalized to make a comparison pass. The nine direct reference files were physically copied, SHA256 pinned and checked unchanged at the end; exact source paths/digests are in the private reference manifest.

Malawi retains 11,075 organic rows: 11,068 positive quantities and seven missing quantities, all Applied=True. Units remain 5,518 Kilogramme, 2,155 Ox-Cart, 1,451 Bucket, 1,204 Other (Specify), 733 Wheelbarrow, eight Unknown, five `Organic unit code 1` and one `Organic unit code 3`. Those six source-qualified raw identities do not acquire an invented kilogram meaning. The complete nonorganic part, including the separately documented 437-row retention residual, is conserved by the full-frame comparison. Tanzania's three missing-first/known-later groups and six mixed-known-unit groups are likewise preserved by exact whole-frame equality; the latter remain the #637 residual. Nigeria retains the reviewed native-name counts: Groundnuts 2,839, Pepper 10,446 and Maize (shelled) 1,425. This check does not reopen any source adjudication or infer container weights.

| Normal Feature request | Rows | Evidence |
|---|---:|---|
| plot_inputs: Ethiopia, Tanzania, Malawi, Uganda | 454,998 | Entire frame/all four country parts exact; complete attrs equal the union of the three frozen corrected pairwise Features |
| community_prices: Mali, Niger | 201,974 | Entire frame/all parts and declared metadata carriers exact to frozen Country references |
| food_acquired: Nigeria, Uganda | 951,296 | Entire frame/all parts and declared metadata carriers exact to frozen Country references |
| food_acquired: Uganda, labels={'u':'Code'}, currency=None | 355,571 | Entire frame and attrs exact to the frozen Code Country reference with the country index prepended |

The first three use Feature's default `currency='index'`; expected frames apply the existing currency attachment, declared index aliases, sentinels and metadata carriers to the frozen Country references. The four-country plot Feature retains promoted-unit fill counts Ethiopia=0, Tanzania=0 and Uganda=0, with `unknown_season` fabricated for Ethiopia, Tanzania and Malawi. Its complete metadata is also independently compared with the full attrs union of the reviewed Ethiopia/Uganda, Tanzania/Uganda and Malawi/Uganda Feature outputs. The earlier Ethiopia/Tanzania unit-promotion counts 37,370/408 are already zero in those corrected references; no further alignment delta occurs after integration. Native Mali obs=1/2/3 and the distinct Niger variety/visit/obs axes remain intact. Requested Code unit vocabulary is compared with the reviewed Code output, not with Preferred labels.

One reviewer-only expectation error is preserved in the initial log: the first harness cleared expected attrs before attaching carriers, incorrectly discarding pandas' inherited `country` and `id_converted` attrs for a single-country Feature. The original public Code Feature already matched the frozen Code Country reference, including both attrs. A direct complete comparison of that saved original output passed; the expected-frame construction was corrected and the fresh-process run passed all four Features. No production code/data was changed. See [initial-comparison-correction.json](initial-comparison-correction.json); the original initial result/log remains unchanged, including its historical NEEDS_FIX marker from that harness assertion. [final-proof.json](final-proof.json) records the reconciled PASS.

Preparation physically copied all 1,519 files in the quiescent release manifest (341,221,428 bytes), then 1,113 already held raw L1 blobs (3,245,387,849 bytes) from completed private review roots. L1 keys were independently checked against the integration sidecars, using byte MD5 or the established legacy DVC md5-dos2unix digest for text where required. Source/destination SHA256 and distinct physical inodes were verified. No shared cache, source acquisition or DVC CLI was used; absent/unheld sources were not fetched. The original release files, all copied raw blobs and their completed-review source copies retain their recorded hashes.

The initial normal API pass starts from those release parquets, not an empty whole-country cache. All eight distinct requested country/table caches initially grade stale; the second Uganda unit-vocabulary request naturally sees the fresh food cache built by its preceding default call. Ordinary calls rebuild to fresh current hashes with exact all-wave manifests, without assume_cache_fresh, trust bypasses, LSMS_NO_CACHE, backend overrides or manual stamps. The five supporting sample caches for Ethiopia, Tanzania, Malawi, Uganda and Nigeria also end with fresh hashes and exact all-wave coverage. The initial pass changed 77 existing private parquets, with 1,519 present before and after and none deleted; its full verification workload took 258.558s. This is a normal stale-cache rebuild proof, not a blanket empty-cache claim for unrelated tables.

A separate process then repeats all nine Country and four Feature requests. All targets are fresh before and after, all complete frames/attrs equal both their individual references and the initial public outputs, and all requested coverage manifests remain exact. The warm verification workload takes 45.927s. SHA256 plus mtime_ns checks across every private parquet show **zero changes, creations or deletions among all 1,519 files**. Full warnings are captured privately without printing records or example keys; known quantity, null-key, cluster and population caveats remain visible.

Relevant country CONTENTS sections were read before runtime work, including live TODO/WAITING/LOGBOOK entries. The decisions that matter here are:

- Ethiopia (`CONTENTS.org:1313`): three input-source grains, unique-plot attachment of holder/crop seed purchases, and use-only organic/pesticide records. The #1183/#1184 missing-purchase-answer and all-NA-sum caveats remain separate; no False/zero payload or retention decision changes.
- Tanzania (`CONTENTS.org:1798` and live WAITING #114 at line 1087): long-rainy-season agriculture only in the two modern waves; total seed unit differs from purchase unit; duplicate reduction precedes Unknown filling. UPHI back-casting, disjoint subpanels and the population statement gap are retained. The quoted old claim "u is meaningless where there is no quantity" is explicitly identified as contrary to #1041 and already removed by the reviewed fix.
- Malawi (`CONTENTS.org:3018`): the statement "Module D records no organic quantity" is explicitly corrected as false. Recorded ag_d37a/b amounts/units, optional Applied and source-qualified code-1/code-3 fallbacks are the accepted boundary. Other (Specify), inorganic-unit doubts and the 437-row retention residual are not reinterpreted.
- Uganda (`CONTENTS.org:826`): build decoding uses Code -> Preferred Label, while per-wave columns can participate at query time. Unknown for reported quantities remains distinct from value-only Value; accepted raw code residuals and off-map drops remain documented. The old blanket "unused at runtime" claim has the reviewed build/query qualification. EA/parish/GPS identity transitions and live panel-weight, crop-collapse, sales-rescaling and nonfood recall caveats remain unresolved.
- Mali (`CONTENTS.org:1248`): passage-2 readings 1/2 lack native value-label dictionaries; the accepted replacement is the common community-grid dictionary, not household-food units. The old "consolidated" reading-3 description is explicitly contradicted by the native three-slot design. Quantity is the lot's Poids (KG), while Price is the lot price.
- Niger (`CONTENTS.org:1221`): variety is a code because labels drift; source duplicate-entry policy leaves 30 nonrepeat obs=4-6 records. Quantity masking recognizes the exact promoted float32 9999.9 representation without extending the Price filter. Missing-unit/product-absent price rows retain their existing policy, and the unwired later community module remains outside scope.
- Nigeria (`CONTENTS.org:6`): W2 food meanings differ between the two quarters, and the old quarter table was source-contradicted. Native names do not gain unrecorded specificity; W1 stated card weights remain quantity-aware; W5 food is unsupported. The nonfood WAITING "Recall windows are POOLED, and no column records it" remains a caveat.

No new contradiction with those accepted source decisions was reproduced. Historical statements explicitly corrected by their later dated notes are not treated as current rules.

Executed commands, with all API output redirected to private logs:

```sh
cd /local/job40255479/worktrees/isa-integration
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  PYTHONDONTWRITEBYTECODE=1 /local/job40255479/venv_img/bin/python \
  /local/job40255479/control/isa-backlog-20261021/integration/api-nine-review/prepare_private.py
bash /local/job40255479/control/isa-backlog-20261021/integration/api-nine-review/run.sh initial
bash /local/job40255479/control/isa-backlog-20261021/integration/api-nine-review/run.sh warm
bash /local/job40255479/control/isa-backlog-20261021/integration/api-nine-review/run.sh finish
```

Preparation and initial mode are setup-only for this populated evidence root; reproduce them in a new private root with physical copies and updated reviewer-script OUT locations. The runner pins exact integration cwd/PYTHONPATH/LSMS_COUNTRIES_ROOT and private LSMS_DATA_DIR, mounted image Python, numerical/build/make threads=1, PYTHONDONTWRITEBYTECODE and private issue logs. It runs under `unshare -rn` and asserts active connect/connect_ex guards without replacing the socket class. Realpath assertions guard package/config identity; no API requests use freshness bypasses. Warm mode is directly repeatable and asserts parquet bytes/mtimes unchanged.

Final immutable proof: HEAD remains `483b4aa117c2c05ac00bf023adab97e47782df46`, git status is empty, and all 14,977 tracked file contents/path names equal the initial manifest. Its canonical manifest SHA256 is `4c701479ab816779ec511755bb45ad012c30c7cdae8d153a309fcb51ecefec4d`. No in-tree raw Stata or parquet file exists. No tracked edit, ref mutation, dependency/fixture/golden change, GitHub write, shared-cache write, acquisition or cache promotion occurred. [final-proof.json](final-proof.json), [warm-result.json](warm-result.json), [initial-comparison-correction.json](initial-comparison-correction.json) and the private manifests/runners make the result reproducible.

Limits: this verifies the specified combined Country/Feature behavior of the nine-fix cohort. It does not repeat the independent source-oracle studies, FCT/nutrition/income routing checks, full test suite, complete scanner phases or all-549 hash census already covered separately. It does not diagnose or repair Tanzania mixed-unit groups, Malawi's nonorganic residuals, unheld questionnaires, Uganda source decisions or later-wave Nigeria specificity. Existing residuals remain part of the exact reference outputs.

-- Sue
