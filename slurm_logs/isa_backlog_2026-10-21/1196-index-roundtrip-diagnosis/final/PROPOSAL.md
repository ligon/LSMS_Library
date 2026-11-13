# Proposed bounded GH1196 implementation

Diagnosis only; no production/test/config patch or Git mutation was made.

Restore the recorded pandas string missing-value policy on reconstructed **index fields** at the generic parquet-reader boundary in `local_tools.get_dataframe`. Use the actual Arrow pandas metadata for each field named by `index_columns`, rather than assigning one dtype to all string fields or guessing from a country/table/wave. The observed metadata differentiates `numpy_type="string"` (pd.NA) from `"str"` (NaN). Keep the actual reader's Arrow storage and existing values/null masks. Preserve the complete reconstructed Index/MultiIndex names, levels, codes, code dtypes, ordering and unused reconstructed levels. Range descriptors, unknown/missing metadata, nonstring axes, data columns and existing parser failures must retain their existing behavior. Missing or malformed metadata must not be repaired by a blanket cast or a broad exception handler.

Exact implementation choices still need writer authorization and review, especially the field-to-logical-index mapping for unnamed/duplicate logical names and mixed string policies in one index. A single Arrow `types_mapper` keyed only by physical type cannot distinguish differently declared string fields and is not sufficient. Do not infer Python-vs-Arrow storage from metadata that did not retain it; the proven target is the na_value difference on the existing Arrow-backed cases.

Expected product boundary: `local_tools.py` reader/helper only; a new focused regression file; task ledger; dated Niger/Uganda CONTENTS notes. No country decode/ID/unit changes, writer coercion/JSON change, finalizer/grain/reducer change, dependency update, new schema flag, freshness exclusion or cache-hash lever. Root must approve the concrete patch before editing.

Alternatives:

- An upstream Arrow field-aware index fix addresses the underlying mechanism, but changing the installed image/dependency is outside this task and needs independent compatibility review.
- A country hook or finalizer-wide cast would hide the error without respecting per-field recorded policy; it would miss direct generic reads and other axes.
- Patching only the excluded Country country-cache gate has a small invalidation footprint but misses native-wave/script/direct reads. The footprint is not a reason to select an incomplete boundary.
- The route limitation is concrete for this issue: neither target declares materialize:make. Default's current-country hit enters that gate; Make takes load_from_waves/YAML Wave gates and bypasses it. Four immutable Uganda1198 Make endpoints (both heads, cold/warm) record4805rows and v string->str, so a Country-gate-only repair misses an actually observed case. Niger Make food has only source-route evidence, not a saved/new Make runtime measurement in this diagnosis. ROUTE-COVERAGE.md provides the exact source and operand pins; no new API was run for this addition.
- Changing `to_parquet` to return a reread frame would make cold agree with warm by discarding the recorded policy. It also broadens writer attrs and structural behavior and is not proposed.
- New writer metadata is unnecessary for these two retained `numpy_type="string"` records. No schema bump or manual stamping is justified by the present diagnosis.

The no-op source-marker simulation predicts that an ordinary generic-reader or writer edit moves 549/549 country API hashes,39/39 per-table framework fingerprints and1,362/1,362 declared-wave hashes; an excluded country-read-gate marker moves0. These are measured **counterfactual fingerprint predictions**, not post-fix hashes. The future writer must independently measure its actual complete549/39/relevant-wave inventory before builds and stop on unexpected changes. No other-country rebuilt-output claim follows from this diagnosis.

Regression contract:

1. Preserve the eight private failing tests: both writer branches, plain and MultiIndex, complete and missing-bearing Arrow-string indexes, duplicate labels, genuine missing values and literal `None`/`nan`/`<NA>`. Their data-column mirror distinguishes index reconstruction from ordinary extension-column reconstruction. Add mixed declared `string`/`str` axes, unnamed axes and malformed/absent field-metadata controls during authorized implementation.
2. Keep the positive already-NaN string-index return exact. The empty pd.NA-index control currently fails too. Preserve all values/masks, data-column dtypes, complete axes and recursively typed logical attrs at matching stages with check_exact=True and rtol=atol=0.
3. The existing writer reset/set step prunes unused source MultiIndex levels before Arrow. Preserve and explicitly test that existing prepared-frame boundary; do not broaden this fix to recover information already absent before Arrow. Reconstructed unused levels/codes must not be pruned by the new reader helper.
4. Keep all seven selected GH645 controls and both partial-write atomicity controls. The existing writer JSON conversion of public immutable records is a separate serialization contract, not an expected typed-attrs equality across API and serialized stages.
5. Use actual ordinary source-cold builds followed by fresh-process/new-object warm reads for Niger food_acquired and Uganda cluster_features on the new candidate. Existing saved cold operands and this diagnosis's new held-compatible warm APIs do not replace that future source-backed acceptance. Complete native/raw/country/public values, axes, masks, dtypes, full typed attrs, source gates, current hashes/full coverage, complete warnings and zero-write warm endpoints must be retained. Both original real cases are full MultiIndex frames; u/v are their extracted pandas.Index levels.

Sue
