# Prior-Art Ledger — GH1485 Malawi transform coverage

Base: authoritative GitHub development `c780562b299bb100cdd006037ba424c8ad3264e9`; new isolated branch `fix/1485-malawi-coverage-20261108`. Search tier: git/ripgrep/direct AST floor. No source/API acquisition or shared-cache access is authorized. Failed remote Malawi actor/job41017645 and its question file remain untouched.

## §1 Task and authority

Root transfers the Malawi country lane and selects TLU Option A: retain the existing returned rows and TLU values; expose structured unsupported-species coverage with per-label positive/zero/missing/negative head-count categories, and warn specifically when unsupported positive heads are omitted. No factor, species equivalence, NA-return or raising default is selected. Separately diagnose anonymous `fertilizer_rate(on='parcel')` partial-wave loss against merged1475 and the existing explicit-country contract. Site Q livestock belongs to the Burkina Faso owner and is excluded. An identity ambiguity must produce a precise design/residual, not a guessed dispatch or false whole-issue closure.

## §2 Existing machinery and reuse

| Machinery | Existing role / tests | Decision |
|---|---|---|
| `transformations.tlu`, `_TLU_FACTORS` | Exact lowercase-label factor lookup, head×factor, drop missing contributions, sum(t,i); no pre-existing TLU-specific tests found by full tests/bench ripgrep; new GH1485 controls required | Preserve arithmetic and return identity; extend coverage only |
| `NutrientCoverageWarning`, `_nitrogen_shares`, `_warn_nitrogen_coverage` | Structured result attrs plus dedicated omitted-label warning; nutrient tests | Reuse this diagnostic pattern; species counts have a different head-sign partition |
| `fertilizer_rate`, `nitrogen_kg`, `_level_or_column`, parcel helpers | Existing paired-country validation, per-row literal Malawi/Ethiopia dispatch, both nutrient bases; `test_gh1110_fertilizer_land_keys.py`, `test_gh1160_pooled_fertilizer_rate.py` | Reuse unchanged for diagnosis; no country inference from plot syntax |
| `_waves_joining_nothing`, `PlotGrainMismatchWarning` | Existing yield partial-wave disclosure; GH1460/1545 controls | Assess as a future reusable diagnostic; do not invent routing to recover unknown identity |
| ledgers1110/1160/1475, cross-country skill | 1475 explicitly excluded fertilizer policy;1475 preserves country elsewhere, not this anonymous case | Reconcile rather than re-fix completed row-country behavior |

## §3 Grounding and definitions

Inherit STANDING§§3–4, CLAUDE Data Access/derived-value/grain contracts and canonical data_info.yml. Frozen GH1485 body and ALL comments read from round2 task-context.json, observed2026-11-08T02:11:21Z; zero comments. No factor/default ruling appears there; root's explicit Option A direction is authority for this patch.

The personally FULL-read Malawi record at c964 (including LOGBOOK/WAITING/TODO; prior1475 independent review) is authenticated against the current record. `git diff c964 HEAD -- Malawi/_/CONTENTS.org` contains ONLY the19-line GH1174 nonseed deletion-provenance insertion; all new text read. Current5392lines/369779bytes SHA256 `20736c8bccb4846f1e750780f3b85e468a8d34cb7655609f8b3c4081a113426b`.

Idiosyncrasies and contradictions: the two livestock species-code namespaces produce22 labels, while the current TLU map recognizes only Chicken/Sheep in the documented cohort. HeadCount blank is unanswered, not zero; RaisedOrOwned can preserve a row with every numeric head field blank. The historical species roster totals and1435 corrections are distinct cohorts, not fresh acceptance constants. Malawi later land keys are full `garden_plot`; Area is canonical hectares despite native AreaUnit acres. The country record quotes fertilizer's former “measured no-op” claim and says “2016-17 and 2019-20 vanish with no PlotGrainMismatchWarning” on an anonymous frame. GH1110 fixes labeled/agreed-attrs literal keys; anonymous dispatch deliberately remains legacy. GH1475 expressly did not alter fertilizer behavior. These statements coexist; no stale-document assumption licenses a new routing guess.

## §4 Invariants

- Preserve `_TLU_FACTORS`, custom-map matching, numeric coercion, accepted zero factors, missing contribution deletion, output rows/index/values and input attrs/values. Attach additive coverage metadata; do not reattach dropped unsupported-only households as zero/NA.
- Count unsupported rows BEFORE dropna/grouping; distinguish positive, zero, missing/nonnumeric-coerced and negative heads. Warn only for unsupported positive heads, naming omitted contributions and the attrs contract. A missing converted factor includes custom-map None/NaN; a valid factor0 remains supported. The current pd.NA factor conversion raises TypeError and is preserved, not silently reinterpreted.
- No normalization beyond existing lowercase lookup; no invented livestock factors, species equivalence, clipping, positivity screen, source change, or numerical-baseline refresh.
- If an actual synthetic country-index TLU collision is reproduced, report it as a separate root-cause residual before widening this patch. Existing `(t,i)` output is intentionally retained in this bounded Option A patch.
- Anonymous fertilizer operands cannot establish country from delimiters. Preserve explicit index-before-column country identity, matching scalar attrs as vocabulary metadata only, paired presence/null refusals, existing area sum and numerator definitions.
- Private empty data/config roots, absent credentials, mounted MAIN/exact checkout imports, network namespace, threads/build1 and pytest2/loadfile; source/native/API entrypoints fatal. No data campaign, GitHub or shared-cache writes.

## §5 Reuse / implementation and verification decisions

Extend `tlu` with one dedicated warning class and small coverage tally patterned on nitrogen's existing attrs diagnostic. The new tally is justified by sign/missing partitions specific to head counts; no second TLU estimator is introduced. Test unchanged numerical results against independent direct head×existing-map references, unsupported-only/mixed/empty cases, all four head categories, arbitrary head_col, custom factors including0/NA, animal level/column precedence, duplicate indices and input immutability. Existing baseline tests remain unchanged.

Diagnose fertilizer with invented four-wave full-native-key frames through the ACTUAL transform, both N/product bases, anonymous/current dispatch versus supplied row country, agreed/missing/conflicting attrs and explicit `on='plot'`. Separate genuinely missing/nonpositive area from land-key mismatch. If the method cannot identify the anonymous vocabulary, retain current behavior and surface options: use existing explicit `on='plot'` or restore agreed provenance; optionally add the existing lost-wave warning machinery without claiming recovered rates. A new identity inference/default is not authorized.

## §6 Open questions / remaining gates

No choice is needed for approved TLU Option A. Anonymous land recovery without identity is underdetermined; warning-only disclosure can be proposed independently but is not full GH1485 closure. Site Q is separately owned. Independent review, hash-scope measurement and any full configured CI remain later frozen-head gates; source acceptance is not claimed or required for these invented-data controls. Root owns publication, residual filing and scientific/default rulings.

### Phase 3 — verified bounded result

Runtime freeze `952eb819d5f492aff0c3847062f44d6fb0d31bea` changes only `tlu`, adds `TLUCoverageWarning` and `_tlu_species_coverage`, plus dedicated invented-data tests and Malawi documentation. Actual AST/source proof keeps every other top-level node, `_TLU_FACTORS` and `fertilizer_rate` exact. The tally's `supported_rows` means nonmissing factor lookup before head-value filtering, not a certified herd or count of returned contributions.

- OK (§§2–5):32 new focused controls pass; coverage counts partition each unsupported label's rows by positive/zero/missing/negative head values. Supported zero factors, arbitrary head columns, duplicate/null labels, empty and unsupported-only output, input immutability and current conversion exceptions remain covered. No factor or species equivalence is added.
- OK (§§3–4):100 randomized invented operands execute the EXACT prior c780 `tlu` AST beside the candidate; every typed output and serialized input is exact. This includes existing cross-country pooling, missing/zero/negative/nonfinite heads and custom factor maps, without endorsing the inherited identity issue.
- OK (§§4–5):frozen private/false-credential/unshare pytest2/loadfile selection passes468, skips12 credential/source gates,345.01s. Actual strict `make api-audit` exits0 using the mounted-interpreter adapter, without Poetry installs. Full configured offline CI/GitHub checks are NOT run by this lane.
- OK (§4):reuse of the guarded existing metadata census gives0/450country,0/1895wave,0/35build and0/186import fingerprints changed;0errors/denials,38registeredcountries. Only the pinned tracked Malawi conversion CSV is allowed as config metadata; no microdata/harmonized source, network or native/API build is permitted. No shared re-warm is needed by these measured hashes.
- Fertilizer remains diagnosed/unfixed (§6):14 actual-transform synthetic arms confirm anonymous/one-attr/conflicting-attrs calls silently lose two of four waves on both nutrient bases, while explicit country/agreed provenance or existing plot mode retains all4. Source counts in CONTENTS are historical, not remeasured. `FERTILIZER-DESIGN.md` explains why anonymous recovery is underdetermined and separates an optional reused lost-wave warning from numeric recovery/full issue closure. No fertilizer product edit is selected.
- Separate inherited TLU country pooling reproduced before code and reported immediately:explicit countries sharing(t,i) merge because group_by omits country. `TLU-COUNTRY-RESIDUAL.md` records the root cause. This Option A patch preserves output identity until independently scoped; Site Q remains the Burkina Faso owner's lane.

Failures remain evidence:initial baseline25fail/1pass comprises24 expected missing diagnostic failures and1 new-test oracle error (pd.NA customfactor already raises TypeError). The corrected test uses None for missing converted factors and explicitly protects the legacy pd.NA TypeError; no production conversion was changed. The initial external operator filename shadowed stdlib `operator` before execution and was retained under a non-importable `.txt` name. The first synthetic pooled-TLU assertion compared a literal0.3 with the existing float sum0.1+0.2; corrected to exact prior arithmetic. The first candidate hash launch passed an abbreviated head and refused before any census; full-SHA rerun is authoritative. All native/source/old task artifacts remain untouched.

Evidence: `B/reports/malawi-1485-transform-coverage-20261108/` (`B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103`). All verification actors are terminal before this ledger-only commit. Independent review and configured full CI remain gates; GH1485 as a whole is not closed by this bounded patch. Root owns GitHub/residual filing/rulings.
