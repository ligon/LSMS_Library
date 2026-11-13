From the #569 plot-labor registration follow-up.

**Tier A - Mali casing, implemented and verified; awaiting review/merge.** Mali serves `Family/Hired/Other`; its two EAC-I wave scripts emit lower-case synthetic block codes that `Mali/_/categorical_mapping.org` maps to capitals. The bounded fix changes only those three Preferred Labels to `family/hired/other`, with a matching schema comment and tests. It changes no labor-source meaning or row eligibility.

Private held-source cold/warm diagnosis at `f8d236694` and Mali `aba2badfe` is exact: 52,057 Mali rows (14,843 / 37,214 by wave); Family 33,655, Hired 9,565, Other 8,837. A fresh current-hash Niger comparator contributes 53,041 rows. `Feature('plot_labor')(['Mali','Niger'])` preserves all 105,098 rows but has six source groups split by case. Exact lower-case family/hired Country transforms currently raise KeyError on Mali. Actual cold and fresh-process warm complete frames equal the one-to-one source-case projection with every non-source value/key/dtype/null mask and attribute preserved. All 335 focused tests pass, zero skips; the ten new regressions fail 7/pass 3 at baseline.

Independent review and merge are still required before marking this Tier A part resolved. No #822 implementation dependency is needed: all relevant source drivers, mapping table and labor helper bodies are identical to f8, and complete actual labor outputs/attrs are exact across the two heads. The actual full hash census changes only the 24 Mali pairs; the other 525 country/table pairs and all 39 tagged fingerprints remain unchanged.

**Remaining Tier C - cross-country `harmonize_labor_source` design.** GhanaSPS retains seven instrument-specific values (`casual`, `communal`, `family`, `hired`, `other`, `permanent`, `self`); GhanaLSS retains `exchange/hired`. Decide and document a shared taxonomy or explicit containment semantics before changing any of them. Mali's case-only repair is not authorization to fold Ghana categories, alter stage/worker/season grain, or rewrite labor aggregation/valuation. Keep #1121 open for this Tier C scope after the Mali part lands.

Source evidence and full logs/frames remain private; only aggregate diagnosis is exported.

Sue
