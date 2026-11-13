# Partial GH218 narrowing draft (Tier A cleanup)

Proposed remaining title: Remove remaining retired derived-food build references outside Malawi.

Resolved in this Malawi-only change: remove the two null food_expenditures and food_prices raw Data Scheme registrations. Country retains expenditures/prices/quantities in their existing order; Feature discovers each through food_acquired. Add the existing-loader-based raw-derived-registration guard, after confirming Malawi was the only offender. This is not retirement of the public products. Independent review and public PR remain pending.

Keep GH218 OPEN for the original remaining items: Togo/CotedIvoire/Afghanistan grouped Make prerequisites referring to the retired food_prices_quantities_and_expenditures.py; country.py allowlist/comment cleanup with its separate fingerprint assessment; shared food skill/AGENTS references; the stale gitignore unit_labels.csv exception; a missing-script Make guard only after its out-of-scope offenders are fixed; and the separate remaining phase3 countries GhanaSPS/Guatemala/Panama/Serbia. No shared/framework/Make cleanup was performed here. The new raw-registration guard is only the corresponding half of original item6.

# Separate Malawi Make residual (Tier B diagnosis-first)

Proposed title: Malawi Make food_acquired still invokes a pre-canonical country concatenator.

Measured on actual release f8 and the two-registration cleanup, independently source-built private roots: Default food_acquired returns971531 rows over five supported waves; the Make backend raises RuntimeError for all waves and the supported2013-14 singleton. Its three derived food callers also raise RuntimeError in populated native-wave state. The source traceback contains KeyError: 'units': country _/food_acquired.py renames old u_consumed to units, then reads units, while all five current wave scripts produce Quantity/Expenditure/Quantity_kg at(t,i,j,u,s). The explicit country var target invokes that obsolete shape; full native wave bytes/frames are preserved. No result is an empty payload.

Diagnose callers and the framework-authoritative retirement boundary before a separate fix; retain wave scripts/targets, grain, sample joins, units and science. Do not expand GH218's two-key patch or invent another concatenator. Live all-state title/body search found GH218 as the only open match, plus historic closed213/878 and cache issues; this is not a complete ownership ruling, so dispatcher dedup/routing remains required before filing.

# Separate invalid-wave cache side effect (Tier B framework diagnosis-first)

Proposed title: Invalid Malawi wave request evicts five valid hashless food wave companions before KeyError.

On both exact source heads, a current all-five-wave Country cache serves correctly. Calling food_acquired(waves=['1900-00']) raises KeyError and deletes the five script-written food wave companions, while the country parquet is preserved. Complete pre/post receipts survive. Existing Make native parquets bind byte-for-byte to all ten pre-invalid Default receipts, so native preservation proof required no rebuild/replay. Ordinary fresh-process all/supported reads then serve unchanged country data with zero cache writes.

This is measured only for this invalid Malawi request, not a corpus-wide output claim. Inspect validation versus wave-rebuild/eviction ordering in a separate framework diagnosis; do not change eviction/read gates here. Live all-state search for invalid wave/cache/evict returned no match, but dispatcher dedup is still required before assigning an owner.

Sue.
