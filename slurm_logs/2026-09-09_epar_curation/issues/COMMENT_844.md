EPAR disabled its `sa3q18` valuation in the crop-revenue construction and reads the question live somewhere else. Both facts matter before `Value_if_sold` lands. Paths below are relative to `reference/epar/LSMS-Agricultural-Indicators-Code/` at HEAD `abfbdc9301242527b7543606aad1ad6eed2e6530`.

`Nigeria GHS/Nigeria GHS Wave 2/EPAR_UW_Nigeria_GHS_W2.do:644-685` is a single `/* ... */` block. It opens at `:644`; `:645-647` are

```
ren sa3q18 value_harvest
gen val_unit = value_harvest/quantity_harvested
gen val_kg = value_harvest/quant_harv_kg
```

and it closes at `:685`. The entire alternative per-unit and per-kg valuation from `sa3q18` is disabled, with no comment giving a reason. Immediately after (`:686`, `//We're going to prefer observed prices first`) the live code builds `price_unit` / `price_kg` from actual SALE transactions (`sa3q11a/b`, `sa3q12`, `sa3q16a/b`, `sa3q17`; the collapses are at `:499-534`), ladders them by geography, and computes `value_harvest = price_unit * quantity_harvested` (`:709-710`).

`sa3q18` is read live in a later section: `ren sa3q18 value_harvested` (`:1944`), collapsed to `price_per_unit = value_harvested / quantity_harvested` by `(hhid, crop_code, unit_cd)` (`:1954-1955`), saved as `Nigeria_GHS_W2_hh_crop_unit_values.dta` (`:1956`). It is merged back at `:1967` into the PROCESSED-crop-sales block and used there as `price_as_input` (`:1970`) -- not into the crop-revenue ladder.

That resolves the loose end in the ladder. `price_unit_hh` at `:707` is never declared in the file because it is Stata's unique abbreviation of `price_unit_hhid`, the household rung of the same collapse (`:503`, merged at `:688`); the household rung is excluded from the ladder loop at `:702`, which iterates `country zone state lga ea` only. Malawi and Nigeria W4 do the same rename explicitly (`Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:1351-1352`, `Nigeria GHS/Nigeria GHS Wave 4/EPAR_UW_Nigeria_GHS_W4.do:866-867`). So in W2 `sa3q18` feeds nothing in the harvest valuation: the household price there is a sale price, not the hypothetical one this issue is about. EPAR's stance moved by W4, which is the "check the later waves" item in the issue body. `EPAR_UW_Nigeria_GHS_W4.do` contains no `sa3q18` at all; it builds `val_harvest_est` from `sa3iq6a`, falling back to `sa3iiiq14` (`:833-834`), and uses it as the last-resort fill for `value_harvest` after the ladder and the household price have both failed (`:873`). Whether `sa3iq6a` is W4's renumbering of the same question is not established here.

Before wiring `sa3q18` as `Value_if_sold`: run the comparison EPAR's abandoned block would have produced -- `sa3q18` against a sale-price-ladder imputation (`price_unit x Quantity`, via `median_price_valuation`) on the same rows -- and report where they diverge. EPAR left no comment saying why theirs was commented out, so the disabled code is not evidence that the approach failed.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
