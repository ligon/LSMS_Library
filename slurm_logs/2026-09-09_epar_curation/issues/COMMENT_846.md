Corroboration for the `not-asked` verdict, and one distinction to keep separate from it: EPAR does produce a Tanzania own-consumption value, but only by imputing one, because -- as this issue's C4 establishes -- nothing was asked.

`Tanzania_W5_Food Consumption by source.do:341` (Household-Consumption-Data @ `0e9ccf2981e7cb8448605925a5111d2835bf1e31`) opens the loop over `consu prod gift` with

```
gen food_`f'_value=food_`f'_qty*price_unit if food_`f'_unit==food_purch_unit
```

-- the household's own PURCHASE unit price, and only where the consumed unit equals the purchased unit. It then fills narrow to broad at ea / ward / district / region / country (`:348`, `:354`, `:360`, `:366`, `:372`), each gated `obs_X>10 & obs_X!=.`, i.e. N>=11. (The section comments in that file say "at least 10 observations"; the code requires 11. The Ag repo's crop-price ladders gate `obs_X>9`, N>=10.) Every rung is a purchased-item price. Ethiopia (`EthiopiaW5_Food Consumption by source.do:423`) and Uganda (`UgandaW4_Food Consumption by source.do:693`) open identically.

So EPAR's construction is proof by necessity, not by choice: there is no own-consumption price question to read, which is what C4 found in Section J. Had one been asked, EPAR would have used it directly, as it does for purchases.

What this does not settle, and should not be folded into the capability record: whether the library should also impute an own-consumption value via `median_price_valuation` (per #585) is a separate, analyst-facing decision. The `not-asked` verdict closes the coverage-matrix cell; it says nothing about whether a `valuation=` kwarg should later synthesize one for Tanzania the way EPAR's do-file does. Keeping the two in separate issues stops closing this one from reading as licence to auto-impute by default.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
