EPAR ships a second repository relevant to this ticket's non-ISA extension: `Household-Consumption-Data` (HEAD `0e9ccf2981e7cb8448605925a5111d2835bf1e31`, per `reference/epar/NOTES.md`), one do-file per country-wave computing the value of food consumption by source.

Its README names sixteen countries: "Benin, Burkina Faso, Cote d'Ivoire, Ethiopia, Ghana, Guinea Bissau, Kenya, Malawi, Mali, Niger, Nigeria, Senegal, Sierra Leone, Tanzania, Togo, and Uganda". Two things the README's own table does not match:

- **Every Tier-1 EHCVM country in this issue is covered**: Benin (W1, W2), Burkina Faso (W2, W3), Cote d'Ivoire (W1, W2), Guinea-Bissau (W1, W2), Senegal (W1, W2), Togo (W1).
- **Four countries outside Tier 1 are also covered**: GhanaSPS (as `GhanaW1`), Mali (W1, W2), Niger (W3, W4), and India (`IndiaW2_Food Consumption by source.do`) -- India appears in neither the README's country sentence nor its data-source table, so treat its provenance as unconfirmed.

The wave counts above come from the do-file inventory, which is wider than the README table in several rows (the table lists one EHCVM round for Cote d'Ivoire and Niger where two do-files exist).

The shared `crop_category1` / `crop_category2` crosswalk is the candidate input for this issue's EHCVM Aggregate-labels work. The second stage is country-invariant: `UgandaW4_Food Consumption by source.do:888-922` and `NigeriaW4_Food Consumption by source.do:972-1006` are byte-identical -- one `gen` plus 34 `replace` lines folding roughly thirty `crop_category1` values into sixteen `crop_category2` buckets. The first stage is country-specific and runs at raw-item read time (`UgandaW4...do:202-283`, 28 distinct `crop_category1` values; Nigeria W4 and Senegal W1 have 30 each).

It needs the #833 screen before use, not after. #833 is "harmonize_* folds raw and processed into one `j` in 5 countries" (Malawi groundnut 4.99x, Uganda maize 3.33x, both milling or shelling), and this crosswalk has the same shape of risk: it assigns a bucket at read time, before any per-item row survives, so a raw/processed conflation baked into `crop_category1` cannot be un-folded later the way our `labels='Aggregate'` query-time transform can (`add-feature/food-acquired/aggregate-labels/SKILL.md`; `food_acquired` keeps `j` at survey grain always). Screen each proposed fold against #833's raw/processed list crop by crop; do not import the crosswalk wholesale.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
