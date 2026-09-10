EPAR ships a TLU coefficient table in all five ISA countries, not two, and it differs from ours on exactly one species. Paths below are relative to `reference/epar/LSMS-Agricultural-Indicators-Code/` at HEAD `abfbdc9301242527b7543606aad1ad6eed2e6530`; 27 do-files define `tlu_coefficient`.

All 27 set cattle to 0.5: Malawi `Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:1514-1518`, Nigeria `Nigeria GHS/Nigeria GHS Wave 4/EPAR_UW_Nigeria_GHS_W4.do:1851-1856` (generated as `tlu`, renamed at `:1858`), Tanzania `Tanzania NPS/Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:991-995`, Ethiopia `Ethiopia ESS/Ethiopia ESS Wave 5/EPAR_UW_Ethiopia_ESS_W5.do:2241-2247`, Uganda `Uganda UNPS/Uganda UNPS Wave 5/EPAR_UW_Uganda_UNPS_W5.do:1525-1526`, `:1535-1536`, `:1545`.

Everywhere else the two tables agree. Sheep and goats 0.1, pigs 0.2, poultry 0.01 -- identical to `_TLU_FACTORS` (`transformations.py:2612-2623`) in all five countries. Equines and camels vary by country and code (Malawi 0.3 on code 305; Uganda 0.5 for horses and 0.3 for donkeys and mules; Ethiopia 0.7, 0.5, 0.6 and 0.3 on codes 9, 13, 14, 15).

That bears on the roster-recovered ERHS table in the 2026-08-25 comment above. EPAR's independent 0.1 for sheep and goats corroborates ours against ERHS's 0.2, so "small ruminants are off by exactly 2x" is one table against two rather than a correction of ours. Cattle is the only line where EPAR sits on the other side.

On sourcing: no EPAR do-file or wave readme cites an authority for 0.5. A case-insensitive grep for `jahnke`, `FAO/ILCA` and `250 kg` across every `.do` and `readme.md` in that repo returns nothing, and Malawi W1's readme TLU section (`:93-97`) records "Known Issues: None". Ours states its convention in the comment above the dict: "1 TLU = one 250 kg adult bovine, the FAO/ILCA convention widely used in LSMS-ISA work (e.g. Jahnke 1982; the World Bank 'Livestock data innovation' tables)" (`transformations.py:2608-2610`).

So this pass adds no third microdata source to adjudicate 0.5 against 0.70. Both are desk tables; only ours says where its number came from, and the ERHS 0.83 remains the only one of the three recoverable from a roster. The decision left is a sourcing decision, and EPAR's number is not evidence against 0.70 -- it is one more unsourced constant, shipped in 27 files.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
