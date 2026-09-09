EPAR bounds its own section-12 handling to W1-W3, and states a reason. `EPAR_UW_Ethiopia_ESS_W4.do:759-760` (LSMS-Agricultural-Indicators-Code @ `abfbdc9301242527b7543606aad1ad6eed2e6530`, `Ethiopia ESS/Ethiopia ESS Wave 4/`), directly under the "Crop Values" banner:

```
* Section 11 includes perm crops
use "${Ethiopia_ESS_W4_temp_data}/sect11_ph_w4.dta", clear
```

W4's gross-crop-revenue block (`:1019-1033`) reads `sect11_ph_W4` only, and the women's sales-decision block that would have used section 12 is commented out with `/* no section on perm crops, trees and roots include in previous section` (`:3687`; the W5 twin at `EPAR_UW_Ethiopia_ESS_W5.do:4081`). That is EPAR's reading of the instrument, not a questionnaire check -- I did not open the ESS4/ESS5 questionnaires.

Consistent on our side: `find lsms_library/countries/Ethiopia -name '*sect12*'` returns `sect12_ph_w{1,2,3}.dta.dvc` for 2011-12/2013-14/2015-16 and no `sect12_ph` file for 2018-19 or 2021-22. What those two waves carry is `sect12a/b1/b2_hh_w4`, `sect12a..f_hh_w5` and `sect12_com_w5` -- household and community modules, not the post-harvest tree-crop section. Corroboration, not proof.

None of this touches the measurement in the issue body. 2015-16 is a W3 wave, ships `sect12_ph_w3.dta`, and the 2,627-versus-1,984 sale count stands as filed.

What it does bound is the fix. `Ethiopia/2018-19/_/crop_production.py` reads `sect9_ph_w4` for harvest (`:22`) and `sect11_ph_w4` for sale (`:25-26`); `Ethiopia/2021-22/_/crop_production.py` does the same with the `_w5` files (`:21`, `:24-25`). Neither reads any section-12 file, so the wiring proposed here applies to W1-W3. It overlaps #852, which wires the shipped WB crop conversion tables into `harvest_kg` across four Ethiopia waves, on these two scripts. Whether W4/W5 already pick up the tree, fruit and root crops through section 11 is a crop-code comparison against the pre-2018-19 section-11 header, and it has not been run.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
