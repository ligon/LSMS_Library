**PROJECT : YALE EGC-ISSER PANEL***************************************************************************************************
**AUTHOR : MANAVI SHARMA
**PURPOSE : CLEAN UP THE CONSUMPTION SECTION AND CREATE A METRIC OF AGGREGATE CONSUMPTION EXPENDITURE

**DATA: Uses from S11 of the EGC-ISSER Panel and saves in 'Papers on Yale Panel'
**Also saves the aggregates in the public folder in Wilson Data

*12/10 - MANAVI SHARMA EDITS : creating a composite file with expenditures from the education and health sections as well

/*
Structure of this do file 
	Appending the S11 datasets
	Cleaning the data
	Imputing missing price values from the community data
	Imputing missing price values from within the data
	Generating aggregates
	Creating consumption datasets
*/	

************************************************************************************************************************************

if "`c(username)'"=="cru2" {
                local dropbox "c:\Users\cru2\Dropbox\Research\EGC-ISSER Panel"
}

else if "`c(username)'"=="ManaviSharma" {
                local dropbox "c:\Users\ManaviSharma\Dropbox"
				local dropbox "c:\Users\Manavi\Dropbox"
}
global dropbox `dropbox'

//program to keep only one aggregated observation per HH
capture program drop keep_one
program define keep_one
	bys hhno `1': gen i_`1' = _n
	replace i_`1'=. if i_`1'!=1
	replace `1'=. if i_`1'==.
	drop i_`1'
end

capture program drop count_5
program define count_5
loc m b c e
foreach x of varlist `1' `2' `3' {
	loc `4' = ``4'' + 1
	loc s : word ``4'' of `m'
	egen count_quantity_`x' = concat(`5' itname unit `x'), punct(-)
	*replace count_quantity_`x'=""  if `5'==. | itname=="" | unit==. | `x'==.
	egen temp = rownonmiss(`5' itname unit `x'), strok 
	bys count_quantity_`x': gen number_`x' = _N if temp==4
	drop count_quantity_`x' temp
}
end

************************************************************************************************************************************

******************************
* APPENDING THE S11 DATASETS *
******************************

cd "${dropbox}\Wilson DATA\EGC-ISSER Wave 1 Final Data\Private\Cleaned Data\Consumption Data (S11)"
loc files : dir . files "*.dta"
foreach F of loc files {
	use "`F'", clear 
	sort hhno
	save "`F'", replace 
}
use s11a, clear
append using s11b s11c s11d

cd "${dropbox}\Papers on Yale Panel\data\consumption expenditure"

**********************************************************************************************************************************

***************************************************************************
* CLEANING UP THE DATASETS - GENERATING PRICE VARIABLES AND ADDING LABELS *
***************************************************************************

*Combining the cedi and pesewa columns 
gen uid = _n
foreach x of varlist s11a_*iii s11b*_2 s11c_2 s11d_b s11d_d s11d_f {
	qui replace `x'=`x'/100 //pesewa is 1/100th of a cedi
}
//s11a
loc m b c d e
foreach x of varlist s11a_bii s11a_cii s11a_dii s11a_eii  {
	loc j = `j' + 1
	loc s : word `j' of `m'
	egen s11a`s'= rowtotal(`x' `x'i)
}

//s11b
loc m a b c d e 
foreach x of varlist s11b*1 {
	loc k = `k' + 1
	loc s: word `k' of `m'
	egen s11b`s'= rowtotal(`x' s11b`s'_2)
}	
//s11c
egen s11c= rowtotal(s11c_1 s11c_2)
//s11d
loc m b d f
foreach x of varlist s11d_a s11d_c s11d_e {
	loc p = `p' + 1
	loc s : word `p' of `m'
	egen s11d`s'= rowtotal(`x' s11d_`s')
}
//s11a*, s11db is monthly 
//keeping the id & aggregated variables 
keep id1 id2 id3 id4 hhno food_id itname s11a_bi s11ab s11a_ci s11ac s11a_di ///
s11ad s11a_ei s11ae s11a_f item_id itemname s11ba s11bb s11bc s11bd s11be s11c ///
fuel_id s11d_1 s11db s11dd s11df uid

ren s11a_bi Qbi
ren s11a_ci Qci
ren s11a_di Qdi
ren s11a_ei Qei
ren s11a_f unit
ren s11d_1 months_used 

recode unit (-8=.)

*Attaching labels
loc labels own_produce purchase gift_rcvd gift_given children elderly male_adults female_adults total_SB other_items avg_value produced purchased
foreach x of varlist s11* {
	loc c = `c' + 1
	loc s : word `c' of `labels'
	la var `x' "Expenditure on `s'"
}


******************************************************************************************************************************

*********************************************************************
* IMPUTING MISSING PRICE VALUES FROM S4 OF THE RURAL COMMUNITY DATA *
*********************************************************************

loc m b c d e
foreach x of varlist Q* {
	loc b = `b' + 1
	loc s : word `b' of `m'
	replace s11a`s'=. if `x'!=. & s11a`s'==0
}

label define unit 2 "American tin" 27 "Balls" 28 "Bar" 3 "Barrel" 4 "Basket" 5 "Beer Bottle" 6 "Bowl" 7 "Box" 29 "Bucket" 8 "Bunch" 9 "Bundle" ///
30 "Crate" 31 "Dozen" 10 "Fanta/Coke bottle" 11 "Fingers" 12 "Fruits" 13 "Gallon" 14 "Kilogram" 15 "Litre" 32 "Loaf" 16 "Log" 17 "Margarine tin" ///
18 "Maxi bag" 19 "Mini bag" 20 "Nut" 33 "Pair" 34 "Pieces" 35 "Pots" 21 "Pounds" 36 "Set" 22 "Sheet" 37 "Singles" 23 "Stick" 24 "Tonne" 25 "Tree" ///
26 "Tubers" 38 "Yard/Metre" 39 "Calabash" 40 "Milk Tin" 41 "Tin" 42 "Other (specify)"
label values unit unit 

loc letter b c e 
foreach x of varlist Qbi Qci Qei {
	loc h = `h' + 1
	loc s : word `h' of `letter'
	gen q`s'i_dummy = (`x'!=. & s11a`s'==. & `x'!=0)
	tab q`s'i_dummy
}
//These dummies equal one when quantity is there but value is missing
//As I impute prices, I keep updating these dummies so I know how many prices are still missing


//Since we are looking at prices on a district level, I created a variable which concatenates the district, food id and unit id.
//I used this variable to merge the per unit prices with this file
//Even though the merge doesn't uniquely identify, it doesn't matter, as the per unit price is simply a scalar that is the same for the 
//entire distrct per crop

*save "consumption_expenditure.dta", replace 

*qui do "${dropbox}\Papers on Yale Panel\do-files\Data prep\Unit Price.do"
*use "consumption_expenditure.dta", clear 

egen merge_col = concat(id2 itname unit), punct(-) 
sort merge_col
merge m:1 merge_col using "prices_per_unit.dta"
tab _merge

replace s11ab = Qbi*s45i if qbi_dummy==1 & _merge==3
replace s11ac = Qci*s45i if qci_dummy==1 & _merge==3
replace s11ae = Qei*s45i if qei_dummy==1 & _merge==3

drop _merge merge_col s45i

//The issue here is that the imputed numbers are much larger than any of the others in that category 

***************************************************************************************************************************

*****************************************************************************************************************************************
* IMPUTING MISSING PRICE VALUES FROM WITHIN THE DATASET FOR THE SAME ITEM-UNIT COMBINATION, PROVIDED THERE ARE MORE THAN 5 OBSERVATIONS *
*****************************************************************************************************************************************

//DISTRICT LEVEL

replace qbi_dummy = 0 if s11ab!=. //131  										4/17 - 18
replace qci_dummy = 0 if s11ac!=. //428											4/17 - 14			
replace qei_dummy = 0 if s11ae!=. //26											4/17 - 9					

loc m b c e
foreach x of varlist Qbi Qci Qei {
	loc y = `y' + 1
	loc s : word `y' of `m'
	gen one_`x'=s11a`s'/`x'
	bys id2 itname unit : egen median_one_`x' = median(one_`x')
	drop one_`x'
	la var median_one_`x' "Median price of one unit of `s'"
	*sum median_one_`x'
}

//Need to count if there are more than 5 instances at a district level for a missing price-quantity
count_5 median_one_Qbi median_one_Qci median_one_Qei ms id2
//This replaces missing prices by the median price, if more than 5 people have bought the food item in the same quantity in the 
//same unit and in the same ditrict
replace s11ab = median_one_Qbi*Qbi if qbi_dummy==1 & number_median_one_Qbi>4
replace s11ac = median_one_Qci*Qci if qci_dummy==1 & number_median_one_Qci>4
replace s11ae = median_one_Qei*Qei if qei_dummy==1 & number_median_one_Qei>4
//Dropping unnecessary variables
drop median_* number_*
//Updating dummy variables
replace qbi_dummy = 0 if s11ab!=. //51											4/17 - 80
replace qci_dummy = 0 if s11ac!=. //138											4/17 - 290 
replace qei_dummy = 0 if s11ae!=. //18											4/17 - 8


//REGIONAL LEVEL

//This generates a column with the price per unit by food_id and region
loc m b c e
foreach x of varlist Qbi Qci Qei {
	loc e = `e' + 1
	loc s : word `e' of `m'
	bys id1 itname unit : gen price_of_one_`s' = s11a`s'/`x' if `x'!=. & `x'!=0 & s11a`s'!=.
	bys id1 itname unit : egen med_reg_`x' = median(price_of_one_`s')
}
//Counts if there are more than 5
count_5 med_reg_Qbi med_reg_Qci med_reg_Qei ms1 id1
//Replacing with median regional price
replace s11ab = med_reg_Qbi*Qbi if qbi_dummy==1 & number_med_reg_Qbi>4
replace s11ac = med_reg_Qci*Qci if qci_dummy==1 & number_med_reg_Qci>4
replace s11ae = med_reg_Qei*Qei if qei_dummy==1 & number_med_reg_Qei>4
//Dropping unnecessary variables
drop number* med* price*
//Updating dummy variables
replace qbi_dummy = 0 if s11ab!=. //26											4/17 - 25
replace qci_dummy = 0 if s11ac!=. //46											4/17 - 92
replace qei_dummy = 0 if s11ae!=. //8											4/17 - 10			


//NATIONAL LEVEL

loc m b c e
foreach x of varlist Qbi Qci Qei {
	loc f = `f' + 1
	loc s : word `f' of `m'
	bys itname unit : gen national_one_`s' = s11a`s'/`x' if `x'!=. & `x'!=0 & s11a`s'!=.
	bys itname unit : egen med_nat_`x' = median(national_one_`s')
}
//Count if there are more than 5
//Cannot use program as this step has only 4 arguments, not 5
loc m b c e
foreach x of varlist med_nat_Qbi med_nat_Qci med_nat_Qei {
	loc ms2 = `ms2' + 1
	loc s : word `ms2' of `m'
	egen count_quantity_`x' = concat(itname unit `x'), punct(-)
	replace count_quantity_`x'=""  if itname=="" | unit==. | `x'==.
	sort count_quantity_`x'
	bys count_quantity_`x': gen number_`x' = _N
	//drop count_quantity_`x'
}
//Replacing with national median price
replace s11ab = med_nat_Qbi*Qbi if qbi_dummy==1 & number_med_nat_Qbi>4
replace s11ac = med_nat_Qci*Qci if qci_dummy==1 & number_med_nat_Qci>4
replace s11ae = med_nat_Qei*Qei if qei_dummy==1 & number_med_nat_Qei>4
//Dropping unnecessary variables
drop med* national* number*
//Updating dummy variables
replace qbi_dummy = 0 if s11ab!=. //11											4/17 - 15
replace qci_dummy = 0 if s11ac!=. //8											4/17 - 38
replace qei_dummy = 0 if s11ae!=. //4											4/17 - 4

//These remaining missing values have less than 4 (mostly only 1) occurence nationally

note : As of 4/17, qbi_dummy had 138/149 missing prices resolved, qci_dummy had 434/442 missing prices resolved and qei_dummy had 31/35 missing prices resolved

**********************************************************************************************************************************

*****************************
* GENERATING THE AGGREGATES *
*****************************

//Checking whether my calcuated total matches the total in the data
egen total_exp_s11b = rowtotal(s11ba s11bb s11bc s11bd)
la var total_exp "MS calculation of total exp for S11B"
gen diff=1 if total_exp_s11b!=s11be //1 for every unequal value
sort hhno food_id item_id fuel_id
bys diff: distinct hhno //639 distinct HH; 8347 obs with differing totals
//I checked this with the individual S11B file as well, and the number of differing answers is the same
drop diff


//Recoding the values to monthly
foreach x of varlist s11b* s11c {
	replace `x' = `x'/12
}
gen fuel_dummy = (s11db!=0) 
gen s11db_new =  s11db*fuel_dummy //only keeps monthly value for those HH that purchased some fuel
drop fuel_dummy


//Section wise aggregates
bys hhno: egen food_exp = total(s11ac)
la var food_exp "Total monthly expenditure on food"
bys hhno: egen gifts_given = total(s11ae)
la var gifts_given "Total monthly expenditure on giving gifts"
bys hhno: egen own_produce = total(s11ab)
la var own_produce "Total monthly value of own produce"

loc list "children elderly maleAdults femaleAdults otherItems fuel"
loc i = 0
foreach var of varlist s11ba s11bb s11bc s11bd s11c s11db_new {
	loc labelName : var label `var'
	loc i = `i' + 1
	loc name : word `i' of `list'
	bys hhno : egen `name'_exp = total(`var')
	la var `name'_exp "`labelName' at hh level"
}

//keeping only value per HH
foreach x of varlist gifts_given own_produce *exp{
	keep_one `x'
}

************************************************************************************************************************************

*******************************
* CREATING AGGREGATE DATASETS *
*******************************

//Generating total expenditure, including gifts and durables
egen total_expenditure = rowtotal (s11ab s11ac s11ae s11ba s11bb s11bc s11bd s11c s11db_new)
	la var total_expenditure "Row wise total of expenditure"
bys hhno: egen avg_monthly_exp = total(total_expenditure)
	la var avg_monthly_exp "Average Monthly Expenditure by HH (hhh wise total of total_expenditure)"
//Keeping only one observation per HH
keep_one avg_monthly_exp
drop uid qbi_dummy qci_dummy qei_dummy district CropName CropUnit count_quantity_med_nat_Qbi count_quantity_med_nat_Qci count_quantity_med_nat_Qei
order id1 id2 id3 id4 hhno itname food_id unit Q* s11a* s11b* total_exp_s11b itemname item_id s11c* fuel_id months_used s11d* food_exp gifts_given ///
	own_produce children_exp elderly_exp maleAdults_exp femaleAdults_exp otherItems_exp fuel_exp total_expenditure avg_monthly_exp
save "consumption_expenditure.dta", replace 

keep id1 id2 id3 id4 hhno total_exp_s11b food_exp gifts_given own_produce avg_monthly_exp *exp
drop if id4==.
drop if mi(avg_monthly_exp)
save "aggregated_expenditure.dta", replace

************************************************************************************************************************************

******************************************
* ADDING HEALTH, EDUCATION AND HOUSEHOLD *
******************************************

global filepath "${dropbox}\Wilson DATA\EGC-ISSER Wave 1 Final Data\Private\Cleaned Data"


*EDUCATION (YEARLY)
tempfile education

use "${filepath}\S1FI.dta", clear
keep hhno s1fi_13i-s1fi_21ii
	
foreach var of varlist s1fi_13i s1fi_14i s1fi_15i s1fi_16i s1fi_17i s1fi_18i s1fi_19i s1fi_20i s1fi_21i {
	
	loc labelName : var label `var'
	
	egen `var'ii_combined = rowtotal (`var' `var'i)
		la var `var'ii_combined "`labelName'"

}
keep hhno *combined
renvars *combined, postdrop(12)

egen itemized_total = rowtotal(s1fi_13-s1fi_20)
ren s1fi_21 non_itemized_total
collapse (sum)itemized_total (sum)non_itemized_total, by(hhno)

replace itemized_total = 0 if itemized_total!=0 & non_itemized_total!=0	//people might have had a better idea of an overall number so I am discarding even the few items they entered
gen education_expenditure = itemized_total + non_itemized_total
drop *itemized*
save "`education'", replace


use "${filepath}\S1FIII.dta", clear
keep hhno *67*
egen training_exp = rowtotal(s1fiii_67i s1fiii_67ii)
collapse (sum)training_exp, by(hhno)

merge 1:1 hhno using "`education'", nogen
replace education_expenditure = education_expenditure + training_exp
drop training_exp 
la var education_expenditure "Education expenses yearly"
save "`education'", replace


*HEALTH (INSURANCE IS FOR A YEAR, MEDICAL EXPENSES (A QUESTION IN S6F) IS FOR 2 WEEKS)
tempfile health

use "${filepath}\S6A.dta", clear
keep hhno *7* 
egen insurance_premium = rowtotal(s6a_a7ai s6a_a7aii)
collapse (sum)insurance_premium, by(hhno)
save "`health'", replace


use "${filepath}\S6C", clear
keep hhno s6c_10i-s6c_117ii
foreach var of varlist s6c_10i s6c_111i s6c_112i s6c_113i s6c_114i s6c_115i s6c_116i s6c_117i {
	
	loc labelName : var label `var'
	
	egen `var'ii_combined = rowtotal (`var' `var'i)
		la var `var'ii_combined "`labelName'"

}
keep hhno *combined
renvars *combined, postdrop(12)

egen vaccine_total = rowtotal(s6c_10-s6c_117)
collapse (sum)vaccine_total, by(hhno)

merge 1:1 hhno using "`health'", nogen
save "`health'", replace


use "${filepath}\S6F.dta", clear 
keep hhno s6f_15i-s6f_16ii s6f_22i s6f_22ii s6f_24i-s6f_25ii 
foreach var of varlist s6f_15i s6f_16i s6f_22i s6f_24i s6f_25i {
	
	loc labelName : var label `var'
	
	egen `var'ii_combined = rowtotal (`var' `var'i)
		la var `var'ii_combined "`labelName'"

}
keep hhno *combined
renvars *combined, postdrop(12)

egen health_expenses = rowtotal(s6f_15 s6f_16 s6f_22 s6f_24 s6f_25)
collapse (sum)health_expenses, by(hhno)

merge 1:1 hhno using "`health'", nogen
replace health_expenses = health_expenses + vaccine_total + insurance_premium
drop vaccine_total insurance_premium
save "`health'", replace 


*DWELLING (MONTHLY)
tempfile dwelling

use "${filepath}\S12AI.dta", clear 
keep hhno *2i* *4i* *7i* *8i* *18i* *19i* *23i* *26i* *29i*
foreach var of varlist s12a_2i s12a_4i s12a_7i s12a_8i s12a_18i s12a_19i s12a_23i s12a_26i s12a_29i {
	
	loc labelName : var label `var'
	
	egen `var'ii_combined = rowtotal (`var' `var'i)
		la var `var'ii_combined "`labelName'"
		
	
	replace `var'ii_combined = `var'ii_combined*30 if `var'ii == 1
	replace `var'ii_combined = `var'ii_combined*4 if `var'ii == 2
	replace `var'ii_combined = `var'ii_combined/3 if `var'ii == 4 
	replace `var'ii_combined = `var'ii_combined/6 if `var'ii == 5
	replace `var'ii_combined = `var'ii_combined/12 if `var'ii == 6

}
keep hhno *combined
renvars *combined, postdrop(12)

egen dwelling_expenses = rowtotal(s12a_2-s12a_29)
collapse (sum)dwelling_expenses, by(hhno)
	la var dwelling_expenses "Dwelling exp monthly"
save "`dwelling'", replace


use "aggregated_expenditure.dta", clear
merge 1:1 hhno using "`education'", nogen
merge 1:1 hhno using "`health'", nogen
merge 1:1 hhno using "`dwelling'", nogen

ren avg_monthly_exp avg_s11_monthly_exp
gen avg_monthly_exp_overall = avg_s11_monthly_exp + education_expenditure/12 + health_expenses + dwelling_expenses
save "aggregated_expenditure", replace
