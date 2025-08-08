*** PURPOSE : Consumption Analysis
*** AUTHOR : Manavi Sharma

if "`c(username)'"=="cru2" {
                local dropbox "c:\Users\cru2\Dropbox\research\egc-isser panel"
}

else if "`c(username)'"=="ManaviSharma" {
                local dropbox "c:\Users\ManaviSharma\Dropbox"
				local dropbox "c:\Users\Manavi\Dropbox"		
}
else if "`c(username)'"=="chris_000" {
                local dropbox "c:\Users\chris_000\Dropbox\research\egc-isser panel"
}
global dropbox `dropbox'
cd "${dropbox}\Papers on Yale Panel\data\consumption expenditure"


*ssc install parmest
*ssc install egen_inequal

*program to generate hhno 
capture program drop gen_hhno
program define gen_hhno
	gen str1 leading_one="1"
	gen str_id1=string(int(id1),"%02.0f")
	gen str_id3=string(int(id3),"%03.0f")
	gen str_id4=string(int(id4),"%03.0f")
	egen hhno=concat(leading_one str_id1 str_id3 str_id4)
	destring hhno, replace
	format hhno %9.0f
	drop leading_one str_id1 str_id3 str_id4
	la var hhno "Household Number"
end 

*************************************************************************************************************************************************

*GENERATING AND MERGING THE VARIABLES NEEDED TO ANALYSE THE SPREAD OF MONTHLY & PER CAPITA EXPENDITURE

use "aggregated_expenditure.dta", clear 
bys id1 : egen regionexpenditure = total(avg_monthly_exp_overall)
la var regionexpenditure "Total household expenditure by Region"

*Merging in important household information
tempfile exp
save `exp'
	use "${dropbox}\Wilson DATA\EGC-ISSER Wave 1 Final Data\Private\Cleaned Data\key hhld info v2.dta", clear 
	keep hhno hhsize urbrur
	tempfile hhsize
	save `hhsize'
use `exp', clear
merge 1:1 hhno using `hhsize'
drop if _merge!=3
drop _merge
tempfile exp
save `exp'
	use "${dropbox}\Wilson DATA\EGC-ISSER Wave 1 Final Data\Private\Cleaned Data\S1D.dta", clear 
	keep hhno hhmid s1d_4i
	tempfile age
	save `age'
use `exp', clear
merge 1:m hhno using `age'
sort hhno hhmid

*Dropping outliers
sum avg_monthly_exp, d
replace avg_monthly_exp=. if avg_monthly_exp<`r(p1)' | avg_monthly_exp>`r(p99)'

*Per capita expenditure
gen percapita_exp = avg_monthly_exp_overall/hhsize
la var percapita_exp "Per capita expediture by household"
sum percapita_exp, d
replace percapita_exp=. if percapita_exp<`r(p1)' | percapita_exp>`r(p99)'

*Adult equivalence per capita expenditure
ren s1d_4i age
gen adult_equivalence = .
*values are used from the paper http://siteresources.worldbank.org/PGLP/Resources/PMch2.pdf which says this scale was used by researchers analysing
* LSMS surveys in Ghana
replace adult_equivalence = 1 if age>17
replace adult_equivalence = 0.5 if age>12 & age<18
replace adult_equivalence = 0.3 if age>6 & age<13
replace adult_equivalence = 0.2 if age<7
la var adult_equivalence "Adult Equivalence Scale"

bys hhno : egen adult_eq_hhsize = total(adult_equivalence)
la var adult_eq_hhsize "Household size by adult equivalence"
gen adulteq_exp = avg_monthly_exp_overall/adult_eq_hhsize
la var adulteq_exp "Adult equivalence expediture by household"
sum adulteq_exp, d
replace adulteq_exp=. if adulteq_exp<`r(p1)' | adulteq_exp>`r(p99)'

bys hhno : keep if _n == 1

saveold "percapita_expenditure.dta", replace
