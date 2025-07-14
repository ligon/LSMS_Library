
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)
var = ../var/food_expenditures.parquet ../var/food_quantities.parquet ../var/food_prices.parquet \
	  ../var/household_characteristics.parquet ../var/other_features.parquet \
      ../var/shocks.parquet ../var/nonfood_expenditures.parquet ../var/enterprise_income.parquet \
	  ../var/assets.parquet ../var/earnings.parquet ../var/housing.parquet ../var/income.parquet \
	  ../var/fct.parquet ../var/nutrition.parquet

all: unitlabels.csv $(parquet) $(var)

unitlabels.csv: unitlabels.py
	python unitlabels.py

panel_ids.json updated_ids.json: panel_ids.py
	python panel_ids.py

$(parquet) $(var): panel_ids.json updated_ids.json

interview_date = $(shell find $(rounds) -name interview_date.py)
interview_date_parquet := $(interview_date:.py=.parquet)

../var/interview_date.parquet: interview_date.py $(interview_date_parquet)
	python interview_date.py

enterprise_income = $(shell find $(rounds) -name enterprise_income.py)
enterprise_income_parquet := $(enterprise_income:.py=.parquet)

../var/enterprise_income.parquet: enterprise_income.py $(enterprise_income_parquet)
	python enterprise_income.py

assets = $(shell find $(rounds) -name assets.py)
assets_parquet := $(assets:.py=.parquet)

../var/assets.parquet: assets.py $(assets_parquet)
	python assets.py

earnings = $(shell find $(rounds) -name earnings.py)
earnings_parquet := $(earnings:.py=.parquet)

../var/earnings.parquet: earnings.py $(earnings_parquet)
	python earnings.py

shocks = $(shell find $(rounds) -name shocks.py)
shocks_parquet := $(shocks:.py=.parquet)

../var/shocks.parquet: shocks.py $(shocks_parquet)
	python shocks.py


nonfood_expenditures = $(shell find $(rounds) -name nonfood_expenditures.py)
nonfood_expenditures_parquet := $(nonfood_expenditures:.py=.parquet)

../var/nonfood_expenditures.parquet: nonfood_expenditures.py $(nonfood_expenditures_parquet)
	python nonfood_expenditures.py

../var/food_quantities.parquet ../var/food_expenditures.parquet ../var/food_prices.parquet &: ../var/food_acquired.parquet food_prices_quantities_and_expenditures.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../var/food_acquired.parquet: unitlabels.csv food_acquired.py $(food_acquired_parquet) ../var/other_features.parquet
	python food_acquired.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../var/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet) ../var/other_features.parquet
	python household_characteristics.py

other_features = $(shell find $(rounds) -name other_features.py)
other_features_parquet := $(other_features:.py=.parquet)

../var/other_features.parquet: other_features.py $(other_features_parquet)
	python other_features.py

housing = $(shell find $(rounds) -name housing.py)
housing_parquet := $(housing:.py=.parquet)

../var/housing.parquet: housing.py $(housing_parquet)
	python housing.py

../var/income.parquet: income.py ../var/enterprise_income.parquet ../var/earnings.parquet
	python income.py

../var/nutrition.parquet ../var/fct.parquet: nutrition.py ../var/food_quantities.parquet nutrition.org
	python nutrition.py

%.parquet: %.py food_items.org uganda.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm ../var/*.parquet
