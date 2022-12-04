
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)
var = ../var/food_expenditures.parquet ../var/food_quantities.parquet ../var/unitvalues.parquet \
	  ../var/household_characteristics.parquet ../var/other_features.parquet \
      ../var/shocks.parquet ../var/nonfood_expenditures.parquet ../var/enterprise_income.parquet \
	  ../var/assets.parquet ../var/earnings.parquet


all: $(parquet) $(var)

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

food_expenditures = $(shell find $(rounds) -name food_expenditures.py)
food_expenditures_parquet := $(food_expenditures:.py=.parquet)

../var/food_expenditures.parquet: food_expenditures.py $(food_expenditures_parquet)
	python food_expenditures.py

nonfood_expenditures = $(shell find $(rounds) -name nonfood_expenditures.py)
nonfood_expenditures_parquet := $(nonfood_expenditures:.py=.parquet)

../var/nonfood_expenditures.parquet: nonfood_expenditures.py $(nonfood_expenditures_parquet)
	python nonfood_expenditures.py

food_quantities = $(shell find $(rounds) -name food_quantities.py)
food_quantities_parquet := $(food_quantities:.py=.parquet)

../var/food_quantities.parquet: food_quantities.py $(food_quantities_parquet)
	python food_quantities.py

../var/unitvalues.parquet: unitvalues.py $(food_expenditure_parquet) $(food_quantities_parquet)
	python unitvalues.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../var/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet)
	python household_characteristics.py

other_features = $(shell find $(rounds) -name other_features.py)
other_features_parquet := $(other_features:.py=.parquet)

../var/other_features.parquet: other_features.py $(other_features_parquet)
	python other_features.py

%.parquet: %.py food_items.org uganda.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm ../var/*.parquet
