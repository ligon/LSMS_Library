
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(or $(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share))/lsms_library
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet \
	  $(VAR_DIR)/household_characteristics.parquet $(VAR_DIR)/other_features.parquet $(VAR_DIR)/locality.parquet \
      $(VAR_DIR)/shocks.parquet $(VAR_DIR)/nonfood_expenditures.parquet $(VAR_DIR)/enterprise_income.parquet \
	  $(VAR_DIR)/assets.parquet $(VAR_DIR)/earnings.parquet $(VAR_DIR)/housing.parquet $(VAR_DIR)/income.parquet \
	  $(VAR_DIR)/fct.parquet $(VAR_DIR)/nutrition.parquet

all: unitlabels.csv $(parquet) $(var)

unitlabels.csv: unitlabels.py
	python unitlabels.py

panel_ids.json updated_ids.json: panel_ids.py
	python panel_ids.py

$(parquet) $(var): panel_ids.json updated_ids.json

interview_date = $(shell find $(rounds) -name interview_date.py)
interview_date_parquet := $(interview_date:.py=.parquet)

$(VAR_DIR)/interview_date.parquet: interview_date.py $(interview_date_parquet)
	python interview_date.py

enterprise_income = $(shell find $(rounds) -name enterprise_income.py)
enterprise_income_parquet := $(enterprise_income:.py=.parquet)

$(VAR_DIR)/enterprise_income.parquet: enterprise_income.py $(enterprise_income_parquet) $(VAR_DIR)/other_features.parquet
	python enterprise_income.py

assets = $(shell find $(rounds) -name assets.py)
assets_parquet := $(assets:.py=.parquet)

$(VAR_DIR)/assets.parquet: assets.py $(assets_parquet)
	python assets.py

earnings = $(shell find $(rounds) -name earnings.py)
earnings_parquet := $(earnings:.py=.parquet)

$(VAR_DIR)/earnings.parquet: earnings.py $(earnings_parquet) $(VAR_DIR)/other_features.parquet
	python earnings.py

shocks = $(shell find $(rounds) -name shocks.py)
shocks_parquet := $(shocks:.py=.parquet)

$(VAR_DIR)/shocks.parquet: shocks.py $(shocks_parquet) $(VAR_DIR)/other_features.parquet
	python shocks.py


nonfood_expenditures = $(shell find $(rounds) -name nonfood_expenditures.py)
nonfood_expenditures_parquet := $(nonfood_expenditures:.py=.parquet)

$(VAR_DIR)/nonfood_expenditures.parquet: nonfood_expenditures.py $(nonfood_expenditures_parquet)
	python nonfood_expenditures.py

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_prices_quantities_and_expenditures.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

$(VAR_DIR)/food_acquired.parquet: unitlabels.csv food_acquired.py $(food_acquired_parquet) $(VAR_DIR)/other_features.parquet
	python food_acquired.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

$(VAR_DIR)/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet) $(VAR_DIR)/other_features.parquet
	python household_characteristics.py

other_features = $(shell find $(rounds) -name other_features.py)
other_features_parquet := $(other_features:.py=.parquet)

$(VAR_DIR)/other_features.parquet: other_features.py $(other_features_parquet)
	python other_features.py

locality = $(shell find $(rounds) -name locality.py)
locality_parquet := $(locality:.py=.parquet)

$(VAR_DIR)/locality.parquet: locality.py $(locality_parquet)
	python locality.py

housing = $(shell find $(rounds) -name housing.py)
housing_parquet := $(housing:.py=.parquet)

$(VAR_DIR)/housing.parquet: housing.py $(housing_parquet)
	python housing.py

$(VAR_DIR)/income.parquet: income.py $(VAR_DIR)/enterprise_income.parquet $(VAR_DIR)/earnings.parquet
	python income.py

$(VAR_DIR)/nutrition.parquet $(VAR_DIR)/fct.parquet: nutrition.py $(VAR_DIR)/food_quantities.parquet nutrition.org
	python nutrition.py

%.parquet: %.py food_items.org uganda.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
