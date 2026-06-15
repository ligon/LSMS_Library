
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet \
      $(VAR_DIR)/shocks.parquet


all: panel_ids.json updated_ids.json $(parquet) $(var)

panel_ids.json updated_ids.json: panel_ids.py
	python panel_ids.py
	

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_acquired.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet) $(VAR_DIR)/cluster_features.parquet
	python food_acquired.py

shocks = $(shell find $(rounds) -name shocks.py)
shocks_parquet := $(shocks:.py=.parquet)

$(VAR_DIR)/shocks.parquet: shocks.py $(shocks_parquet)
	python shocks.py

../%/_/shocks.parquet:
	(cd $(@D) && python shocks.py)

life_satisfaction = $(shell find $(rounds) -name life_satisfaction.py)
life_satisfaction_parquet := $(life_satisfaction:.py=.parquet)

$(VAR_DIR)/life_satisfaction.parquet: life_satisfaction.py $(life_satisfaction_parquet)
	python life_satisfaction.py

../%/_/life_satisfaction.parquet:
	(cd $(@D) && python life_satisfaction.py)

food_coping = $(shell find $(rounds) -name food_coping.py)
food_coping_parquet := $(food_coping:.py=.parquet)

$(VAR_DIR)/food_coping.parquet: food_coping.py $(food_coping_parquet)
	python food_coping.py

../%/_/food_coping.parquet:
	(cd $(@D) && python food_coping.py)

plot_features = $(shell find $(rounds) -name plot_features.py)
plot_features_parquet := $(plot_features:.py=.parquet)

$(VAR_DIR)/plot_features.parquet: plot_features.py $(plot_features_parquet)
	python plot_features.py

../%/_/plot_features.parquet:
	(cd $(@D) && python plot_features.py)

crop_production = $(shell find $(rounds) -name crop_production.py)
crop_production_parquet := $(crop_production:.py=.parquet)

$(VAR_DIR)/crop_production.parquet: crop_production.py $(crop_production_parquet)
	python crop_production.py

../%/_/crop_production.parquet:
	(cd $(@D) && python crop_production.py)

plot_inputs = $(shell find $(rounds) -name plot_inputs.py)
plot_inputs_parquet := $(plot_inputs:.py=.parquet)

$(VAR_DIR)/plot_inputs.parquet: plot_inputs.py $(plot_inputs_parquet)
	python plot_inputs.py

../%/_/plot_inputs.parquet:
	(cd $(@D) && python plot_inputs.py)

livestock = $(shell find $(rounds) -name livestock.py)
livestock_parquet := $(livestock:.py=.parquet)

$(VAR_DIR)/livestock.parquet: livestock.py $(livestock_parquet)
	python livestock.py

../%/_/livestock.parquet:
	(cd $(@D) && python livestock.py)

anthropometry = $(shell find $(rounds) -name anthropometry.py)
anthropometry_parquet := $(anthropometry:.py=.parquet)

$(VAR_DIR)/anthropometry.parquet: anthropometry.py $(anthropometry_parquet)
	python anthropometry.py

../%/_/anthropometry.parquet:
	(cd $(@D) && python anthropometry.py)

plot_labor = $(shell find $(rounds) -name plot_labor.py)
plot_labor_parquet := $(plot_labor:.py=.parquet)

$(VAR_DIR)/plot_labor.parquet: plot_labor.py $(plot_labor_parquet)
	python plot_labor.py

../%/_/plot_labor.parquet:
	(cd $(@D) && python plot_labor.py)

people_last7days = $(shell find $(rounds) -name people_last7days.py)
people_last7days_parquet := $(people_last7days:.py=.parquet)

$(VAR_DIR)/people_last7days.parquet: people_last7days.py $(people_last7days_parquet)
	python people_last7days.py

../%/_/people_last7days.parquet:
	(cd $(@D) && python people_last7days.py)

community_prices = $(shell find $(rounds) -name community_prices.py)
community_prices_parquet := $(community_prices:.py=.parquet)

$(VAR_DIR)/community_prices.parquet: community_prices.py $(community_prices_parquet)
	python community_prices.py

../%/_/community_prices.parquet:
	(cd $(@D) && python community_prices.py)

%.parquet: %.py CONTENTS.org tanzania.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
