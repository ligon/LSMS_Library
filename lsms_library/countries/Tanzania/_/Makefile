
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet \
	  $(VAR_DIR)/household_characteristics.parquet \
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

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../%/_/household_characteristics.parquet:
	(cd $(@D) && python household_characteristics.py)

$(VAR_DIR)/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet)
	python household_characteristics.py


shocks = $(shell find $(rounds) -name shocks.py)
shocks_parquet := $(shocks:.py=.parquet)

$(VAR_DIR)/shocks.parquet: shocks.py $(shocks_parquet)
	python shocks.py

../%/_/shocks.parquet:
	(cd $(@D) && python shocks.py)

%.parquet: %.py CONTENTS.org tanzania.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
