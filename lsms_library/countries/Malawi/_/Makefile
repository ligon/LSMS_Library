rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(or $(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share))/lsms_library
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet \
	  $(VAR_DIR)/household_characteristics.parquet $(VAR_DIR)/other_features.parquet 

all: $(parquet) $(var)

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_acquired.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)  $(VAR_DIR)/other_features.parquet
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../%/_/household_characteristics.parquet:
	(cd $(@D) && python household_characteristics.py)

$(VAR_DIR)/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet) $(VAR_DIR)/other_features.parquet
	python household_characteristics.py

other_features = $(shell find $(rounds) -name other_features.py)
other_features_parquet := $(other_features:.py=.parquet)

../%/_/other_features.parquet: $(other_features)
	(cd $(@D) && python other_features.py)

$(VAR_DIR)/other_features.parquet: other_features.py $(other_features_parquet)
	python other_features.py

%.parquet: %.py food_items.org malawi.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
