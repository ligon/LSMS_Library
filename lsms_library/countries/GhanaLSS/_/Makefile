rounds := $(shell ls -d ../19* ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(or $(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share))/lsms_library
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet \
	  $(VAR_DIR)/household_characteristics.parquet

all: $(parquet) $(var)

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_acquired.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../%/_/household_characteristics.parquet:
	(cd $(@D) && python household_characteristics.py)

$(VAR_DIR)/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet)
	python household_characteristics.py

household_roster = $(shell find $(rounds) -name household_roster.py)
household_roster_parquet := $(household_roster:.py=.parquet)

../%/_/household_roster.parquet: @D/household_roster.py
	(cd $(@D) && python household_roster.py)

$(VAR_DIR)/household_roster.parquet: household_roster.py $(household_roster_parquet)
	python household_roster.py


%.parquet: %.py food_items.org ghana.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
