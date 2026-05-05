rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

# food_expenditures / food_prices / food_quantities are derived at API time
# from food_acquired via lsms_library.transformations._FOOD_DERIVED — no
# country-level parquet is built or consulted.  See GH #218.
var = $(VAR_DIR)/household_characteristics.parquet

all: $(parquet) $(var)

food_acquired = $(shell find $(rounds) -name food_acquired.py)
# Wave-level parquets live in data_root, not in-tree
food_acquired_parquet := $(foreach r,$(rounds),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(notdir $(r))/_/food_acquired.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/food_acquired.parquet:
	(cd ../$(*)/_/ && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../%/_/household_characteristics.parquet:
	(cd $(@D) && python household_characteristics.py)

$(VAR_DIR)/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet)
	python household_characteristics.py

%.parquet: %.py malawi.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
