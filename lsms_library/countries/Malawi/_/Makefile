rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

# food_{expenditures,prices,quantities} and household_characteristics are
# derived at API time via lsms_library.transformations._FOOD_DERIVED /
# _ROSTER_DERIVED — no country-level parquet is built or consulted.
# See GH #218.

all: $(parquet) $(VAR_DIR)/food_acquired.parquet $(VAR_DIR)/food_coping.parquet $(VAR_DIR)/months_food_inadequate.parquet $(VAR_DIR)/crop_production.parquet $(VAR_DIR)/plot_inputs.parquet

food_acquired = $(shell find $(rounds) -name food_acquired.py)
# Wave-level parquets live in data_root, not in-tree
food_acquired_parquet := $(foreach r,$(rounds),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(notdir $(r))/_/food_acquired.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/food_acquired.parquet:
	(cd ../$(*)/_/ && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

# --- non-FIES food security (GH #332) -------------------------------------
# food_coping and months_food_inadequate are built only for the four IHS3+/
# IHPS waves carrying Module H (2010-11, 2013-14, 2016-17, 2019-20).  Each
# wave script writes its parquet into data_root; the country script
# concatenates.  2004-05 is ABSENT (its Module H is a food-consumption
# recall, not the rCSI/months battery).
nonfies_waves := 2010-11 2013-14 2016-17 2019-20

food_coping_parquet := $(foreach r,$(nonfies_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/food_coping.parquet)
months_food_inadequate_parquet := $(foreach r,$(nonfies_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/months_food_inadequate.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/food_coping.parquet: malawi.py
	(cd ../$(*)/_/ && python food_coping.py)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/months_food_inadequate.parquet: malawi.py
	(cd ../$(*)/_/ && python months_food_inadequate.py)

$(VAR_DIR)/food_coping.parquet: food_coping.py $(food_coping_parquet)
	python food_coping.py

$(VAR_DIR)/months_food_inadequate.parquet: months_food_inadequate.py $(months_food_inadequate_parquet)
	python months_food_inadequate.py

# --- crop_production / harvest (GAP 1) ------------------------------------
# Item-level (t, i, plot, crop) harvest for the four IHS3+/IHPS waves.
# Each wave script writes its parquet into data_root; the country script
# concatenates.  2004-05 is ABSENT (household-level aggregated ag file).
crop_waves := 2010-11 2013-14 2016-17 2019-20
crop_production_parquet := $(foreach r,$(crop_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/crop_production.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/crop_production.parquet: malawi.py
	(cd ../$(*)/_/ && python crop_production.py)

$(VAR_DIR)/crop_production.parquet: crop_production.py $(crop_production_parquet)
	python crop_production.py

# --- plot_inputs / agricultural inputs (GAP 2) ----------------------------
# Item-level (t, i, plot, input, crop, u) inputs for the four IHS3+/IHPS
# waves.  Each wave script writes its parquet into data_root; the country
# script concatenates.  2004-05 is ABSENT (household-level aggregated ag
# file, no plot-input roster).
input_waves := 2010-11 2013-14 2016-17 2019-20
plot_inputs_parquet := $(foreach r,$(input_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/plot_inputs.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/plot_inputs.parquet: malawi.py
	(cd ../$(*)/_/ && python plot_inputs.py)

$(VAR_DIR)/plot_inputs.parquet: plot_inputs.py $(plot_inputs_parquet)
	python plot_inputs.py

%.parquet: %.py malawi.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
