rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet

all: $(parquet) $(var)

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_acquired.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

# Wave-level plot_features (GH #167).  The framework's grab_data calls
# `make ../<wave>/_/plot_features.parquet` from this `_/` dir; this
# pattern rule runs the wave script that materializes it.  (Same gap as
# the EHCVM siblings Mali/Niger; the generic %.parquet rule below never
# fires for it because its food_items.org/malawi.py prereqs are absent.)
../%/_/plot_features.parquet: ../%/_/plot_features.py
	(cd $(@D) && python plot_features.py)

# Wave-level livestock (EHCVM cluster; cloned from Niger).  Same gap as
# plot_features above: the generic %.parquet rule never fires for it
# because its food_items.org/malawi.py prereqs are absent, so give it an
# explicit pattern rule.
../%/_/livestock.parquet: ../%/_/livestock.py
	(cd $(@D) && python livestock.py)

# Wave-level EHCVM parity features (cloned from Niger 2018-19).  Same gap as
# plot_features / livestock above: the generic %.parquet rule never fires
# for them (the food_items.org/malawi.py prereqs are absent), so each gets
# an explicit pattern rule that runs its self-contained wave script.
../%/_/crop_production.parquet: ../%/_/crop_production.py
	(cd $(@D) && python crop_production.py)

../%/_/plot_inputs.parquet: ../%/_/plot_inputs.py
	(cd $(@D) && python plot_inputs.py)

../%/_/plot_labor.parquet: ../%/_/plot_labor.py
	(cd $(@D) && python plot_labor.py)

../%/_/people_last7days.parquet: ../%/_/people_last7days.py
	(cd $(@D) && python people_last7days.py)

%.parquet: %.py food_items.org malawi.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
