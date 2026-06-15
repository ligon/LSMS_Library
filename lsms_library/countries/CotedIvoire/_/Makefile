
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet \
      $(VAR_DIR)/shocks.parquet


all: $(parquet) $(var)

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_acquired.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

shocks = $(shell find $(rounds) -name shocks.py)
shocks_parquet := $(shocks:.py=.parquet)

$(VAR_DIR)/shocks.parquet: shocks.py $(shocks_parquet)
	python shocks.py

../%/_/shocks.parquet:
	(cd $(@D) && python shocks.py)

# livestock (GAP 4, item-level).  Script-path wave table: each wave's
# _/livestock.py reads its s17 roster and writes the wave-level parquet via
# to_parquet().  The wave-level grab_data() invokes
# `make ../<wave>/_/livestock.parquet`, so this rule runs that wave script.
../%/_/livestock.parquet: ../%/_/livestock.py categorical_mapping.org
	(cd $(@D) && python livestock.py)

# EHCVM parity features (plot_features / crop_production / plot_inputs /
# plot_labor / people_last7days).  Script-path wave tables like livestock:
# each wave's _/<feature>.py reads its EHCVM source module(s) under
# ../Data/Menage/ and writes the wave-level parquet via to_parquet().  The
# wave-level grab_data() has no script fallback, so the per-feature pattern
# rule runs the wave script (one rule per feature, mirroring livestock).
../%/_/plot_features.parquet: ../%/_/plot_features.py categorical_mapping.org
	(cd $(@D) && python plot_features.py)

../%/_/crop_production.parquet: ../%/_/crop_production.py categorical_mapping.org
	(cd $(@D) && python crop_production.py)

../%/_/plot_inputs.parquet: ../%/_/plot_inputs.py categorical_mapping.org
	(cd $(@D) && python plot_inputs.py)

../%/_/plot_labor.parquet: ../%/_/plot_labor.py categorical_mapping.org
	(cd $(@D) && python plot_labor.py)

../%/_/people_last7days.parquet: ../%/_/people_last7days.py
	(cd $(@D) && python people_last7days.py)

%.parquet: %.py CONTENTS.org tanzania.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
