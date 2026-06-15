
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

# food_expenditures, food_quantities, food_prices are auto-derived at
# runtime from food_acquired (_FOOD_DERIVED in country.py; see CLAUDE.md
# "Derived Tables") — NOT materialized as country-level parquets.  The
# pre-Phase-3 food_prices_quantities_and_expenditures.py was dropped
# (GH #387); it crashed KeyError 'm' on the canonical food_acquired
# schema and shadowed the runtime derivation with a stale parquet.
# household_characteristics is auto-derived too (_ROSTER_DERIVED, GH #260).
var = \
      $(VAR_DIR)/shocks.parquet $(VAR_DIR)/fct.parquet $(VAR_DIR)/nutrition.parquet \
      $(VAR_DIR)/food_coping.parquet $(VAR_DIR)/crop_production.parquet \
      $(VAR_DIR)/plot_inputs.parquet $(VAR_DIR)/livestock.parquet \
      $(VAR_DIR)/anthropometry.parquet $(VAR_DIR)/plot_labor.parquet \
      $(VAR_DIR)/people_last7days.parquet $(VAR_DIR)/community_prices.parquet

all: panel_ids.json updated_ids.json $(parquet) $(var)

panel_ids.json updated_ids.json: panel_ids.py
	python panel_ids.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

shocks = $(shell find $(rounds) -name shocks.py)
shocks_parquet := $(shocks:.py=.parquet)

../%/_/shocks.parquet:
	(cd $(@D) && python shocks.py)

$(VAR_DIR)/shocks.parquet: shocks.py $(shocks_parquet)
	python shocks.py

sample = $(shell find $(rounds) -name sample.py)
sample_parquet := $(sample:.py=.parquet)

../%/_/sample.parquet:
	(cd $(@D) && python sample.py)

# plot_features (GH #167).  The framework's grab_data calls
# `make ../<wave>/_/plot_features.parquet` from this `_/` dir; the
# pattern rule runs the wave script.  The VAR_DIR rule mirrors the
# food_acquired country-level convention above.
plot_features = $(shell find $(rounds) -name plot_features.py)
plot_features_parquet := $(plot_features:.py=.parquet)

../%/_/plot_features.parquet: ../%/_/plot_features.py
	(cd $(@D) && python plot_features.py)

$(VAR_DIR)/plot_features.parquet: plot_features.py $(plot_features_parquet)
	python plot_features.py

# crop_production (GAP 1; ESS §9 post-harvest item-level crop harvest).
# Mirrors the plot_features convention: the framework's grab_data calls
# `make ../<wave>/_/crop_production.parquet`, whose pattern rule runs the
# wave script; the VAR_DIR rule concatenates them via the country script.
crop_production = $(shell find $(rounds) -name crop_production.py)
crop_production_parquet := $(crop_production:.py=.parquet)

../%/_/crop_production.parquet: ../%/_/crop_production.py
	(cd $(@D) && python crop_production.py)

$(VAR_DIR)/crop_production.parquet: crop_production.py $(crop_production_parquet)
	python crop_production.py

# plot_inputs (GAP 2; ESS §3/§4/§5 post-planting item-level ag inputs).
# Mirrors the crop_production convention: the framework's grab_data calls
# `make ../<wave>/_/plot_inputs.parquet`, whose pattern rule runs the wave
# script; the VAR_DIR rule concatenates them via the country script.
plot_inputs = $(shell find $(rounds) -name plot_inputs.py)
plot_inputs_parquet := $(plot_inputs:.py=.parquet)

../%/_/plot_inputs.parquet: ../%/_/plot_inputs.py
	(cd $(@D) && python plot_inputs.py)

$(VAR_DIR)/plot_inputs.parquet: plot_inputs.py $(plot_inputs_parquet)
	python plot_inputs.py

# livestock (GAP 4; ESS §8 item-level livestock roster at (t, i, animal)).
# Mirrors the crop_production convention: the framework's grab_data calls
# `make ../<wave>/_/livestock.parquet`, whose pattern rule runs the wave
# script; the VAR_DIR rule concatenates them via the country script.
livestock = $(shell find $(rounds) -name livestock.py)
livestock_parquet := $(livestock:.py=.parquet)

../%/_/livestock.parquet: ../%/_/livestock.py
	(cd $(@D) && python livestock.py)

$(VAR_DIR)/livestock.parquet: livestock.py $(livestock_parquet)
	python livestock.py

# food_coping (GH #332, Family B; ESS §7 CSI/rCSI day-count battery).
# Mirrors the plot_features convention: the framework's grab_data calls
# `make ../<wave>/_/food_coping.parquet`, whose pattern rule runs the wave
# script; the VAR_DIR rule concatenates them via the country script.
food_coping = $(shell find $(rounds) -name food_coping.py)
food_coping_parquet := $(food_coping:.py=.parquet)

../%/_/food_coping.parquet: ../%/_/food_coping.py
	(cd $(@D) && python food_coping.py)

$(VAR_DIR)/food_coping.parquet: food_coping.py $(food_coping_parquet)
	python food_coping.py

# anthropometry (GAP 5; NEW item-level body measures at (t, i, pid) from
# the ESS §3 health module + §1 roster age/sex).  Mirrors the livestock
# convention: the framework's grab_data calls
# `make ../<wave>/_/anthropometry.parquet`, whose pattern rule runs the wave
# script; the VAR_DIR rule concatenates them via the country script.
anthropometry = $(shell find $(rounds) -name anthropometry.py)
anthropometry_parquet := $(anthropometry:.py=.parquet)

../%/_/anthropometry.parquet: ../%/_/anthropometry.py
	(cd $(@D) && python anthropometry.py)

$(VAR_DIR)/anthropometry.parquet: anthropometry.py $(anthropometry_parquet)
	python anthropometry.py

# plot_labor (GAP 3.2; ESS §3-PP + §10-PH plot labor person-days at
# (t, i, plot_id, source) with source in {family, hired, other}).  Mirrors
# the crop_production convention: the framework's grab_data calls
# `make ../<wave>/_/plot_labor.parquet`, whose pattern rule runs the wave
# script; the VAR_DIR rule concatenates them via the country script.
plot_labor = $(shell find $(rounds) -name plot_labor.py)
plot_labor_parquet := $(plot_labor:.py=.parquet)

../%/_/plot_labor.parquet: ../%/_/plot_labor.py
	(cd $(@D) && python plot_labor.py)

$(VAR_DIR)/plot_labor.parquet: plot_labor.py $(plot_labor_parquet)
	python plot_labor.py

# people_last7days (GAP 3.1; ESS §4 individual 7-day activity at (t, i, pid),
# mirroring Uganda's per-individual schema).  Mirrors the crop_production
# convention: the framework's grab_data calls
# `make ../<wave>/_/people_last7days.parquet`, whose pattern rule runs the
# wave script; the VAR_DIR rule concatenates them via the country script.
people_last7days = $(shell find $(rounds) -name people_last7days.py)
people_last7days_parquet := $(people_last7days:.py=.parquet)

../%/_/people_last7days.parquet: ../%/_/people_last7days.py
	(cd $(@D) && python people_last7days.py)

$(VAR_DIR)/people_last7days.parquet: people_last7days.py $(people_last7days_parquet)
	python people_last7days.py

# community_prices (GAP C; ESS §10 community price questionnaire, item-level
# surveyed FOOD prices at the cluster grain (t, v, j, u)).  Mirrors the
# crop_production convention: the framework's grab_data calls
# `make ../<wave>/_/community_prices.parquet`, whose pattern rule runs the
# wave script; the VAR_DIR rule concatenates them via the country script.
community_prices = $(shell find $(rounds) -name community_prices.py)
community_prices_parquet := $(community_prices:.py=.parquet)

../%/_/community_prices.parquet: ../%/_/community_prices.py
	(cd $(@D) && python community_prices.py)

$(VAR_DIR)/community_prices.parquet: community_prices.py $(community_prices_parquet)
	python community_prices.py

$(VAR_DIR)/fct.parquet $(VAR_DIR)/nutrition.parquet: nutrition.py categorical_mapping.org $(VAR_DIR)/food_acquired.parquet
	python nutrition.py


clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
