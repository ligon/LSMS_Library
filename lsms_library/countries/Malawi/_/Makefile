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

all: $(parquet) $(VAR_DIR)/food_acquired.parquet $(VAR_DIR)/food_coping.parquet $(VAR_DIR)/months_food_inadequate.parquet $(VAR_DIR)/crop_production.parquet $(VAR_DIR)/plot_inputs.parquet $(VAR_DIR)/livestock.parquet $(VAR_DIR)/anthropometry.parquet $(VAR_DIR)/plot_labor.parquet $(VAR_DIR)/people_last7days.parquet $(VAR_DIR)/community_prices.parquet

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

# --- livestock (GAP 4) ----------------------------------------------------
# Item-level (t, i, animal) livestock roster for the four IHS3+/IHPS waves.
# Each wave script writes its parquet into data_root; the country script
# concatenates.  2004-05 is ABSENT (household-level aggregated ag file, no
# Module R livestock roster).
livestock_waves := 2010-11 2013-14 2016-17 2019-20
livestock_parquet := $(foreach r,$(livestock_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/livestock.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/livestock.parquet: malawi.py
	(cd ../$(*)/_/ && python livestock.py)

$(VAR_DIR)/livestock.parquet: livestock.py $(livestock_parquet)
	python livestock.py

# --- anthropometry (GAP 5) ------------------------------------------------
# Item-level (t, i, pid) body measures (Weight/Height/Age_months/Sex)
# for the four IHS3+/IHPS waves carrying Module V.  Each wave script writes
# its parquet into data_root; the country script concatenates.  2004-05
# (IHS2) is ABSENT (no Module V body-measurement roster).  z-scores /
# wasting / stunting are transformations, NOT stored.
anthropometry_waves := 2010-11 2013-14 2016-17 2019-20
anthropometry_parquet := $(foreach r,$(anthropometry_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/anthropometry.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/anthropometry.parquet: malawi.py
	(cd ../$(*)/_/ && python anthropometry.py)

$(VAR_DIR)/anthropometry.parquet: anthropometry.py $(anthropometry_parquet)
	python anthropometry.py

# --- plot_labor (GAP 3, plot grain) ---------------------------------------
# Item-level (t, i, plot, source) plot labor (PersonDays, Wage) for the four
# IHS3+/IHPS waves.  source in {family, hired, other}.  Each wave script
# writes its parquet into data_root; the country script concatenates.
# 2004-05 (IHS2) is ABSENT (household-level aggregated ag file, no
# plot-level labor roster).  total_*_labor_days / hired_labor_value are
# transformations, NOT stored.
plot_labor_waves := 2010-11 2013-14 2016-17 2019-20
plot_labor_parquet := $(foreach r,$(plot_labor_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/plot_labor.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/plot_labor.parquet: malawi.py
	(cd ../$(*)/_/ && python plot_labor.py)

$(VAR_DIR)/plot_labor.parquet: plot_labor.py $(plot_labor_parquet)
	python plot_labor.py

# --- people_last7days (GAP 3, individual grain) ---------------------------
# Item-level (t, i, pid) 7-day individual time-use for the four IHS3+/IHPS
# waves carrying the hh_mod_e labor module.  Each wave script writes its
# parquet into data_root; the country script concatenates.  2004-05 (IHS2)
# is ABSENT (predates the 7-day activity battery).  The six WB ind_* dummies
# collapse to one wage_industry label; per-industry dummies are transforms.
people_last7days_waves := 2010-11 2013-14 2016-17 2019-20
people_last7days_parquet := $(foreach r,$(people_last7days_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/people_last7days.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/people_last7days.parquet: malawi.py
	(cd ../$(*)/_/ && python people_last7days.py)

$(VAR_DIR)/people_last7days.parquet: people_last7days.py $(people_last7days_parquet)
	python people_last7days.py

# --- community_prices (GAP C) ---------------------------------------------
# Item-level (t, v, j, u) Community/Market Module CK reference prices,
# collected in three of the five waves: 2010-11 (IHS3), 2013-14 (IHPS),
# 2019-20 (IHS5).  2016-17 (IHS4) ran a community questionnaire but DROPPED
# the Module CK price section; 2004-05 (IHS2) has no community price
# questionnaire -- both ABSENT.  Each wave script writes its parquet into
# data_root; the country script concatenates.  v is the community EA id on
# the sample().v keyspace (NATIVE -- no household i).  Per-unit prices and
# cross-cluster medians are transformations, NOT stored.
community_prices_waves := 2010-11 2013-14 2019-20
community_prices_parquet := $(foreach r,$(community_prices_waves),$(LSMS_DATA_ROOT)/$(COUNTRY)/$(r)/_/community_prices.parquet)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/community_prices.parquet: malawi.py
	(cd ../$(*)/_/ && python community_prices.py)

$(VAR_DIR)/community_prices.parquet: community_prices.py $(community_prices_parquet)
	python community_prices.py

%.parquet: %.py malawi.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
