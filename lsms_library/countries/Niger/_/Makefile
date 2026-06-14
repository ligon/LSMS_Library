rounds := $(shell ls -d ../[12]*)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

all: panel_ids.json

# Panel IDs are constructed from raw ECVMA and EHCVM cover sheets.
# See panel_ids.py for the two-program disjoint linkage logic.
panel_ids.json updated_ids.json: panel_ids.py niger.py
	python panel_ids.py

# Wave-level plot_features (GH #167).  The framework's grab_data calls
# `make ../<wave>/_/plot_features.parquet` from this `_/` dir; this
# pattern rule runs the wave script that materializes it.
../%/_/plot_features.parquet: ../%/_/plot_features.py
	(cd $(@D) && python plot_features.py)

# crop_production (GAP 1, item-level).  Country-level var/ parquet built by
# the concatenator, which depends on each wave's parquet; the pattern rule
# below runs each wave script.  Wave parquets land under data_root() (via
# to_parquet); the in-tree paths here are only the make prerequisites'
# script files, so we run the country script unconditionally to (re)build
# the wave parquets it reads, then concatenate.
CROP_WAVES := 2011-12 2014-15 2018-19 2021-22

$(VAR_DIR)/crop_production.parquet: crop_production.py niger.py categorical_mapping.org \
		$(foreach w,$(CROP_WAVES),../$(w)/_/crop_production.py)
	$(foreach w,$(CROP_WAVES),(cd ../$(w)/_ && python crop_production.py);)
	python crop_production.py

../%/_/crop_production.parquet: ../%/_/crop_production.py niger.py categorical_mapping.org
	(cd $(@D) && python crop_production.py)

# plot_inputs (GAP 2, item-level).  Same shape as crop_production: the
# country-level var/ parquet is built by the concatenator, which reads each
# wave's parquet; the country script is run unconditionally to (re)build the
# wave parquets it reads, then concatenates.
INPUT_WAVES := 2011-12 2014-15 2018-19 2021-22

$(VAR_DIR)/plot_inputs.parquet: plot_inputs.py niger.py categorical_mapping.org \
		$(foreach w,$(INPUT_WAVES),../$(w)/_/plot_inputs.py)
	$(foreach w,$(INPUT_WAVES),(cd ../$(w)/_ && python plot_inputs.py);)
	python plot_inputs.py

../%/_/plot_inputs.parquet: ../%/_/plot_inputs.py niger.py categorical_mapping.org
	(cd $(@D) && python plot_inputs.py)

# livestock (GAP 4, item-level).  Same shape as crop_production / plot_inputs:
# the country-level var/ parquet is built by the concatenator, which reads
# each wave's parquet; the country script is run unconditionally to (re)build
# the wave parquets it reads, then concatenates.
LIVESTOCK_WAVES := 2011-12 2014-15 2018-19 2021-22

$(VAR_DIR)/livestock.parquet: livestock.py niger.py categorical_mapping.org \
		$(foreach w,$(LIVESTOCK_WAVES),../$(w)/_/livestock.py)
	$(foreach w,$(LIVESTOCK_WAVES),(cd ../$(w)/_ && python livestock.py);)
	python livestock.py

../%/_/livestock.parquet: ../%/_/livestock.py niger.py categorical_mapping.org
	(cd $(@D) && python livestock.py)

# plot_labor (GAP 3, item-level).  Same shape as the siblings above: the
# country-level var/ parquet is built by the concatenator, which reads each
# wave's parquet; the country script is run unconditionally to (re)build the
# wave parquets it reads, then concatenates.  (No categorical table dep --
# the labor `source` vocabulary is an inline code mapping.)
PLOT_LABOR_WAVES := 2011-12 2014-15 2018-19 2021-22

$(VAR_DIR)/plot_labor.parquet: plot_labor.py niger.py \
		$(foreach w,$(PLOT_LABOR_WAVES),../$(w)/_/plot_labor.py)
	$(foreach w,$(PLOT_LABOR_WAVES),(cd ../$(w)/_ && python plot_labor.py);)
	python plot_labor.py

../%/_/plot_labor.parquet: ../%/_/plot_labor.py niger.py
	(cd $(@D) && python plot_labor.py)

# people_last7days (GAP 3, item-level).  Same shape; Industry / hours are
# derived in code (no categorical table dep).
PEOPLE7_WAVES := 2011-12 2014-15 2018-19 2021-22

$(VAR_DIR)/people_last7days.parquet: people_last7days.py niger.py \
		$(foreach w,$(PEOPLE7_WAVES),../$(w)/_/people_last7days.py)
	$(foreach w,$(PEOPLE7_WAVES),(cd ../$(w)/_ && python people_last7days.py);)
	python people_last7days.py

../%/_/people_last7days.parquet: ../%/_/people_last7days.py niger.py
	(cd $(@D) && python people_last7days.py)

# community_prices (GAP C, item-level).  Same concatenator shape as the GAP
# siblings, but only the two ECVMA waves have an EA-level community price
# module (the EHCVM waves replaced it with a region/market price survey, not
# this feature), so only those two wave scripts are prerequisites; the
# concatenator silently skips a wave with no parquet.
COMMUNITY_PRICE_WAVES := 2011-12 2014-15

$(VAR_DIR)/community_prices.parquet: community_prices.py niger.py categorical_mapping.org \
		$(foreach w,$(COMMUNITY_PRICE_WAVES),../$(w)/_/community_prices.py)
	$(foreach w,$(COMMUNITY_PRICE_WAVES),(cd ../$(w)/_ && python community_prices.py);)
	python community_prices.py

../%/_/community_prices.parquet: ../%/_/community_prices.py niger.py categorical_mapping.org
	(cd $(@D) && python community_prices.py)

clean:
	rm -f panel_ids.json updated_ids.json

.PHONY: all clean
