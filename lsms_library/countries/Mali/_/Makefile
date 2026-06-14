rounds := $(shell ls -d ../[12]*)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

all: panel_ids.json

# Panel IDs are constructed from raw 2018-19 and 2021-22 cover sheets.
# See panel_ids.py for the EHCVM PanelHH-based linkage (mixed
# explicit/implicit panel structure).
panel_ids.json updated_ids.json: panel_ids.py mali.py
	python panel_ids.py

# Wave-level plot_features (GH #167).  The framework's grab_data calls
# `make ../<wave>/_/plot_features.parquet` from this `_/` dir; this
# pattern rule runs the wave script that materializes it.
../%/_/plot_features.parquet: ../%/_/plot_features.py
	(cd $(@D) && python plot_features.py)

# food_coping (non-FIES rCSI day-counts, #332).  Only the EACI 2014-15
# wave carries the Section-17 coping battery.  The wave pattern rule
# materializes the wave parquet; the country-level concatenator
# (food_coping.py) aggregates into var/food_coping.parquet.
../%/_/food_coping.parquet: ../%/_/food_coping.py
	(cd $(@D) && python food_coping.py)

$(VAR_DIR)/food_coping.parquet: food_coping.py ../2014-15/_/food_coping.parquet
	python food_coping.py

# crop_production (item-level harvest, GAP 1 / parity loop).  The crop
# module lives in the EACI waves only (2014-15, 2017-18).  The wave
# pattern rule materializes each wave parquet; the country-level
# concatenator (crop_production.py) aggregates into var/crop_production.parquet.
../%/_/crop_production.parquet: ../%/_/crop_production.py
	(cd $(@D) && python crop_production.py)

$(VAR_DIR)/crop_production.parquet: crop_production.py ../2014-15/_/crop_production.parquet ../2017-18/_/crop_production.parquet
	python crop_production.py

# plot_inputs (item-level ag inputs, GAP 2 / parity loop).  The input
# module lives in the EACI waves only (2014-15, 2017-18).  The wave pattern
# rule materializes each wave parquet; the country-level concatenator
# (plot_inputs.py) aggregates into var/plot_inputs.parquet.
../%/_/plot_inputs.parquet: ../%/_/plot_inputs.py
	(cd $(@D) && python plot_inputs.py)

$(VAR_DIR)/plot_inputs.parquet: plot_inputs.py ../2014-15/_/plot_inputs.parquet ../2017-18/_/plot_inputs.parquet
	python plot_inputs.py

# livestock (item-level animal roster, GAP 4 / parity loop).  The livestock
# module lives in the EACI waves only (2014-15, 2017-18).  The wave pattern
# rule materializes each wave parquet; the country-level concatenator
# (livestock.py) aggregates into var/livestock.parquet.
../%/_/livestock.parquet: ../%/_/livestock.py
	(cd $(@D) && python livestock.py)

$(VAR_DIR)/livestock.parquet: livestock.py ../2014-15/_/livestock.parquet ../2017-18/_/livestock.parquet
	python livestock.py

# plot_labor (item-level plot labor by source, GAP 3a / parity loop).  The
# plot-labor module lives in the EACI waves only (2014-15, 2017-18).  The
# wave pattern rule materializes each wave parquet; the country-level
# concatenator (plot_labor.py) aggregates into var/plot_labor.parquet.
../%/_/plot_labor.parquet: ../%/_/plot_labor.py
	(cd $(@D) && python plot_labor.py)

$(VAR_DIR)/plot_labor.parquet: plot_labor.py ../2014-15/_/plot_labor.parquet ../2017-18/_/plot_labor.parquet
	python plot_labor.py

# people_last7days (per-individual 7-day activity, GAP 3b / parity loop).
# The individual labor/time-use module lives in the EACI waves only
# (2014-15, 2017-18).  The wave pattern rule materializes each wave parquet;
# the country-level concatenator (people_last7days.py) aggregates into
# var/people_last7days.parquet.
../%/_/people_last7days.parquet: ../%/_/people_last7days.py
	(cd $(@D) && python people_last7days.py)

$(VAR_DIR)/people_last7days.parquet: people_last7days.py ../2014-15/_/people_last7days.parquet ../2017-18/_/people_last7days.parquet
	python people_last7days.py

clean:
	rm -f panel_ids.json updated_ids.json

.PHONY: all clean
