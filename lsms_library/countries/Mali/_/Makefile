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

$(VAR_DIR)/food_coping.parquet ../var/food_coping.parquet: food_coping.py ../2014-15/_/food_coping.parquet
	python food_coping.py

clean:
	rm -f panel_ids.json updated_ids.json

.PHONY: all clean
