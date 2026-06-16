COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

all: panel_ids.json

# Panel IDs (constructed from the ERHS cover sheets; see panel_ids.py).
panel_ids.json updated_ids.json: panel_ids.py ethiopiarhs.py
	python panel_ids.py

# community_prices (#438 / #275): PA-level surveyed food prices.  Country-level
# script reads the three dedicated price files (price1234_rev R1-R4 broadcast
# Woreda->PA, rd6_kgpr_Mkt R6, price2009 R7), maps item codes -> Preferred
# Labels via harmonize_community_food, and writes the (t,v,j,u) parquet.
$(VAR_DIR)/community_prices.parquet: community_prices.py ethiopiarhs.py categorical_mapping.org
	python community_prices.py

clean:
	rm -f panel_ids.json updated_ids.json
