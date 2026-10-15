COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

all: panel_ids.json

# Panel IDs (constructed from the ERHS cover sheets; see panel_ids.py).
# panel_ids.json / updated_ids.json are REVIEWED SOURCES, committed to the
# config tree -- NOT build products (GH #914, #894).  They deliberately carry
# no rule.  A rule here fires whenever panel_ids.py happens to be the newer
# file, and on a pip install which of the two is newer is decided by the
# millisecond the wheel was unpacked (measured on the real 0.13.0 wheel: 5 of
# these 9 countries land with the script 1 ms ahead).  The recipe then writes
# into site-packages, which on any shared install is not writable by the user
# running the code -- PermissionError from three layers inside a `make`
# subprocess.  Regenerating the crosswalk is a deliberate maintainer action,
# reviewed as a diff to the committed JSON:
#
#     make panel-ids
#
.PHONY: panel-ids
panel-ids:
	python panel_ids.py

# community_prices (#438 / #275): PA-level surveyed food prices.  Country-level
# script reads the three dedicated price files (price1234_rev R1-R4 broadcast
# Woreda->PA, rd6_kgpr_Mkt R6, price2009 R7), maps item codes -> Preferred
# Labels via harmonize_community_food, and writes the (t,v,j,u) parquet.
$(VAR_DIR)/community_prices.parquet: community_prices.py ethiopiarhs.py categorical_mapping.org
	python community_prices.py

clean:
	@echo "nothing to clean: panel_ids.json / updated_ids.json are committed"
	@echo "sources, not build products (GH #914).  Regenerate: make panel-ids"
