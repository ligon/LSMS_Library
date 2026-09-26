rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name "*.py")
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

var = $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_prices.parquet #$(VAR_DIR)/nutrition.parquet

all: $(parquet) $(var) panel_ids.json

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

$(VAR_DIR)/food_quantities.parquet $(VAR_DIR)/food_expenditures.parquet $(VAR_DIR)/food_prices.parquet &: $(VAR_DIR)/food_acquired.parquet food_acquired.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

$(VAR_DIR)/fct.parquet $(VAR_DIR)/nutrition.parquet: nutrition.py food_items.org
	python nutrition.py

%.parquet: %.py burkina_faso.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
