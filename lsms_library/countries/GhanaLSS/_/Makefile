rounds := $(shell ls -d ../19* ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

# food_expenditures / food_quantities / food_prices are NOT built here:
# they are auto-derived at runtime by the framework's _FOOD_DERIVED from
# the canonical food_acquired.parquet (cf. Uganda post-#245).  Registering
# or building them is forbidden.
var = $(VAR_DIR)/food_acquired.parquet

all: $(parquet) $(var) panel_ids.json

panel_ids.json updated_ids.json: panel_ids.py ghanalss.py
	python panel_ids.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

household_roster = $(shell find $(rounds) -name household_roster.py)
household_roster_parquet := $(household_roster:.py=.parquet)

../%/_/household_roster.parquet: $(household_roster)
	(cd $(@D) && python household_roster.py)

$(VAR_DIR)/household_roster.parquet: household_roster.py $(household_roster_parquet)
	python household_roster.py


%.parquet: %.py food_items.org ghanalss.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
