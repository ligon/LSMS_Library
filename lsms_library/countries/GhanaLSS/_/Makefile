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
# food_acquired has ONE builder (GH #808): the wave scripts below, run per
# wave by the framework, concatenated by load_from_waves, with panel ids
# applied at read time from updated_ids.  There is deliberately no
# $(VAR_DIR)/food_acquired.parquet target here -- the retired country-level
# _/food_acquired.py re-keyed the GLSS1<->GLSS2 split households with
# ghanalss.change_id's `_0` convention, contradicting panel_ids.py, and its
# hashless output was trusted as `legacy` (see CONTENTS.org).
var = $(VAR_DIR)/community_prices.parquet \
      $(VAR_DIR)/nutrition.parquet

all: $(parquet) $(var) panel_ids.json

panel_ids.json updated_ids.json: panel_ids.py ghanalss.py
	python panel_ids.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

community_prices = $(shell find $(rounds) -name community_prices.py)
community_prices_parquet := $(community_prices:.py=.parquet)

../%/_/community_prices.parquet: ../%/_/community_prices.py glss_prices.py
	(cd $(@D) && python community_prices.py)

$(VAR_DIR)/community_prices.parquet: community_prices.py glss_prices.py $(community_prices_parquet)
	python community_prices.py

# nutrition: food_quantities (derived from food_acquired) x the West African
# Food Composition Table.  Depends on the FCT extract and on
# categorical_mapping.org, whose harmonize_food table carries the
# label -> FCT Code map (GH #783; was the standalone food_items.org).  See
# nutrition.py's docstring for why it filters non-physical units before
# deriving kg.  nutrition.py reads food_acquired through
# Country('GhanaLSS').food_acquired(), i.e. the framework's own build/cache
# path, so the parquet is NOT a prerequisite here (GH #808: listing it made
# make run the retired country-level script).
$(VAR_DIR)/nutrition.parquet: nutrition.py fct_west_africa.org categorical_mapping.org
	python nutrition.py

household_roster = $(shell find $(rounds) -name household_roster.py)
household_roster_parquet := $(household_roster:.py=.parquet)

../%/_/household_roster.parquet: $(household_roster)
	(cd $(@D) && python household_roster.py)

$(VAR_DIR)/household_roster.parquet: household_roster.py $(household_roster_parquet)
	python household_roster.py


%.parquet: %.py categorical_mapping.org ghanalss.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
