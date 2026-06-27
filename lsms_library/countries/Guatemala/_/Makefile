
rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

# food_expenditures / food_prices / food_quantities are NO LONGER materialized
# here: they auto-derive from food_acquired at API time via the framework's
# _FOOD_DERIVED transforms (see country.py).  We materialize food_acquired only.
var = $(VAR_DIR)/food_acquired.parquet \
      $(VAR_DIR)/nutrition.parquet $(VAR_DIR)/fct.parquet

all: $(parquet) $(var)

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

$(VAR_DIR)/fct.parquet : fct.py food_items.org
	./fct.py

$(VAR_DIR)/nutrition.parquet : nutrition.py $(VAR_DIR)/food_acquired.parquet $(VAR_DIR)/fct.parquet
	./nutrition.py

%.parquet: %.py food_items.org guatemala.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
