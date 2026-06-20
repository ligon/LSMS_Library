rounds := $(shell ls -d ../[12]*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

# food_prices / food_quantities / food_expenditures are derived at API time by
# _FOOD_DERIVED from food_acquired -- NOT materialized to var/ (the per-country
# food_prices_quantities_and_expenditures.py was retired, GH #108/#218).
var = $(VAR_DIR)/food_acquired.parquet $(VAR_DIR)/interview_date.parquet

all: $(parquet) $(var)

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

$(VAR_DIR)/food_acquired.parquet: food_acquired.py $(food_acquired_parquet)
	python food_acquired.py

../2019-20/_/interview_date.parquet: ../2019-20/_/interview_date.py
	(cd ../2019-20/_; python interview_date.py)

interview_date = $(shell find $(rounds) -name interview_date.py)
interview_date_parquet := $(interview_date:.py=.parquet)

$(VAR_DIR)/interview_date.parquet: interview_date.py $(interview_date_parquet)
	python interview_date.py

%.parquet: %.py cambodia.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm $(VAR_DIR)/*.parquet
