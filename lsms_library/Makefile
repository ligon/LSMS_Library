##
# LSMS_Library
#

.SILENT: help

empty :=
space := $(empty) $(empty)

PYTHON ?= python3
DVC ?= $(PYTHON) -m dvc
format ?= parquet

slug = $(shell printf '%s' "$(1)" | tr '[:upper:]' '[:lower:]' | tr ' -' '__')
DEFAULT_WAVE_RAW := $(if $(strip $(wave)),$(firstword $(wave)),all_waves)
DEFAULT_WAVE := $(subst $(space),_,$(strip $(DEFAULT_WAVE_RAW)))
OUT_FILE ?= countries/build/$(country)/$(DEFAULT_WAVE)/$(table).$(format)
stage_name = $(call slug,$(country))_$(call slug,$(DEFAULT_WAVE_RAW))_$(call slug,$(table))


COUNTRIES := $(shell ls .. | grep -e ^[A-Z] | grep / | sed -e 's/\//,/g')

help:
	echo "USAGE:"
	echo "Either <test> or <build> datasets, <materialize>/<persist> tables, <register> stages, or estimate <demands>"
	echo "for a particular country using, e.g.,"
	echo "	make test country=Malawi"
	echo "or"
	echo "	make build country=Malawi"
	echo "or"
	echo "	make demands country=Malawi"
	echo "or"
	echo "	make materialize country=Malawi wave=2019-20 table=household_roster"
	echo "or"
	echo "	make persist country=Malawi wave=2019-20 table=household_roster"
	echo "or"
	echo "	make register country=Uganda wave=2023-24 table=household_roster"
	echo ""
	echo "where country is one of:"
	echo $(COUNTRIES)


build:
	echo $(country)
	+make -C ../$(country)/_

test:
	./harmony.py $(country)

materialize:
	$(PYTHON) -m lsms_library.cli materialize \
		$(if $(strip $(wave)),$(foreach w,$(wave),--wave $(w) ),$(if $(strip $(all_waves)),--all-waves ,))\
		--country $(country) \
		--table $(table) \
		--format $(format) \
		--out $(OUT_FILE)

register:
	$(PYTHON) -m lsms_library.cli register \
		--country $(country) \
		--wave $(wave) \
		--table $(table) \
		--format $(format)

persist:
	cd countries && $(DVC) repro materialize@$(stage_name)

demands: build test
	./demands.py $(country)

clean:
	+make -C ../$(country)/_ clean

# end
