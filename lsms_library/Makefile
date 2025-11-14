##
# LSMS_Library
#

.SILENT: help

PYTHON ?= python3
DVC ?= $(PYTHON) -m dvc
format ?= parquet

slug = $(shell printf '%s' "$(1)" | tr '[:upper:]' '[:lower:]' | tr ' -' '__')
stage_name = $(call slug,$(country))_$(call slug,$(wave))_$(call slug,$(table))


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
		--country $(country) \
		--wave $(wave) \
		--table $(table) \
		--format $(format) \
		--out countries/build/$(country)/$(wave)/$(table).$(format)

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
