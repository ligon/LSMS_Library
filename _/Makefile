##
# LSMS_Library
#

.SILENT: help

COUNTRIES := $(shell ls .. | grep -e ^[A-Z] | grep / | sed -e 's/\//,/g')

help:
	echo "USAGE:"
	echo "Either <test> or <build> datasets or estimate <demands>"
	echo "for a particular country using, e.g.,"
	echo "	make test country=Malawi"
	echo "or"
	echo "	make build country=Malawi"
	echo "or"
	echo "	make demands country=Malawi"
	echo ""
	echo "where country is one of:"
	echo $(COUNTRIES)

build:
	echo $(country)
	+make -C ../$(country)/_

test:
	./harmony.py $(country)

demands: build test
	./demands.py $(country)


# end
