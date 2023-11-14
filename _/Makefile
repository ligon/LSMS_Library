##
# LSMS_Library
#
# @file
# @version 0.1

.SILENT: help

COUNTRIES := $(shell ls .. | grep -e ^[A-Z] | grep / | sed -e 's/\//,/g')

help:
	echo "USAGE:"
	echo "Either test or build code for a particular country using, e.g.,"
	echo "	make test country=Uganda"
	echo "or"
	echo "	make build country=Uganda"
	echo ""
	echo "where country is one of:"
	echo $(COUNTRIES)

build:
	echo $(country)
	(cd ../$(country)/_; make)

test:
	./harmony.py $(country)


# end
