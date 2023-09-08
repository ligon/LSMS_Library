##
# LSMS_Library
#
# @file
# @version 0.1

help:
	@echo "USAGE:"
	@echo "Either test or build code for a particular country using, e.g.,"
	@echo "	make test country=Uganda"
	@echo "or"
	@echo "	make build country=Uganda"

build:
	echo $(country)
	(cd ../$(country)/_; make)

test:
	./harmony.py $(country)


# end
