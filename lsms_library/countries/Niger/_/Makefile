rounds := $(shell ls -d ../[12]*)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

all: panel_ids.json

# Panel IDs are constructed from raw ECVMA and EHCVM cover sheets.
# See panel_ids.py for the two-program disjoint linkage logic.
panel_ids.json updated_ids.json: panel_ids.py niger.py
	python panel_ids.py

clean:
	rm -f panel_ids.json updated_ids.json

.PHONY: all clean
