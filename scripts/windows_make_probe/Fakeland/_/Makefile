# Mirrors the shape of the real country Makefiles (Uganda/_/Makefile).
rounds := $(shell ls -d ../2*-*)
source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)
COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var
things = $(shell find $(rounds) -name things.py)

all: $(VAR_DIR)/things.parquet

../%/_/things.parquet: ../%/_/things.py
	(cd $(@D) && python ./$(<F))

$(VAR_DIR)/things.parquet: things.py $(things:.py=.parquet)
	mkdir -p $(@D)
	python things.py $@

clean:
	rm -f $(parquet)
