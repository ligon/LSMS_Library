# GhanaAHIES build rules.  Every table is script-path (`materialize: make`).
#
# The framework calls, from THIS directory,
#     make -s ../<year>/_/<table>.parquet
# (Wave.grab_data / Country.run_make_target).  The `../%/_/<table>.parquet`
# pattern rules below match that name and run the wave script in its own
# `_/`; the script's `to_parquet()` redirects the output to
# $(LSMS_DATA_ROOT)/GhanaAHIES/<year>/_/<table>.parquet (GH #803), which is
# the only place the framework READS it.  run_make_target also tries the
# data_root() spelling first, so a second rule keyed on that path lets the
# first attempt succeed instead of warning "No rule to make target".
#
# One wave script serves four quarters (t = YYYYQn) from one year folder.
# Each person-file build re-reads a 2.4-2.7 GB .dta (~20 s, ~3 GB RAM):
# build tables serially (LSMS_BUILD_WORKERS=1) on a small node.
#
# No .json target has a rule here, and there is no panel crosswalk yet
# (GH #914: a committed crosswalk must never be on a rule's left-hand side).

rounds := $(shell ls -d ../2*)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

TABLES := sample household_roster individual_education food_security housing

all:
	@echo "Targets: ../<year>/_/<table>.parquet for table in $(TABLES)"

# $(1) = table name.  Two rules per table: the in-tree target NAME the
# framework's wave-level fallback asks for, and the data_root() path its
# country-level fallback asks for first.  Both run the same wave script.
define WAVE_RULE
../%/_/$(1).parquet: ../%/_/$(1).py ghanaahies.py
	(cd $$(@D) && python $(1).py)

$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/$(1).parquet: ../%/_/$(1).py ghanaahies.py
	(cd $$(<D) && python $(1).py)
endef

$(foreach t,$(TABLES),$(eval $(call WAVE_RULE,$(t))))

clean:
	-rm -f $(VAR_DIR)/*.parquet

.PHONY: all clean
