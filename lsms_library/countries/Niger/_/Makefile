rounds := $(shell ls -d ../[12]*)

COUNTRY := $(shell basename "$$(cd .. && pwd)")
LSMS_DATA_ROOT ?= $(if $(LSMS_DATA_DIR),$(LSMS_DATA_DIR),$(if $(XDG_DATA_HOME),$(XDG_DATA_HOME),$(HOME)/.local/share)/lsms_library)
VAR_DIR ?= $(LSMS_DATA_ROOT)/$(COUNTRY)/var

all: panel_ids.json

# Panel IDs are constructed from raw ECVMA and EHCVM cover sheets.
# See panel_ids.py for the two-program disjoint linkage logic.
# panel_ids.json / updated_ids.json are REVIEWED SOURCES, committed to the
# config tree -- NOT build products (GH #914, #894).  They deliberately carry
# no rule.  A rule here fires whenever panel_ids.py happens to be the newer
# file, and on a pip install which of the two is newer is decided by the
# millisecond the wheel was unpacked (measured on the real 0.13.0 wheel: 5 of
# these 9 countries land with the script 1 ms ahead).  The recipe then writes
# into site-packages, which on any shared install is not writable by the user
# running the code -- PermissionError from three layers inside a `make`
# subprocess.  Regenerating the crosswalk is a deliberate maintainer action,
# reviewed as a diff to the committed JSON:
#
#     make panel-ids
#
.PHONY: panel-ids
panel-ids:
	python panel_ids.py

# Wave-level plot_features (GH #167).  The framework's grab_data calls
# `make ../<wave>/_/plot_features.parquet` from this `_/` dir; this
# pattern rule runs the wave script that materializes it.
../%/_/plot_features.parquet: ../%/_/plot_features.py
	(cd $(@D) && python plot_features.py)

# Five native country compilers consume the declared wave producers (#1287).
# Discovery uses Country/Wave registration, declarations and physical folders;
# no four-wave list and no prerequisite for an absent native script. Deferred
# implicit-rule expansion supplies declaration metadata only for a matching
# country target, including transitive requests. Explicit secondary expansion
# can eagerly inspect unrelated tables on older GNU make, contaminating a clean
# missing-target probe. The stem is VAR_DIR's final directory component; only
# its exact directory is eligible. Native pattern rules remain unchanged.
# Discovery/representation errors are fatal during expansion: a rejected
# implicit rule must never make an existing stale target look up to date.
# See .coder/ledger/1287-niger-producer-routing.md, sections 2-5.
PYTHON ?= python
.SECONDEXPANSION:
NIGER_VAR_DIR = $(patsubst %/,%,$(VAR_DIR))
NIGER_VAR_PARENT = $(patsubst ./,,$(dir $(NIGER_VAR_DIR)))
niger_dependencies_or_fail = $(if $(filter __niger_producer_discovery_failed__,$(1)),$(error Niger producer discovery failed; refusing country compilation),$(1))
niger_wave_dependencies = $(if $(filter $(abspath $(VAR_DIR)),$(abspath $(@D))),$(call niger_dependencies_or_fail,$(shell $(PYTHON) wave_producers.py --dependencies $(1) || printf '%s\n' __niger_producer_discovery_failed__)),__niger_outside_var_directory__)

$(NIGER_VAR_PARENT)%/crop_production.parquet: $$(call niger_wave_dependencies,crop_production)
	@test "$(abspath $(@D))" = "$(abspath $(VAR_DIR))"
	$(PYTHON) wave_producers.py crop_production && $(PYTHON) crop_production.py

../%/_/crop_production.parquet: ../%/_/crop_production.py niger.py categorical_mapping.org
	(cd $(@D) && python crop_production.py)

$(NIGER_VAR_PARENT)%/plot_inputs.parquet: $$(call niger_wave_dependencies,plot_inputs)
	@test "$(abspath $(@D))" = "$(abspath $(VAR_DIR))"
	$(PYTHON) wave_producers.py plot_inputs && $(PYTHON) plot_inputs.py

../%/_/plot_inputs.parquet: ../%/_/plot_inputs.py niger.py categorical_mapping.org
	(cd $(@D) && python plot_inputs.py)

$(NIGER_VAR_PARENT)%/livestock.parquet: $$(call niger_wave_dependencies,livestock)
	@test "$(abspath $(@D))" = "$(abspath $(VAR_DIR))"
	$(PYTHON) wave_producers.py livestock && $(PYTHON) livestock.py

../%/_/livestock.parquet: ../%/_/livestock.py niger.py categorical_mapping.org
	(cd $(@D) && python livestock.py)

$(NIGER_VAR_PARENT)%/plot_labor.parquet: $$(call niger_wave_dependencies,plot_labor)
	@test "$(abspath $(@D))" = "$(abspath $(VAR_DIR))"
	$(PYTHON) wave_producers.py plot_labor && $(PYTHON) plot_labor.py

../%/_/plot_labor.parquet: ../%/_/plot_labor.py niger.py
	(cd $(@D) && python plot_labor.py)

$(NIGER_VAR_PARENT)%/people_last7days.parquet: $$(call niger_wave_dependencies,people_last7days)
	@test "$(abspath $(@D))" = "$(abspath $(VAR_DIR))"
	$(PYTHON) wave_producers.py people_last7days && $(PYTHON) people_last7days.py

../%/_/people_last7days.parquet: ../%/_/people_last7days.py niger.py
	(cd $(@D) && python people_last7days.py)


../%/_/community_prices.parquet: ../%/_/community_prices.py niger.py categorical_mapping.org
	(cd $(@D) && python community_prices.py)

clean:
	@echo "nothing to clean: panel_ids.json / updated_ids.json are committed"
	@echo "sources, not build products (GH #914).  Regenerate: make panel-ids"

.PHONY: all clean
