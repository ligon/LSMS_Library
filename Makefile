POETRY = poetry

# Pytest worker count.
#
# Prefer $SLURM_CPUS_ON_NODE if set: it's the Slurm-authoritative,
# cgroup-correct count of cores actually allocated to this job.
# Otherwise use `nproc`, which is cgroup-aware on modern Linux.
# Both beat xdist's `-n auto`, which calls `os.cpu_count()` — that
# reads /sys/devices/system/cpu/ and reports the **physical node**
# count regardless of any cgroup affinity restriction.  On a 56-core
# node where Slurm gave us 8 logical CPUs, `os.cpu_count()` still
# returns 56, and `pytest -n auto` would oversubscribe by 7×.
PYTEST_WORKERS ?= $(or $(SLURM_CPUS_ON_NODE),$(shell nproc 2>/dev/null || echo 1))

# Default pytest args: run in parallel (requires pytest-xdist, in the
# test-group dependencies) and group tests from the same file into a
# single worker so file-scoped caches (e.g. test_sample.py's
# _sample_cache) and `make` invocations (test_uganda_tables.py) don't
# race across workers.
#
# Override via env or command-line to fall back to serial:
#   make test PYTEST_ARGS=""
#   make release v=0.6.0 PYTEST_ARGS="--tb=short"
#   make test PYTEST_WORKERS=4
PYTEST_ARGS ?= -n $(PYTEST_WORKERS) --dist=loadfile

.PHONY: setup test test-full retest test-ff build release clean help \
        profile profile-cold profile-cprofile

# Use a stamp file in .make/ rather than .venv/pyvenv.cfg --- Poetry's
# default ``virtualenvs.in-project = false`` puts the venv under
# ``~/.cache/pypoetry/`` so .venv/ never exists in the project root,
# which made the previous ``touch .venv/pyvenv.cfg`` fail (the parent
# directory doesn't exist).  ``--with test`` explicitly installs the
# pytest-xdist / pytest-instafail / pytest-timeout group so a fresh
# clone never trips on ``unrecognized arguments: -n --dist=loadfile``.
setup: .make/setup.stamp

.make/setup.stamp: pyproject.toml poetry.lock
	@mkdir -p $(@D)
	$(POETRY) install --no-interaction --with test
	@touch $@

test: setup
	$(POETRY) run pytest $(PYTEST_ARGS)

test-full: setup
	$(POETRY) run pytest $(PYTEST_ARGS) --rebuild-caches

# Iterative-development test targets.  Both use pytest's cache at
# .pytest_cache/ to remember which tests failed on the previous run.
#
#   retest   — run ONLY last-failed tests, stop on the first failure.
#              Fast feedback loop when fixing a specific failure.
#   test-ff  — run last-failed tests first, then the rest.
#              Verify the fix and keep going.
#
# Both inherit PYTEST_ARGS for worker count + distribution.
retest: setup
	$(POETRY) run pytest $(PYTEST_ARGS) --lf -x

test-ff: setup
	$(POETRY) run pytest $(PYTEST_ARGS) --ff -x

build: setup
	$(POETRY) build

# ── Release ───────────────────────────────────────────────
# Usage: make release v=0.6.0
#   1. Validates the tag doesn't already exist
#   2. Runs the test suite in parallel (see PYTEST_ARGS above)
#   3. Creates an annotated git tag (vX.Y.Z)
#   4. Builds the distribution
#
# The version is derived from the tag by poetry-dynamic-versioning,
# so no files need editing.
#
# For a long release test run, prefer a compute node over the login
# node --- the test suite triggers cold country builds that can
# saturate a shared login node for 45+ minutes.
release:
ifndef v
	$(error Usage: make release v=0.6.0)
endif
	@if git rev-parse "v$(v)" >/dev/null 2>&1; then \
		echo "Error: tag v$(v) already exists"; exit 1; \
	fi
	$(POETRY) run pytest $(PYTEST_ARGS)
	git tag -a "v$(v)" -m "Release $(v)"
	$(POETRY) build
	@echo ""
	@echo "Tagged v$(v) and built dist/. When ready:"
	@echo "  git push origin v$(v)"

# Delegate country-specific targets to the inner Makefile.
# Usage: make country-test country=Uganda
#        make country-build country=Uganda
country-%: setup
	$(MAKE) -C lsms_library $* country=$(country)

# ── Profiling ─────────────────────────────────────────────
# Attribute CPU / I/O cost inside Country.<feature>() calls.  Requires the
# optional 'profile' poetry group: `poetry install --with profile`.
# See .claude/skills/profiling/SKILL.md for recipes and interpretation.
#
# Usage:
#   make profile           country=Niger feature=household_roster  # pyinstrument, whatever cache state
#   make profile-cold      country=Niger feature=household_roster  # force cold (LSMS_NO_CACHE=1)
#   make profile-cprofile  country=Niger feature=food_acquired     # deterministic, view with snakeviz
#
# Output: bench/results/{YYYY-MM-DD}/{Country}-{feature}-phase{3,4}-*.{html,prof}
# and an appended JSON record in bench/results/{YYYY-MM-DD}.jsonl
PROFILE_JSON = bench/results/$(shell date -u +%Y-%m-%d).jsonl

# Guard: fire a parse-time error BEFORE `setup` runs if the user invoked
# a profile target without supplying country=... feature=...
ifneq (,$(filter profile profile-cold profile-cprofile,$(MAKECMDGOALS)))
ifndef country
$(error Usage: make $(firstword $(filter profile profile-cold profile-cprofile,$(MAKECMDGOALS))) country=Niger feature=household_roster)
endif
ifndef feature
$(error Usage: make $(firstword $(filter profile profile-cold profile-cprofile,$(MAKECMDGOALS))) country=Niger feature=household_roster)
endif
endif

profile: setup
	$(POETRY) run python bench/build_feature.py $(country) $(feature) \
	    --profile pyinstrument --json $(PROFILE_JSON)

profile-cold: setup
	LSMS_NO_CACHE=1 $(POETRY) run python bench/build_feature.py $(country) $(feature) \
	    --profile pyinstrument --json $(PROFILE_JSON) --label cold

profile-cprofile: setup
	$(POETRY) run python bench/build_feature.py $(country) $(feature) \
	    --profile cprofile --json $(PROFILE_JSON)

clean:
	rm -rf dist/ build/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

help:
	@echo "Top-level targets:"
	@echo "  setup    Install dependencies via Poetry"
	@echo "  test       Run pytest suite (fast tier, uses L2 parquet cache)"
	@echo "  test-full  Run pytest with cold cache (--rebuild-caches: purges L1+L2)"
	@echo "  retest     Re-run only last-failed tests, stop on first failure"
	@echo "  test-ff    Run last-failed first, stop on first failure"
	@echo "  build      Build distribution"
	@echo "  release  Tag & build a release (make release v=0.6.0)"
	@echo "  clean    Remove build artifacts"
	@echo ""
	@echo "Knobs:"
	@echo "  PYTEST_ARGS  Args passed to pytest (default: -n auto --dist=loadfile)"
	@echo "               Set empty to run serially: make test PYTEST_ARGS=\"\""
	@echo ""
	@echo "Country-specific (delegated to lsms_library/Makefile):"
	@echo "  make country-test  country=Uganda"
	@echo "  make country-build country=Uganda"
	@echo ""
	@echo "Or work directly:  make -C lsms_library help"
	@echo ""
	@echo "Profiling (requires: poetry install --with profile):"
	@echo "  make profile          country=Niger feature=household_roster"
	@echo "  make profile-cold     country=Niger feature=household_roster  # LSMS_NO_CACHE=1"
	@echo "  make profile-cprofile country=Niger feature=food_acquired     # open with snakeviz"
	@echo "  See .claude/skills/profiling/SKILL.md for recipes."
