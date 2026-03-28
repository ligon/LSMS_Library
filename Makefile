POETRY = poetry

.PHONY: setup test build release clean help

setup: .venv/pyvenv.cfg

.venv/pyvenv.cfg: pyproject.toml
	$(POETRY) install
	@touch $@

test: setup
	$(POETRY) run pytest

build: setup
	$(POETRY) build

# ── Release ───────────────────────────────────────────────
# Usage: make release v=0.6.0
#   1. Validates the tag doesn't already exist
#   2. Runs the test suite
#   3. Creates an annotated git tag (vX.Y.Z)
#   4. Builds the distribution
#
# The version is derived from the tag by poetry-dynamic-versioning,
# so no files need editing.
release:
ifndef v
	$(error Usage: make release v=0.6.0)
endif
	@if git rev-parse "v$(v)" >/dev/null 2>&1; then \
		echo "Error: tag v$(v) already exists"; exit 1; \
	fi
	$(POETRY) run pytest
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

clean:
	rm -rf dist/ build/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

help:
	@echo "Top-level targets:"
	@echo "  setup    Install dependencies via Poetry"
	@echo "  test     Run pytest suite"
	@echo "  build    Build distribution"
	@echo "  release  Tag & build a release (make release v=0.6.0)"
	@echo "  clean    Remove build artifacts"
	@echo ""
	@echo "Country-specific (delegated to lsms_library/Makefile):"
	@echo "  make country-test  country=Uganda"
	@echo "  make country-build country=Uganda"
	@echo ""
	@echo "Or work directly:  make -C lsms_library help"
