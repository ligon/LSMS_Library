POETRY = poetry

.PHONY: setup test build clean help

setup: .venv/pyvenv.cfg

.venv/pyvenv.cfg: pyproject.toml
	$(POETRY) install
	@touch $@

test: setup
	$(POETRY) run pytest

build: setup
	$(POETRY) build

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
	@echo "  clean    Remove build artifacts"
	@echo ""
	@echo "Country-specific (delegated to lsms_library/Makefile):"
	@echo "  make country-test  country=Uganda"
	@echo "  make country-build country=Uganda"
	@echo ""
	@echo "Or work directly:  make -C lsms_library help"
