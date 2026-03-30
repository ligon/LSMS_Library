# Contributing

## Adding New Surveys

Adding a new LSMS survey requires no Python programming -- just YAML
configuration files that map the survey's variables to the standardized
interface. See [CONTRIBUTING.org](https://github.com/ligon/LSMS_Library/blob/master/CONTRIBUTING.org)
for the full walkthrough.

Brief overview:

1. Create directory structure: `Country/Year/Documentation` and `Country/Year/Data`
2. Add source data using DVC
3. Create `data_scheme.yml` (country-level) declaring available tables
4. Create `data_info.yml` (per-wave) mapping survey variables to standard names
5. Submit a pull request

## Building the Documentation

The docs use [MkDocs](https://www.mkdocs.org/) with the
[Material](https://squidfinity.github.io/mkdocs-material/) theme and
[mkdocstrings](https://mkdocstrings.github.io/) for API reference generation.

```bash
# Install doc dependencies
pip install mkdocs-material mkdocstrings[python]

# Live preview
mkdocs serve

# Build static site
mkdocs build
```

## Running Tests

```bash
pytest tests/
```

## Contact

- **GitHub Issues**: Report bugs or request features at the
  [repository](https://github.com/ligon/LSMS_Library/issues)
- **Email**: Contact ligon@berkeley.edu to discuss contributions
