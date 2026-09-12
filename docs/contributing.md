# Contributing

## Adding New Surveys

Adding a new LSMS survey usually requires no Python programming -- just YAML
configuration files that map the survey's variables to the standardized
interface. Some cases do need a small script: multiple rounds in one wave
directory, a source file spanning several waves, or elaborate unit conversions
and cross-file joins. See [CONTRIBUTING.org](https://github.com/ligon/LSMS_Library/blob/master/CONTRIBUTING.org)
for the full walkthrough.

Brief overview:

1. Create the directory structure under `lsms_library/countries/` --
   `{Country}/_/`, `{Country}/{Wave}/_/`, `{Country}/{Wave}/Documentation/` and
   `{Country}/{Wave}/Data/`. The `{Country}` symlinks at the repository root are
   a convenience; the loader reads the tree under `lsms_library/countries/`, so
   a new country created only at the root is invisible to it.
2. Add source data with `data_access.push_to_cache()` (do not invoke the `dvc`
   CLI directly -- it takes a global lock and fails under concurrency)
3. Create `{Country}/_/data_scheme.yml` declaring available tables
4. Create `{Country}/{Wave}/_/data_info.yml` mapping survey variables to
   standard names
5. Test your country: `make test country={Country}` from `lsms_library/`
6. Open a pull request **against `development`** (see below)

## Building the Documentation

The docs use [MkDocs](https://www.mkdocs.org/) with the
[Material](https://squidfunk.github.io/mkdocs-material/) theme and
[mkdocstrings](https://mkdocstrings.github.io/) for API reference generation.

```bash
# Install doc dependencies
pip install mkdocs-material mkdocstrings[python]

# Live preview
mkdocs serve

# Build static site
mkdocs build
```

## Which branch to target

The repository's default branch is `master`, but **routine pull requests target
`development`**; `master` receives `development` only at release time. Opening
against the default branch is the common first-time mistake.

This also changes how you reference issues. GitHub auto-closes an issue only
when a closing keyword reaches the *default* branch, so `Closes #123` in a PR
merging to `development` does **not** close it. In a fix PR write
`Addresses #123` for traceability; the closing keywords belong on the
`development -> master` release-merge PR, which closes them all at once. Note
that the `fix(#123):` commit-subject scope we use is not a GitHub closing
keyword either -- the parenthesis breaks the pattern.

## Running Tests

```bash
# Whole suite (installs the test dependency group first)
make test

# One country: the per-country feature-audit sanity scan.
# Run this before opening a data PR.
make -C lsms_library test country=Uganda

# Whole suite, rebuilding every cache first -- use when verifying a
# wave-script fix, since a stale parquet can otherwise pass a test the
# source-only fix would have failed
make test-full
```

`pytest tests/` also works for a quick run. Tests that need credentials skip
rather than fail; CI sets `LSMS_SKIP_AUTH=1` to bypass credential handling
entirely.

## Contact

- **GitHub Issues**: Report bugs or request features at the
  [repository](https://github.com/ligon/LSMS_Library/issues)
- **Email**: Contact ligon@berkeley.edu to discuss contributions
