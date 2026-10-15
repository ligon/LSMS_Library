#!/usr/bin/env python3
"""Read-only comparison of frozen validation sources with the integration checkpoint."""
import datetime
import json
import subprocess

CANDIDATE = "b1b7bae7ceadeb1a6be2812c9ddb3b571281a056"
VALIDATED = {
    "corpus_and_feature_audit": "7fa05179c8519f44ea329ba6387f5d6d6441a8ea",
    "installed_package_and_native_windows": "74c9398ecf3c1908feb539a99bce3f3f6bf6d04f",
    "full_credentialed_suite": "ec1a5976751dbb4423bb4a9bb43efb62a8bd8077",
    "cache_presence_followup": "4f1f044bb9b3079e7f427898f77627ad1503ec5e",
    "controlled_credentialed_CI": "cf1840e02317bac052b99e1e6c9fa066a8ca45bc",
}
SCOPE = ["lsms_library", "tests", "bench", ".github/workflows", "pyproject.toml", "poetry.lock", "Makefile"]

def changed(before, after):
    return subprocess.check_output(
        ["git", "diff", "--name-only", before, after, "--", *SCOPE], text=True
    ).splitlines()

results = []
for validation, revision in VALIDATED.items():
    paths = changed(revision, CANDIDATE)
    runtime = [p for p in paths if p.startswith("lsms_library/") and not p.endswith("/CONTENTS.org")]
    docs = [p for p in paths if p.endswith("/CONTENTS.org")]
    tests = [p for p in paths if p.startswith("tests/")]
    workflow = [p for p in paths if p.startswith(".github/workflows/")]
    remaining = sorted(set(paths) - set(runtime + docs + tests + workflow))
    results.append({"validation": validation, "source_sha": revision,
        "runtime_or_country_build_configuration_differences": runtime,
        "country_documentation_differences": docs, "test_differences": tests,
        "workflow_differences": workflow, "build_dependency_or_harness_differences": remaining})
    assert not runtime and not remaining, (validation, runtime, remaining)
print(json.dumps({"observed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "integration_sha": CANDIDATE, "scope": SCOPE, "comparisons": results,
    "interpretation": "All five frozen validation revisions have identical tracked package runtime and country build configuration, dependency/build inputs, and audit harness to this integration checkpoint. The only package differences are CONTENTS.org documentation, excluded from build hashes. Test and workflow differences are listed explicitly; earlier test results are not described as a rerun of later tests. This is source equivalence, not byte equality of versioned wheel artifacts, passing CI, approval of nutrition, or proof that pending #967 has landed."}, indent=2))
