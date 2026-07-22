"""Shared test configuration.

## Why this file exists

The CI ``unit-tests`` job runs deliberately **data-free**: `LSMS_SKIP_AUTH=1`,
no S3 secrets (only the `data-tests` job carries them).  Any test that builds a
real country therefore hits DVC -> S3 and raises
``botocore.exceptions.NoCredentialsError`` — regardless of whether its logic is
correct.

Most data-dependent tests already guard for this, but each one grew its own
private copy of the same `_aws_creds_available()` helper
(`test_canonical_shape_via_cache_miss.py`, `test_declared_spellings.py`,
`test_nigeria_cluster_identity.py`, ...).  A newly added test module that
*forgets* the guard does not skip — it turns the PR red with an error that has
nothing to do with the change under review.  That has now happened on at least
four PRs in one day (#625, #641, #644, #632), each costing a diagnosis cycle to
establish that the failure was environmental.

So: one helper, and one net that catches the modules which forget it.

## What this does NOT do

It does not make a genuine failure disappear.  The conversion applies **only**
when credentials are actually absent — when they are present and an S3 call
still fails, that is a real failure and is reported as one.  A test that is
silently skipped everywhere is worse than a test that is red somewhere, so the
guard is deliberately conditioned on the environment rather than on the
exception alone.
"""
import os
from pathlib import Path

import pytest


def aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    The single source of truth for this question.  Test modules should import
    this rather than re-implementing it::

        from conftest import aws_creds_available

    (Kept as a plain function rather than a fixture so it can be used at module
    scope in a ``pytest.mark.skipif``, which is where most callers need it.)
    NOTE a subtlety that cost an hour to pin down: the in-tree
    ``lsms_library/countries/.dvc/s3_creds`` is **not tracked in git** -- it is
    WRITTEN AT IMPORT TIME by the auto-unlock in ``data_access``.  So it exists
    on any machine that has imported the package with a valid WB API key, and
    does NOT exist under ``LSMS_SKIP_AUTH=1`` (which is exactly what the CI
    ``unit-tests`` job sets).  That is why this check reflects CI's real state
    rather than merely the checkout's.  Testing this helper without also
    setting ``LSMS_SKIP_AUTH=1`` will mislead you: a bare ``git worktree`` looks
    credential-free until the first import creates the file.
    """
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


requires_s3 = pytest.mark.skipif(
    not aws_creds_available(),
    reason="needs S3 credentials to build country data; the unit-tests job is "
           "deliberately data-free (see the data-tests job for the "
           "credentialed run)",
)


def _mentions_missing_credentials(exc: BaseException | None) -> bool:
    """Whether *exc*, or anything it was raised from, is a missing-creds error.

    Walks ``__cause__`` / ``__context__`` because the failure usually surfaces
    wrapped: ``CalledProcessError`` from a ``make`` subprocess, or a test's own
    ``pytest.fail(...)`` re-raise.  The string check is the pragmatic half of
    that -- a test that catches ``NoCredentialsError`` and re-raises it as its
    own message (as `test_gh323_ethiopia_config.py` does, reporting it as a
    column-spelling problem) leaves nothing else to match on.
    """
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if type(exc).__name__ == "NoCredentialsError":
            return True
        if "NoCredentialsError" in str(exc) or "Unable to locate credentials" in str(exc):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Convert a missing-credentials failure into a skip, and ONLY that.

    Covers the ``setup`` phase too, because the common shape is a module-scoped
    fixture that builds a country -- so the error lands in fixture setup and
    every test in the module errors at once.
    """
    outcome = yield
    report = outcome.get_result()

    if report.outcome != "failed" or call.excinfo is None:
        return
    if aws_creds_available():
        return  # creds ARE present: a genuine failure, report it as one
    if not _mentions_missing_credentials(call.excinfo.value):
        return

    report.outcome = "skipped"
    report.longrepr = (
        str(item.fspath),
        item.location[1],
        "Skipped: no S3 credentials, so this test cannot build country data. "
        "Add `requires_s3` from tests/conftest.py to skip it explicitly.",
    )
