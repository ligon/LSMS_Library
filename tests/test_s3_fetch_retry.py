"""Unit tests for the transient-S3-error retry in local_tools.

Concurrent multipart S3 reads under heavy parallel build load (make -jN
fetching several large .dta blobs at once) occasionally corrupt a TLS record
(`ssl.SSLError: ... DECRYPTION_FAILED_OR_BAD_RECORD_MAC`) or truncate a payload
(`aiohttp.ClientPayloadError`).  `_get_file_with_retry` retries those; it must
NOT retry `FileNotFoundError` (wrong cache layout) or non-transient errors.
"""
import ssl

import pytest

from lsms_library.local_tools import (
    _get_file_with_retry,
    _is_transient_fetch_error,
)

_NOSLEEP = lambda _delay: None  # noqa: E731 -- keep tests instant


class _FakeFS:
    """fs.get_file that raises a queued sequence of errors, then succeeds."""

    def __init__(self, errors):
        self.errors = list(errors)
        self.calls = 0

    def get_file(self, src, dst):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)


def test_retry_succeeds_after_transient_tls_errors():
    fs = _FakeFS([ssl.SSLError("DECRYPTION_FAILED_OR_BAD_RECORD_MAC")] * 2)
    _get_file_with_retry(fs, "src", "/tmp/nope.blob", attempts=3, sleep=_NOSLEEP)
    assert fs.calls == 3  # two failures + one success


def test_file_not_found_is_not_retried():
    fs = _FakeFS([FileNotFoundError("wrong layout")])
    with pytest.raises(FileNotFoundError):
        _get_file_with_retry(fs, "src", "/tmp/nope.blob", attempts=3, sleep=_NOSLEEP)
    assert fs.calls == 1  # caller tries the next layout, no retry


def test_non_transient_error_is_not_retried():
    fs = _FakeFS([ValueError("programmer bug")])
    with pytest.raises(ValueError):
        _get_file_with_retry(fs, "src", "/tmp/nope.blob", attempts=3, sleep=_NOSLEEP)
    assert fs.calls == 1


def test_exhausts_attempts_then_reraises():
    fs = _FakeFS([ssl.SSLError("BAD_RECORD_MAC")] * 5)
    with pytest.raises(ssl.SSLError):
        _get_file_with_retry(fs, "src", "/tmp/nope.blob", attempts=3, sleep=_NOSLEEP)
    assert fs.calls == 3  # capped at attempts


def test_classifier():
    assert _is_transient_fetch_error(ssl.SSLError("x"))
    assert _is_transient_fetch_error(ConnectionError())
    assert _is_transient_fetch_error(TimeoutError())
    assert not _is_transient_fetch_error(FileNotFoundError())
    assert not _is_transient_fetch_error(ValueError("x"))

    class ClientPayloadError(Exception):
        pass

    assert _is_transient_fetch_error(ClientPayloadError("Response payload is not completed"))
