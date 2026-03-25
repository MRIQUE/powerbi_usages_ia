import pytest
import requests
from unittest.mock import Mock, patch
from deploy.retry import retryable


def _mock_response(status_code, retry_after=None):
    m = Mock(spec=requests.Response)
    m.status_code = status_code
    m.headers = {"Retry-After": str(retry_after)} if retry_after else {}
    m.raise_for_status = Mock()
    if status_code >= 400:
        m.raise_for_status.side_effect = requests.HTTPError(response=m)
    return m


def test_retryable_passes_through_on_success():
    success_resp = _mock_response(200)
    func = Mock(return_value=success_resp)
    wrapped = retryable(func)
    result = wrapped("arg1", key="val")
    assert result == success_resp
    func.assert_called_once_with("arg1", key="val")


def test_retryable_retries_on_429():
    resp_429 = _mock_response(429, retry_after=1)
    resp_200 = _mock_response(200)
    func = Mock(side_effect=[resp_429, resp_200])
    wrapped = retryable(func, max_attempts=3)

    with patch("deploy.retry.time.sleep"):
        result = wrapped()

    assert result == resp_200
    assert func.call_count == 2


def test_retryable_retries_on_500():
    resp_500 = _mock_response(500)
    resp_200 = _mock_response(200)
    func = Mock(side_effect=[resp_500, resp_200])
    wrapped = retryable(func, max_attempts=3)

    with patch("deploy.retry.time.sleep"):
        result = wrapped()

    assert result == resp_200


def test_retryable_raises_after_max_attempts():
    resp_429 = _mock_response(429)
    func = Mock(return_value=resp_429)
    wrapped = retryable(func, max_attempts=3)

    with patch("deploy.retry.time.sleep"):
        with pytest.raises(requests.HTTPError):
            wrapped()

    assert func.call_count == 3


def test_retryable_does_not_retry_400():
    resp_400 = _mock_response(400)
    func = Mock(return_value=resp_400)
    wrapped = retryable(func, max_attempts=5)

    with patch("deploy.retry.time.sleep"):
        with pytest.raises(requests.HTTPError):
            wrapped()

    assert func.call_count == 1


def test_retryable_uses_retry_after_header():
    resp_429 = _mock_response(429, retry_after=42)
    resp_200 = _mock_response(200)
    func = Mock(side_effect=[resp_429, resp_200])
    wrapped = retryable(func, max_attempts=3)

    sleep_calls = []
    with patch("deploy.retry.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        wrapped()

    assert sleep_calls[0] == 42
