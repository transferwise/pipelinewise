from unittest import mock

import pytest
import requests

import tap_github


def make_response(status_code: int, content: bytes = b'[]') -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = content
    response._content_consumed = True
    return response


@pytest.fixture(autouse=True)
def no_waiting():
    with mock.patch.object(tap_github.time, 'sleep') as sleep:
        yield sleep


@pytest.mark.parametrize('status_code', [500, 502, 503, 504])
def test_transient_http_error_recovers(status_code):
    success = make_response(200)
    with mock.patch.object(tap_github.session, 'request', side_effect=[make_response(status_code), success]):
        assert tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews') is success


@pytest.mark.parametrize('error_type', [requests.ConnectionError, requests.Timeout])
def test_transient_connection_failure_recovers(error_type):
    success = make_response(200)
    with mock.patch.object(tap_github.session, 'request', side_effect=[error_type('disconnected'), success]):
        assert tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews') is success


def test_pagination_retries_failed_page_without_repeating_records():
    first_page = make_response(200, b'[{"id": 1}]')
    next_url = 'https://api.github.com/repos/example/repo/pulls/1/reviews?page=2'
    first_page.headers['Link'] = f'<{next_url}>; rel="next"'
    second_page = make_response(200, b'[{"id": 2}]')
    with mock.patch.object(
        tap_github.session, 'request', side_effect=[first_page, make_response(502, b''), second_page],
    ) as request:
        records = [
            record
            for response in tap_github.authed_get_all_pages(
                'reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews',
            )
            for record in response.json()
        ]

    assert records == [{'id': 1}, {'id': 2}]
    assert [call.kwargs['url'] for call in request.call_args_list[1:]] == [next_url, next_url]


@pytest.mark.parametrize('content', [b'', b'<html>Bad Gateway</html>', b'null'])
def test_exhausted_http_retries_preserve_status(content, no_waiting):
    with mock.patch.object(tap_github.session, 'request', return_value=make_response(502, content)) as request:
        with pytest.raises(tap_github.GithubException, match='502'):
            next(tap_github.authed_get_all_pages('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews'))

    assert request.call_count == 5
    assert no_waiting.call_count == 4
    for sleep_call, minimum_delay in zip(no_waiting.call_args_list, [1, 2, 4, 8], strict=True):
        assert minimum_delay <= sleep_call.args[0] <= minimum_delay + 1


def test_exhausted_connection_retries_raise_original_error(no_waiting):
    error = requests.ConnectionError('disconnected')
    with mock.patch.object(tap_github.session, 'request', side_effect=error) as request:
        with pytest.raises(requests.ConnectionError) as raised:
            tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews')

    assert raised.value is error
    assert request.call_count == 5
    assert no_waiting.call_count == 4


@pytest.mark.parametrize('status_code', [401, 403, 404, 422])
def test_permanent_empty_http_error_raises_without_retry(status_code, no_waiting):
    with mock.patch.object(tap_github.session, 'request', return_value=make_response(status_code, b'')) as request:
        with pytest.raises(tap_github.GithubException, match=str(status_code)):
            tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews')

    assert request.call_count == 1
    no_waiting.assert_not_called()


def test_certificate_error_raises_without_retry(no_waiting):
    error = requests.exceptions.SSLError('certificate')
    with mock.patch.object(tap_github.session, 'request', side_effect=error) as request:
        with pytest.raises(requests.exceptions.SSLError):
            tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews')

    assert request.call_count == 1
    no_waiting.assert_not_called()


def test_existing_rate_limit_recovery_is_preserved(no_waiting):
    rate_limited = make_response(403, b'{"message": "API rate limit exceeded"}')
    rate_limited.headers['Retry-After'] = '5'
    success = make_response(200)
    with mock.patch.object(tap_github.session, 'request', side_effect=[rate_limited, success]):
        assert tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews') is success

    no_waiting.assert_called_once_with(65)


def test_request_keeps_existing_timeout_behaviour():
    with mock.patch.object(tap_github.session, 'request', return_value=make_response(200)) as request:
        tap_github.authed_get('reviews', 'https://api.github.com/repos/example/repo/pulls/1/reviews')

    assert 'timeout' not in request.call_args.kwargs
