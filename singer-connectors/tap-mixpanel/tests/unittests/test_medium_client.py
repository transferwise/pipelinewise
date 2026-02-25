import unittest
from collections.abc import Generator
from unittest import mock
from unittest.mock import Mock, PropertyMock, patch

import backoff
import requests
import requests_mock
import urllib3
from requests import Session
from urllib3 import Timeout

import pytest
from pytest import raises
from tap_mixpanel import MixpanelClient, client
from tap_mixpanel.client import ReadTimeoutError, Server429Error, Server5xxError
from tests.configuration.fixtures import mixpanel_client


@mock.patch('time.sleep', return_value=None)
def test_request_export_backoff_on_timeout(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', exc=requests.exceptions.Timeout('Timeout on request'))

        with raises(ReadTimeoutError) as ex:
            for record in mixpanel_client.request_export('GET', url='http://test.com'):
                pass
        # Assert backoff retry count as expected    
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_request_export_backoff_on_remote_timeout(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', text=None, status_code=504)
        result = mixpanel_client.request_export('GET', url='http://test.com')

        with raises(Server5xxError) as ex:
            for record in result:
                pass
        # Assert backoff retry count as expected    
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_request_export_backoff_on_rate_limit(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', text='rate limit reached', status_code=429)
        result = mixpanel_client.request_export('GET', url='http://test.com')

        with raises(Server429Error):
            for record in result:
                pass

        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1


@mock.patch('time.sleep', return_value=None)
def test_request_backoff_on_timeout(mock_sleep, mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', exc=requests.exceptions.Timeout('Timeout on request'))
        
        with raises(ReadTimeoutError) as ex:
            result = mixpanel_client.request('GET', url='http://test.com')

        # Assert backoff retry count as expected    
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES_REQUEST - 1

def test_request_returns_json(mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', json={'a': 'b'})
        result = mixpanel_client.request('GET', url='http://test.com')
        assert result == {'a': 'b'}

def test_request_export_returns_generator(mixpanel_client):
    with requests_mock.Mocker() as m:
        m.request('GET', 'http://test.com', json={'a': 'b'})
        result = mixpanel_client.request_export('GET', url='http://test.com')
        assert isinstance(result, Generator)
