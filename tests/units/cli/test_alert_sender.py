import pytest
import collections
from unittest.mock import patch
from slack.errors import SlackApiError

import pipelinewise.cli.alert_handlers.errors as errors

from pipelinewise.cli.alert_sender import AlertHandler, AlertSender
from pipelinewise.cli.alert_handlers.slack_alert_handler import SlackAlertHandler
from pipelinewise.cli.alert_handlers.victorops_alert_handler import VictoropsAlertHandler


# pylint: disable=no-self-use,too-few-public-methods
class TestAlertSender:
    """
    Unit tests for PipelineWise CLI alert sender classes
    """
    def test_alert_sender(self):
        """Test function for AlertSender class"""
        # Should raise an exception if alert handlers not initialised by a dictionary
        with pytest.raises(errors.InvalidAlertHandlerException):
            AlertSender(123)
        with pytest.raises(errors.InvalidAlertHandlerException):
            AlertSender('123')
        with pytest.raises(errors.InvalidAlertHandlerException):
            AlertSender([1, 2, 3])

        # Should get the correct alert handler tuple from a list of alert handlers
        alert_sender = AlertSender({
            'handler1': {'unknown-prop1': 'alert-handler-property1'},
            'handler2': {'unknown-prop2': 'alert-handler-property2'}})
        # pylint: disable=protected-access
        assert alert_sender._AlertSender__get_alert_handler('handler1') == \
            AlertHandler(type='handler1', config={'unknown-prop1': 'alert-handler-property1'})

        # Should raise an exception when trying to get a not configured alert handler
        with pytest.raises(errors.NotConfiguredAlertHandlerException):
            alert_sender = AlertSender({
                'handler1': {'unknown-prop1': 'alert-handler-property1'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'}})
            # pylint: disable=protected-access
            alert_sender._AlertSender__get_alert_handler('handler3')

        # send_to_handler: Should raise an exception if alert handler not configured
        with pytest.raises(errors.NotConfiguredAlertHandlerException):
            alert_sender = AlertSender({
                'handler1': {'unknown-prop1': 'alert-handler-property1'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'}})
            alert_sender.send_to_handler('handler3', 'test message to an alert handler')

        # send_to_handler: Should raise an exception if alert handler not implemented
        with pytest.raises(errors.NotImplementedAlertHandlerException):
            alert_sender = AlertSender({
                'handler1': {'unknown-prop1': 'alert-handler-property1'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'}})
            alert_sender.send_to_handler('handler1', 'test message to an alert handler')

        # send_to_all_handlers: Should send an alert if the alert handler configured correctly
        with patch('slack.WebClient.chat_postMessage') as slack_post_message_mock:
            slack_post_message_mock.return_value = []
            alert_sender = AlertSender({
                'slack': {'token': 'test-slack-token', 'channel': '#test-channel'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'}})
            assert alert_sender.send_to_handler('slack', 'test message to all alert handlers') is True

        # send_to_all_handlers: Should raise an exception if alert handler not configured
        with pytest.raises(errors.NotImplementedAlertHandlerException):
            alert_sender = AlertSender({
                'handler1': {'unknown-prop1': 'alert-handler-property1'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'}})
            alert_sender.send_to_all_handlers('test message to all alert handlers')

        # send_to_all_handlers: Should raise an exception if alert handler not implemented
        with pytest.raises(errors.NotImplementedAlertHandlerException):
            alert_sender = AlertSender({
                'handler1': {'unknown-prop1': 'alert-handler-property1'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'}})
            alert_sender.send_to_all_handlers('test message to all alert handlers')

        # send_to_all_handlers: Should send an alert if the alert handler configured correctly
        with patch('slack.WebClient.chat_postMessage') as slack_post_message_mock:
            slack_post_message_mock.return_value = []
            alert_sender = AlertSender({
                'slack': {'token': 'test-slack-token', 'channel': '#test-channel'}})
            assert alert_sender.send_to_all_handlers('test message to all alert handlers') == {'sent': 1}

    def test_slack_handler(self):
        """Functions to test slack alert handler"""
        # Should raise an exception if no token provided
        with pytest.raises(errors.InvalidAlertHandlerException):
            SlackAlertHandler({'no-slack-token': 'no-token'})

        # Should raise an exception if no channel provided
        with pytest.raises(errors.InvalidAlertHandlerException):
            SlackAlertHandler({'no-slack-channel': '#no-channel'})

        # Should raise an exception if no valid token provided
        with pytest.raises(SlackApiError):
            slack = SlackAlertHandler({'token': 'invalid-token', 'channel': '#my-channel'})
            slack.send('test message')

        # Should send message if valid token and channel provided
        with patch('slack.WebClient.chat_postMessage') as slack_post_message_mock:
            slack_post_message_mock.return_value = []
            slack = SlackAlertHandler({'token': 'valid-token', 'channel': '#my-channel'})
            slack.send('test message')

    def test_victorops_handler(self):
        """Functions to test victorops alert handler"""
        # Should raise an exception if no base url and routing_key provided
        with pytest.raises(errors.InvalidAlertHandlerException):
            VictoropsAlertHandler({'no-victorops-url': 'no-url'})

        # Should raise an exception if no base_url provided
        with pytest.raises(errors.InvalidAlertHandlerException):
            VictoropsAlertHandler({'routing_key': 'some-routing-key'})

        # Should raise an exception if no routing_key provided
        with pytest.raises(errors.InvalidAlertHandlerException):
            VictoropsAlertHandler({'base_url': 'some-url'})

        # Should send alert if valid victorops REST endpoint URL provided
        with patch('requests.post') as victorops_post_message_mock:
            VictorOpsResponseMock = collections.namedtuple('VictorOpsResponseMock', 'status_code')
            victorops_post_message_mock.return_value = VictorOpsResponseMock(status_code=200)
            victorops = VictoropsAlertHandler({'base_url': 'some-url', 'routing_key': 'some-routing-key'})
            victorops.send('test message')
