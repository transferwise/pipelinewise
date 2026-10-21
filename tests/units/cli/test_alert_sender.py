import json
import pytest
import collections
from unittest.mock import patch
from slack.errors import SlackApiError

from pipelinewise.cli.alert_handlers import errors
from pipelinewise.cli.alert_sender import AlertHandler, AlertSender
from pipelinewise.cli.alert_handlers.slack_alert_handler import SlackAlertHandler
from pipelinewise.cli.alert_handlers.victorops_alert_handler import (
    VictoropsAlertHandler,
)


@pytest.mark.parametrize('data_diff_channel,tap_channel,expected_channels', [
    (None, '#tap-owner', ['#default', '#tap-owner']),
    ('#data-diff', '#tap-owner', ['#data-diff', '#tap-owner']),
    ('#data-diff', None, ['#data-diff']),
    ('#data-diff', '#data-diff', ['#data-diff']),
    (None, '#default', ['#default']),
])
def test_structured_data_diff_alert_uses_configured_channel_and_tap_channel(
    data_diff_channel, tap_channel, expected_channels,
):
    config = {'token': 'test-slack-token', 'channel': '#default'}
    if data_diff_channel is not None:
        config['data_diff_channel'] = data_diff_channel
    sender = AlertSender({'slack': config})
    next_action = (
        '- Check replication lag and investigate the mismatch.\n'
        '- Failed windows retry automatically on scheduled runs.\n'
        '- To verify sooner, use `rerun_data_diff_check` with `--run-id` and `--remediation-ref`.'
    )

    with patch('slack.WebClient.chat_postMessage') as post:
        result = sender.send_to_all_handlers(
            message='FAIL data-diff payments/public.transfers — target has 20 fewer rows',
            details='run_id : test-run\nrow_count 100000 99980 -20',
            next_action=next_action,
            tap_slack_channel=tap_channel,
            data_diff=True,
        )

    assert result == {'sent': 1}
    assert [call.kwargs['channel'] for call in post.call_args_list] == expected_channels
    for call in post.call_args_list:
        assert call.kwargs['text'].startswith(':exclamation: *FAIL data-diff')
        attachment = call.kwargs['attachments'][0]
        assert attachment['color'] == 'danger'
        assert 'title' not in attachment
        assert 'text' in attachment['mrkdwn_in']
        assert '```run_id : test-run\nrow_count 100000 99980 -20```' in attachment['text']
        assert attachment['text'].endswith(f'\n\n*Next action:*\n{next_action}')


def test_replication_alert_keeps_legacy_slack_payload_and_default_channel():
    sender = AlertSender({'slack': {
        'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff',
    }})

    with patch('slack.WebClient.chat_postMessage') as post:
        sender.send_to_all_handlers(
            message='Replication failed', exc=ValueError('Connection failed'), tap_slack_channel='#tap-owner',
        )

    assert [call.kwargs['channel'] for call in post.call_args_list] == ['#default', '#tap-owner']
    for call in post.call_args_list:
        assert call.kwargs['text'] == '```Connection failed```'
        assert call.kwargs['attachments'] == [{'color': 'danger', 'title': 'Replication failed'}]


def test_legacy_slack_alert_without_exception_keeps_null_text():
    sender = AlertSender({'slack': {'token': 'test-slack-token', 'channel': '#default'}})

    with patch('slack.WebClient.chat_postMessage') as post:
        sender.send_to_handler('slack', message='Replication failed')

    assert post.call_args.kwargs == {
        'channel': '#default', 'text': None,
        'attachments': [{'color': 'danger', 'title': 'Replication failed'}],
    }


def test_slack_details_escape_mentions_links_and_embedded_code_fences():
    sender = AlertSender({'slack': {'token': 'test-slack-token', 'channel': '#default'}})

    with patch('slack.WebClient.chat_postMessage') as post:
        sender.send_to_handler(
            'slack', message='ERROR data-diff <@U123> & <https://example.org|tap>',
            details='error_detail : <@U456> & ```unexpected diagnostic```',
            next_action='Resolve <@U789> & rerun.', data_diff=True,
        )

    text = post.call_args.kwargs['text']
    body = post.call_args.kwargs['attachments'][0]['text']
    assert '&lt;@U123&gt; &amp; &lt;https://example.org|tap&gt;' in text
    assert '&lt;@U456&gt; &amp;' in body
    assert '&lt;@U789&gt; &amp;' in body
    assert 'unexpected diagnostic' in body
    assert body.count('```') == 2
    assert '<@' not in text + body


def test_dispatcher_sends_structured_details_to_victorops_without_slack_markup():
    sender = AlertSender({
        'slack': {'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff'},
        'victorops': {'base_url': 'https://example.org/alerts', 'routing_key': 'route'},
    })
    message = 'FAIL data-diff payments/public.transfers — target has 20 fewer rows'
    details = 'run_id : test-run\nrow_count 100000 99980 -20'
    next_action = '- Check replication lag.\n- Failed windows retry automatically on scheduled runs.'

    with patch('slack.WebClient.chat_postMessage') as slack_post, patch('requests.post') as victorops_post:
        victorops_post.return_value.status_code = 200
        result = sender.send_to_all_handlers(
            message=message, details=details, next_action=next_action,
            tap_slack_channel='#tap-owner', data_diff=True,
        )

    assert result == {'sent': 2}
    assert [call.kwargs['channel'] for call in slack_post.call_args_list] == ['#data-diff', '#tap-owner']
    sent = json.loads(victorops_post.call_args.kwargs['data'])
    combined = '\n'.join(str(value) for value in sent.values())
    assert message in combined
    assert details in combined
    assert next_action in combined
    assert f'\n\nNext action:\n{next_action}' in sent['entity_display_name']
    assert sent['message_type'] == 'CRITICAL'
    assert ':exclamation:' not in combined
    assert '```' not in combined


def test_structured_alert_with_no_configured_handlers_makes_no_external_calls():
    with patch('slack.WebClient.chat_postMessage') as slack_post, patch('requests.post') as victorops_post:
        result = AlertSender({}).send_to_all_handlers(
            message='ERROR data-diff payments/public.transfers', details='error_detail : unavailable',
            next_action='Resolve the error.', data_diff=True,
        )

    assert result == {'sent': 0}
    slack_post.assert_not_called()
    victorops_post.assert_not_called()


@pytest.mark.parametrize('failed_channel', ['#data-diff', '#tap-owner'])
def test_best_effort_warning_keeps_partial_slack_delivery(failed_channel):
    sender = AlertSender({'slack': {
        'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff',
    }})

    def post_message(**kwargs):
        if kwargs['channel'] == failed_channel:
            raise SlackApiError('Channel unavailable', {'error': 'channel_not_found'})
        return []

    with (
        patch('slack.WebClient.chat_postMessage', side_effect=post_message) as post,
        patch('pipelinewise.cli.alert_handlers.slack_alert_handler.LOGGER.warning') as warning,
    ):
        result = sender.send_to_all_handlers(
            message='WARNING data-diff payments/public.transfers — source index needed soon',
            details='source_rows : 75000', tap_slack_channel='#tap-owner', data_diff=True, best_effort=True,
        )

    assert result == {'sent': 1}
    assert [call.kwargs['channel'] for call in post.call_args_list] == ['#data-diff', '#tap-owner']
    assert failed_channel in str(warning.call_args.args)


def test_best_effort_slack_reports_successful_destination_count():
    handler = SlackAlertHandler({
        'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff',
    })

    with patch('slack.WebClient.chat_postMessage', return_value=[]) as post:
        delivered = handler.send(
            message='Source index needed soon', tap_slack_channel='#tap-owner', data_diff=True, best_effort=True,
        )

    assert delivered == 2
    assert [call.kwargs['channel'] for call in post.call_args_list] == ['#data-diff', '#tap-owner']


def test_best_effort_warning_reports_no_delivery_when_every_slack_channel_fails():
    sender = AlertSender({'slack': {
        'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff',
    }})

    with (
        patch(
            'slack.WebClient.chat_postMessage',
            side_effect=SlackApiError('Channel unavailable', {'error': 'channel_not_found'}),
        ) as post,
        patch('pipelinewise.cli.alert_handlers.slack_alert_handler.LOGGER.warning') as warning,
    ):
        result = sender.send_to_all_handlers(
            message='Source index needed soon', tap_slack_channel='#tap-owner', data_diff=True, best_effort=True,
        )

    assert result == {'sent': 0}
    assert [call.kwargs['channel'] for call in post.call_args_list] == ['#data-diff', '#tap-owner']
    logged = str([call.args for call in warning.call_args_list])
    assert '#data-diff' in logged
    assert '#tap-owner' in logged


@pytest.mark.parametrize('handler_order', [('slack', 'victorops'), ('victorops', 'slack')])
def test_best_effort_warning_keeps_delivery_when_another_handler_fails(handler_order):
    configs = {
        'slack': {'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff'},
        'victorops': {'base_url': 'https://example.org/alerts', 'routing_key': 'route'},
    }
    sender = AlertSender({handler: configs[handler] for handler in handler_order})

    with (
        patch('slack.WebClient.chat_postMessage', return_value=[]) as slack_post,
        patch('requests.post', side_effect=ValueError('VictorOps unavailable')) as victorops_post,
        patch('pipelinewise.cli.alert_sender.LOGGER.warning') as warning,
    ):
        result = sender.send_to_all_handlers(
            message='Source index needed soon', data_diff=True, best_effort=True,
        )

    assert result == {'sent': 1}
    slack_post.assert_called_once()
    victorops_post.assert_called_once()
    assert 'victorops' in str(warning.call_args.args)


def test_best_effort_warning_with_no_handlers_reports_no_delivery():
    with patch('slack.WebClient.chat_postMessage') as slack_post, patch('requests.post') as victorops_post:
        result = AlertSender({}).send_to_all_handlers(
            message='Source index needed soon', data_diff=True, best_effort=True,
        )

    assert result == {'sent': 0}
    slack_post.assert_not_called()
    victorops_post.assert_not_called()


@pytest.mark.parametrize('failed_channel,expected_channels', [
    ('#data-diff', ['#data-diff']),
    ('#tap-owner', ['#data-diff', '#tap-owner']),
])
def test_ordinary_alert_raises_after_slack_delivery_failure(failed_channel, expected_channels):
    sender = AlertSender({'slack': {
        'token': 'test-slack-token', 'channel': '#default', 'data_diff_channel': '#data-diff',
    }})

    def post_message(**kwargs):
        if kwargs['channel'] == failed_channel:
            raise SlackApiError('Channel unavailable', {'error': 'channel_not_found'})
        return []

    with patch('slack.WebClient.chat_postMessage', side_effect=post_message) as post:
        with pytest.raises(SlackApiError):
            sender.send_to_all_handlers(
                message='Data-diff comparison failed', tap_slack_channel='#tap-owner', data_diff=True,
            )

    assert [call.kwargs['channel'] for call in post.call_args_list] == expected_channels


def test_ordinary_alert_handler_failure_stops_dispatch():
    sender = AlertSender({
        'victorops': {'base_url': 'https://example.org/alerts', 'routing_key': 'route'},
        'slack': {'token': 'test-slack-token', 'channel': '#default'},
    })

    with (
        patch('requests.post', side_effect=ValueError('VictorOps unavailable')) as victorops_post,
        patch('slack.WebClient.chat_postMessage') as slack_post,
    ):
        with pytest.raises(ValueError, match='VictorOps unavailable'):
            sender.send_to_all_handlers(message='Replication failed')

    victorops_post.assert_called_once()
    slack_post.assert_not_called()


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
        alert_sender = AlertSender(
            {
                'handler1': {'unknown-prop1': 'alert-handler-property1'},
                'handler2': {'unknown-prop2': 'alert-handler-property2'},
            }
        )

        assert alert_sender._AlertSender__get_alert_handler('handler1') == AlertHandler(
            type='handler1', config={'unknown-prop1': 'alert-handler-property1'}
        )

        # Should raise an exception when trying to get a not configured alert handler
        with pytest.raises(errors.NotConfiguredAlertHandlerException):
            alert_sender = AlertSender(
                {
                    'handler1': {'unknown-prop1': 'alert-handler-property1'},
                    'handler2': {'unknown-prop2': 'alert-handler-property2'},
                }
            )

            alert_sender._AlertSender__get_alert_handler('handler3')

        # send_to_handler: Should raise an exception if alert handler not configured
        with pytest.raises(errors.NotConfiguredAlertHandlerException):
            alert_sender = AlertSender(
                {
                    'handler1': {'unknown-prop1': 'alert-handler-property1'},
                    'handler2': {'unknown-prop2': 'alert-handler-property2'},
                }
            )
            alert_sender.send_to_handler('handler3', 'test message to an alert handler')

        # send_to_handler: Should raise an exception if alert handler not implemented
        with pytest.raises(errors.NotImplementedAlertHandlerException):
            alert_sender = AlertSender(
                {
                    'handler1': {'unknown-prop1': 'alert-handler-property1'},
                    'handler2': {'unknown-prop2': 'alert-handler-property2'},
                }
            )
            alert_sender.send_to_handler('handler1', 'test message to an alert handler')

        # send_to_all_handlers: Should send an alert if the alert handler configured correctly
        with patch('slack.WebClient.chat_postMessage') as slack_post_message_mock:
            slack_post_message_mock.return_value = []
            alert_sender = AlertSender(
                {
                    'slack': {'token': 'test-slack-token', 'channel': '#test-channel'},
                    'handler2': {'unknown-prop2': 'alert-handler-property2'},
                }
            )
            assert (
                alert_sender.send_to_handler(
                    'slack', 'test message to all alert handlers'
                )
                is True
            )

        # send_to_all_handlers: Should raise an exception if alert handler not configured
        with pytest.raises(errors.NotImplementedAlertHandlerException):
            alert_sender = AlertSender(
                {
                    'handler1': {'unknown-prop1': 'alert-handler-property1'},
                    'handler2': {'unknown-prop2': 'alert-handler-property2'},
                }
            )
            alert_sender.send_to_all_handlers('test message to all alert handlers')

        # send_to_all_handlers: Should raise an exception if alert handler not implemented
        with pytest.raises(errors.NotImplementedAlertHandlerException):
            alert_sender = AlertSender(
                {
                    'handler1': {'unknown-prop1': 'alert-handler-property1'},
                    'handler2': {'unknown-prop2': 'alert-handler-property2'},
                }
            )
            alert_sender.send_to_all_handlers('test message to all alert handlers')

        # send_to_all_handlers: Should send an alert if the alert handler configured correctly
        with patch('slack.WebClient.chat_postMessage') as slack_post_message_mock:
            slack_post_message_mock.return_value = []
            alert_sender = AlertSender(
                {'slack': {'token': 'test-slack-token', 'channel': '#test-channel'}}
            )
            assert alert_sender.send_to_all_handlers(
                'test message to all alert handlers'
            ) == {'sent': 1}

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
            slack = SlackAlertHandler(
                {'token': 'invalid-token', 'channel': '#my-channel'}
            )
            slack.send('test message')

        # Should send message if valid token and channel provided
        with patch('slack.WebClient.chat_postMessage') as slack_post_message_mock:
            slack_post_message_mock.return_value = []
            slack = SlackAlertHandler(
                {'token': 'valid-token', 'channel': '#my-channel'}
            )
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
            VictorOpsResponseMock = collections.namedtuple(
                'VictorOpsResponseMock', 'status_code'
            )
            victorops_post_message_mock.return_value = VictorOpsResponseMock(
                status_code=200
            )
            victorops = VictoropsAlertHandler(
                {'base_url': 'some-url', 'routing_key': 'some-routing-key'}
            )
            victorops.send('test message')

    def test_victorops_handler_sends_alerts_that_carry_an_exception(self):
        """An exception must be stringified: the object is not JSON serializable"""
        with patch('requests.post') as victorops_post_message_mock:
            VictorOpsResponseMock = collections.namedtuple(
                'VictorOpsResponseMock', 'status_code'
            )
            victorops_post_message_mock.return_value = VictorOpsResponseMock(
                status_code=200
            )
            victorops = VictoropsAlertHandler(
                {'base_url': 'some-url', 'routing_key': 'some-routing-key'}
            )
            victorops.send('test message', exc=ValueError('some failure'))

            sent = json.loads(victorops_post_message_mock.call_args.kwargs['data'])
            assert sent['state_message'] == 'some failure'

    def test_victorops_handler_bounds_the_request(self):
        """An unresponsive endpoint must not stall the run reporting the failure."""
        with patch('requests.post') as victorops_post_message_mock:
            VictorOpsResponseMock = collections.namedtuple(
                'VictorOpsResponseMock', 'status_code'
            )
            victorops_post_message_mock.return_value = VictorOpsResponseMock(
                status_code=200
            )
            victorops = VictoropsAlertHandler(
                {'base_url': 'some-url', 'routing_key': 'some-routing-key'}
            )
            victorops.send('test message')

            assert victorops_post_message_mock.call_args.kwargs['timeout'] > 0
